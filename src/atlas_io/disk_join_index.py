import heapq
import sqlite3
from collections.abc import Iterable, Iterator
from itertools import islice
from pathlib import Path

_MAX_ROW_ORDINAL = 2**63 - 1
_MAX_JOIN_INDEX_CACHE_KIB = 32_768
_MAX_KEYS_PER_QUERY = 128

type JoinKey = int | str | tuple[JoinKey, ...]


def _unsigned_varint(value: int) -> bytes:
    encoded = bytearray()
    while value >= 128:
        encoded.append((value & 127) | 128)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _canonical_key(key: object) -> bytes:
    if type(key) is int:
        return _encode_integer_key(key)
    if type(key) is str:
        return _encode_string_key(key)
    if type(key) is tuple:
        return _encode_tuple_key(key)
    raise TypeError("join keys must be integers, strings, or tuples of join keys")


def _encode_integer_key(key: int) -> bytes:
    value = key * 2 if key >= 0 else -key * 2 - 1
    return b"\x00" + _unsigned_varint(value)


def _encode_string_key(key: str) -> bytes:
    return b"\x01" + key.encode("utf-8")


def _encode_tuple_key(key: tuple[object, ...]) -> bytes:
    components = tuple(_canonical_key(component) for component in key)
    encoded_components = (
        _unsigned_varint(len(component)) + component for component in components
    )
    return b"\x02" + _unsigned_varint(len(components)) + b"".join(encoded_components)


def _encoded_key_map(keys: Iterable[JoinKey]) -> dict[bytes, JoinKey]:
    return {_canonical_key(key): key for key in keys}


def _occurrence_rows(
    cursor: sqlite3.Cursor,
) -> Iterator[tuple[int, bytes, int]]:
    for sequence, encoded_key, row_ordinal in cursor:
        yield int(sequence), bytes(encoded_key), int(row_ordinal)


def _occurrence_streams(
    connection: sqlite3.Connection, key_by_encoded: dict[bytes, JoinKey]
) -> list[Iterator[tuple[int, bytes, int]]]:
    encoded_keys = list(key_by_encoded)
    streams: list[Iterator[tuple[int, bytes, int]]] = []
    for start in range(0, len(encoded_keys), _MAX_KEYS_PER_QUERY):
        batch = encoded_keys[start : start + _MAX_KEYS_PER_QUERY]
        placeholders = ", ".join("?" for _ in batch)
        cursor = connection.execute(
            "SELECT sequence, key, row_ordinal FROM row_occurrences "
            f"WHERE key IN ({placeholders}) ORDER BY sequence",
            batch,
        )
        streams.append(_occurrence_rows(cursor))
    return streams


def _merge_row_occurrences(
    streams: list[Iterator[tuple[int, bytes, int]]],
    key_by_encoded: dict[bytes, JoinKey],
) -> Iterator[tuple[JoinKey, int]]:
    for _, encoded_key, row_ordinal in heapq.merge(*streams):
        yield key_by_encoded[encoded_key], row_ordinal


def _row_ordinal(value: int) -> int:
    if type(value) is not int or not 0 <= value <= _MAX_ROW_ORDINAL:
        raise ValueError("row ordinal must be an integer in the SQLite range")
    return value


class _SQLiteFile:
    def __init__(self, path: str | Path) -> None:
        self._connection = sqlite3.connect(path)
        self._closed = False

    def commit(self) -> None:
        self._ensure_open()
        self._connection.commit()

    def close(self) -> None:
        if not self._closed:
            self._connection.close()
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("database is closed")


class DiskJoinIndex(_SQLiteFile):
    def __init__(self, path: str | Path) -> None:
        super().__init__(path)
        self._connection.execute(f"PRAGMA cache_size = -{_MAX_JOIN_INDEX_CACHE_KIB}")
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS row_occurrences "
            "(sequence INTEGER PRIMARY KEY, key BLOB NOT NULL, "
            "row_ordinal INTEGER NOT NULL CHECK(row_ordinal >= 0))"
        )
        self._occurrence_index_ready = False
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS unique_rows "
            "(key BLOB PRIMARY KEY, row_ordinal INTEGER NOT NULL "
            "CHECK(row_ordinal >= 0))"
        )

    def _ensure_occurrence_index(self) -> None:
        if self._occurrence_index_ready:
            return
        self._connection.execute(
            "CREATE INDEX IF NOT EXISTS row_occurrences_key_sequence "
            "ON row_occurrences(key, sequence)"
        )
        self._occurrence_index_ready = True

    def add(self, key: JoinKey, row_ordinal: int) -> None:
        self._ensure_open()
        self._connection.execute(
            "INSERT INTO row_occurrences(key, row_ordinal) VALUES (?, ?)",
            (_canonical_key(key), _row_ordinal(row_ordinal)),
        )

    def iter_row_indices(self, key: JoinKey) -> Iterator[int]:
        self._ensure_open()
        self._ensure_occurrence_index()
        cursor = self._connection.execute(
            "SELECT row_ordinal FROM row_occurrences WHERE key = ? ORDER BY sequence",
            (_canonical_key(key),),
        )
        return (int(row[0]) for row in cursor)

    def iter_lookup_unique(
        self, keys: Iterable[JoinKey]
    ) -> Iterator[tuple[JoinKey, int | None]]:
        self._ensure_open()
        requested_keys = iter(keys)
        while batch := list(islice(requested_keys, _MAX_KEYS_PER_QUERY)):
            encoded_keys = [_canonical_key(key) for key in batch]
            unique_encoded_keys = list(dict.fromkeys(encoded_keys))
            placeholders = ", ".join("?" for _ in unique_encoded_keys)
            cursor = self._connection.execute(
                "SELECT key, row_ordinal FROM unique_rows "
                f"WHERE key IN ({placeholders})",
                unique_encoded_keys,
            )
            row_ordinals = {bytes(row[0]): int(row[1]) for row in cursor}
            yield from (
                (key, row_ordinals.get(encoded_key))
                for key, encoded_key in zip(batch, encoded_keys, strict=True)
            )

    def iter_row_occurrences_for_keys(
        self, keys: Iterable[JoinKey], *, max_unique_keys: int
    ) -> Iterator[tuple[JoinKey, int]]:
        self._ensure_open()
        if type(max_unique_keys) is not int or max_unique_keys < 0:
            raise ValueError("max_unique_keys must be a non-negative integer")
        key_by_encoded: dict[bytes, JoinKey] = {}
        for key in keys:
            encoded_key = _canonical_key(key)
            if encoded_key in key_by_encoded:
                continue
            if len(key_by_encoded) >= max_unique_keys:
                raise ValueError("number of distinct join keys exceeds max_unique_keys")
            key_by_encoded[encoded_key] = key
        if not key_by_encoded:
            return
        self._ensure_occurrence_index()
        streams = _occurrence_streams(self._connection, key_by_encoded)
        yield from _merge_row_occurrences(streams, key_by_encoded)

    def add_unique(self, key: JoinKey, row_ordinal: int) -> None:
        self._ensure_open()
        try:
            self._connection.execute(
                "INSERT INTO unique_rows(key, row_ordinal) VALUES (?, ?)",
                (_canonical_key(key), _row_ordinal(row_ordinal)),
            )
        except sqlite3.IntegrityError as error:
            raise ValueError(f"duplicate unique key {key!r}") from error

    def lookup_unique(self, key: JoinKey) -> int | None:
        self._ensure_open()
        row = self._connection.execute(
            "SELECT row_ordinal FROM unique_rows WHERE key = ?",
            (_canonical_key(key),),
        ).fetchone()
        return None if row is None else int(row[0])


class DiskMembershipSet(_SQLiteFile):
    def __init__(self, path: str | Path) -> None:
        super().__init__(path)
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS members (key BLOB PRIMARY KEY)"
        )

    def add(self, key: JoinKey) -> bool:
        self._ensure_open()
        cursor = self._connection.execute(
            "INSERT OR IGNORE INTO members(key) VALUES (?)",
            (_canonical_key(key),),
        )
        return cursor.rowcount == 1

    def __contains__(self, key: object) -> bool:
        self._ensure_open()
        encoded = _canonical_key(key)
        return (
            self._connection.execute(
                "SELECT 1 FROM members WHERE key = ?", (encoded,)
            ).fetchone()
            is not None
        )
