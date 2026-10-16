import sqlite3
from collections.abc import Iterator
from pathlib import Path

_MAX_ROW_ORDINAL = 2**63 - 1
_MAX_JOIN_INDEX_CACHE_KIB = 32_768

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
        value = key * 2 if key >= 0 else -key * 2 - 1
        return b"\x00" + _unsigned_varint(value)
    if type(key) is str:
        return b"\x01" + key.encode("utf-8")
    if type(key) is tuple:
        components = tuple(_canonical_key(component) for component in key)
        return (
            b"\x02"
            + _unsigned_varint(len(components))
            + b"".join(
                _unsigned_varint(len(component)) + component for component in components
            )
        )
    raise TypeError("join keys must be integers, strings, or tuples of join keys")


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

    def commit(self) -> None:
        self._ensure_open()
        if not self._occurrence_index_ready:
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS row_occurrences_key_sequence "
                "ON row_occurrences(key, sequence)"
            )
            self._occurrence_index_ready = True
        self._connection.commit()

    def add(self, key: JoinKey, row_ordinal: int) -> None:
        self._ensure_open()
        self._connection.execute(
            "INSERT INTO row_occurrences(key, row_ordinal) VALUES (?, ?)",
            (_canonical_key(key), _row_ordinal(row_ordinal)),
        )

    def iter_row_indices(self, key: JoinKey) -> Iterator[int]:
        self._ensure_open()
        encoded = _canonical_key(key)
        cursor = self._connection.execute(
            "SELECT row_ordinal FROM row_occurrences WHERE key = ? ORDER BY sequence",
            (encoded,),
        )
        return (int(row[0]) for row in cursor)

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
