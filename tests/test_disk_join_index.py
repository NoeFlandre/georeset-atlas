import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from atlas_io.disk_join_index import (
    DiskJoinIndex,
    DiskMembershipSet,
    JoinKey,
    _canonical_key,
    _row_ordinal,
    _unsigned_varint,
)


def _unique_rows_hash(index: DiskJoinIndex) -> str:
    digest = hashlib.sha256()
    for key, row_ordinal in index._connection.execute(
        "SELECT key, row_ordinal FROM unique_rows ORDER BY row_ordinal"
    ):
        digest.update(bytes(key))
        digest.update(int(row_ordinal).to_bytes(8, "little"))
    return digest.hexdigest()


def _query_plan_uses_unique_key_search(plan: list[tuple[Any, ...]]) -> bool:
    return any(str(row[-1]).startswith("SEARCH unique_rows") for row in plan)


def test_unique_index_plan_check_rejects_a_full_table_scan() -> None:
    scan = [(0, 0, 0, "SCAN unique_rows")]
    search = [
        (
            0,
            0,
            0,
            "SEARCH unique_rows USING INDEX sqlite_autoindex_unique_rows_1 (key=?)",
        )
    ]

    assert not _query_plan_uses_unique_key_search(scan)
    assert _query_plan_uses_unique_key_search(search)


def test_occurrence_lookup_uses_ordered_key_index(tmp_path: Path) -> None:
    index = DiskJoinIndex(tmp_path / "occurrence-plan.sqlite")
    index.add("probe", 1)
    index.commit()
    assert list(index.iter_row_indices("probe")) == [1]

    plan = index._connection.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT row_ordinal FROM row_occurrences "
        "WHERE key = ? ORDER BY sequence",
        (b"probe",),
    ).fetchall()

    assert any(
        str(row[-1]).startswith(
            "SEARCH row_occurrences USING INDEX row_occurrences_key_sequence"
        )
        for row in plan
    ), plan
    index.close()


def test_occurrence_index_is_deferred_until_first_lookup(tmp_path: Path) -> None:
    index = DiskJoinIndex(tmp_path / "deferred-occurrence-index.sqlite")

    def occurrence_index_count() -> int:
        return int(
            index._connection.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type = 'index' AND name = 'row_occurrences_key_sequence'"
            ).fetchone()[0]
        )

    assert occurrence_index_count() == 0
    statements: list[str] = []
    index._connection.set_trace_callback(statements.append)
    for ordinal in range(32):
        index.add(("document", str(ordinal % 4)), ordinal)
        index.add_unique(("sentence", str(ordinal)), ordinal)
    assert index._connection.in_transaction
    assert occurrence_index_count() == 0

    index.commit()

    assert not index._connection.in_transaction
    assert occurrence_index_count() == 0
    assert list(index.iter_row_indices(("document", "2"))) == [
        2,
        6,
        10,
        14,
        18,
        22,
        26,
        30,
    ]
    assert occurrence_index_count() == 1
    normalized = [statement.strip().upper() for statement in statements]
    assert sum(statement.startswith("BEGIN") for statement in normalized) == 1
    assert sum(statement == "COMMIT" for statement in normalized) == 1
    assert sum(statement.startswith("INSERT INTO") for statement in normalized) == 64
    assert sum(statement.startswith("CREATE INDEX") for statement in normalized) == 1
    index.close()


def test_unsigned_varint_encodes_boundary_values() -> None:
    assert [_unsigned_varint(value) for value in (0, 127, 128, 255)] == [
        b"\x00",
        b"\x7f",
        b"\x80\x01",
        b"\xff\x01",
    ]


@pytest.mark.parametrize(
    ("key", "encoded"),
    [
        (0, b"\x00\x00"),
        (1, b"\x00\x02"),
        (-1, b"\x00\x01"),
        (-2, b"\x00\x03"),
        ("x", b"\x01x"),
        ((), b"\x02\x00"),
        (("x", 1), b"\x02\x02\x02\x01x\x02\x00\x02"),
    ],
)
def test_canonical_key_encoding_boundaries(key: object, encoded: bytes) -> None:
    assert _canonical_key(key) == encoded


def test_row_ordinal_accepts_sqlite_integer_maximum() -> None:
    maximum = 2**63 - 1
    assert _row_ordinal(maximum) == maximum


def test_join_index_close_is_idempotent_and_closed_queries_fail(
    tmp_path: Path,
) -> None:
    index = DiskJoinIndex(tmp_path / "closed.sqlite")
    index.close()
    index.close()

    with pytest.raises(RuntimeError, match="database is closed"):
        index.lookup_unique("closed")


def test_unique_index_cache_is_bounded_and_reapplied_after_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / "unique-cache.sqlite"
    keys = [("wiki-sentence", f"Q{value:04d}") for value in (4, 1, 9, 2, 7)]
    index = DiskJoinIndex(path)
    assert index._connection.execute("PRAGMA cache_size").fetchone()[0] == -32_768

    for row_ordinal, key in enumerate(keys):
        index.add_unique(key, row_ordinal)
    index.commit()
    expected_hash = _unique_rows_hash(index)
    index.close()

    reopened = DiskJoinIndex(path)
    assert reopened._connection.execute("PRAGMA cache_size").fetchone()[0] == -32_768
    assert [reopened.lookup_unique(key) for key in keys] == list(range(len(keys)))
    assert _unique_rows_hash(reopened) == expected_hash
    plan = reopened._connection.execute(
        "EXPLAIN QUERY PLAN SELECT row_ordinal FROM unique_rows WHERE key = ?",
        (b"probe",),
    ).fetchall()
    assert _query_plan_uses_unique_key_search(plan), plan
    with pytest.raises(ValueError, match="duplicate unique key"):
        reopened.add_unique(keys[0], len(keys))
    reopened.close()


def test_join_index_persists_ordered_occurrences_and_typed_keys(
    tmp_path: Path,
) -> None:
    path = tmp_path / "join.sqlite"
    index = DiskJoinIndex(path)
    index.add("7", 20)
    index.add(7, 4)
    index.add(7, 12)
    index.commit()

    occurrences = index.iter_row_indices(7)
    assert isinstance(occurrences, Iterator)
    assert next(occurrences) == 4
    assert list(occurrences) == [12]
    assert list(index.iter_row_indices("7")) == [20]
    index.close()

    assert path.is_file()
    reopened = DiskJoinIndex(path)
    assert list(reopened.iter_row_indices(7)) == [4, 12]
    assert list(reopened.iter_row_indices("7")) == [20]
    reopened.close()


def test_nested_composite_keys_persist_without_type_or_boundary_collisions(
    tmp_path: Path,
) -> None:
    nested_key = ("outer", (1, "inner"))
    distinct_keys = (
        nested_key,
        "('outer', (1, 'inner'))",
        ("outer", 1, "inner"),
        (("outer", 1), "inner"),
        ("outer", ("1", "inner")),
    )
    index_path = tmp_path / "composite-join.sqlite"
    index = DiskJoinIndex(index_path)
    for row_ordinal, key in enumerate(distinct_keys, start=10):
        index.add(key, row_ordinal)
    index.add(nested_key, 15)

    unique_key = ("unique", 7, ("typed", "key"))
    index.add_unique(unique_key, 90)
    index.commit()
    index.close()

    reopened_index = DiskJoinIndex(index_path)
    for row_ordinal, key in enumerate(distinct_keys, start=10):
        expected = [row_ordinal, 15] if row_ordinal == 10 else [row_ordinal]
        assert list(reopened_index.iter_row_indices(key)) == expected
    assert list(reopened_index.iter_row_indices(nested_key)) == [10, 15]
    assert reopened_index.lookup_unique(unique_key) == 90
    assert reopened_index.lookup_unique(("unique", (7, "typed"), "key")) is None
    reopened_index.close()

    membership_path = tmp_path / "composite-members.sqlite"
    members = DiskMembershipSet(membership_path)
    for key in distinct_keys:
        members.add(key)
    members.commit()
    members.close()

    reopened_members = DiskMembershipSet(membership_path)
    for key in distinct_keys:
        assert key in reopened_members
    assert ("outer", (1,)) not in reopened_members
    reopened_members.close()


def test_multi_key_occurrences_preserve_global_sequence_with_bounded_queries(
    tmp_path: Path,
) -> None:
    keys = [("key", ordinal) for ordinal in range(260)]
    index = DiskJoinIndex(tmp_path / "many-keys.sqlite")
    for ordinal, key in enumerate(keys):
        index.add(key, ordinal)
    index.add("7", 260)
    index.add(7, 261)

    statements: list[str] = []
    index._connection.set_trace_callback(statements.append)
    requested = [*reversed(keys), ("key", 0), "7", 7]

    occurrences = list(
        index.iter_row_occurrences_for_keys(requested, max_unique_keys=len(requested))
    )

    assert occurrences == [
        *((key, ordinal) for ordinal, key in enumerate(keys)),
        ("7", 260),
        (7, 261),
    ]
    assert list(index.iter_row_occurrences_for_keys([], max_unique_keys=0)) == []
    selects = [
        statement
        for statement in statements
        if "FROM row_occurrences WHERE key IN (" in statement
    ]
    assert len(selects) == 3
    assert all("ORDER BY sequence" in statement for statement in selects)
    index.close()


def test_join_index_unique_lookup_rejects_duplicate_keys(tmp_path: Path) -> None:
    index = DiskJoinIndex(tmp_path / "join.sqlite")
    index.add_unique("row", 8)

    assert index.lookup_unique("row") == 8
    assert index.lookup_unique("missing") is None
    with pytest.raises(ValueError, match="duplicate"):
        index.add_unique("row", 9)

    index.close()


@pytest.mark.parametrize("ordinal", [-1, True, 1.5, "0", 2**63])
def test_join_index_rejects_invalid_row_ordinals(
    tmp_path: Path,
    ordinal: Any,
) -> None:
    index = DiskJoinIndex(tmp_path / "join.sqlite")

    with pytest.raises(ValueError):
        index.add("key", ordinal)

    index.close()


def test_disk_membership_set_persists_distinct_key_types(tmp_path: Path) -> None:
    path = tmp_path / "members.sqlite"
    members = DiskMembershipSet(path)
    assert members.add(7)
    assert members.add("7")
    assert not members.add(7)
    members.commit()
    members.close()

    assert path.is_file()
    reopened = DiskMembershipSet(path)
    assert 7 in reopened
    assert "7" in reopened
    assert 8 not in reopened
    reopened.close()


def test_occurrence_index_is_created_on_first_lookup(tmp_path: Path) -> None:
    index = DiskJoinIndex(tmp_path / "join.sqlite")
    index.add("same", 9)
    index.add("same", 3)
    names_before = {
        str(row[0])
        for row in index._connection.execute(
            'SELECT name FROM sqlite_master WHERE type = "index"'
        )
    }

    assert "row_occurrences_key_sequence" not in names_before
    assert list(index.iter_row_indices("same")) == [9, 3]

    names_after = {
        str(row[0])
        for row in index._connection.execute(
            'SELECT name FROM sqlite_master WHERE type = "index"'
        )
    }
    assert "row_occurrences_key_sequence" in names_after
    index.close()


@pytest.mark.parametrize("key_count", [0, 1, 127, 128, 129, 260])
def test_unique_lookup_batches_keep_order_and_respect_fixed_query_bound(
    tmp_path: Path,
    key_count: int,
) -> None:
    index = DiskJoinIndex(tmp_path / f"join-{key_count}.sqlite")
    keys = [("key", f"{ordinal}") for ordinal in range(key_count)]
    for ordinal, key in enumerate(keys):
        index.add_unique(key, ordinal)
    index.commit()

    statements: list[str] = []
    index._connection.set_trace_callback(statements.append)
    lookups = list(index.iter_lookup_unique(keys))
    index._connection.set_trace_callback(None)

    assert lookups == list(zip(keys, range(key_count), strict=True))
    query_count = sum(
        "SELECT key, row_ordinal FROM unique_rows WHERE key IN" in statement
        for statement in statements
    )
    assert query_count == (key_count + 127) // 128
    index.close()


def test_unique_lookup_preserves_typed_duplicate_and_missing_requests(
    tmp_path: Path,
) -> None:
    index = DiskJoinIndex(tmp_path / "typed-batch.sqlite")
    keys: list[JoinKey] = [7, "7", ("outer", (1, "inner"))]
    for ordinal, key in enumerate(keys, start=20):
        index.add_unique(key, ordinal)
    index.commit()

    requested: list[JoinKey] = [keys[1], "missing", keys[0], keys[1], keys[2]]

    assert list(index.iter_lookup_unique(requested)) == [
        (keys[1], 21),
        ("missing", None),
        (keys[0], 20),
        (keys[1], 21),
        (keys[2], 22),
    ]
    index.close()


def test_batched_occurrence_lookup_enforces_declared_key_bound(
    tmp_path: Path,
) -> None:
    index = DiskJoinIndex(tmp_path / "bounded-occurrences.sqlite")
    keys = [("document", str(ordinal)) for ordinal in range(129)]

    index.add("duplicate", 7)
    assert list(
        index.iter_row_occurrences_for_keys(
            ["duplicate", "duplicate"], max_unique_keys=1
        )
    ) == [("duplicate", 7)]

    with pytest.raises(ValueError, match="distinct join keys exceeds"):
        list(index.iter_row_occurrences_for_keys(keys, max_unique_keys=128))

    index.close()


def test_batched_occurrence_lookup_merges_global_sequence_across_key_chunks(
    tmp_path: Path,
) -> None:
    index = DiskJoinIndex(tmp_path / "occurrences.sqlite")
    keys = [("document", str(ordinal)) for ordinal in range(140)]
    for sequence in range(280):
        index.add(keys[sequence % len(keys)], sequence)
    index.commit()

    requested = list(reversed(keys)) + [keys[0], ("missing", "document")]

    assert list(
        index.iter_row_occurrences_for_keys(requested, max_unique_keys=len(keys) + 1)
    ) == [(keys[sequence % len(keys)], sequence) for sequence in range(280)]
    index.close()
