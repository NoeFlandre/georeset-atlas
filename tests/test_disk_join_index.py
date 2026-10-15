from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from atlas_io.disk_join_index import DiskJoinIndex, DiskMembershipSet


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


def test_occurrence_read_does_not_commit_pending_rows(tmp_path: Path) -> None:
    path = tmp_path / "pending.sqlite"
    index = DiskJoinIndex(path)
    index.add("pending", 1)

    assert list(index.iter_row_indices("pending")) == [1]
    index.close()

    reopened = DiskJoinIndex(path)
    assert list(reopened.iter_row_indices("pending")) == []
    reopened.close()


def test_occurrence_index_tracks_additions_after_a_read(tmp_path: Path) -> None:
    index = DiskJoinIndex(tmp_path / "append.sqlite")
    index.add("shared", 3)

    assert list(index.iter_row_indices("shared")) == [3]

    index.add("shared", 8)
    index.commit()

    assert list(index.iter_row_indices("shared")) == [3, 8]
    index.close()


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
