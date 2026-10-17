from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when

from atlas_domain.joined_outputs import (
    JoinedPartition,
    join_description_partition,
    join_website_partition,
    join_wiki_partition,
    normalize_label_decision,
)
from atlas_domain.source_catalog import SOURCE_CATALOG, select_input_file
from atlas_domain.source_records import DataJoinError

scenarios("../features/joined_source_partitions.feature")

STATUSES = ("yes", "no", "failed", "skipped")
REGIONAL_PATHS = {
    "description": "labels/language-v1/data/region-a.parquet",
    "website": "labels/polygons/region-a.parquet",
    "wiki": "labels/wikipedia/sentences/region-a.parquet",
}

DESCRIPTION_LABELS: list[dict[str, Any]] = [
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "yes",
    },
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 1,
        "decision": "skipped_unsplit",
    },
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "failed",
    },
    {
        "description_identity": "description-2",
        "tag_key": "name:fr",
        "sentence_index": 0,
        "decision": "no",
    },
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "yes",
    },
]
DESCRIPTION_TEXT_UNITS: list[dict[str, Any]] = [
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "split",
        "original_text": "First. Second.",
        "sentences": ["First.", "Second."],
        "source_url": "https://www.openstreetmap.org/way/1",
        "raw_language_code": "en-GB",
    },
    {
        "description_identity": "description-2",
        "tag_key": "name:fr",
        "split_status": "unsupported",
        "original_text": "Une seule phrase.",
        "sentences": None,
        "source_url": "https://www.openstreetmap.org/way/2",
        "raw_language_code": "fr",
    },
]
DESCRIPTION_POLYGONS: list[dict[str, Any]] = [
    {
        "polygon_id": "polygon-1",
        "description_identity": "description-1",
        "geometry_wkb": b"geometry-1",
    },
    {
        "polygon_id": "polygon-2",
        "description_identity": "description-1",
        "geometry_wkb": b"geometry-2",
    },
    {
        "polygon_id": "polygon-3",
        "description_identity": "description-2",
        "geometry_wkb": b"geometry-3",
    },
]


def _join_description(
    *,
    labels: list[dict[str, Any]] = DESCRIPTION_LABELS,
    texts: list[dict[str, Any]] = DESCRIPTION_TEXT_UNITS,
    polygons: list[dict[str, Any]] = DESCRIPTION_POLYGONS,
) -> JoinedPartition:
    return join_description_partition(
        label_rows=labels, text_unit_rows=texts, polygon_rows=polygons
    )


def _units_by_key(partition: JoinedPartition) -> dict[Any, Mapping[str, Any]]:
    return {row["text_unit_key"]: row for row in partition.text_units}


def test_description_joins_exact_sentence_by_identity_tag_and_index() -> None:
    partition = _join_description()
    units = _units_by_key(partition)

    assert len(units) == len(partition.text_units)
    assert [units[row["text_unit_key"]]["text"] for row in partition.labels] == [
        "First.",
        "Second.",
        "First.",
        "Une seule phrase.",
        "First.",
    ]
    assert [row["source_row_index"] for row in partition.labels] == [0, 1, 2, 3, 4]


def test_description_keeps_raw_decisions_and_normalizes_statuses() -> None:
    partition = _join_description()

    assert [row["raw_decision"] for row in partition.labels] == [
        "yes",
        "skipped_unsplit",
        "failed",
        "no",
        "yes",
    ]
    assert [row["status"] for row in partition.labels] == [
        "yes",
        "skipped",
        "failed",
        "no",
        "yes",
    ]


def test_description_text_units_expose_polygon_ids_and_native_fields() -> None:
    units = _units_by_key(_join_description())

    first = units[("description-1", "name", 0)]
    assert first["polygon_ids"] == ["polygon-1", "polygon-2"]
    assert first["source_url"] == "https://www.openstreetmap.org/way/1"
    assert first["raw_language_code"] == "en-GB"
    assert first["split_status"] == "split"
    assert units[("description-2", "name:fr", 0)]["polygon_ids"] == ["polygon-3"]


def test_description_stores_each_geometry_row_once_in_polygon_records() -> None:
    partition = _join_description()

    assert [row["polygon_id"] for row in partition.polygon_records] == [
        "polygon-1",
        "polygon-2",
        "polygon-3",
    ]
    assert partition.polygon_records[0]["geometry_wkb"] == b"geometry-1"
    assert all("geometry_wkb" not in row for row in partition.text_units)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"labels": [{**DESCRIPTION_LABELS[0], "tag_key": "missing"}]},
        {"labels": [{**DESCRIPTION_LABELS[0], "sentence_index": 5}]},
        {"texts": [*DESCRIPTION_TEXT_UNITS, DESCRIPTION_TEXT_UNITS[0]]},
        {"polygons": [DESCRIPTION_POLYGONS[2]]},
        {"polygons": [*DESCRIPTION_POLYGONS, DESCRIPTION_POLYGONS[0]]},
    ],
)
def test_description_join_fails_closed_on_unmatched_or_ambiguous_rows(
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(DataJoinError):
        _join_description(**kwargs)


def test_description_join_rejects_unknown_raw_decisions() -> None:
    with pytest.raises(ValueError, match="decision"):
        _join_description(labels=[{**DESCRIPTION_LABELS[0], "decision": "maybe"}])


WEBSITE_POLYGONS_AND_TEXT: list[dict[str, Any]] = [
    {
        "polygon_id": "polygon-1",
        "decision": "split",
        "website_text": " Website first. Website second. ",
        "website_sentences": ["Website first.", "Website second."],
        "contact_website_text": "Contact first.",
        "contact_website_sentences": ["Contact first."],
        "source_url": "https://example.test/one",
        "raw_language_code": "de-AT",
        "geometry_wkb": b"geometry-1",
    },
    {
        "polygon_id": "polygon-2",
        "decision": "skipped",
        "website_text": "Full site text.",
        "website_sentences": [],
        "contact_website_text": "Full contact text.",
        "contact_website_sentences": [],
        "source_url": "https://example.test/two",
        "raw_language_code": "und",
        "geometry_wkb": b"geometry-2",
    },
]
WEBSITE_LABELS: list[dict[str, Any]] = [
    {
        "polygon_id": "polygon-1",
        "field": "website",
        "sentence_index": 1,
        "decision": "yes",
    },
    {
        "polygon_id": "polygon-1",
        "field": "contact_website",
        "sentence_index": 0,
        "decision": "no",
    },
    {
        "polygon_id": "polygon-2",
        "field": "website",
        "sentence_index": 0,
        "decision": "skipped_unsplit",
    },
    {
        "polygon_id": "polygon-1",
        "field": "website",
        "sentence_index": 1,
        "decision": "failed",
    },
]


def _join_website(
    *,
    labels: list[dict[str, Any]] = WEBSITE_LABELS,
    polygons: list[dict[str, Any]] = WEBSITE_POLYGONS_AND_TEXT,
) -> JoinedPartition:
    return join_website_partition(label_rows=labels, polygon_text_rows=polygons)


def test_website_selects_the_sentence_and_keeps_full_text_separately() -> None:
    units = _units_by_key(_join_website())

    selected = units[("polygon-1", "website", 1)]
    assert selected["text"] == "Website second."
    assert selected["full_text"] == " Website first. Website second. "
    assert selected["polygon_ids"] == ["polygon-1"]
    assert selected["source_url"] == "https://example.test/one"
    assert selected["raw_language_code"] == "de-AT"
    assert units[("polygon-1", "contact_website", 0)]["text"] == "Contact first."
    skipped = units[("polygon-2", "website", 0)]
    assert skipped["text"] == "Full site text."
    assert skipped["full_text"] == "Full site text."


def test_website_labels_join_one_occurrence_each_and_keep_raw_decisions() -> None:
    partition = _join_website()

    assert [row["text_unit_key"] for row in partition.labels] == [
        ("polygon-1", "website", 1),
        ("polygon-1", "contact_website", 0),
        ("polygon-2", "website", 0),
        ("polygon-1", "website", 1),
    ]
    assert [row["raw_decision"] for row in partition.labels] == [
        "yes",
        "no",
        "skipped_unsplit",
        "failed",
    ]
    assert [row["status"] for row in partition.labels] == [
        "yes",
        "no",
        "skipped",
        "failed",
    ]


def test_website_stores_each_geometry_row_once_in_polygon_records() -> None:
    partition = _join_website()

    assert [row["polygon_id"] for row in partition.polygon_records] == [
        "polygon-1",
        "polygon-2",
    ]
    assert all("geometry_wkb" not in row for row in partition.text_units)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"labels": [{**WEBSITE_LABELS[0], "polygon_id": "polygon-9"}]},
        {"labels": [{**WEBSITE_LABELS[0], "field": "description"}]},
        {"labels": [{**WEBSITE_LABELS[0], "sentence_index": 2}]},
        {"polygons": [*WEBSITE_POLYGONS_AND_TEXT, WEBSITE_POLYGONS_AND_TEXT[0]]},
    ],
)
def test_website_join_fails_closed_on_unmatched_or_ambiguous_rows(
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(DataJoinError):
        _join_website(**kwargs)


WIKI_SENTENCES: list[dict[str, Any]] = [
    {
        "sentence_id": "sentence-1",
        "document_id": "document-1",
        "project": "project-a",
        "sentence_text": "Alpha sentence.",
        "raw_language_code": "en",
    },
    {
        "sentence_id": "sentence-2",
        "document_id": "document-1",
        "project": "project-a",
        "sentence_text": "Beta sentence.",
        "raw_language_code": "en",
    },
    {
        "sentence_id": "sentence-3",
        "document_id": "document-2",
        "project": "project-b",
        "sentence_text": "Gamma.",
        "raw_language_code": "und",
    },
]
WIKI_DOCUMENTS: list[dict[str, Any]] = [
    {
        "document_id": "document-1",
        "project": "project-a",
        "title": "Alpha",
        "page_id": 42,
        "updated_at": datetime(2026, 1, 2, 3, 4, 5),
        "source_url": "https://en.wikipedia.org/wiki/Alpha",
    },
    {
        "document_id": "document-2",
        "project": "project-b",
        "title": "Gamma",
        "page_id": 7,
        "updated_at": datetime(2025, 12, 31, 23, 59, 59),
        "source_url": "https://wikivoyage.example/Gamma",
    },
    {
        "document_id": "document-3",
        "project": "project-a",
        "title": "Unreferenced",
        "page_id": 9,
        "updated_at": datetime(2024, 5, 6, 7, 8, 9),
        "source_url": "https://en.wikipedia.org/wiki/Unreferenced",
    },
]
WIKI_LINKS: list[dict[str, Any]] = [
    {"document_id": "document-1", "project": "project-a", "polygon_id": "polygon-1"},
    {"document_id": "document-1", "project": "project-a", "polygon_id": "polygon-2"},
    {"document_id": "document-1", "project": "project-a", "polygon_id": "polygon-1"},
    {"document_id": "document-1", "project": "project-b", "polygon_id": "polygon-3"},
]
WIKI_POLYGONS: list[dict[str, Any]] = [
    {"polygon_id": "polygon-1", "geometry_wkb": b"geometry-1"},
    {"polygon_id": "polygon-2", "geometry_wkb": b"geometry-2"},
    {"polygon_id": "polygon-3", "geometry_wkb": b"geometry-3"},
    {"polygon_id": "polygon-4", "geometry_wkb": b"geometry-4"},
]
WIKI_LABELS: list[dict[str, Any]] = [
    {"sentence_id": "sentence-1", "decision": "yes"},
    {"sentence_id": "sentence-2", "decision": "skipped_unsplit"},
    {"sentence_id": "sentence-1", "decision": "no"},
    {"sentence_id": "sentence-3", "decision": "failed"},
]


def _join_wiki(
    *,
    labels: list[dict[str, Any]] = WIKI_LABELS,
    sentences: list[dict[str, Any]] = WIKI_SENTENCES,
    documents: list[dict[str, Any]] = WIKI_DOCUMENTS,
    links: list[dict[str, Any]] = WIKI_LINKS,
    polygons: list[dict[str, Any]] = WIKI_POLYGONS,
) -> JoinedPartition:
    return join_wiki_partition(
        label_rows=labels,
        sentence_rows=sentences,
        document_rows=documents,
        link_rows=links,
        polygon_rows=polygons,
    )


def test_wiki_labels_join_by_sentence_then_document_and_project_links() -> None:
    partition = _join_wiki()
    units = _units_by_key(partition)

    assert [row["text_unit_key"] for row in partition.labels] == [
        "sentence-1",
        "sentence-2",
        "sentence-1",
        "sentence-3",
    ]
    assert units["sentence-1"]["polygon_ids"] == [
        "polygon-1",
        "polygon-2",
        "polygon-1",
    ]
    assert units["sentence-2"]["polygon_ids"] == [
        "polygon-1",
        "polygon-2",
        "polygon-1",
    ]
    assert "polygon-3" not in units["sentence-1"]["polygon_ids"]


def test_wiki_label_without_a_matching_link_keeps_an_empty_polygon_list() -> None:
    partition = _join_wiki()
    units = _units_by_key(partition)

    assert units["sentence-3"]["polygon_ids"] == []
    assert partition.labels[3]["raw_decision"] == "failed"
    assert partition.labels[3]["status"] == "failed"


def test_wiki_keeps_referenced_documents_with_native_typed_fields() -> None:
    partition = _join_wiki()

    assert list(partition.documents) == WIKI_DOCUMENTS[:2]
    assert isinstance(partition.documents[0]["page_id"], int)
    assert isinstance(partition.documents[0]["updated_at"], datetime)


def test_wiki_persists_only_referenced_document_links_in_source_order() -> None:
    linked_at = datetime(2026, 4, 5, 6, 7, 8)
    first = {
        "document_id": "document-1",
        "project": "project-a",
        "polygon_id": "polygon-1",
        "revision_id": 123,
        "linked_at": linked_at,
    }
    unreferenced = {
        "document_id": "document-1",
        "project": "project-b",
        "polygon_id": "polygon-3",
        "revision_id": None,
        "linked_at": None,
    }
    second = {
        "document_id": "document-2",
        "project": "project-b",
        "polygon_id": "polygon-3",
        "input_revision": "snapshot-7",
    }
    links = [first, unreferenced, second, dict(first)]

    partition = _join_wiki(links=links)

    assert list(partition.polygon_document_links) == [first, second, first]
    assert type(partition.polygon_document_links[0]["revision_id"]) is int
    assert partition.polygon_document_links[0]["linked_at"] == linked_at
    assert "revision_id" not in partition.polygon_document_links[1]


def test_wiki_keeps_raw_language_code_and_source_url_per_text_unit() -> None:
    units = _units_by_key(_join_wiki())

    assert units["sentence-3"]["raw_language_code"] == "und"
    assert units["sentence-3"]["source_url"] == "https://wikivoyage.example/Gamma"
    assert units["sentence-1"]["source_url"] == "https://en.wikipedia.org/wiki/Alpha"


def test_wiki_stores_only_linked_geometry_rows_once() -> None:
    partition = _join_wiki()

    assert [row["polygon_id"] for row in partition.polygon_records] == [
        "polygon-1",
        "polygon-2",
    ]
    assert partition.polygon_records[0]["geometry_wkb"] == b"geometry-1"
    assert all("geometry_wkb" not in row for row in partition.text_units)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"labels": [{"sentence_id": "sentence-9", "decision": "yes"}]},
        {"sentences": [*WIKI_SENTENCES, WIKI_SENTENCES[0]]},
        {"documents": [*WIKI_DOCUMENTS, WIKI_DOCUMENTS[0]]},
        {"polygons": WIKI_POLYGONS[1:]},
    ],
)
def test_wiki_join_fails_closed_on_unmatched_or_ambiguous_rows(
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(DataJoinError):
        _join_wiki(**kwargs)


PartitionAndInputs = tuple[JoinedPartition, list[dict[str, Any]]]
PartitionsBySource = dict[str, PartitionAndInputs]


@pytest.mark.parametrize(
    ("raw", "status"),
    [
        ("yes", "yes"),
        ("no", "no"),
        ("failed", "failed"),
        ("skipped_unsplit", "skipped"),
    ],
)
def test_raw_label_decisions_normalize_to_the_four_statuses(
    raw: str,
    status: str,
) -> None:
    assert normalize_label_decision(raw) == status


@pytest.mark.parametrize("raw", ["", "Yes", "skipped-unsplit", "unknown"])
def test_unknown_raw_label_decisions_are_rejected(raw: str) -> None:
    with pytest.raises(ValueError):
        normalize_label_decision(raw)


def _all_partitions() -> PartitionsBySource:
    return {
        "description": (_join_description(), DESCRIPTION_LABELS),
        "website": (_join_website(), WEBSITE_LABELS),
        "wiki": (_join_wiki(), WIKI_LABELS),
    }


def _expected_status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(normalize_label_decision(row["decision"]) for row in rows)
    return {status: counts[status] for status in STATUSES}


def test_status_and_raw_counts_reconcile_with_every_input_label_row() -> None:
    for partition, rows in _all_partitions().values():
        assert len(partition.labels) == len(rows)
        assert {
            status: partition.status_counts[status] for status in STATUSES
        } == _expected_status_counts(rows)
        assert dict(partition.raw_decision_counts) == dict(
            Counter(row["decision"] for row in rows)
        )


@given(
    "a synthetic regional label file from each frozen source",
    target_fixture="regional_paths",
)
def regional_label_files() -> dict[str, str]:
    for source_id, path in REGIONAL_PATHS.items():
        select_input_file(
            SOURCE_CATALOG, source_id=source_id, table="labels", path=path
        )
    return REGIONAL_PATHS


@when(
    "each regional label file is joined to its native text and polygon rows",
    target_fixture="joined_partitions",
)
def join_regional_files() -> PartitionsBySource:
    return _all_partitions()


@then("every label row joins to exactly one text occurrence in its partition")
def every_label_joins_once(joined_partitions: PartitionsBySource) -> None:
    for partition, _ in joined_partitions.values():
        known = [row["text_unit_key"] for row in partition.text_units]
        assert len(known) == len(set(known))
        assert {row["text_unit_key"] for row in partition.labels} <= set(known)
        assert [row["source_row_index"] for row in partition.labels] == list(
            range(len(partition.labels))
        )


@then("the per-status label counts reconcile with the input label rows")
def per_status_counts_reconcile(joined_partitions: PartitionsBySource) -> None:
    for partition, rows in joined_partitions.values():
        assert {
            status: partition.status_counts[status] for status in STATUSES
        } == _expected_status_counts(rows)
        assert sum(partition.status_counts.values()) == len(rows)


@then("each polygon geometry row is stored once per partition")
def geometry_is_stored_once(joined_partitions: PartitionsBySource) -> None:
    for partition, _ in joined_partitions.values():
        polygon_ids = [row["polygon_id"] for row in partition.polygon_records]
        assert len(polygon_ids) == len(set(polygon_ids))
        assert all("geometry_wkb" not in row for row in partition.text_units)
