from collections import Counter
from datetime import datetime
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from atlas_domain.joined_outputs import (
    join_description_partition,
    join_wiki_partition,
    normalize_label_decision,
)

STATUSES = ("yes", "no", "failed", "skipped")
RAW_DECISION = st.sampled_from(("yes", "no", "failed", "skipped_unsplit"))
DESCRIPTION_LABEL = st.fixed_dictionaries(
    {
        "description_identity": st.just("description-1"),
        "tag_key": st.just("name"),
        "sentence_index": st.integers(min_value=0, max_value=2),
        "decision": RAW_DECISION,
    }
)
WIKI_LINK = st.fixed_dictionaries(
    {
        "document_id": st.just("document-1"),
        "project": st.just("project-a"),
        "polygon_id": st.sampled_from(("polygon-1", "polygon-2")),
    }
)
WIKI_LABEL = st.fixed_dictionaries(
    {
        "sentence_id": st.sampled_from(("sentence-1", "sentence-2")),
        "decision": RAW_DECISION,
    }
)
DESCRIPTION_TEXTS: list[dict[str, Any]] = [
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "split",
        "original_text": "One. Two. Three.",
        "sentences": ["One.", "Two.", "Three."],
        "source_url": "https://www.openstreetmap.org/way/1",
        "raw_language_code": "en",
    }
]
DESCRIPTION_POLYGONS: list[dict[str, Any]] = [
    {
        "polygon_id": "polygon-1",
        "description_identity": "description-1",
        "geometry_wkb": b"geometry-1",
    }
]
WIKI_SENTENCES: list[dict[str, Any]] = [
    {
        "sentence_id": sentence_id,
        "document_id": "document-1",
        "project": "project-a",
        "sentence_text": sentence_id,
        "raw_language_code": "en",
    }
    for sentence_id in ("sentence-1", "sentence-2")
]
WIKI_DOCUMENTS: list[dict[str, Any]] = [
    {
        "document_id": "document-1",
        "project": "project-a",
        "title": "Alpha",
        "page_id": 42,
        "updated_at": datetime(2026, 1, 2, 3, 4, 5),
        "source_url": "https://en.wikipedia.org/wiki/Alpha",
    }
]
WIKI_POLYGONS: list[dict[str, Any]] = [
    {"polygon_id": "polygon-1", "geometry_wkb": b"geometry-1"},
    {"polygon_id": "polygon-2", "geometry_wkb": b"geometry-2"},
]


@settings(database=None)
@given(labels=st.lists(DESCRIPTION_LABEL, max_size=30))
def test_description_joins_never_deduplicate_labels(
    labels: list[dict[str, Any]],
) -> None:
    partition = join_description_partition(
        label_rows=labels,
        text_unit_rows=DESCRIPTION_TEXTS,
        polygon_rows=DESCRIPTION_POLYGONS,
    )

    assert [row["source_row_index"] for row in partition.labels] == list(
        range(len(labels))
    )
    assert [row["raw_decision"] for row in partition.labels] == [
        label["decision"] for label in labels
    ]
    assert [row["text_unit_key"] for row in partition.labels] == [
        ("description-1", "name", label["sentence_index"]) for label in labels
    ]
    assert len(partition.text_units) == len(
        {label["sentence_index"] for label in labels}
    )
    assert sum(partition.status_counts.values()) == len(labels)


@settings(database=None)
@given(
    links=st.lists(WIKI_LINK, max_size=20),
    labels=st.lists(WIKI_LABEL, max_size=12),
)
def test_wiki_links_are_never_deduplicated_across_shared_sentences(
    links: list[dict[str, Any]],
    labels: list[dict[str, Any]],
) -> None:
    partition = join_wiki_partition(
        label_rows=labels,
        sentence_rows=WIKI_SENTENCES,
        document_rows=WIKI_DOCUMENTS,
        link_rows=links,
        polygon_rows=WIKI_POLYGONS,
    )

    expected_polygon_ids = [link["polygon_id"] for link in links]
    units = {row["text_unit_key"]: row for row in partition.text_units}
    assert [row["text_unit_key"] for row in partition.labels] == [
        label["sentence_id"] for label in labels
    ]
    for sentence_id in {label["sentence_id"] for label in labels}:
        assert units[sentence_id]["polygon_ids"] == expected_polygon_ids


@settings(database=None)
@given(labels=st.lists(WIKI_LABEL, max_size=12))
def test_status_counts_reconcile_with_arbitrary_repeated_wiki_labels(
    labels: list[dict[str, Any]],
) -> None:
    partition = join_wiki_partition(
        label_rows=labels,
        sentence_rows=WIKI_SENTENCES,
        document_rows=WIKI_DOCUMENTS,
        link_rows=[],
        polygon_rows=WIKI_POLYGONS,
    )

    expected = Counter(normalize_label_decision(label["decision"]) for label in labels)
    assert {status: partition.status_counts[status] for status in STATUSES} == {
        status: expected[status] for status in STATUSES
    }
    assert dict(partition.raw_decision_counts) == dict(
        Counter(label["decision"] for label in labels)
    )
    assert all(row["polygon_ids"] == [] for row in partition.text_units)


NATIVE_SNAPSHOTS = ("afghanistan-2025.osm.pbf", "afghanistan-2026.osm.pbf")


@settings(database=None, derandomize=True)
@given(
    osm_ids=st.lists(
        st.integers(min_value=1, max_value=50), min_size=1, max_size=5, unique=True
    ),
    osm_type=st.sampled_from(("node", "way", "relation")),
)
def test_colliding_osm_ids_across_source_pbf_never_merge(
    osm_ids: list[int],
    osm_type: str,
) -> None:
    polygons: list[dict[str, Any]] = []
    texts: list[dict[str, Any]] = []
    labels: list[dict[str, Any]] = []
    for source_pbf in NATIVE_SNAPSHOTS:
        for osm_id in osm_ids:
            identity = f"{source_pbf}-{osm_type}-{osm_id}"
            polygons.append(
                {
                    "source_pbf": source_pbf,
                    "osm_type": osm_type,
                    "osm_id": osm_id,
                    "geometry": identity.encode(),
                }
            )
            texts.append(
                {
                    "description_identity": identity,
                    "source_pbf": source_pbf,
                    "osm_type": osm_type,
                    "osm_id": osm_id,
                    "tag_key": "name",
                    "original_text": f"Text for {identity}.",
                    "language_code": "en",
                    "split_status": "unsupported",
                    "sentences": None,
                }
            )
            labels.append(
                {
                    "description_identity": identity,
                    "tag_key": "name",
                    "sentence_index": 0,
                    "decision": "yes",
                }
            )
    partition = join_description_partition(
        label_rows=labels, text_unit_rows=texts, polygon_rows=polygons
    )

    records = partition.polygon_records
    geometry_by_id = {row["polygon_id"]: row["geometry"] for row in records}
    assert len(records) == len(polygons)
    assert len(geometry_by_id) == len(polygons)
    for unit in partition.text_units:
        entity = f"{unit['source_pbf']}-{unit['osm_type']}-{unit['osm_id']}"
        resolved = [geometry_by_id[pid] for pid in unit["polygon_ids"]]
        assert resolved == [entity.encode()]
