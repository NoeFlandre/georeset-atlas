from collections.abc import Iterable, Mapping
from typing import Any

from pytest_bdd import given, scenarios, then, when

from atlas_domain.joined_outputs import (
    JoinedPartition,
    join_description_partition,
    join_website_partition,
    join_wiki_partition,
)

scenarios("../features/joined_native_source_fields.feature")

PBF = "afghanistan-latest.osm.pbf"
OTHER_PBF = "afghanistan-2025.osm.pbf"
GEOMETRY_WAY_1 = b"\x00\xffway-1\x01"
GEOMETRY_WAY_2 = b"\x00\xffway-2\x01"
GEOMETRY_RELATION_1 = b"\x00\xffrelation-1\x01"
CLINIC_GEOJSON = '{"type":"Point","coordinates":[69.2075,34.5553]}'
BAZAAR_GEOJSON = '{"type":"Point","coordinates":[69.1723,34.5282]}'
WIKI_GEOJSON_1 = '{"type":"Point","coordinates":[69.2,34.5]}'
WIKI_GEOJSON_2 = '{"type":"Point","coordinates":[69.1,34.4]}'
LINK_FIELDS = ("document_id", "project", "polygon_id", "input_revision")

DESCRIPTION_NATIVE_TEXTS: list[dict[str, Any]] = [
    {
        "description_identity": "description-way-1",
        "source_pbf": PBF,
        "osm_type": "way",
        "osm_id": 1,
        "tag_key": "name",
        "original_text": "Kabul bazaar. Open daily.",
        "language_code": "en-GB",
        "split_status": "split",
        "sentences": ["Kabul bazaar.", "Open daily."],
    },
    {
        "description_identity": "description-way-2",
        "source_pbf": PBF,
        "osm_type": "way",
        "osm_id": 2,
        "tag_key": "name:fa",
        "original_text": "Bazaar in Kabul.",
        "language_code": "fa",
        "split_status": "unsupported",
        "sentences": None,
    },
    {
        "description_identity": "description-relation-1",
        "source_pbf": PBF,
        "osm_type": "relation",
        "osm_id": 1,
        "tag_key": "name",
        "original_text": "Afghanistan.",
        "language_code": "en",
        "split_status": "unsupported",
        "sentences": None,
    },
]
DESCRIPTION_NATIVE_POLYGONS: list[dict[str, Any]] = [
    {"source_pbf": PBF, "osm_type": "way", "osm_id": 1, "geometry": GEOMETRY_WAY_1},
    {"source_pbf": PBF, "osm_type": "way", "osm_id": 2, "geometry": GEOMETRY_WAY_2},
    {
        "source_pbf": PBF,
        "osm_type": "relation",
        "osm_id": 1,
        "geometry": GEOMETRY_RELATION_1,
    },
]
DESCRIPTION_NATIVE_LABELS: list[dict[str, Any]] = [
    {
        "label_id": "label-1",
        "description_identity": "description-way-1",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "yes",
        "language": "en",
    },
    {
        "label_id": "label-2",
        "description_identity": "description-way-1",
        "tag_key": "name",
        "sentence_index": 1,
        "decision": "failed",
        "language": "en",
    },
    {
        "label_id": "label-3",
        "description_identity": "description-way-2",
        "tag_key": "name:fa",
        "sentence_index": 0,
        "decision": "skipped_unsplit",
        "language": "fa",
    },
    {
        "label_id": "label-4",
        "description_identity": "description-relation-1",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "no",
        "language": "en",
    },
]

# Same OSM text on two '|' or ':' joins: naive string keys would collide.
SEPARATOR_TEXTS: list[dict[str, Any]] = [
    {
        "description_identity": "description-a",
        "source_pbf": "north|south",
        "osm_type": "way",
        "osm_id": 1,
        "tag_key": "name",
        "original_text": "Alpha.",
        "language_code": "en",
        "split_status": "unsupported",
        "sentences": None,
    },
    {
        "description_identity": "description-b",
        "source_pbf": "north",
        "osm_type": "south|way",
        "osm_id": 1,
        "tag_key": "name",
        "original_text": "Beta.",
        "language_code": "en",
        "split_status": "unsupported",
        "sentences": None,
    },
]
SEPARATOR_POLYGONS: list[dict[str, Any]] = [
    {
        "source_pbf": "north|south",
        "osm_type": "way",
        "osm_id": 1,
        "geometry": b"geometry-a",
    },
    {
        "source_pbf": "north",
        "osm_type": "south|way",
        "osm_id": 1,
        "geometry": b"geometry-b",
    },
]
SEPARATOR_LABELS: list[dict[str, Any]] = [
    {
        "description_identity": "description-a",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "yes",
    },
    {
        "description_identity": "description-b",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "no",
    },
]

# The same OSM entity appears in two PBF snapshots with distinct identities.
SNAPSHOT_TEXTS: list[dict[str, Any]] = [
    {
        "description_identity": "description-2025",
        "source_pbf": OTHER_PBF,
        "osm_type": "way",
        "osm_id": 1,
        "tag_key": "name",
        "original_text": "Older name.",
        "language_code": "en",
        "split_status": "unsupported",
        "sentences": None,
    },
    {
        "description_identity": "description-2026",
        "source_pbf": PBF,
        "osm_type": "way",
        "osm_id": 1,
        "tag_key": "name",
        "original_text": "Newer name.",
        "language_code": "en",
        "split_status": "unsupported",
        "sentences": None,
    },
]
SNAPSHOT_POLYGONS: list[dict[str, Any]] = [
    {
        "source_pbf": OTHER_PBF,
        "osm_type": "way",
        "osm_id": 1,
        "geometry": b"geometry-2025",
    },
    {
        "source_pbf": PBF,
        "osm_type": "way",
        "osm_id": 1,
        "geometry": b"geometry-2026",
    },
]
SNAPSHOT_LABELS: list[dict[str, Any]] = [
    {
        "description_identity": "description-2025",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "yes",
    },
    {
        "description_identity": "description-2026",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "no",
    },
]

WEBSITE_NATIVE_POLYGONS: list[dict[str, Any]] = [
    {
        "polygon_id": "website-way-1",
        "source_pbf": PBF,
        "osm_type": "way",
        "osm_id": 1,
        "website": "  Clinic hours. Call first.  ",
        "website_sentences": ["Clinic hours.", "Call first."],
        "website_language": "en",
        "website_sentence_status": "split",
        "contact_website": "Contact by phone.\nOr by email.",
        "contact_website_sentences": None,
        "contact_website_language": "fr",
        "contact_website_sentence_status": "unsupported",
        "geometry": CLINIC_GEOJSON,
        "lat": 34.5553,
        "lon": 69.2075,
    },
    {
        "polygon_id": "website-way-2",
        "source_pbf": PBF,
        "osm_type": "way",
        "osm_id": 2,
        "website": "Full bazaar site.",
        "website_sentences": [],
        "website_language": "und",
        "website_sentence_status": "skipped",
        "contact_website": "Bazaar line one. Bazaar line two.",
        "contact_website_sentences": ["Bazaar line one.", "Bazaar line two."],
        "contact_website_language": "ps",
        "contact_website_sentence_status": "split",
        "geometry": BAZAAR_GEOJSON,
        "lat": 34.5282,
        "lon": 69.1723,
    },
]
WEBSITE_NATIVE_LABELS: list[dict[str, Any]] = [
    {
        "label_id": "label-1",
        "polygon_id": "website-way-1",
        "field": "website",
        "sentence_index": 1,
        "decision": "yes",
        "language": "en",
    },
    {
        "label_id": "label-2",
        "polygon_id": "website-way-1",
        "field": "contact_website",
        "sentence_index": 0,
        "decision": "no",
        "language": "fr",
    },
    {
        "label_id": "label-3",
        "polygon_id": "website-way-2",
        "field": "website",
        "sentence_index": 0,
        "decision": "skipped_unsplit",
        "language": "und",
    },
    {
        "label_id": "label-4",
        "polygon_id": "website-way-2",
        "field": "contact_website",
        "sentence_index": 1,
        "decision": "failed",
        "language": "ps",
    },
]

WIKI_NATIVE_SENTENCES: list[dict[str, Any]] = [
    {
        "sentence_id": "sentence-kabul-1",
        "document_id": "document-kabul",
        "project": "wikipedia",
        "language": "en",
        "text": "Kabul is the capital.",
        "license": "CC BY-SA 4.0",
        "attribution": "Wikipedia contributors",
    },
    {
        "sentence_id": "sentence-kabul-2",
        "document_id": "document-kabul",
        "project": "wikipedia",
        "language": "en",
        "text": "  Padded sentence.  ",
        "license": "CC BY-SA 4.0",
        "attribution": "Wikipedia contributors",
    },
    {
        "sentence_id": "sentence-gamma-1",
        "document_id": "document-gamma",
        "project": "wikivoyage",
        "language": "und",
        "text": "Gamma.",
        "license": "CC BY-SA 3.0",
        "attribution": "Wikivoyage contributors",
    },
]
WIKI_NATIVE_DOCUMENTS: list[dict[str, Any]] = [
    {
        "document_id": "document-kabul",
        "project": "wikipedia",
        "title": "Kabul",
        "url": "https://en.wikipedia.org/wiki/Kabul",
        "page_id": 42,
        "revision_id": 1234567,
        "license": "CC BY-SA 4.0",
        "attribution": "Wikipedia contributors",
    },
    {
        "document_id": "document-gamma",
        "project": "wikivoyage",
        "title": "Gamma",
        "url": "https://en.wikivoyage.org/wiki/Gamma",
        "page_id": 7,
        "revision_id": 7654321,
        "license": "CC BY-SA 3.0",
        "attribution": "Wikivoyage contributors",
    },
]
WIKI_NATIVE_POLYGONS: list[dict[str, Any]] = [
    {"polygon_id": "polygon-kabul-1", "geometry": WIKI_GEOJSON_1},
    {"polygon_id": "polygon-kabul-2", "geometry": WIKI_GEOJSON_2},
]
# The third row repeats the first link; the last links one polygon to two documents.
WIKI_NATIVE_LINKS: list[dict[str, Any]] = [
    {
        "document_id": "document-kabul",
        "project": "wikipedia",
        "polygon_id": "polygon-kabul-1",
        "input_revision": "rev-1",
    },
    {
        "document_id": "document-kabul",
        "project": "wikipedia",
        "polygon_id": "polygon-kabul-2",
        "input_revision": "rev-1",
    },
    {
        "document_id": "document-kabul",
        "project": "wikipedia",
        "polygon_id": "polygon-kabul-1",
        "input_revision": "rev-2",
    },
    {
        "document_id": "document-gamma",
        "project": "wikivoyage",
        "polygon_id": "polygon-kabul-1",
        "input_revision": "rev-1",
    },
]
WIKI_NATIVE_LABELS: list[dict[str, Any]] = [
    {
        "label_id": "label-1",
        "sentence_id": "sentence-kabul-1",
        "decision": "yes",
        "language": "en",
    },
    {
        "label_id": "label-2",
        "sentence_id": "sentence-kabul-2",
        "decision": "skipped_unsplit",
        "language": "en",
    },
    {
        "label_id": "label-3",
        "sentence_id": "sentence-kabul-1",
        "decision": "no",
        "language": "en",
    },
    {
        "label_id": "label-4",
        "sentence_id": "sentence-gamma-1",
        "decision": "failed",
        "language": "und",
    },
]


def _join_native_description(
    *,
    labels: list[dict[str, Any]] = DESCRIPTION_NATIVE_LABELS,
    texts: list[dict[str, Any]] = DESCRIPTION_NATIVE_TEXTS,
    polygons: list[dict[str, Any]] = DESCRIPTION_NATIVE_POLYGONS,
) -> JoinedPartition:
    return join_description_partition(
        label_rows=labels, text_unit_rows=texts, polygon_rows=polygons
    )


def _join_native_website() -> JoinedPartition:
    return join_website_partition(
        label_rows=WEBSITE_NATIVE_LABELS, polygon_text_rows=WEBSITE_NATIVE_POLYGONS
    )


def _join_native_wiki() -> JoinedPartition:
    return join_wiki_partition(
        label_rows=WIKI_NATIVE_LABELS,
        sentence_rows=WIKI_NATIVE_SENTENCES,
        document_rows=WIKI_NATIVE_DOCUMENTS,
        link_rows=WIKI_NATIVE_LINKS,
        polygon_rows=WIKI_NATIVE_POLYGONS,
    )


def _units_by_key(partition: JoinedPartition) -> dict[Any, Mapping[str, Any]]:
    return {row["text_unit_key"]: row for row in partition.text_units}


def _geometry_by_polygon_id(partition: JoinedPartition) -> dict[Any, Any]:
    return {row["polygon_id"]: row["geometry"] for row in partition.polygon_records}


def _geometry_per_unit(
    partition: JoinedPartition,
    keys: Iterable[Any],
) -> dict[Any, list[Any]]:
    geometry_by_id = _geometry_by_polygon_id(partition)
    units = _units_by_key(partition)
    resolved: dict[Any, list[Any]] = {}
    for key in keys:
        resolved[key] = [geometry_by_id[pid] for pid in units[key]["polygon_ids"]]
    return resolved


def _polygon_id_by_entity(partition: JoinedPartition) -> dict[tuple[Any, ...], Any]:
    return {
        (row["source_pbf"], row["osm_type"], row["osm_id"]): row["polygon_id"]
        for row in partition.polygon_records
    }


def test_native_description_labels_join_the_exact_sentence_per_osm_entity() -> None:
    partition = _join_native_description()
    units = _units_by_key(partition)

    assert [row["text_unit_key"] for row in partition.labels] == [
        ("description-way-1", "name", 0),
        ("description-way-1", "name", 1),
        ("description-way-2", "name:fa", 0),
        ("description-relation-1", "name", 0),
    ]
    assert [units[row["text_unit_key"]]["text"] for row in partition.labels] == [
        "Kabul bazaar.",
        "Open daily.",
        "Bazaar in Kabul.",
        "Afghanistan.",
    ]


def test_native_description_text_units_resolve_to_their_own_geometry() -> None:
    expected = {
        ("description-way-1", "name", 0): [GEOMETRY_WAY_1],
        ("description-way-1", "name", 1): [GEOMETRY_WAY_1],
        ("description-way-2", "name:fa", 0): [GEOMETRY_WAY_2],
        ("description-relation-1", "name", 0): [GEOMETRY_RELATION_1],
    }

    assert _geometry_per_unit(_join_native_description(), expected) == expected


def test_native_description_polygon_records_keep_exact_geometry_bytes() -> None:
    records = _join_native_description().polygon_records

    stored = {
        (row["source_pbf"], row["osm_type"], row["osm_id"]): row["geometry"]
        for row in records
    }
    assert stored == {
        (PBF, "way", 1): GEOMETRY_WAY_1,
        (PBF, "way", 2): GEOMETRY_WAY_2,
        (PBF, "relation", 1): GEOMETRY_RELATION_1,
    }
    polygon_ids = [row["polygon_id"] for row in records]
    assert all(isinstance(pid, str) and pid for pid in polygon_ids)
    assert all("geometry_wkb" not in row for row in records)


def test_native_description_text_units_keep_raw_language_and_osm_provenance() -> None:
    unit = _units_by_key(_join_native_description())[("description-way-1", "name", 0)]

    assert unit["language_code"] == "en-GB"
    assert unit["original_text"] == "Kabul bazaar. Open daily."
    assert unit["split_status"] == "split"
    assert (unit["source_pbf"], unit["osm_type"], unit["osm_id"]) == (PBF, "way", 1)
    osm_url = f"https://www.openstreetmap.org/{unit['osm_type']}/{unit['osm_id']}"
    assert osm_url == "https://www.openstreetmap.org/way/1"


def test_native_description_polygon_ids_are_deterministic() -> None:
    first = _polygon_id_by_entity(_join_native_description())
    again = _polygon_id_by_entity(_join_native_description())
    reordered = _polygon_id_by_entity(
        _join_native_description(polygons=list(reversed(DESCRIPTION_NATIVE_POLYGONS)))
    )

    assert first == again == reordered
    assert len(set(first.values())) == len(DESCRIPTION_NATIVE_POLYGONS)


def test_native_description_polygon_ids_do_not_collide_on_separators() -> None:
    partition = _join_native_description(
        labels=SEPARATOR_LABELS,
        texts=SEPARATOR_TEXTS,
        polygons=SEPARATOR_POLYGONS,
    )
    expected = {
        ("description-a", "name", 0): [b"geometry-a"],
        ("description-b", "name", 0): [b"geometry-b"],
    }

    assert len(partition.polygon_records) == 2
    assert _geometry_per_unit(partition, expected) == expected


def test_native_description_same_osm_entity_stays_separate_per_snapshot() -> None:
    partition = _join_native_description(
        labels=SNAPSHOT_LABELS,
        texts=SNAPSHOT_TEXTS,
        polygons=SNAPSHOT_POLYGONS,
    )
    expected = {
        ("description-2025", "name", 0): [b"geometry-2025"],
        ("description-2026", "name", 0): [b"geometry-2026"],
    }

    assert len(partition.polygon_records) == 2
    assert _geometry_per_unit(partition, expected) == expected


def test_native_website_polygon_records_keep_geometry_and_field_provenance() -> None:
    partition = _join_native_website()
    records = {row["polygon_id"]: row for row in partition.polygon_records}
    record = records["website-way-1"]

    assert record["geometry"] == CLINIC_GEOJSON
    assert record["source_pbf"] == PBF
    assert (record["osm_type"], record["osm_id"]) == ("way", 1)
    assert (record["lat"], record["lon"]) == (34.5553, 69.2075)
    assert record["website_language"] == "en"
    assert record["contact_website_language"] == "fr"
    assert records["website-way-2"]["geometry"] == BAZAAR_GEOJSON
    assert all("geometry_wkb" not in row for row in partition.polygon_records)


def test_native_website_selects_field_specific_sentences_and_languages() -> None:
    units = _units_by_key(_join_native_website())
    website = units[("website-way-1", "website", 1)]
    contact = units[("website-way-2", "contact_website", 1)]

    assert website["text"] == "Call first."
    assert website["language"] == "en"
    assert website["polygon_ids"] == ["website-way-1"]
    assert contact["text"] == "Bazaar line two."
    assert contact["language"] == "ps"
    assert units[("website-way-1", "contact_website", 0)]["language"] == "fr"


def test_native_website_unsupported_or_skipped_index_zero_is_full_text() -> None:
    units = _units_by_key(_join_native_website())

    contact = units[("website-way-1", "contact_website", 0)]
    skipped = units[("website-way-2", "website", 0)]
    assert contact["text"] == "Contact by phone.\nOr by email."
    assert skipped["text"] == "Full bazaar site."


def test_native_website_keeps_full_field_text_separate_and_exact() -> None:
    units = _units_by_key(_join_native_website())
    selected = units[("website-way-1", "website", 1)]
    contact = units[("website-way-1", "contact_website", 0)]
    bazaar = units[("website-way-2", "contact_website", 1)]

    assert selected["text"] == "Call first."
    assert selected["full_text"] == "  Clinic hours. Call first.  "
    assert contact["full_text"] == "Contact by phone.\nOr by email."
    assert bazaar["full_text"] == "Bazaar line one. Bazaar line two."
    assert bazaar["text"] == "Bazaar line two."


def test_native_wiki_sentences_join_native_text_language_and_url() -> None:
    units = _units_by_key(_join_native_wiki())

    padded = units["sentence-kabul-2"]
    assert padded["text"] == "  Padded sentence.  "
    assert padded["language"] == "en"
    assert padded["license"] == "CC BY-SA 4.0"
    assert padded["attribution"] == "Wikipedia contributors"
    assert padded["source_url"] == "https://en.wikipedia.org/wiki/Kabul"
    gamma = units["sentence-gamma-1"]
    assert gamma["language"] == "und"
    assert gamma["source_url"] == "https://en.wikivoyage.org/wiki/Gamma"


def test_native_wiki_documents_keep_license_attribution_and_typed_ids() -> None:
    documents = {row["document_id"]: row for row in _join_native_wiki().documents}
    kabul = documents["document-kabul"]

    assert kabul["url"] == "https://en.wikipedia.org/wiki/Kabul"
    assert kabul["license"] == "CC BY-SA 4.0"
    assert kabul["attribution"] == "Wikipedia contributors"
    assert type(kabul["page_id"]) is int
    assert kabul["page_id"] == 42
    assert type(kabul["revision_id"]) is int
    assert kabul["revision_id"] == 1234567
    assert documents["document-gamma"]["license"] == "CC BY-SA 3.0"


def test_native_wiki_polygon_geometry_strings_are_stored_unchanged() -> None:
    partition = _join_native_wiki()
    records = {row["polygon_id"]: row for row in partition.polygon_records}

    assert records["polygon-kabul-1"]["geometry"] == WIKI_GEOJSON_1
    assert records["polygon-kabul-2"]["geometry"] == WIKI_GEOJSON_2
    assert all("geometry_wkb" not in row for row in partition.polygon_records)


def test_native_wiki_polygon_document_links_keep_every_source_occurrence() -> None:
    links = _join_native_wiki().polygon_document_links

    kept = sorted(tuple(row[name] for name in LINK_FIELDS) for row in links)
    sourced = sorted(
        tuple(row[name] for name in LINK_FIELDS) for row in WIKI_NATIVE_LINKS
    )
    assert kept == sourced
    pairs = [(row["document_id"], row["polygon_id"]) for row in links]
    assert pairs.count(("document-kabul", "polygon-kabul-1")) == 2
    assert ("document-gamma", "polygon-kabul-1") in pairs


def test_native_wiki_text_units_expose_polygon_ids_from_every_link() -> None:
    units = _units_by_key(_join_native_wiki())

    assert units["sentence-kabul-1"]["polygon_ids"] == [
        "polygon-kabul-1",
        "polygon-kabul-2",
        "polygon-kabul-1",
    ]
    assert units["sentence-gamma-1"]["polygon_ids"] == ["polygon-kabul-1"]


@given("native description, website, and wiki rows with their pinned column names")
def pinned_native_rows() -> None:
    # The pinned column names are carried by the module fixtures above.
    return None


@when("each native source is joined to its labels", target_fixture="native_partitions")
def join_native_sources() -> dict[str, JoinedPartition]:
    return {
        "description": _join_native_description(),
        "website": _join_native_website(),
        "wiki": _join_native_wiki(),
    }


@then("description geometry bytes stay exact in the geometry column")
def description_geometry_stays_exact(
    native_partitions: dict[str, JoinedPartition],
) -> None:
    partition = native_partitions["description"]
    stored = {row["geometry"] for row in partition.polygon_records}
    assert stored == {GEOMETRY_WAY_1, GEOMETRY_WAY_2, GEOMETRY_RELATION_1}


@then("website and wiki geometry strings stay exact in the geometry column")
def website_and_wiki_geometry_stays_exact(
    native_partitions: dict[str, JoinedPartition],
) -> None:
    website = native_partitions["website"].polygon_records
    wiki = native_partitions["wiki"].polygon_records
    assert {row["geometry"] for row in website} == {CLINIC_GEOJSON, BAZAAR_GEOJSON}
    assert {row["geometry"] for row in wiki} == {WIKI_GEOJSON_1, WIKI_GEOJSON_2}


@then("every wiki link occurrence is kept in the polygon document links table")
def wiki_links_kept_per_occurrence(
    native_partitions: dict[str, JoinedPartition],
) -> None:
    links = native_partitions["wiki"].polygon_document_links
    assert len(links) == len(WIKI_NATIVE_LINKS)
