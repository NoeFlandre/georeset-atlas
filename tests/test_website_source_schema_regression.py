from typing import Any

from atlas_domain.joined_outputs import JoinedPartition, join_website_partition

GEOJSON = '{"type":"Point","coordinates":[69.2075,34.5553]}'
WEBSITE_TEXT = "  Clinic hours. Call first.\n"
CONTACT_WEBSITE_TEXT = "Contact by phone.\n\tOr by email.  "
POLYGON_ROWS: list[dict[str, Any]] = [
    {
        "polygon_id": "website-way-1",
        "website_text": WEBSITE_TEXT,
        "website_sentences": ["Clinic hours.", "Call first."],
        "website_language": "en",
        "website_sentence_status": "split",
        "contact_website_text": CONTACT_WEBSITE_TEXT,
        "contact_website_sentences": None,
        "contact_website_language": "fr",
        "contact_website_sentence_status": "unsupported_language",
        "geometry": GEOJSON,
    },
]
LABEL_ROWS: list[dict[str, Any]] = [
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
        "decision": "skipped_unsplit",
        "language": "fr",
    },
]


def _join() -> JoinedPartition:
    return join_website_partition(label_rows=LABEL_ROWS, polygon_text_rows=POLYGON_ROWS)


def test_native_website_labels_select_exact_sentence_and_keep_full_text() -> None:
    units = {row["text_unit_key"]: row for row in _join().text_units}
    split = units[("website-way-1", "website", 1)]
    contact = units[("website-way-1", "contact_website", 0)]

    assert split["text"] == "Call first."
    assert split["full_text"] == WEBSITE_TEXT
    assert split["language"] == "en"
    assert split["sentence_status"] == "split"
    assert split["polygon_ids"] == ["website-way-1"]
    assert contact["text"] == CONTACT_WEBSITE_TEXT
    assert contact["full_text"] == CONTACT_WEBSITE_TEXT
    assert contact["language"] == "fr"
    assert contact["sentence_status"] == "unsupported_language"
    assert contact["polygon_ids"] == ["website-way-1"]


def test_native_website_polygon_keeps_geometry_without_generic_decision() -> None:
    [record] = _join().polygon_records

    assert record["polygon_id"] == "website-way-1"
    assert record["geometry"] == GEOJSON
    assert "decision" not in record
