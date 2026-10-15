import pytest

from atlas_domain.joined_outputs import (
    JoinedPartition,
    join_description_partition,
    join_website_partition,
    join_wiki_partition,
)

PBF = "afghanistan-latest.osm.pbf"
POINT_GEOJSON = '{"type":"Point","coordinates":[69.2075,34.5553]}'
MAPPING_VERSION = "atlas-language-crosswalk-1.0.0"
LINGUA = "lingua_iso639_3"
GLOTLID = "glotlid_iso639_3_script"
WIKIMEDIA = "wikimedia_project_code"


def _description_partition(*, raw_code: str, reason: str) -> JoinedPartition:
    return join_description_partition(
        label_rows=[
            {
                "label_id": "label-1",
                "description_identity": "description-1",
                "tag_key": "name",
                "sentence_index": 0,
                "decision": "skipped_unsplit",
                "language": "en",
            }
        ],
        text_unit_rows=[
            {
                "description_identity": "description-1",
                "source_pbf": PBF,
                "osm_type": "way",
                "osm_id": 1,
                "tag_key": "name",
                "original_text": "Kabul bazaar.",
                "language_code": raw_code,
                "reason": reason,
                "split_status": "unsupported",
                "sentences": None,
            }
        ],
        polygon_rows=[
            {"source_pbf": PBF, "osm_type": "way", "osm_id": 1, "geometry": b"way-1"}
        ],
    )


def _website_partition(
    *,
    language: str | None,
    sentence_status: str,
    sentences: list[str] | None,
) -> JoinedPartition:
    return join_website_partition(
        label_rows=[
            {
                "label_id": "label-1",
                "polygon_id": "website-way-1",
                "field": "website",
                "sentence_index": 0,
                "decision": "skipped_unsplit",
                "language": "en",
            }
        ],
        polygon_text_rows=[
            {
                "polygon_id": "website-way-1",
                "source_pbf": PBF,
                "osm_type": "way",
                "osm_id": 1,
                "website": "Clinic hours.",
                "website_sentences": sentences,
                "website_language": language,
                "website_sentence_status": sentence_status,
                "contact_website": "Contact by phone.",
                "contact_website_sentences": None,
                "contact_website_language": "fr",
                "contact_website_sentence_status": "unsupported",
                "geometry": POINT_GEOJSON,
                "lat": 34.5553,
                "lon": 69.2075,
            }
        ],
    )


def _wiki_partition(*, language: str) -> JoinedPartition:
    return join_wiki_partition(
        label_rows=[
            {
                "label_id": "label-1",
                "sentence_id": "sentence-1",
                "decision": "yes",
                "language": "en",
            }
        ],
        sentence_rows=[
            {
                "sentence_id": "sentence-1",
                "document_id": "document-1",
                "project": "wikipedia",
                "language": language,
                "text": "Kabul is the capital.",
                "license": "CC BY-SA 4.0",
                "attribution": "Wikipedia contributors",
            }
        ],
        document_rows=[
            {
                "document_id": "document-1",
                "project": "wikipedia",
                "title": "Kabul",
                "url": "https://en.wikipedia.org/wiki/Kabul",
                "page_id": 42,
                "revision_id": 1234567,
                "license": "CC BY-SA 4.0",
                "attribution": "Wikipedia contributors",
            }
        ],
        link_rows=[
            {
                "document_id": "document-1",
                "project": "wikipedia",
                "polygon_id": "polygon-1",
                "input_revision": "rev-1",
            }
        ],
        polygon_rows=[{"polygon_id": "polygon-1", "geometry": POINT_GEOJSON}],
    )


@pytest.mark.parametrize(
    ("raw_code", "reason", "language_code", "scheme", "script", "status"),
    [
        ("eng", "detected", "en", LINGUA, None, "resolved"),
        ("eng_Latn", "fallback_glotlid_v3", "en", GLOTLID, "Latn", "resolved"),
        ("eng", "too_short", None, None, None, "unresolved"),
    ],
)
def test_description_language_resolves_under_the_scheme_named_by_reason(
    raw_code: str,
    reason: str,
    language_code: str | None,
    scheme: str | None,
    script: str | None,
    status: str,
) -> None:
    (unit,) = _description_partition(raw_code=raw_code, reason=reason).text_units

    assert unit["raw_language_code"] == raw_code
    assert unit["language_code"] == language_code
    assert unit["language_scheme"] == scheme
    assert unit["language_script"] == script
    assert unit["language_mapping_version"] == MAPPING_VERSION
    assert unit["language_mapping_status"] == status


def test_native_website_glotlid_language_resolves_with_script() -> None:
    (unit,) = _website_partition(
        language="eng_Latn",
        sentence_status="skipped",
        sentences=[],
    ).text_units

    assert unit["raw_language_code"] == "eng_Latn"
    assert unit["language_code"] == "en"
    assert unit["language_scheme"] == GLOTLID
    assert unit["language_script"] == "Latn"
    assert unit["language_mapping_version"] == MAPPING_VERSION
    assert unit["language_mapping_status"] == "resolved"


def test_native_website_null_language_is_missing_without_raising() -> None:
    (unit,) = _website_partition(
        language=None,
        sentence_status="not_detected",
        sentences=None,
    ).text_units

    assert unit["raw_language_code"] is None
    assert unit["language_code"] is None
    assert unit["language_scheme"] == GLOTLID
    assert unit["language_script"] is None
    assert unit["language_mapping_version"] == MAPPING_VERSION
    assert unit["language_mapping_status"] == "missing"


@pytest.mark.parametrize(
    ("language", "language_code", "status"),
    [
        ("en", "en", "resolved"),
        ("simple", "en", "resolved"),
        ("eml", None, "unresolved"),
    ],
)
def test_wiki_project_codes_resolve_under_wikimedia_scheme(
    language: str,
    language_code: str | None,
    status: str,
) -> None:
    (unit,) = _wiki_partition(language=language).text_units

    assert unit["raw_language_code"] == language
    assert unit["language_code"] == language_code
    assert unit["language_scheme"] == WIKIMEDIA
    assert unit["language_script"] is None
    assert unit["language_mapping_version"] == MAPPING_VERSION
    assert unit["language_mapping_status"] == status
