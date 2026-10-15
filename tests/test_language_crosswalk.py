import json
from collections.abc import Callable
from typing import Any

import pytest

from atlas_domain.language import (
    LanguageCrosswalkError,
    load_language_crosswalk,
    parse_language_crosswalk,
    resolve_crosswalk_language,
    resolve_language,
)

LINGUA = "lingua_iso639_3"
GLOTLID = "glotlid_iso639_3_script"
WIKIMEDIA = "wikimedia_project_code"
MAPPING_VERSION = "atlas-language-crosswalk-1.0.0"
SOURCE_REFERENCES = (
    "IANA Language Subtag Registry File-Date 2026-09-17, 731819 bytes, "
    "SHA-256 755fad43283be7b41ebe3c89ad054b6eaf928f404f9c0edb74799e0eab74beb1",
    "RFC 5646",
    "LOC ISO-639-2 UTF-8 list last-modified 2025-09-29, 16010 bytes, "
    "SHA-256 42b71885e4dc885559fda5ad059fd81838cf4782cedb5fd08cc3431f7d067371",
    "Meta-Wiki Special language codes revision 29853230 (2025-12-28T01:36:49Z), "
    "SHA-1 8073d6c8557f9ddaab5e17340adcc999e3a4fb42",
)


def _asset_payload() -> dict[str, Any]:
    return {
        "manifest": {
            "mapping_version": MAPPING_VERSION,
            "coverage": "explicit known mappings only",
            "sources": list(SOURCE_REFERENCES),
        },
        "entries": [{"scheme": LINGUA, "raw_code": "eng", "public_code": "en"}],
    }


def test_packaged_manifest_records_verbatim_source_references() -> None:
    crosswalk = load_language_crosswalk()

    assert crosswalk.mapping_version == MAPPING_VERSION
    assert crosswalk.sources == SOURCE_REFERENCES


def test_packaged_crosswalk_contains_only_the_explicit_known_mappings() -> None:
    crosswalk = load_language_crosswalk()

    assert crosswalk.entries == {
        (LINGUA, "eng", MAPPING_VERSION): "en",
        (LINGUA, "cmn", MAPPING_VERSION): "cmn",
        (LINGUA, "pbt", MAPPING_VERSION): "pbt",
        (LINGUA, "nrm", MAPPING_VERSION): "nrm",
        (GLOTLID, "eng_Latn", MAPPING_VERSION): "en",
        (WIKIMEDIA, "als", MAPPING_VERSION): "gsw",
        (WIKIMEDIA, "zh-min-nan", MAPPING_VERSION): "nan",
        (WIKIMEDIA, "zh-yue", MAPPING_VERSION): "yue",
        (WIKIMEDIA, "zh-classical", MAPPING_VERSION): "lzh",
        (WIKIMEDIA, "simple", MAPPING_VERSION): "en",
    }


@pytest.mark.parametrize(
    ("scheme", "raw_code", "public_code", "script"),
    [
        (LINGUA, "eng", "en", None),
        (GLOTLID, "eng_Latn", "en", "Latn"),
        (LINGUA, "cmn", "cmn", None),
        (LINGUA, "pbt", "pbt", None),
        (LINGUA, "nrm", "nrm", None),
        (WIKIMEDIA, "als", "gsw", None),
        (WIKIMEDIA, "zh-min-nan", "nan", None),
        (WIKIMEDIA, "zh-yue", "yue", None),
        (WIKIMEDIA, "zh-classical", "lzh", None),
        (WIKIMEDIA, "simple", "en", None),
    ],
)
def test_explicit_mappings_resolve_under_their_exact_scheme(
    scheme: str,
    raw_code: str,
    public_code: str,
    script: str | None,
) -> None:
    crosswalk = load_language_crosswalk()

    result = resolve_crosswalk_language(raw_code, scheme=scheme, crosswalk=crosswalk)

    assert result.raw_code == raw_code
    assert result.scheme == scheme
    assert result.mapping_version == MAPPING_VERSION
    assert result.script == script
    assert result.public_code == public_code
    assert result.status == "resolved"


@pytest.mark.parametrize(
    ("scheme", "raw_code"),
    [
        (WIKIMEDIA, "nrm"),
        (WIKIMEDIA, "eng"),
        (WIKIMEDIA, "eml"),
        (WIKIMEDIA, "cbk-zam"),
        (WIKIMEDIA, "sr-Latn"),
        (LINGUA, "eng_Latn"),
        (LINGUA, "zh-yue"),
        (LINGUA, "ENG"),
        (LINGUA, "eng "),
        (GLOTLID, "eng"),
    ],
)
def test_exact_scheme_misses_and_unmapped_codes_stay_unresolved_without_script(
    scheme: str,
    raw_code: str,
) -> None:
    crosswalk = load_language_crosswalk()

    result = resolve_crosswalk_language(raw_code, scheme=scheme, crosswalk=crosswalk)

    assert result.raw_code == raw_code
    assert result.scheme == scheme
    assert result.mapping_version == MAPPING_VERSION
    assert result.script is None
    assert result.public_code is None
    assert result.status == "unresolved"


def test_packaged_lookup_requires_the_exact_mapping_version() -> None:
    crosswalk = load_language_crosswalk()

    result = resolve_language(
        "eng",
        scheme=LINGUA,
        mapping_version="atlas-language-crosswalk-0.9.0",
        entries=crosswalk.entries,
    )

    assert result.mapping_version == "atlas-language-crosswalk-0.9.0"
    assert result.public_code is None
    assert result.status == "unresolved"


@pytest.mark.parametrize(
    ("raw_code", "status"),
    [
        (None, "missing"),
        ("und", "special"),
        ("mul", "special"),
        ("zxx", "special"),
        ("mis", "special"),
    ],
)
def test_packaged_missing_and_special_codes_keep_their_status(
    raw_code: str | None,
    status: str,
) -> None:
    crosswalk = load_language_crosswalk()

    result = resolve_crosswalk_language(raw_code, scheme=LINGUA, crosswalk=crosswalk)

    assert result.raw_code == raw_code
    assert result.public_code is None
    assert result.status == status


def test_strict_loader_accepts_a_well_formed_payload() -> None:
    crosswalk = parse_language_crosswalk(json.dumps(_asset_payload()))

    assert crosswalk.entries == {(LINGUA, "eng", MAPPING_VERSION): "en"}


@pytest.mark.parametrize(
    "scheme",
    [
        "iso_639_3",
        "lingua",
        "glotlid_iso639_3",
        "wikimedia",
    ],
)
def test_strict_loader_rejects_schemes_outside_the_exact_source_names(
    scheme: str,
) -> None:
    payload = _asset_payload()
    payload["entries"][0]["scheme"] = scheme

    with pytest.raises(LanguageCrosswalkError):
        parse_language_crosswalk(json.dumps(payload))


@pytest.mark.parametrize(
    "text",
    [
        "{",
        "[]",
        '{"manifest": {}, "manifest": {}, "entries": []}',
    ],
)
def test_strict_loader_rejects_invalid_json_non_objects_and_duplicate_keys(
    text: str,
) -> None:
    with pytest.raises(LanguageCrosswalkError):
        parse_language_crosswalk(text)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(extra=1),
        lambda payload: payload["manifest"].pop("coverage"),
        lambda payload: payload["manifest"].update(extra=1),
        lambda payload: payload["manifest"].update(
            mapping_version="atlas-language-crosswalk-0.9.0"
        ),
        lambda payload: payload["manifest"].update(sources=[]),
        lambda payload: payload.update(entries={}),
        lambda payload: payload["entries"][0].update(public_code=None),
        lambda payload: payload["entries"][0].update(note="extra"),
        lambda payload: payload["entries"][0].update(raw_code="und"),
        lambda payload: payload["entries"][0].update(raw_code=" eng"),
        lambda payload: payload["entries"][0].pop("scheme"),
        lambda payload: payload["entries"].append(dict(payload["entries"][0])),
    ],
)
def test_strict_loader_rejects_malformed_payloads(
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    payload = _asset_payload()
    mutate(payload)

    with pytest.raises(LanguageCrosswalkError):
        parse_language_crosswalk(json.dumps(payload))
