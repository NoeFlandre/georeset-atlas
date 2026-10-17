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
    "SHA-256 755fad43283be7b41ebe3c89ad054b6eaf928f404f9c0edb74799e0eab74beb1; "
    "https://www.iana.org/assignments/language-subtag-registry/"
    "language-subtag-registry",
    "RFC 5646; https://www.rfc-editor.org/rfc/rfc5646",
    "LOC ISO-639-2 UTF-8 list last-modified 2025-09-29, 16010 bytes, "
    "SHA-256 42b71885e4dc885559fda5ad059fd81838cf4782cedb5fd08cc3431f7d067371; "
    "https://www.loc.gov/standards/iso639-2/ISO-639-2_utf-8.txt",
    "Meta-Wiki Special language codes revision 29853230 (2025-12-28T01:36:49Z), "
    "SHA-1 8073d6c8557f9ddaab5e17340adcc999e3a4fb42",
)


def _asset_payload() -> dict[str, Any]:
    return {
        "manifest": {
            "mapping_version": MAPPING_VERSION,
            "coverage": "explicit known mappings only",
            "sources": list(SOURCE_REFERENCES),
            "iso6391_code_count": 1,
            "iso6393_code_count": 1,
            "iso6393_to_1_count": 1,
        },
        "entries": [{"scheme": LINGUA, "raw_code": "eng", "public_code": "en"}],
        "iso6391_codes": ["en"],
        "iso6393_codes": ["eng"],
        "iso6393_to_1": {"eng": "en"},
    }


def _payload_with_tables(
    iso6391_codes: list[Any],
    iso6393_codes: list[Any],
    iso6393_to_1: dict[Any, Any],
) -> dict[str, Any]:
    payload = _asset_payload()
    payload["iso6391_codes"] = iso6391_codes
    payload["iso6393_codes"] = iso6393_codes
    payload["iso6393_to_1"] = iso6393_to_1
    payload["manifest"]["iso6391_code_count"] = len(iso6391_codes)
    payload["manifest"]["iso6393_code_count"] = len(iso6393_codes)
    payload["manifest"]["iso6393_to_1_count"] = len(iso6393_to_1)
    return payload


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
        (WIKIMEDIA, "en", MAPPING_VERSION): "en",
        (WIKIMEDIA, "als", MAPPING_VERSION): "gsw",
        (WIKIMEDIA, "zh-min-nan", MAPPING_VERSION): "nan",
        (WIKIMEDIA, "zh-yue", MAPPING_VERSION): "yue",
        (WIKIMEDIA, "zh-classical", MAPPING_VERSION): "lzh",
        (WIKIMEDIA, "simple", MAPPING_VERSION): "en",
    }


def test_packaged_code_tables_match_the_manifest_counts() -> None:
    crosswalk = load_language_crosswalk()

    assert len(crosswalk.iso6391_codes) == 184
    assert len(crosswalk.iso6393_codes) == 7922
    assert len(crosswalk.iso6393_to_1) == 183


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
    ("scheme", "raw_code", "public_code", "script"),
    [
        (LINGUA, "fra", "fr", None),
        (LINGUA, "akk", "akk", None),
        (LINGUA, "gsw", "gsw", None),
        (GLOTLID, "fra_Latn", "fr", "Latn"),
        (GLOTLID, "fra-Latn", "fr", "Latn"),
        (GLOTLID, "akk_Xsux", "akk", "Xsux"),
        (GLOTLID, "eng", "en", None),
        (WIKIMEDIA, "fr", "fr", None),
        (WIKIMEDIA, "fra", "fr", None),
        (WIKIMEDIA, "eng", "en", None),
        (WIKIMEDIA, "cmn", "cmn", None),
        (WIKIMEDIA, "gsw", "gsw", None),
    ],
)
def test_iso_fallback_lookups_resolve_under_their_exact_scheme(
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
        (WIKIMEDIA, "eml"),
        (WIKIMEDIA, "cbk-zam"),
        (WIKIMEDIA, "sr-Latn"),
        (LINGUA, "eng_Latn"),
        (LINGUA, "zh-yue"),
        (LINGUA, "ENG"),
        (LINGUA, "eng "),
        (LINGUA, "afa"),
        (LINGUA, "en"),
        (GLOTLID, "ENG_Latn"),
        (GLOTLID, "eng_Lat"),
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


@pytest.mark.parametrize(
    ("scheme", "raw_code"),
    [
        ("iso_639_3", "fra"),
        ("lingua", "eng"),
        ("glotlid_iso639_3", "fra_Latn"),
        ("wikimedia", "fr"),
    ],
)
def test_unknown_schemes_never_fall_back_to_the_iso_tables(
    scheme: str,
    raw_code: str,
) -> None:
    crosswalk = load_language_crosswalk()

    result = resolve_crosswalk_language(raw_code, scheme=scheme, crosswalk=crosswalk)

    assert result.raw_code == raw_code
    assert result.scheme == scheme
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
    ("raw_code", "scheme", "status"),
    [
        (None, LINGUA, "missing"),
        (None, None, "missing"),
        ("und", LINGUA, "special"),
        ("mul", LINGUA, "special"),
        ("zxx", GLOTLID, "special"),
        ("mis", WIKIMEDIA, "special"),
        ("und", None, "special"),
        ("mul", None, "special"),
        ("zxx", None, "special"),
        ("mis", None, "special"),
        ("eng", None, "unresolved"),
    ],
)
def test_missing_special_and_schemeless_codes_keep_their_status(
    raw_code: str | None,
    scheme: str | None,
    status: str,
) -> None:
    crosswalk = load_language_crosswalk()

    result = resolve_crosswalk_language(raw_code, scheme=scheme, crosswalk=crosswalk)

    assert result.raw_code == raw_code
    assert result.scheme == scheme
    assert result.mapping_version == MAPPING_VERSION
    assert result.script is None
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
        lambda payload: payload.pop("iso6391_codes"),
        lambda payload: payload.pop("iso6393_to_1"),
        lambda payload: payload["manifest"].pop("coverage"),
        lambda payload: payload["manifest"].pop("iso6393_to_1_count"),
        lambda payload: payload["manifest"].update(extra=1),
        lambda payload: payload["manifest"].update(
            mapping_version="atlas-language-crosswalk-0.9.0"
        ),
        lambda payload: payload["manifest"].update(sources=[]),
        lambda payload: payload.update(entries={}),
        lambda payload: payload["entries"][0].update(public_code=None),
        lambda payload: payload["entries"][0].update(public_code="zzz"),
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


@pytest.mark.parametrize(
    ("iso6391_codes", "iso6393_codes", "iso6393_to_1"),
    [
        (["en", "EN"], ["eng"], {"eng": "en"}),
        (["en", "en"], ["eng"], {"eng": "en"}),
        (["en", 1], ["eng"], {"eng": "en"}),
        (["en"], ["eng", "Eng"], {"eng": "en"}),
        (["en"], ["eng", "en"], {"eng": "en"}),
        (["en"], ["eng", "eng"], {"eng": "en"}),
        (["en"], ["eng", "und"], {"eng": "en"}),
        (["en"], ["eng"], {"ENG": "en"}),
        (["en"], ["eng"], {"eng": "EN"}),
        (["en"], ["eng"], {"eng": ["en"]}),
        (["en"], ["eng"], {"eng": "fr"}),
        (["en"], ["eng"], {"deu": "en"}),
        (["en", "fr"], ["eng", "fra"], {"eng": "en", "fra": "en"}),
    ],
)
def test_strict_loader_rejects_invalid_code_tables_and_maps(
    iso6391_codes: list[Any],
    iso6393_codes: list[Any],
    iso6393_to_1: dict[Any, Any],
) -> None:
    payload = _payload_with_tables(iso6391_codes, iso6393_codes, iso6393_to_1)

    with pytest.raises(LanguageCrosswalkError):
        parse_language_crosswalk(json.dumps(payload))


@pytest.mark.parametrize("special_code", ["und", "mul", "zxx", "mis"])
def test_strict_loader_excludes_special_codes_from_the_code_tables(
    special_code: str,
) -> None:
    payload = _payload_with_tables(["en"], ["eng", special_code], {"eng": "en"})

    with pytest.raises(LanguageCrosswalkError):
        parse_language_crosswalk(json.dumps(payload))


@pytest.mark.parametrize(
    "field",
    [
        "iso6391_code_count",
        "iso6393_code_count",
        "iso6393_to_1_count",
    ],
)
@pytest.mark.parametrize("count", [2, True, "1"])
def test_strict_loader_rejects_manifest_counts_that_do_not_match_the_tables(
    field: str,
    count: Any,
) -> None:
    payload = _asset_payload()
    payload["manifest"][field] = count

    with pytest.raises(LanguageCrosswalkError):
        parse_language_crosswalk(json.dumps(payload))


@pytest.mark.parametrize("raw_code", ["nrm", "eml", "cbk-zam"])
def test_strict_loader_rejects_mappings_for_wikimedia_codes_that_stay_unresolved(
    raw_code: str,
) -> None:
    payload = _asset_payload()
    payload["entries"][0].update(scheme=WIKIMEDIA, raw_code=raw_code)

    with pytest.raises(LanguageCrosswalkError):
        parse_language_crosswalk(json.dumps(payload))
