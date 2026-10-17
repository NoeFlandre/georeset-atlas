import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from functools import cache
from importlib import resources
from typing import Any, Literal, TypeAlias

LanguageStatus: TypeAlias = Literal[
    "resolved",
    "unresolved",
    "missing",
    "special",
]
LanguageMappingKey: TypeAlias = tuple[str, str, str]
SPECIAL_LANGUAGE_CODES = frozenset({"und", "mul", "zxx", "mis"})
CROSSWALK_ASSET_NAME = "language-crosswalk-1.0.0.json"
CROSSWALK_MAPPING_VERSION = "atlas-language-crosswalk-1.0.0"
LINGUA_ISO639_3 = "lingua_iso639_3"
GLOTLID_ISO639_3_SCRIPT = "glotlid_iso639_3_script"
WIKIMEDIA_PROJECT_CODE = "wikimedia_project_code"
CROSSWALK_SCHEMES = frozenset(
    {LINGUA_ISO639_3, GLOTLID_ISO639_3_SCRIPT, WIKIMEDIA_PROJECT_CODE}
)
WIKIMEDIA_UNMAPPED_CODES = frozenset({"nrm", "eml", "cbk-zam"})
ISO6391_CODE_PATTERN = re.compile(r"[a-z]{2}")
ISO6393_CODE_PATTERN = re.compile(r"[a-z]{3}")
GLOTLID_CODE_PATTERN = re.compile(r"([a-z]{3})(?:[-_]([A-Za-z]{4}))?")


@dataclass(frozen=True, slots=True)
class LanguageResolution:
    raw_code: str | None
    scheme: str | None
    mapping_version: str
    script: str | None
    public_code: str | None
    status: LanguageStatus


def _script_from_code(raw_code: str) -> str | None:
    for subtag in re.split(r"[-_]", raw_code)[1:]:
        if len(subtag) == 1:
            return None
        if len(subtag) == 4 and subtag.isascii() and subtag.isalpha():
            return subtag
    return None


def resolve_language(
    raw_code: str | None,
    *,
    scheme: str | None,
    mapping_version: str,
    entries: Mapping[LanguageMappingKey, str],
    extract_script: bool = True,
) -> LanguageResolution:
    script: str | None = None
    if extract_script and raw_code is not None and scheme is not None:
        script = _script_from_code(raw_code)
    if raw_code is None:
        return LanguageResolution(
            raw_code=None,
            scheme=scheme,
            mapping_version=mapping_version,
            script=None,
            public_code=None,
            status="missing",
        )
    if raw_code in SPECIAL_LANGUAGE_CODES:
        return LanguageResolution(
            raw_code=raw_code,
            scheme=scheme,
            mapping_version=mapping_version,
            script=script,
            public_code=None,
            status="special",
        )
    if scheme is None:
        return LanguageResolution(
            raw_code=raw_code,
            scheme=None,
            mapping_version=mapping_version,
            script=None,
            public_code=None,
            status="unresolved",
        )

    mapping_key = (scheme, raw_code, mapping_version)
    if mapping_key not in entries:
        return LanguageResolution(
            raw_code=raw_code,
            scheme=scheme,
            mapping_version=mapping_version,
            script=script,
            public_code=None,
            status="unresolved",
        )

    return LanguageResolution(
        raw_code=raw_code,
        scheme=scheme,
        mapping_version=mapping_version,
        script=script,
        public_code=entries[mapping_key],
        status="resolved",
    )


class LanguageCrosswalkError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class LanguageCrosswalk:
    mapping_version: str
    coverage: str
    sources: tuple[str, ...]
    entries: Mapping[LanguageMappingKey, str]
    iso6391_codes: frozenset[str]
    iso6393_codes: frozenset[str]
    iso6393_to_1: Mapping[str, str]


def resolve_crosswalk_language(
    raw_code: str | None,
    *,
    scheme: str | None,
    crosswalk: LanguageCrosswalk,
) -> LanguageResolution:
    explicit = resolve_language(
        raw_code,
        scheme=scheme,
        mapping_version=crosswalk.mapping_version,
        entries=crosswalk.entries,
        extract_script=(
            scheme == GLOTLID_ISO639_3_SCRIPT
            and raw_code is not None
            and GLOTLID_CODE_PATTERN.fullmatch(raw_code) is not None
        ),
    )
    if explicit.status != "unresolved" or raw_code is None or scheme is None:
        return explicit
    public_code = _fallback_public_code(raw_code, scheme=scheme, crosswalk=crosswalk)
    if public_code is None:
        return explicit
    return replace(explicit, public_code=public_code, status="resolved")


def _fallback_public_code(
    raw_code: str,
    *,
    scheme: str,
    crosswalk: LanguageCrosswalk,
) -> str | None:
    if scheme == LINGUA_ISO639_3:
        return _iso6393_public_code(raw_code, crosswalk)
    if scheme == GLOTLID_ISO639_3_SCRIPT:
        match = GLOTLID_CODE_PATTERN.fullmatch(raw_code)
        if match is None:
            return None
        return _iso6393_public_code(match.group(1), crosswalk)
    if scheme == WIKIMEDIA_PROJECT_CODE:
        return _wikimedia_public_code(raw_code, crosswalk)
    return None


def _iso6393_public_code(iso6393: str, crosswalk: LanguageCrosswalk) -> str | None:
    if iso6393 not in crosswalk.iso6393_codes:
        return None
    return crosswalk.iso6393_to_1.get(iso6393, iso6393)


def _wikimedia_public_code(raw_code: str, crosswalk: LanguageCrosswalk) -> str | None:
    if raw_code in WIKIMEDIA_UNMAPPED_CODES:
        return None
    if raw_code in crosswalk.iso6391_codes:
        return raw_code
    if raw_code in crosswalk.iso6393_codes:
        return crosswalk.iso6393_to_1.get(raw_code, raw_code)
    return None


@cache
def load_language_crosswalk() -> LanguageCrosswalk:
    asset = resources.files("atlas_domain").joinpath(CROSSWALK_ASSET_NAME)
    return parse_language_crosswalk(asset.read_text(encoding="utf-8"))


@cache
def resolve_packaged_language(
    raw_code: str | None,
    *,
    scheme: str | None,
) -> LanguageResolution:
    return resolve_crosswalk_language(
        raw_code,
        scheme=scheme,
        crosswalk=load_language_crosswalk(),
    )


def parse_language_crosswalk(text: str) -> LanguageCrosswalk:
    try:
        payload = json.loads(text, object_pairs_hook=_object_without_duplicate_keys)
    except json.JSONDecodeError as error:
        raise LanguageCrosswalkError(f"invalid JSON: {error}") from error
    root = _object(
        payload,
        "asset",
        frozenset(
            {"manifest", "entries", "iso6391_codes", "iso6393_codes", "iso6393_to_1"}
        ),
    )
    manifest = _object(
        root["manifest"],
        "manifest",
        frozenset(
            {
                "mapping_version",
                "coverage",
                "sources",
                "iso6391_code_count",
                "iso6393_code_count",
                "iso6393_to_1_count",
            }
        ),
    )
    if manifest["mapping_version"] != CROSSWALK_MAPPING_VERSION:
        raise LanguageCrosswalkError(
            f"unsupported mapping_version: {manifest['mapping_version']!r}"
        )
    sources = _list(manifest["sources"], "manifest.sources")
    if not sources:
        raise LanguageCrosswalkError("manifest.sources must not be empty")
    iso6391_codes = _code_table(
        root["iso6391_codes"], "iso6391_codes", ISO6391_CODE_PATTERN
    )
    iso6393_codes = _code_table(
        root["iso6393_codes"], "iso6393_codes", ISO6393_CODE_PATTERN
    )
    iso6393_to_1 = _iso6393_to_1(root["iso6393_to_1"], iso6391_codes, iso6393_codes)
    _count(manifest, "iso6391_code_count", len(iso6391_codes))
    _count(manifest, "iso6393_code_count", len(iso6393_codes))
    _count(manifest, "iso6393_to_1_count", len(iso6393_to_1))
    known_codes = iso6391_codes | iso6393_codes
    entries: dict[LanguageMappingKey, str] = {}
    for entry_payload in _list(root["entries"], "entries"):
        entry = _object(
            entry_payload,
            "entry",
            frozenset({"scheme", "raw_code", "public_code"}),
        )
        scheme = _string(entry["scheme"], "entry.scheme")
        if scheme not in CROSSWALK_SCHEMES:
            raise LanguageCrosswalkError(f"unsupported scheme: {scheme!r}")
        raw_code = _string(entry["raw_code"], "entry.raw_code")
        public_code = _member(entry["public_code"], "entry.public_code", known_codes)
        if raw_code in SPECIAL_LANGUAGE_CODES:
            raise LanguageCrosswalkError(f"special code is not mappable: {raw_code!r}")
        if scheme == WIKIMEDIA_PROJECT_CODE and raw_code in WIKIMEDIA_UNMAPPED_CODES:
            raise LanguageCrosswalkError(
                f"wikimedia code must stay unresolved: {raw_code!r}"
            )
        key = (scheme, raw_code, CROSSWALK_MAPPING_VERSION)
        if key in entries:
            raise LanguageCrosswalkError(f"duplicate mapping key: {key!r}")
        entries[key] = public_code
    return LanguageCrosswalk(
        mapping_version=CROSSWALK_MAPPING_VERSION,
        coverage=_string(manifest["coverage"], "manifest.coverage"),
        sources=tuple(_string(source, "manifest.sources") for source in sources),
        entries=entries,
        iso6391_codes=iso6391_codes,
        iso6393_codes=iso6393_codes,
        iso6393_to_1=iso6393_to_1,
    )


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise LanguageCrosswalkError(f"duplicate key: {key!r}")
        result[key] = value
    return result


def _object(value: Any, name: str, fields: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise LanguageCrosswalkError(
            f"{name} must be an object with fields {sorted(fields)}"
        )
    return value


def _list(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise LanguageCrosswalkError(f"{name} must be a list")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise LanguageCrosswalkError(
            f"{name} must be a non-empty string without surrounding whitespace"
        )
    return value


def _member(value: Any, name: str, codes: frozenset[str]) -> str:
    if not isinstance(value, str) or value not in codes:
        raise LanguageCrosswalkError(f"{name} must be a known code: {value!r}")
    return value


def _code_table(value: Any, name: str, pattern: re.Pattern[str]) -> frozenset[str]:
    codes = _list(value, name)
    if not all(
        isinstance(code, str)
        and pattern.fullmatch(code) is not None
        and code not in SPECIAL_LANGUAGE_CODES
        for code in codes
    ):
        raise LanguageCrosswalkError(
            f"{name} must contain only valid, non-special codes"
        )
    table = frozenset(codes)
    if len(table) != len(codes):
        raise LanguageCrosswalkError(f"{name} must not contain duplicates")
    return table


def _iso6393_to_1(
    value: Any,
    iso6391_codes: frozenset[str],
    iso6393_codes: frozenset[str],
) -> dict[str, str]:
    if not isinstance(value, dict):
        raise LanguageCrosswalkError("iso6393_to_1 must be an object")
    mapping: dict[str, str] = {}
    for iso6393, iso6391 in value.items():
        key = _member(iso6393, "iso6393_to_1 key", iso6393_codes)
        mapping[key] = _member(iso6391, "iso6393_to_1 value", iso6391_codes)
    if len(set(mapping.values())) != len(mapping):
        raise LanguageCrosswalkError("iso6393_to_1 must not map two codes to one")
    return mapping


def _count(manifest: dict[str, Any], field: str, expected: int) -> None:
    value = manifest[field]
    if type(value) is not int or value != expected:
        raise LanguageCrosswalkError(f"manifest.{field} must equal {expected}")
