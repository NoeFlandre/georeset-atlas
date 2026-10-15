import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class LanguageResolution:
    raw_code: str | None
    scheme: str
    mapping_version: str
    script: str | None
    public_code: str | None
    status: LanguageStatus


def _script_from_code(raw_code: str) -> str | None:
    for part in re.split(r"[-_]", raw_code)[1:]:
        if len(part) == 1:
            return None
        if len(part) == 4 and part.isascii() and part.isalpha():
            return part
    return None


def resolve_language(
    raw_code: str | None,
    *,
    scheme: str,
    mapping_version: str,
    entries: Mapping[LanguageMappingKey, str],
    extract_script: bool = True,
) -> LanguageResolution:
    script: str | None = None
    if extract_script and raw_code is not None:
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


def resolve_crosswalk_language(
    raw_code: str | None,
    *,
    scheme: str,
    crosswalk: LanguageCrosswalk,
) -> LanguageResolution:
    return resolve_language(
        raw_code,
        scheme=scheme,
        mapping_version=crosswalk.mapping_version,
        entries=crosswalk.entries,
        extract_script=scheme == GLOTLID_ISO639_3_SCRIPT,
    )


def load_language_crosswalk() -> LanguageCrosswalk:
    asset = resources.files("atlas_domain").joinpath(CROSSWALK_ASSET_NAME)
    return parse_language_crosswalk(asset.read_text(encoding="utf-8"))


def parse_language_crosswalk(text: str) -> LanguageCrosswalk:
    try:
        payload = json.loads(text, object_pairs_hook=_object_without_duplicate_keys)
    except json.JSONDecodeError as error:
        raise LanguageCrosswalkError(f"invalid JSON: {error}") from error
    root = _object(payload, "asset", frozenset({"manifest", "entries"}))
    manifest = _object(
        root["manifest"],
        "manifest",
        frozenset({"mapping_version", "coverage", "sources"}),
    )
    if manifest["mapping_version"] != CROSSWALK_MAPPING_VERSION:
        raise LanguageCrosswalkError(
            f"unsupported mapping_version: {manifest['mapping_version']!r}"
        )
    sources = _list(manifest["sources"], "manifest.sources")
    if not sources:
        raise LanguageCrosswalkError("manifest.sources must not be empty")
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
        public_code = _string(entry["public_code"], "entry.public_code")
        if raw_code in SPECIAL_LANGUAGE_CODES:
            raise LanguageCrosswalkError(f"special code is not mappable: {raw_code!r}")
        key = (scheme, raw_code, CROSSWALK_MAPPING_VERSION)
        if key in entries:
            raise LanguageCrosswalkError(f"duplicate mapping key: {key!r}")
        entries[key] = public_code
    return LanguageCrosswalk(
        mapping_version=CROSSWALK_MAPPING_VERSION,
        coverage=_string(manifest["coverage"], "manifest.coverage"),
        sources=tuple(_string(source, "manifest.sources") for source in sources),
        entries=entries,
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
