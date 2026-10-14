import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, TypeAlias

LanguageStatus: TypeAlias = Literal[
    "resolved",
    "unresolved",
    "missing",
    "special",
]
LanguageMappingKey: TypeAlias = tuple[str, str, str]
SPECIAL_LANGUAGE_CODES = frozenset({"und", "mul", "zxx", "mis"})


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
) -> LanguageResolution:
    script = _script_from_code(raw_code) if raw_code is not None else None
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
