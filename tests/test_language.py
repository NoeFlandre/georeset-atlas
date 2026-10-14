import pytest

from atlas_domain.language import resolve_language

ISO_STYLE = "iso-style"
WIKIMEDIA = "wikimedia"
MAPPING_VERSION = "fixture-v1"
MAPPING_ENTRIES = {
    (ISO_STYLE, "eng", MAPPING_VERSION): "en",
    (ISO_STYLE, "eng_Latn", MAPPING_VERSION): "en",
    (ISO_STYLE, "als", MAPPING_VERSION): "als",
    (WIKIMEDIA, "en", MAPPING_VERSION): "en",
    (WIKIMEDIA, "als", MAPPING_VERSION): "gsw",
    (WIKIMEDIA, "sr-Latn", MAPPING_VERSION): "sr-el",
}


@pytest.mark.parametrize(
    ("scheme", "raw_code", "public_code", "script"),
    [
        (ISO_STYLE, "eng", "en", None),
        (ISO_STYLE, "eng_Latn", "en", "Latn"),
        (WIKIMEDIA, "en", "en", None),
        (WIKIMEDIA, "als", "gsw", None),
        (WIKIMEDIA, "sr-Latn", "sr-el", "Latn"),
        (ISO_STYLE, "als", "als", None),
    ],
)
def test_explicit_mapping_preserves_language_provenance(
    scheme: str,
    raw_code: str,
    public_code: str,
    script: str | None,
) -> None:
    result = resolve_language(
        raw_code,
        scheme=scheme,
        mapping_version=MAPPING_VERSION,
        entries=MAPPING_ENTRIES,
    )

    assert result.raw_code == raw_code
    assert result.scheme == scheme
    assert result.mapping_version == MAPPING_VERSION
    assert result.script == script
    assert result.public_code == public_code
    assert result.status == "resolved"


def test_unknown_and_wrong_version_codes_remain_unresolved() -> None:
    for raw_code, version in [
        ("nrm", MAPPING_VERSION),
        ("eng", "fixture-v2"),
    ]:
        result = resolve_language(
            raw_code,
            scheme=ISO_STYLE,
            mapping_version=version,
            entries=MAPPING_ENTRIES,
        )

        assert result.raw_code == raw_code
        assert result.scheme == ISO_STYLE
        assert result.mapping_version == version
        assert result.script is None
        assert result.public_code is None
        assert result.status == "unresolved"


def test_missing_code_differs_from_special_und() -> None:
    missing = resolve_language(
        None,
        scheme=ISO_STYLE,
        mapping_version=MAPPING_VERSION,
        entries=MAPPING_ENTRIES,
    )
    undetermined = resolve_language(
        "und",
        scheme=ISO_STYLE,
        mapping_version=MAPPING_VERSION,
        entries={
            **MAPPING_ENTRIES,
            (ISO_STYLE, "und", MAPPING_VERSION): "en",
        },
    )

    assert missing.raw_code is None
    assert missing.scheme == ISO_STYLE
    assert missing.mapping_version == MAPPING_VERSION
    assert missing.script is None
    assert missing.public_code is None
    assert missing.status == "missing"

    assert undetermined.raw_code == "und"
    assert undetermined.scheme == ISO_STYLE
    assert undetermined.mapping_version == MAPPING_VERSION
    assert undetermined.script is None
    assert undetermined.public_code is None
    assert undetermined.status == "special"


@pytest.mark.parametrize("raw_code", ["und", "mul", "zxx", "mis"])
def test_special_language_codes_keep_provenance_and_ignore_ordinary_mappings(
    raw_code: str,
) -> None:
    result = resolve_language(
        raw_code,
        scheme=ISO_STYLE,
        mapping_version=MAPPING_VERSION,
        entries={(ISO_STYLE, raw_code, MAPPING_VERSION): "en"},
    )

    assert result.raw_code == raw_code
    assert result.scheme == ISO_STYLE
    assert result.mapping_version == MAPPING_VERSION
    assert result.script is None
    assert result.public_code is None
    assert result.status == "special"


@pytest.mark.parametrize("raw_code", ["und-Latn", "mul-Latn", "zxx-Latn", "mis-Latn"])
def test_script_qualified_special_codes_remain_special(raw_code: str) -> None:
    result = resolve_language(
        raw_code,
        scheme=ISO_STYLE,
        mapping_version=MAPPING_VERSION,
        entries={(ISO_STYLE, raw_code, MAPPING_VERSION): "en"},
    )

    assert result.raw_code == raw_code
    assert result.scheme == ISO_STYLE
    assert result.mapping_version == MAPPING_VERSION
    assert result.script == "Latn"
    assert result.public_code is None
    assert result.status == "special"


def test_hyphen_script_subtag_does_not_alias_underscore_mapping_keys() -> None:
    result = resolve_language(
        "sr-Latn",
        scheme=WIKIMEDIA,
        mapping_version=MAPPING_VERSION,
        entries={(WIKIMEDIA, "sr_Latn", MAPPING_VERSION): "sr-el"},
    )

    assert result.raw_code == "sr-Latn"
    assert result.scheme == WIKIMEDIA
    assert result.mapping_version == MAPPING_VERSION
    assert result.script == "Latn"
    assert result.public_code is None
    assert result.status == "unresolved"


def test_script_suffix_is_not_stripped_before_mapping_lookup() -> None:
    assert (ISO_STYLE, "als", MAPPING_VERSION) in MAPPING_ENTRIES

    result = resolve_language(
        "als_Latn",
        scheme=ISO_STYLE,
        mapping_version=MAPPING_VERSION,
        entries=MAPPING_ENTRIES,
    )

    assert result.raw_code == "als_Latn"
    assert result.scheme == ISO_STYLE
    assert result.mapping_version == MAPPING_VERSION
    assert result.script == "Latn"
    assert result.public_code is None
    assert result.status == "unresolved"


@pytest.mark.parametrize(
    ("raw_code", "script"),
    [
        ("en-x-priv", None),
        ("en-u-attr", None),
        ("en-Latn-x-priv", "Latn"),
        ("en-Latn-u-attr", "Latn"),
    ],
)
def test_extension_and_private_use_subtags_are_not_script(
    raw_code: str,
    script: str | None,
) -> None:
    result = resolve_language(
        raw_code,
        scheme=WIKIMEDIA,
        mapping_version=MAPPING_VERSION,
        entries=MAPPING_ENTRIES,
    )

    assert result.raw_code == raw_code
    assert result.script == script


def test_empty_code_is_unresolved_and_differs_from_missing() -> None:
    result = resolve_language(
        "",
        scheme=ISO_STYLE,
        mapping_version=MAPPING_VERSION,
        entries=MAPPING_ENTRIES,
    )

    assert result.raw_code == ""
    assert result.scheme == ISO_STYLE
    assert result.mapping_version == MAPPING_VERSION
    assert result.script is None
    assert result.public_code is None
    assert result.status == "unresolved"
