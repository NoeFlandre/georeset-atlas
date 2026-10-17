from hypothesis import given, settings
from hypothesis import strategies as st

from atlas_domain.language import resolve_language
from atlas_domain.occurrences import retain_occurrences

LANGUAGE_PART = st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1)
SPECIAL_LANGUAGE_CODES = ("mul", "zxx", "mis")
NON_ORDINARY_CODES = frozenset(("und", *SPECIAL_LANGUAGE_CODES))
ORDINARY_RAW_CODE = st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1).filter(
    lambda value: value not in NON_ORDINARY_CODES
)
SPECIAL_CODE = st.sampled_from(SPECIAL_LANGUAGE_CODES)
OCCURRENCE = st.fixed_dictionaries(
    {
        "key": st.tuples(st.text(), st.text(), st.integers()),
        "text": st.text(),
        "label": st.text(),
    }
)


@settings(database=None)
@given(
    raw_code=ORDINARY_RAW_CODE,
    scheme=LANGUAGE_PART,
    mapping_version=LANGUAGE_PART,
)
def test_language_lookup_never_guesses_from_scheme_or_version_near_misses(
    raw_code: str,
    scheme: str,
    mapping_version: str,
) -> None:
    entries = {
        (scheme, raw_code, f"{mapping_version}-other"): "fixture-a",
        (f"{scheme}-other", raw_code, mapping_version): "fixture-b",
    }

    result = resolve_language(
        raw_code,
        scheme=scheme,
        mapping_version=mapping_version,
        entries=entries,
    )

    assert result.raw_code == raw_code
    assert result.scheme == scheme
    assert result.mapping_version == mapping_version
    assert result.public_code is None
    assert result.status == "unresolved"


@settings(database=None)
@given(
    raw_code=SPECIAL_CODE,
    scheme=LANGUAGE_PART,
    mapping_version=LANGUAGE_PART,
)
def test_exact_special_language_codes_remain_special(
    raw_code: str,
    scheme: str,
    mapping_version: str,
) -> None:
    entries = {
        (scheme, raw_code, mapping_version): "fixture-exact",
        (scheme, raw_code, f"{mapping_version}-other"): "fixture-a",
        (f"{scheme}-other", raw_code, mapping_version): "fixture-b",
    }

    result = resolve_language(
        raw_code,
        scheme=scheme,
        mapping_version=mapping_version,
        entries=entries,
    )

    assert result.raw_code == raw_code
    assert result.scheme == scheme
    assert result.mapping_version == mapping_version
    assert result.public_code is None
    assert result.status == "special"


@settings(database=None)
@given(occurrences=st.lists(OCCURRENCE, max_size=30))
def test_retain_occurrences_preserves_arbitrary_sequence_order(
    occurrences: list[dict[str, object]],
) -> None:
    assert retain_occurrences(occurrences) == tuple(occurrences)


@settings(database=None)
@given(
    key=st.tuples(st.text(), st.text(), st.integers()),
    text=st.text(),
    label=st.text(),
    surrounding=st.lists(OCCURRENCE, max_size=10),
)
def test_retain_occurrences_keeps_duplicates_and_conflicting_labels(
    key: tuple[str, str, int],
    text: str,
    label: str,
    surrounding: list[dict[str, object]],
) -> None:
    occurrence = {"key": key, "text": text, "label": label}
    duplicate = occurrence.copy()
    conflict = {"key": key, "text": text, "label": f"{label} conflict"}
    sequence = [occurrence, *surrounding, duplicate, conflict]

    assert retain_occurrences(sequence) == tuple(sequence)
