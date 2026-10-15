from collections.abc import Mapping, MutableMapping
from dataclasses import FrozenInstanceError
from types import MappingProxyType
from typing import cast

import pytest
from pytest_bdd import given, scenarios, then, when

from atlas_domain.source_catalog import (
    SOURCE_CATALOG,
    SourceDataset,
    SourceStatus,
    companion_input_paths,
    validate_catalog,
)

scenarios("../features/source_catalog_totals.feature")


@given("the frozen Atlas source catalog", target_fixture="source_catalog_context")
def source_catalog_context() -> tuple[SourceDataset, ...]:
    return SOURCE_CATALOG


@when(
    "I total each source's recorded label statuses",
    target_fixture="recorded_totals",
)
def recorded_totals(
    source_catalog_context: tuple[SourceDataset, ...],
) -> dict[str, int]:
    validate_catalog(source_catalog_context)
    return {
        dataset.source_id: dataset.expected_label_total
        for dataset in source_catalog_context
    }


@then("the catalog declares the documented totals for each immutable input pin")
def source_catalog_exposes_documented_totals(recorded_totals: dict[str, int]) -> None:
    # These figures are captured from the pinned Hub metadata; no source rows
    # are read in this contract test, so it does not claim a full reconciliation.
    assert recorded_totals == {
        "description": 1_057_002,
        "website": 45_860_920,
        "wiki": 74_791_929,
    }
    assert len({source.source_id for source in SOURCE_CATALOG}) == len(SOURCE_CATALOG)
    assert all(len(source.revision) == 40 for source in SOURCE_CATALOG)


def _source(
    *,
    source_id: str = "description",
    repo_id: str = "owner/repo",
    revision: str = "ce98b328f3c6543800a3d931877b51eb3f0cf768",
    table_roots: dict[str, tuple[str, ...]] | None = None,
    status_counts: Mapping[SourceStatus, int] | None = None,
) -> SourceDataset:
    return SourceDataset(
        source_id=source_id,
        repo_id=repo_id,
        revision=revision,
        table_roots=table_roots or {"labels": ("labels/data",), "texts": ("texts",)},
        expected_status_counts=status_counts
        or {"yes": 1, "no": 1, "failed": 1, "skipped": 1},
    )


def test_catalog_has_the_three_frozen_input_revisions() -> None:
    assert {
        source.source_id: (source.repo_id, source.revision) for source in SOURCE_CATALOG
    } == {
        "description": (
            "NoeFlandre/osm-polygon-description-tag-landuse",
            "ce98b328f3c6543800a3d931877b51eb3f0cf768",
        ),
        "website": (
            "NoeFlandre/osm-polygon-website-tag-landuse",
            "e6524c711ee0033c3cd5990e5fdbbe4b89992dfb",
        ),
        "wiki": (
            "NoeFlandre/osm-polygon-wikidata-and-wikipedia-landuse",
            "49307aeb05b6dcc8a5d393f77f59a1e6d13a967a",
        ),
    }


def test_companion_paths_follow_the_label_project_and_basename() -> None:
    wiki = next(source for source in SOURCE_CATALOG if source.source_id == "wiki")
    paths = companion_input_paths(
        wiki,
        "labels/wikivoyage/sentences",
        "labels/wikivoyage/sentences/afghanistan-latest.parquet",
    )

    assert paths == {
        "polygons": "polygons/afghanistan-latest.parquet",
        "text_units": "wikivoyage/sentences/afghanistan-latest.parquet",
        "documents": "wikivoyage/documents/afghanistan-latest.parquet",
        "polygon_document_links": "polygon_document_links/afghanistan-latest.parquet",
    }


def test_catalog_records_native_table_roots() -> None:
    sources = {source.source_id: source for source in SOURCE_CATALOG}

    assert sources["description"].table_roots == {
        "polygons": ("data",),
        "text_units": ("language-v1/data",),
        "labels": ("labels/language-v1/data",),
    }
    assert sources["website"].table_roots == {
        "polygons_and_text": ("polygons",),
        "labels": ("labels/polygons",),
    }
    assert sources["wiki"].table_roots == {
        "polygons": ("polygons",),
        "text_units": ("wikipedia/sentences", "wikivoyage/sentences"),
        "documents": ("wikipedia/documents", "wikivoyage/documents"),
        "polygon_document_links": ("polygon_document_links",),
        "labels": (
            "labels/wikipedia/sentences",
            "labels/wikivoyage/sentences",
        ),
    }


def test_catalog_records_upstream_status_counts_and_totals() -> None:
    expected = {
        "description": {
            "yes": 725_371,
            "no": 241_475,
            "failed": 11_927,
            "skipped": 78_229,
        },
        "website": {
            "yes": 8_869_643,
            "no": 36_523_593,
            "failed": 430_211,
            "skipped": 37_473,
        },
        "wiki": {
            "yes": 25_868_341,
            "no": 46_068_954,
            "failed": 1_385_457,
            "skipped": 1_469_177,
        },
    }
    sources = {source.source_id: source for source in SOURCE_CATALOG}

    assert {
        key: dict(source.expected_status_counts) for key, source in sources.items()
    } == expected
    assert {key: source.expected_label_total for key, source in sources.items()} == {
        "description": 1_057_002,
        "website": 45_860_920,
        "wiki": 74_791_929,
    }


def test_source_dataset_freezes_nested_mappings_and_defensively_copies_inputs() -> None:
    input_roots = {"labels": ["labels/data"]}
    input_counts: dict[SourceStatus, int] = {
        "yes": 1,
        "no": 2,
        "failed": 3,
        "skipped": 4,
    }
    source = SourceDataset(
        source_id="fixture",
        repo_id="owner/repo",
        revision="ce98b328f3c6543800a3d931877b51eb3f0cf768",
        table_roots=input_roots,
        expected_status_counts=input_counts,
    )

    input_roots["labels"].append("other")
    input_counts["yes"] = 9

    assert source.table_roots["labels"] == ("labels/data",)
    assert source.expected_status_counts["yes"] == 1
    assert isinstance(source.table_roots, MappingProxyType)
    assert isinstance(source.expected_status_counts, MappingProxyType)
    with pytest.raises(TypeError):
        cast(MutableMapping[str, tuple[str, ...]], source.table_roots)["new"] = ("new",)
    with pytest.raises(TypeError):
        cast(MutableMapping[str, int], source.expected_status_counts)["yes"] = 9
    with pytest.raises(FrozenInstanceError):
        setattr(source, "revision", "e6524c711ee0033c3cd5990e5fdbbe4b89992dfb")


@pytest.mark.parametrize(
    "root",
    ["", "/absolute", "../escape", "labels/../escape", "labels//data", r"labels\\data"],
)
def test_source_dataset_rejects_unsafe_table_roots(root: str) -> None:
    with pytest.raises(ValueError):
        _source(table_roots={"labels": (root,)})


@pytest.mark.parametrize("count", [0, -1, True, "1"])
def test_source_dataset_rejects_non_positive_or_non_integer_counts(
    count: object,
) -> None:
    with pytest.raises(ValueError):
        _source(
            status_counts=cast(
                dict[SourceStatus, int],
                {"yes": count, "no": 1, "failed": 1, "skipped": 1},
            )
        )


def test_source_dataset_requires_the_four_known_statuses() -> None:
    with pytest.raises(ValueError, match="status"):
        _source(
            status_counts=cast(
                Mapping[SourceStatus, int],
                {"yes": 1, "no": 1, "unknown": 1, "skipped": 1},
            )
        )


@pytest.mark.parametrize(
    ("repo_id", "revision"),
    [
        ("owner", "ce98b328f3c6543800a3d931877b51eb3f0cf768"),
        ("owner/repo", "main"),
        ("owner/../repo", "ce98b328f3c6543800a3d931877b51eb3f0cf768"),
        (r"owner\repo", "ce98b328f3c6543800a3d931877b51eb3f0cf768"),
    ],
)
def test_source_dataset_rejects_invalid_repo_or_mutable_revision(
    repo_id: str,
    revision: str,
) -> None:
    with pytest.raises(ValueError):
        _source(repo_id=repo_id, revision=revision)


def test_catalog_rejects_duplicate_source_ids_or_repositories() -> None:
    one = _source(source_id="description", repo_id="owner/one")
    duplicate_id = _source(source_id="description", repo_id="owner/two")
    duplicate_repo = _source(source_id="website", repo_id="owner/one")

    with pytest.raises(ValueError, match="unique"):
        validate_catalog((one, duplicate_id))
    with pytest.raises(ValueError, match="unique"):
        validate_catalog((one, duplicate_repo))
