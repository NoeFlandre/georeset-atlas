import json
from typing import Any
from urllib.parse import unquote, urlsplit

import pytest

from atlas_domain.source_catalog import SourceDataset
from atlas_io.hub_inventory import (
    build_assembly_plan,
    estimate_storage_budget,
    fetch_pinned_files,
)


class Response:
    status = 200

    def __init__(self, payload: object) -> None:
        self._payload = json.dumps(payload).encode()

    def read(self, _size: int = -1) -> bytes:
        return self._payload

    def close(self) -> None:
        return None


class MetadataHub:
    def __init__(self, roots: dict[str, list[dict[str, Any]]]) -> None:
        self.roots = roots
        self.requests: list[str] = []

    def __call__(self, request: Any, timeout: float) -> Response:
        self.requests.append(request.full_url)
        path = unquote(urlsplit(request.full_url).path)
        root = path.split("/tree/", 1)[1].split("/", 1)[1]
        return Response(self.roots[root])


def _source() -> SourceDataset:
    return SourceDataset(
        source_id="fixture",
        repo_id="owner/repo",
        revision="0" * 40,
        table_roots={
            "labels": ("labels/data",),
            "text_units": ("texts",),
            "polygons": ("polygons",),
        },
        expected_status_counts={"yes": 1, "no": 1, "failed": 1, "skipped": 1},
    )


def _entry(root: str, name: str, size: int) -> dict[str, object]:
    return {"type": "file", "path": f"{root}/{name}", "size": size}


def _hub() -> MetadataHub:
    return MetadataHub(
        {
            "labels/data": [
                _entry("labels/data", "a.parquet", 100),
                _entry("labels/data", "b.parquet", 200),
            ],
            "texts": [
                _entry("texts", "a.parquet", 50),
                _entry("texts", "b.parquet", 60),
            ],
            "polygons": [
                _entry("polygons", "a.parquet", 30),
                _entry("polygons", "b.parquet", 80),
            ],
        }
    )


def test_pinned_tree_inventory_and_plan_resolve_all_partition_companions() -> None:
    source = _source()
    hub = _hub()
    files = fetch_pinned_files((source,), opener=hub)
    plan = build_assembly_plan((source,), files)

    assert len(hub.requests) == 3
    assert len(plan.files) == 6
    assert plan.unique_source_bytes == 520
    assert plan.execution_transfer_bytes == 520
    assert plan.partition_count == 2
    assert plan.largest_partition.source_id == "fixture"
    assert plan.largest_partition.labels_path == "labels/data/b.parquet"
    assert plan.largest_partition.input_bytes == 340


def test_storage_budget_reserves_output_contingency_and_largest_scratch() -> None:
    plan = build_assembly_plan(
        (_source(),), fetch_pinned_files((_source(),), opener=_hub())
    )
    budget = estimate_storage_budget(plan)

    assert budget.output_allowance_bytes == 2_080
    assert budget.contingency_bytes == 520
    assert budget.workspace_allowance_bytes == 1_360
    assert budget.safety_margin_bytes == 16 * 1024 * 1024
    assert budget.required_free_bytes == 16_781_176


def test_inventory_rejects_missing_companion_files() -> None:
    source = _source()
    hub = _hub()
    hub.roots["texts"].pop(0)

    with pytest.raises(ValueError, match="missing companion"):
        build_assembly_plan((source,), fetch_pinned_files((source,), opener=hub))


def test_inventory_rejects_potentially_truncated_tree_page() -> None:
    source = _source()
    hub = _hub()
    hub.roots["labels/data"] = [
        _entry("labels/data", f"{index}.parquet", 1) for index in range(1000)
    ]

    with pytest.raises(ValueError, match="may be truncated"):
        fetch_pinned_files((source,), opener=hub)
