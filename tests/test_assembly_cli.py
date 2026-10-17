import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import atlas_io.assembly_cli as assembly_cli
from atlas_domain.source_catalog import SourceDataset
from atlas_io.hub_inventory import HubParquetFile
from atlas_io.joined_partitions import joined_manifest_path


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


def _files() -> tuple[HubParquetFile, ...]:
    return (
        HubParquetFile("fixture", "labels", "labels/data/a.parquet", 100),
        HubParquetFile("fixture", "text_units", "texts/a.parquet", 50),
        HubParquetFile("fixture", "polygons", "polygons/a.parquet", 30),
    )


def _write_manifest(output_dir: Path, *, status_counts: dict[str, int]) -> None:
    path = joined_manifest_path(output_dir, "fixture", "labels/data/a.parquet")
    path.write_text(
        json.dumps(
            {
                "source_id": "fixture",
                "repo_id": "owner/repo",
                "revision": "0" * 40,
                "path": "labels/data/a.parquet",
                "label_row_count": sum(status_counts.values()),
                "status_counts": status_counts,
                "row_counts": {
                    "labels": sum(status_counts.values()),
                    "text_units": 1,
                    "polygon_records": 1,
                    "documents": 0,
                    "polygon_document_links": 0,
                },
                "outputs": {
                    table: {
                        "name": f"a.{table}.parquet",
                        "size_bytes": 10,
                        "row_count": rows,
                    }
                    for table, rows in {
                        "labels": sum(status_counts.values()),
                        "text_units": 1,
                        "polygon_records": 1,
                        "documents": 0,
                        "polygon_document_links": 0,
                    }.items()
                },
            }
        )
    )


def test_global_cli_checkpoints_each_partition_and_reconciles_expected_totals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )
    calls: list[str] = []

    def runner(
        argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        calls.append(argv[argv.index("--labels") + 1])
        output_dir = Path(argv[argv.index("--output-dir") + 1])
        path = joined_manifest_path(output_dir, "fixture", "labels/data/a.parquet")
        if not path.exists():
            _write_manifest(
                output_dir,
                status_counts={"yes": 1, "no": 1, "failed": 1, "skipped": 1},
            )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)
    output_dir = tmp_path / "assembly"

    assert assembly_cli.main(["--output-dir", str(output_dir)]) == 0
    assert calls == ["labels/data/a.parquet"]
    progress = json.loads((output_dir / "assembly-progress.json").read_text())
    assert progress["status"] == "complete"
    assert progress["partitions_completed"] == 1
    assert progress["sources"]["fixture"]["status_counts"] == {
        "yes": 1,
        "no": 1,
        "failed": 1,
        "skipped": 1,
    }
    assert (output_dir / "assembly-manifest.json").is_file()
    heartbeat = json.loads((output_dir / "assembly-heartbeat.json").read_text())
    assert heartbeat["status"] == "partition_completed"
    assert heartbeat["phase"] == "checkpoint"
    assert heartbeat["current_partition"] == {
        "source_id": "fixture",
        "labels_path": "labels/data/a.parquet",
    }
    assert heartbeat["plan_sha256"] == progress["plan_sha256"]


def test_global_cli_refreshes_partition_heartbeat_during_join(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )
    monkeypatch.setattr(assembly_cli, "_HEARTBEAT_INTERVAL_SECONDS", 0.01)

    def runner(
        argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        output_dir = Path(argv[argv.index("--output-dir") + 1])
        heartbeat_path = output_dir / "assembly-heartbeat.json"
        before = json.loads(heartbeat_path.read_text())["updated_at_utc"]
        assert progress_callback is not None
        progress_callback("table=labels row_group_start index=1/1")
        time.sleep(0.04)
        heartbeat = json.loads(heartbeat_path.read_text())
        after = heartbeat["updated_at_utc"]
        assert after != before
        assert heartbeat["stage"] == "table=labels row_group_start index=1/1"
        _write_manifest(
            output_dir,
            status_counts={"yes": 1, "no": 1, "failed": 1, "skipped": 1},
        )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)
    output_dir = tmp_path / "assembly"

    assert assembly_cli.main(["--output-dir", str(output_dir)]) == 0
    heartbeat = json.loads((output_dir / "assembly-heartbeat.json").read_text())
    assert heartbeat["stage"] == "table=labels row_group_start index=1/1"


def test_global_cli_persists_join_error_detail_in_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )

    def failing_runner(
        _argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        print("atlas-join: error: orphan temp output", file=sys.stderr)
        return assembly_cli.EXIT_CONFLICT

    monkeypatch.setattr(assembly_cli, "run_joined_partition", failing_runner)
    output_dir = tmp_path / "assembly"

    assert (
        assembly_cli.main(["--output-dir", str(output_dir)])
        == assembly_cli.EXIT_PARTITION
    )

    progress = json.loads((output_dir / "assembly-progress.json").read_text())
    assert progress["status"] == "interrupted_partition"
    assert progress["error"] == (
        "atlas-join exit code 3: atlas-join: error: orphan temp output"
    )
    assert "atlas-join: error: orphan temp output" in capsys.readouterr().err
    heartbeat = json.loads((output_dir / "assembly-heartbeat.json").read_text())
    assert heartbeat["status"] == "failed"
    assert heartbeat["phase"] == "join"
    assert heartbeat["error"] == progress["error"]


def test_global_cli_resumes_by_verifying_existing_partition_manifests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )
    calls = 0

    def runner(
        argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        nonlocal calls
        calls += 1
        output_dir = Path(argv[argv.index("--output-dir") + 1])
        _write_manifest(
            output_dir,
            status_counts={"yes": 1, "no": 1, "failed": 1, "skipped": 1},
        )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)
    output_dir = tmp_path / "assembly"

    assert assembly_cli.main(["--output-dir", str(output_dir)]) == 0
    first_plan = (output_dir / "assembly-plan.json").read_bytes()
    assert assembly_cli.main(["--output-dir", str(output_dir)]) == 0
    assert calls == 2
    assert (output_dir / "assembly-plan.json").read_bytes() == first_plan


def test_global_cli_refuses_to_start_when_storage_budget_is_not_met(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )
    monkeypatch.setattr(
        assembly_cli.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=1),
    )

    def reject_runner(
        _argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        raise AssertionError("partition reads must not start below the storage budget")

    monkeypatch.setattr(assembly_cli, "run_joined_partition", reject_runner)
    output_dir = tmp_path / "assembly"

    assert (
        assembly_cli.main(["--output-dir", str(output_dir)]) == assembly_cli.EXIT_BUDGET
    )
    assert not output_dir.exists()


def test_global_cli_preserves_nonempty_output_without_its_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )
    output_dir = tmp_path / "assembly"
    output_dir.mkdir()
    existing = output_dir / "unrelated.parquet"
    existing.write_bytes(b"preserve")

    assert (
        assembly_cli.main(["--output-dir", str(output_dir)])
        == assembly_cli.EXIT_CONFLICT
    )
    assert existing.read_bytes() == b"preserve"


def test_dry_run_reports_plan_and_never_creates_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )
    output_dir = tmp_path / "assembly"

    assert assembly_cli.main(["--output-dir", str(output_dir), "--dry-run"]) == 0

    summary = json.loads(capsys.readouterr().out)
    assert summary["partitions"] == 1
    assert summary["unique_source_bytes"] == 180
    assert summary["execution_transfer_bytes"] == 180
    assert summary["current_free_bytes"] >= summary["required_free_bytes_now"]
    assert not output_dir.exists()


def test_global_cli_does_not_write_complete_manifest_when_totals_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )

    def runner(
        argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        _write_manifest(
            Path(argv[argv.index("--output-dir") + 1]),
            status_counts={"yes": 1, "no": 1, "failed": 1, "skipped": 0},
        )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)
    output_dir = tmp_path / "assembly"

    assert (
        assembly_cli.main(["--output-dir", str(output_dir)])
        == assembly_cli.EXIT_RECONCILIATION
    )
    progress = json.loads((output_dir / "assembly-progress.json").read_text())
    assert progress["status"] == "reconciliation_failed"
    assert not (output_dir / "assembly-manifest.json").exists()
