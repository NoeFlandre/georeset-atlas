import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when

import atlas_io.assembly_cli as assembly_cli
from atlas_domain.source_catalog import SourceDataset
from atlas_io.hub_inventory import HubParquetFile
from atlas_io.joined_partitions import joined_manifest_path

scenarios("../features/assembly_resume.feature")


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


def _two_partition_files() -> tuple[HubParquetFile, ...]:
    return (
        HubParquetFile("fixture", "labels", "labels/data/a.parquet", 100),
        HubParquetFile("fixture", "labels", "labels/data/b.parquet", 100),
        HubParquetFile("fixture", "text_units", "texts/a.parquet", 50),
        HubParquetFile("fixture", "text_units", "texts/b.parquet", 50),
        HubParquetFile("fixture", "polygons", "polygons/a.parquet", 30),
        HubParquetFile("fixture", "polygons", "polygons/b.parquet", 30),
    )


def _partition_status_counts(labels_path: str) -> dict[str, int]:
    return (
        {"yes": 1, "no": 0, "failed": 1, "skipped": 0}
        if labels_path.endswith("a.parquet")
        else {"yes": 0, "no": 1, "failed": 0, "skipped": 1}
    )


def _write_partition_manifest(
    output_dir: Path,
    *,
    labels_path: str,
    status_counts: dict[str, int],
) -> None:
    path = joined_manifest_path(output_dir, "fixture", labels_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    stem = Path(labels_path).stem
    path.write_text(
        json.dumps(
            {
                "source_id": "fixture",
                "repo_id": "owner/repo",
                "revision": "0" * 40,
                "path": labels_path,
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
                        "name": f"{stem}.{table}.parquet",
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


def _write_manifest(output_dir: Path, *, status_counts: dict[str, int]) -> None:
    _write_partition_manifest(
        output_dir,
        labels_path="labels/data/a.parquet",
        status_counts=status_counts,
    )


@given(
    "a two-partition fixture with one row of each status",
    target_fixture="assembly_resume",
)
def assembly_resume_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli,
        "fetch_pinned_files",
        lambda *_args, **_kwargs: _two_partition_files(),
    )
    state: dict[str, Any] = {
        "calls": [],
        "row_builds": [],
        "output_dir": tmp_path / "assembly-resume-bdd",
    }

    def runner(
        argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        labels_path = argv[argv.index("--labels") + 1]
        output_dir = Path(argv[argv.index("--output-dir") + 1])
        state["calls"].append(labels_path)
        manifest_path = joined_manifest_path(output_dir, "fixture", labels_path)
        if not manifest_path.exists():
            state["row_builds"].append(labels_path)
            _write_partition_manifest(
                output_dir,
                labels_path=labels_path,
                status_counts=_partition_status_counts(labels_path),
            )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)
    return state


@when("the builder pauses after one new partition and then resumes")
def pause_then_resume_assembly(assembly_resume: dict[str, Any]) -> None:
    args = [
        "--output-dir",
        str(assembly_resume["output_dir"]),
        "--max-new-partitions",
        "1",
    ]
    assembly_resume["pause_exit"] = assembly_cli.main(args)
    assembly_resume["paused_progress"] = json.loads(
        (assembly_resume["output_dir"] / "assembly-progress.json").read_text()
    )
    assembly_resume["resume_exit"] = assembly_cli.main(args)
    assembly_resume["complete_progress"] = json.loads(
        (assembly_resume["output_dir"] / "assembly-progress.json").read_text()
    )


@then("the completed prefix is verified and totals are counted exactly once")
def completed_prefix_is_reused_once(assembly_resume: dict[str, Any]) -> None:
    assert assembly_resume["pause_exit"] == assembly_cli.EXIT_PAUSED
    assert assembly_resume["paused_progress"]["pending_partition"] == (
        "labels/data/b.parquet"
    )
    assert assembly_resume["resume_exit"] == assembly_cli.EXIT_OK
    assert assembly_resume["complete_progress"]["partitions_completed"] == 2
    assert assembly_resume["complete_progress"]["sources"]["fixture"]["label_rows"] == 4
    assert assembly_resume["complete_progress"]["sources"]["fixture"][
        "status_counts"
    ] == {
        "yes": 1,
        "no": 1,
        "failed": 1,
        "skipped": 1,
    }
    assert assembly_resume["row_builds"] == [
        "labels/data/a.parquet",
        "labels/data/b.parquet",
    ]


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


def test_cli_pauses_after_a_bounded_number_of_new_partitions_and_resumes_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli,
        "fetch_pinned_files",
        lambda *_args, **_kwargs: _two_partition_files(),
    )
    calls: list[str] = []
    row_builds: list[str] = []

    def runner(
        argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        labels_path = argv[argv.index("--labels") + 1]
        output_dir = Path(argv[argv.index("--output-dir") + 1])
        calls.append(labels_path)
        manifest_path = joined_manifest_path(output_dir, "fixture", labels_path)
        if not manifest_path.exists():
            row_builds.append(labels_path)
            _write_partition_manifest(
                output_dir,
                labels_path=labels_path,
                status_counts=_partition_status_counts(labels_path),
            )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)
    output_dir = tmp_path / "assembly"
    args = ["--output-dir", str(output_dir), "--max-new-partitions", "1"]

    assert assembly_cli.main(args) == assembly_cli.EXIT_PAUSED

    paused = json.loads((output_dir / "assembly-progress.json").read_text())
    assert paused["status"] == "paused"
    assert paused["partitions_completed"] == 1
    assert paused["pending_partition"] == "labels/data/b.parquet"
    heartbeat = json.loads((output_dir / "assembly-heartbeat.json").read_text())
    assert heartbeat["status"] == "paused"
    assert not (output_dir / "assembly-manifest.json").exists()
    assert calls == ["labels/data/a.parquet"]

    assert assembly_cli.main(args) == assembly_cli.EXIT_OK

    complete = json.loads((output_dir / "assembly-progress.json").read_text())
    assert complete["status"] == "complete"
    assert complete["partitions_completed"] == 2
    assert complete["sources"]["fixture"]["label_rows"] == 4
    assert complete["sources"]["fixture"]["status_counts"] == {
        "yes": 1,
        "no": 1,
        "failed": 1,
        "skipped": 1,
    }
    assert calls == [
        "labels/data/a.parquet",
        "labels/data/a.parquet",
        "labels/data/b.parquet",
    ]
    assert row_builds == ["labels/data/a.parquet", "labels/data/b.parquet"]


def test_graceful_interruption_checkpoints_and_retries_only_the_pending_partition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli,
        "fetch_pinned_files",
        lambda *_args, **_kwargs: _two_partition_files(),
    )
    calls: list[str] = []
    row_builds: list[str] = []
    interrupt = True

    def runner(
        argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        nonlocal interrupt
        labels_path = argv[argv.index("--labels") + 1]
        output_dir = Path(argv[argv.index("--output-dir") + 1])
        calls.append(labels_path)
        if labels_path.endswith("b.parquet") and interrupt:
            interrupt = False
            raise KeyboardInterrupt
        manifest_path = joined_manifest_path(output_dir, "fixture", labels_path)
        if not manifest_path.exists():
            row_builds.append(labels_path)
            _write_partition_manifest(
                output_dir,
                labels_path=labels_path,
                status_counts=_partition_status_counts(labels_path),
            )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)
    output_dir = tmp_path / "assembly"

    assert (
        assembly_cli.main(["--output-dir", str(output_dir)]) == assembly_cli.EXIT_PAUSED
    )

    paused = json.loads((output_dir / "assembly-progress.json").read_text())
    assert paused["status"] == "paused"
    assert paused["partitions_completed"] == 1
    assert paused["pending_partition"] == "labels/data/b.parquet"
    assert "graceful interrupt" in paused["error"]
    heartbeat = json.loads((output_dir / "assembly-heartbeat.json").read_text())
    assert heartbeat["status"] == "paused"
    assert not (output_dir / "assembly-manifest.json").exists()

    assert assembly_cli.main(["--output-dir", str(output_dir)]) == assembly_cli.EXIT_OK

    complete = json.loads((output_dir / "assembly-progress.json").read_text())
    assert complete["partitions_completed"] == 2
    assert complete["sources"]["fixture"]["label_rows"] == 4
    assert complete["sources"]["fixture"]["status_counts"] == {
        "yes": 1,
        "no": 1,
        "failed": 1,
        "skipped": 1,
    }
    assert row_builds == ["labels/data/a.parquet", "labels/data/b.parquet"]
    assert calls == [
        "labels/data/a.parquet",
        "labels/data/b.parquet",
        "labels/data/a.parquet",
        "labels/data/b.parquet",
    ]


def test_abrupt_child_exit_leaves_atomic_completed_prefix_that_resumes_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "assembly"
    repository = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    source_path = str(repository / "src")
    environment["PYTHONPATH"] = os.pathsep.join(
        part for part in (source_path, environment.get("PYTHONPATH", "")) if part
    )
    worker = repository / "tests" / "fixtures" / "assembly_crash_worker.py"

    crashed = subprocess.run(
        [sys.executable, str(worker), str(output_dir)],
        cwd=repository,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    first_manifest = joined_manifest_path(
        output_dir, "fixture", "labels/data/a.parquet"
    )
    second_manifest = joined_manifest_path(
        output_dir, "fixture", "labels/data/b.parquet"
    )
    assert crashed.returncode == 73
    assert first_manifest.is_file()
    first_manifest_bytes = first_manifest.read_bytes()
    assert not second_manifest.exists()
    interrupted = json.loads((output_dir / "assembly-progress.json").read_text())
    assert interrupted["status"] == "running"
    assert interrupted["partitions_completed"] == 1

    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli,
        "fetch_pinned_files",
        lambda *_args, **_kwargs: _two_partition_files(),
    )
    calls: list[str] = []
    row_builds: list[str] = []

    def runner(
        argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        labels_path = argv[argv.index("--labels") + 1]
        calls.append(labels_path)
        manifest_path = joined_manifest_path(output_dir, "fixture", labels_path)
        if not manifest_path.exists():
            row_builds.append(labels_path)
            _write_partition_manifest(
                output_dir,
                labels_path=labels_path,
                status_counts=_partition_status_counts(labels_path),
            )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)

    assert assembly_cli.main(["--output-dir", str(output_dir)]) == assembly_cli.EXIT_OK

    complete = json.loads((output_dir / "assembly-progress.json").read_text())
    assert complete["partitions_completed"] == 2
    assert complete["sources"]["fixture"]["label_rows"] == 4
    assert complete["sources"]["fixture"]["status_counts"] == {
        "yes": 1,
        "no": 1,
        "failed": 1,
        "skipped": 1,
    }
    assert first_manifest.read_bytes() == first_manifest_bytes
    assert calls == ["labels/data/a.parquet", "labels/data/b.parquet"]
    assert row_builds == ["labels/data/b.parquet"]


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


def _legacy_plan_document(
    source: SourceDataset,
    files: tuple[HubParquetFile, ...],
) -> dict[str, Any]:
    plan = assembly_cli.build_assembly_plan((source,), files)
    budget = assembly_cli.estimate_storage_budget(plan)
    document = assembly_cli._plan_document((source,), plan, budget)
    document.pop("execution_identity", None)
    document.pop("execution_identity_sha256", None)
    document.pop("legacy_checkpoint", None)
    return document


def test_legacy_checkpoint_requires_explicit_acceptance_and_migrates_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    files = _files()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: files
    )
    monkeypatch.setattr(
        assembly_cli, "run_joined_partition", lambda *_args, **_kwargs: 0
    )
    output_dir = tmp_path / "legacy-assembly"
    output_dir.mkdir()
    legacy_plan = _legacy_plan_document(source, files)
    plan_path = output_dir / "assembly-plan.json"
    plan_path.write_bytes(assembly_cli._canonical_json(legacy_plan))
    original_plan_sha256 = legacy_plan["plan_sha256"]
    _write_manifest(
        output_dir,
        status_counts={"yes": 1, "no": 1, "failed": 1, "skipped": 1},
    )

    assert (
        assembly_cli.main(["--output-dir", str(output_dir)])
        == assembly_cli.EXIT_CONFLICT
    )
    assert json.loads(plan_path.read_text()) == legacy_plan

    assert (
        assembly_cli.main(
            ["--output-dir", str(output_dir), "--allow-legacy-checkpoint"]
        )
        == assembly_cli.EXIT_OK
    )

    migrated = json.loads(plan_path.read_text())
    progress = json.loads((output_dir / "assembly-progress.json").read_text())
    manifest = json.loads((output_dir / "assembly-manifest.json").read_text())
    assert migrated["plan_sha256"] == original_plan_sha256
    assert migrated["legacy_checkpoint"]["prior_plan_sha256"] == original_plan_sha256
    assert migrated["execution_identity"]["input_plan_sha256"]
    assert progress["execution_identity"] == migrated["execution_identity"]
    assert (
        progress["execution_identity_sha256"] == migrated["execution_identity_sha256"]
    )
    assert manifest["execution_identity"] == migrated["execution_identity"]
    assert assembly_cli.main(["--output-dir", str(output_dir)]) == assembly_cli.EXIT_OK
    assert json.loads(plan_path.read_text()) == migrated


def test_legacy_checkpoint_rejects_changed_inputs_without_rewriting_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    original_files = _files()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: original_files
    )
    output_dir = tmp_path / "legacy-assembly"
    output_dir.mkdir()
    legacy_plan = _legacy_plan_document(source, original_files)
    plan_path = output_dir / "assembly-plan.json"
    plan_path.write_bytes(assembly_cli._canonical_json(legacy_plan))
    original_bytes = plan_path.read_bytes()
    changed_files = (
        HubParquetFile("fixture", "labels", "labels/data/a.parquet", 101),
        *original_files[1:],
    )
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: changed_files
    )

    assert (
        assembly_cli.main(
            ["--output-dir", str(output_dir), "--allow-legacy-checkpoint"]
        )
        == assembly_cli.EXIT_CONFLICT
    )
    assert plan_path.read_bytes() == original_bytes


def test_execution_identity_change_rejects_resume_even_with_legacy_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )

    def runner(argv: list[str], *, opener: Any = None) -> int:
        _write_manifest(
            Path(argv[argv.index("--output-dir") + 1]),
            status_counts={"yes": 1, "no": 1, "failed": 1, "skipped": 1},
        )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)
    output_dir = tmp_path / "identity-assembly"
    assert assembly_cli.main(["--output-dir", str(output_dir)]) == assembly_cli.EXIT_OK
    plan_path = output_dir / "assembly-plan.json"
    original_bytes = plan_path.read_bytes()

    monkeypatch.setattr(
        assembly_cli, "_source_tree_sha256", lambda _root: "f" * 64, raising=False
    )

    assert (
        assembly_cli.main(
            ["--output-dir", str(output_dir), "--allow-legacy-checkpoint"]
        )
        == assembly_cli.EXIT_CONFLICT
    )
    assert plan_path.read_bytes() == original_bytes


def test_dry_run_reports_model_input_device_and_batch_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )
    output_dir = tmp_path / "identity-dry-run"

    assert (
        assembly_cli.main(
            [
                "--output-dir",
                str(output_dir),
                "--max-bytes",
                "8192",
                "--dry-run",
            ]
        )
        == assembly_cli.EXIT_OK
    )

    summary = json.loads(capsys.readouterr().out)
    identity = summary["execution_identity"]
    assert identity["assembly_version"] == assembly_cli.ASSEMBLY_VERSION
    assert identity["schema_version"] == assembly_cli.SCHEMA_VERSION
    assert len(identity["input_plan_sha256"]) == 64
    assert len(identity["source_tree_sha256"]) == 64
    assert identity["device_kind"] == "cpu"
    assert identity["historical_label_model_identity"] is None
    assert identity["historical_label_model_status"] == (
        "unknown_not_evidenced_in_provenance"
    )
    assert identity["max_bytes"] == 8192
    assert identity["sqlite_batch_rows"] > 0
    assert identity["output_batch_rows"] > 0
    assert identity["output_batch_bytes"] > 0
    assert summary["execution_identity_sha256"]
    assert not output_dir.exists()


def test_legacy_completed_manifest_migrates_only_with_explicit_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source()
    files = _files()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: files
    )

    def runner(argv: list[str], *, opener: Any = None) -> int:
        _write_manifest(
            Path(argv[argv.index("--output-dir") + 1]),
            status_counts={"yes": 1, "no": 1, "failed": 1, "skipped": 1},
        )
        return 0

    monkeypatch.setattr(assembly_cli, "run_joined_partition", runner)
    output_dir = tmp_path / "legacy-complete"
    assert assembly_cli.main(["--output-dir", str(output_dir)]) == assembly_cli.EXIT_OK

    plan_path = output_dir / "assembly-plan.json"
    plan = json.loads(plan_path.read_text())
    original_plan_sha256 = plan["plan_sha256"]
    plan.pop("execution_identity")
    plan.pop("execution_identity_sha256")
    plan_path.write_bytes(assembly_cli._canonical_json(plan))

    manifest_path = output_dir / "assembly-manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.pop("execution_identity")
    manifest.pop("execution_identity_sha256")
    manifest_path.write_bytes(assembly_cli._canonical_json(manifest))

    assert (
        assembly_cli.main(
            ["--output-dir", str(output_dir), "--allow-legacy-checkpoint"]
        )
        == assembly_cli.EXIT_OK
    )

    migrated_plan = json.loads(plan_path.read_text())
    migrated_manifest = json.loads(manifest_path.read_text())
    assert migrated_plan["plan_sha256"] == original_plan_sha256
    assert migrated_manifest["plan_sha256"] == original_plan_sha256
    assert (
        migrated_manifest["execution_identity"] == migrated_plan["execution_identity"]
    )
    assert "legacy_checkpoint" in migrated_manifest


def test_source_tree_identity_covers_source_and_ignores_generated_files(
    tmp_path: Path,
) -> None:
    from atlas_io.execution_identity import source_tree_sha256

    source = tmp_path / "src" / "atlas_io" / "module.py"
    source.parent.mkdir(parents=True)
    source.write_text("VALUE = 1\n")
    (tmp_path / "pyproject.toml").write_text("[project]\nversion = '1'\n")
    (tmp_path / "uv.lock").write_text("version = 1\n")
    original = source_tree_sha256(tmp_path)

    (source.parent / "__pycache__").mkdir()
    (source.parent / "__pycache__" / "module.pyc").write_bytes(b"generated")
    assert source_tree_sha256(tmp_path) == original
    source.write_text("VALUE = 2\n")
    assert source_tree_sha256(tmp_path) != original


def test_source_tree_identity_supports_installed_package_layout(
    tmp_path: Path,
) -> None:
    from atlas_io.execution_identity import source_tree_sha256

    package = tmp_path / "site-packages"
    (package / "atlas_io").mkdir(parents=True)
    (package / "atlas_domain").mkdir()
    (package / "atlas_io" / "module.py").write_text("VALUE = 1\n")
    (package / "atlas_domain" / "module.py").write_text("VALUE = 2\n")

    assert len(source_tree_sha256(package)) == 64


def test_dry_run_reports_source_identity_read_error_cleanly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = _source()
    monkeypatch.setattr(assembly_cli, "SOURCE_CATALOG", (source,))
    monkeypatch.setattr(
        assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files()
    )

    def fail_identity(_repository: Path) -> str:
        raise OSError("source tree unavailable")

    monkeypatch.setattr(assembly_cli, "_source_tree_sha256", fail_identity)
    output_dir = tmp_path / "identity-error"

    assert (
        assembly_cli.main(["--output-dir", str(output_dir), "--dry-run"])
        == assembly_cli.EXIT_INPUT
    )
    assert (
        "execution identity error: source tree unavailable" in capsys.readouterr().err
    )
    assert not output_dir.exists()
