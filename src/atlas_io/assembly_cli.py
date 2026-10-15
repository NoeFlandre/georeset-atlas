import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

from atlas_domain.joined_outputs import STATUSES
from atlas_domain.source_catalog import SOURCE_CATALOG, SourceDataset
from atlas_io.hub_inventory import (
    AssemblyPartition,
    AssemblyPlan,
    HubInventoryError,
    HubParquetFile,
    StorageBudget,
    build_assembly_plan,
    estimate_storage_budget,
    fetch_pinned_files,
)
from atlas_io.joined_cli import main as run_joined_partition
from atlas_io.joined_partitions import joined_manifest_path

ASSEMBLY_VERSION = 1
SCHEMA_VERSION = 1
DATA_VERSION = "v0"
DEFAULT_MAX_BYTES = 2_500_000_000
EXIT_OK = 0
EXIT_INPUT = 1
EXIT_USAGE = 2
EXIT_CONFLICT = 3
EXIT_BUDGET = 4
EXIT_PARTITION = 5
EXIT_RECONCILIATION = 6
EXIT_PAUSED = 7
_PLAN_FILE = "assembly-plan.json"
_PROGRESS_FILE = "assembly-progress.json"
_MANIFEST_FILE = "assembly-manifest.json"


class AssemblyConflictError(ValueError):
    pass


class StorageBudgetError(OSError):
    pass


def _positive_int(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected an integer, got {text!r}") from None
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="atlas-assemble",
        description=(
            "Inventory the frozen inputs, preflight disk space, and assemble every "
            "labels partition sequentially with per-partition hash manifests."
        ),
        epilog=(
            "The assembly resumes by verifying completed partition manifests. "
            "Inputs are read with strict Range requests and are never modified. "
            "An unfinished partition is retried when the assembly resumes."
        ),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-bytes", type=_positive_int, default=DEFAULT_MAX_BYTES)
    parser.add_argument(
        "--max-new-partitions",
        type=_positive_int,
        help=(
            "pause after this many partitions without an existing manifest have "
            "completed; previously completed partitions are verified and do not "
            "count toward the limit"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=("fetch pinned metadata and report storage needs without writing files"),
    )
    return parser


def _canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _implementation_revision() -> str:
    repository = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    revision = result.stdout.strip()
    if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
        raise AssemblyConflictError("git returned an invalid implementation revision")
    return revision


def _plan_document(
    sources: Sequence[SourceDataset], plan: AssemblyPlan, budget: StorageBudget
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "assembly_version": ASSEMBLY_VERSION,
        "schema_version": SCHEMA_VERSION,
        "data_version": DATA_VERSION,
        "implementation_revision": _implementation_revision(),
        "sources": [
            {
                "source_id": source.source_id,
                "repo_id": source.repo_id,
                "revision": source.revision,
                "expected_status_counts": dict(source.expected_status_counts),
            }
            for source in sources
        ],
        "files": [asdict(item) for item in plan.files],
        "partitions": [asdict(item) for item in plan.partitions],
        "unique_source_bytes": plan.unique_source_bytes,
        "execution_transfer_bytes": plan.execution_transfer_bytes,
        "largest_partition": asdict(plan.largest_partition),
        "storage_budget": asdict(budget),
    }
    digest = hashlib.sha256(_canonical_json(body)).hexdigest()
    return {**body, "plan_sha256": digest}


def _existing_disk_path(path: Path) -> Path:
    current = path.absolute()
    while not current.exists():
        parent = current.parent
        if parent == current:
            raise OSError(f"no existing parent for output path {path}")
        current = parent
    return current


def _write_json_atomic(path: Path, payload: bytes) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _prepare_plan(output_dir: Path, plan: dict[str, Any]) -> None:
    plan_path = output_dir / _PLAN_FILE
    expected = _canonical_json(plan)
    if plan_path.exists():
        try:
            existing = plan_path.read_bytes()
        except OSError as error:
            raise AssemblyConflictError(
                f"cannot read existing {plan_path.name}: {error}"
            ) from error
        if existing != expected:
            raise AssemblyConflictError(
                f"existing {plan_path.name} does not match the pinned input inventory"
            )
        return
    if any(output_dir.iterdir()):
        raise AssemblyConflictError(
            f"output directory {output_dir} is non-empty and has no matching "
            "assembly plan"
        )
    _write_json_atomic(plan_path, expected)


def _status_zeroes() -> dict[str, int]:
    return {status: 0 for status in sorted(STATUSES)}


def _manifest_for_partition(
    output_dir: Path,
    source: SourceDataset,
    partition: AssemblyPartition,
) -> dict[str, Any]:
    manifest_path = joined_manifest_path(
        output_dir, source.source_id, partition.labels_path
    )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise AssemblyConflictError(
            f"partition command returned without a readable manifest for "
            f"{source.source_id}/{partition.labels_path}: {error}"
        ) from error
    identity = {
        "source_id": source.source_id,
        "repo_id": source.repo_id,
        "revision": source.revision,
        "path": partition.labels_path,
    }
    if not isinstance(manifest, dict) or any(
        manifest.get(key) != value for key, value in identity.items()
    ):
        raise AssemblyConflictError(
            "partition manifest identity differs for "
            f"{source.source_id}/{partition.labels_path}"
        )
    counts = manifest.get("status_counts")
    if not (
        isinstance(counts, dict)
        and set(counts) == set(STATUSES)
        and all(type(value) is int and value >= 0 for value in counts.values())
        and sum(counts.values()) == manifest.get("label_row_count")
    ):
        raise AssemblyConflictError(
            f"partition manifest status totals do not reconcile for "
            f"{source.source_id}/{partition.labels_path}"
        )
    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict):
        raise AssemblyConflictError("partition manifest has no output inventory")
    for output in outputs.values():
        if not (
            isinstance(output, dict)
            and isinstance(output.get("name"), str)
            and Path(output["name"]).name == output["name"]
            and type(output.get("size_bytes")) is int
            and output["size_bytes"] >= 0
            and type(output.get("row_count")) is int
            and output["row_count"] >= 0
        ):
            raise AssemblyConflictError(
                "partition manifest has a malformed output entry"
            )
    return manifest


def _expected_totals(sources: Sequence[SourceDataset]) -> dict[str, dict[str, int]]:
    return {
        source.source_id: {
            status: count for status, count in source.expected_status_counts.items()
        }
        for source in sources
    }


def _progress_payload(
    *,
    plan_sha256: str,
    status: str,
    partitions_completed: int,
    partitions_total: int,
    sources: dict[str, dict[str, Any]],
    output_bytes: int,
    error: str | None = None,
    pending_partition: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "assembly_version": ASSEMBLY_VERSION,
        "plan_sha256": plan_sha256,
        "status": status,
        "partitions_completed": partitions_completed,
        "partitions_total": partitions_total,
        "sources": sources,
        "unique_output_bytes": output_bytes,
    }
    if error is not None:
        payload["error"] = error
    if pending_partition is not None:
        payload["pending_partition"] = pending_partition
    return payload


def _check_budget(
    output_dir: Path,
    *,
    budget: StorageBudget,
    output_bytes: int,
) -> tuple[int, int]:
    free_bytes = shutil.disk_usage(_existing_disk_path(output_dir)).free
    remaining_output = budget.output_allowance_bytes - output_bytes
    if remaining_output < 0:
        raise StorageBudgetError(
            f"current output {output_bytes} B exceeded output allowance "
            f"{budget.output_allowance_bytes} B"
        )
    required = (
        remaining_output
        + budget.contingency_bytes
        + budget.workspace_allowance_bytes
        + budget.safety_margin_bytes
    )
    if free_bytes < required:
        raise StorageBudgetError(
            f"need {required} B free for remaining output, contingency, workspace, "
            f"and safety margin; only {free_bytes} B remain"
        )
    return free_bytes, required


def _run_partition(
    source: SourceDataset,
    partition: AssemblyPartition,
    *,
    max_bytes: int,
    output_dir: Path,
    opener: Any,
) -> int:
    argv = [
        "--source",
        source.source_id,
        "--labels",
        partition.labels_path,
        "--max-bytes",
        str(max_bytes),
        "--output-dir",
        str(output_dir),
        "--resume-orphans",
    ]
    return run_joined_partition(argv, opener=opener)


def main(
    argv: Sequence[str] | None = None,
    *,
    metadata_opener: Any = None,
    join_opener: Any = None,
) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        files: tuple[HubParquetFile, ...] = fetch_pinned_files(
            SOURCE_CATALOG, opener=metadata_opener
        )
        plan = build_assembly_plan(SOURCE_CATALOG, files)
        budget = estimate_storage_budget(plan)
    except (HubInventoryError, OSError, ValueError) as error:
        print(f"atlas-assemble: inventory error: {error}", file=sys.stderr)
        return EXIT_INPUT

    plan_doc = _plan_document(SOURCE_CATALOG, plan, budget)
    try:
        free, required = _check_budget(
            args.output_dir,
            budget=budget,
            output_bytes=0,
        )
    except (OSError, StorageBudgetError) as error:
        print(f"atlas-assemble: storage preflight failed: {error}", file=sys.stderr)
        return EXIT_BUDGET

    if args.dry_run:
        summary = {
            "plan_sha256": plan_doc["plan_sha256"],
            "partitions": plan.partition_count,
            "inventory_files": len(plan.files),
            "unique_source_bytes": plan.unique_source_bytes,
            "execution_transfer_bytes": plan.execution_transfer_bytes,
            "largest_partition": asdict(plan.largest_partition),
            "budget_required_free_bytes": budget.required_free_bytes,
            "current_free_bytes": free,
            "required_free_bytes_now": required,
            "output_dir": str(args.output_dir),
        }
        print(json.dumps(summary, sort_keys=True))
        return EXIT_OK

    try:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        _prepare_plan(args.output_dir, plan_doc)
    except (OSError, AssemblyConflictError) as error:
        print(f"atlas-assemble: output conflict: {error}", file=sys.stderr)
        return EXIT_CONFLICT

    source_by_id = {source.source_id: source for source in SOURCE_CATALOG}
    totals = _expected_totals(SOURCE_CATALOG)
    source_progress: dict[str, dict[str, Any]] = {
        source.source_id: {
            "status_counts": _status_zeroes(),
            "label_rows": 0,
            "partitions_completed": 0,
            "expected_status_counts": dict(source.expected_status_counts),
        }
        for source in SOURCE_CATALOG
    }
    seen_outputs: set[str] = set()
    output_bytes = 0
    output_rows = {
        "labels": 0,
        "text_units": 0,
        "polygon_records": 0,
        "documents": 0,
        "polygon_document_links": 0,
    }
    completed = 0
    new_partitions_completed = 0
    print(
        "ASSEMBLY_START "
        + json.dumps(
            {
                "plan_sha256": plan_doc["plan_sha256"],
                "partitions": plan.partition_count,
                "unique_source_bytes": plan.unique_source_bytes,
                "execution_transfer_bytes": plan.execution_transfer_bytes,
                "budget_required_free_bytes": budget.required_free_bytes,
                "current_free_bytes": free,
                "output_dir": str(args.output_dir),
            },
            sort_keys=True,
        ),
        flush=True,
    )

    for partition in plan.partitions:
        source = source_by_id[partition.source_id]
        try:
            free, required = _check_budget(
                args.output_dir,
                budget=budget,
                output_bytes=output_bytes,
            )
        except (OSError, StorageBudgetError) as error:
            payload = _progress_payload(
                plan_sha256=plan_doc["plan_sha256"],
                status="interrupted_budget",
                partitions_completed=completed,
                partitions_total=plan.partition_count,
                sources=source_progress,
                output_bytes=output_bytes,
                error=str(error),
            )
            _write_json_atomic(
                args.output_dir / _PROGRESS_FILE, _canonical_json(payload)
            )
            print(
                "atlas-assemble: storage budget stopped before next partition: "
                f"{error}",
                file=sys.stderr,
            )
            return EXIT_BUDGET
        manifest_path = joined_manifest_path(
            args.output_dir, source.source_id, partition.labels_path
        )
        had_manifest = manifest_path.exists()
        try:
            partition_exit = _run_partition(
                source,
                partition,
                max_bytes=args.max_bytes,
                output_dir=args.output_dir,
                opener=join_opener,
            )
        except KeyboardInterrupt:
            payload = _progress_payload(
                plan_sha256=plan_doc["plan_sha256"],
                status="paused",
                partitions_completed=completed,
                partitions_total=plan.partition_count,
                sources=source_progress,
                output_bytes=output_bytes,
                error=(
                    "graceful interrupt during partition; unfinished partition will "
                    "be retried when the assembly resumes"
                ),
                pending_partition=partition.labels_path,
            )
            _write_json_atomic(
                args.output_dir / _PROGRESS_FILE, _canonical_json(payload)
            )
            print(
                "atlas-assemble: paused after graceful interrupt; resume to retry "
                f"{partition.labels_path}",
                file=sys.stderr,
            )
            return EXIT_PAUSED
        if partition_exit != 0:
            payload = _progress_payload(
                plan_sha256=plan_doc["plan_sha256"],
                status="interrupted_partition",
                partitions_completed=completed,
                partitions_total=plan.partition_count,
                sources=source_progress,
                output_bytes=output_bytes,
                error=f"atlas-join exit code {partition_exit}",
            )
            _write_json_atomic(
                args.output_dir / _PROGRESS_FILE, _canonical_json(payload)
            )
            return EXIT_PARTITION
        try:
            manifest = _manifest_for_partition(args.output_dir, source, partition)
        except AssemblyConflictError as error:
            print(f"atlas-assemble: manifest conflict: {error}", file=sys.stderr)
            return EXIT_CONFLICT

        counts = manifest["status_counts"]
        stats = source_progress[source.source_id]
        stats["label_rows"] += manifest["label_row_count"]
        stats["partitions_completed"] += 1
        for status, count in counts.items():
            stats["status_counts"][status] += count

        for table, output in manifest["outputs"].items():
            name = output["name"]
            if name in seen_outputs:
                continue
            seen_outputs.add(name)
            output_bytes += output["size_bytes"]
            if table in output_rows:
                output_rows[table] += output["row_count"]

        completed += 1
        if not had_manifest:
            new_partitions_completed += 1
        try:
            free, required = _check_budget(
                args.output_dir,
                budget=budget,
                output_bytes=output_bytes,
            )
        except (OSError, StorageBudgetError) as error:
            progress = _progress_payload(
                plan_sha256=plan_doc["plan_sha256"],
                status="interrupted_budget",
                partitions_completed=completed,
                partitions_total=plan.partition_count,
                sources=source_progress,
                output_bytes=output_bytes,
                error=str(error),
            )
            _write_json_atomic(
                args.output_dir / _PROGRESS_FILE, _canonical_json(progress)
            )
            print(
                f"atlas-assemble: live storage reserve failed after partition: {error}",
                file=sys.stderr,
            )
            return EXIT_BUDGET
        progress = _progress_payload(
            plan_sha256=plan_doc["plan_sha256"],
            status="running",
            partitions_completed=completed,
            partitions_total=plan.partition_count,
            sources=source_progress,
            output_bytes=output_bytes,
        )
        _write_json_atomic(args.output_dir / _PROGRESS_FILE, _canonical_json(progress))
        if (
            args.max_new_partitions is not None
            and new_partitions_completed >= args.max_new_partitions
            and completed < plan.partition_count
        ):
            next_partition = plan.partitions[completed]
            paused = _progress_payload(
                plan_sha256=plan_doc["plan_sha256"],
                status="paused",
                partitions_completed=completed,
                partitions_total=plan.partition_count,
                sources=source_progress,
                output_bytes=output_bytes,
                error="paused at the requested new-partition limit",
                pending_partition=next_partition.labels_path,
            )
            _write_json_atomic(
                args.output_dir / _PROGRESS_FILE, _canonical_json(paused)
            )
            print(
                "ASSEMBLY_PAUSED "
                + json.dumps(
                    {
                        "partitions_completed": completed,
                        "partitions_total": plan.partition_count,
                        "new_partitions_completed": new_partitions_completed,
                        "pending_partition": next_partition.labels_path,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            return EXIT_PAUSED
        print(
            "ASSEMBLY_PROGRESS "
            + json.dumps(
                {
                    "partitions_completed": f"{completed}/{plan.partition_count}",
                    "source": source.source_id,
                    "labels_path": partition.labels_path,
                    "partition_label_rows": manifest["label_row_count"],
                    "partition_status_counts": counts,
                    "source_status_counts": stats["status_counts"],
                    "unique_output_bytes": output_bytes,
                    "free_bytes": free,
                    "required_free_bytes": required,
                },
                sort_keys=True,
            ),
            flush=True,
        )

    mismatches = {
        source_id: {
            status: {
                "expected": expected,
                "actual": source_progress[source_id]["status_counts"][status],
            }
            for status, expected in counts.items()
            if source_progress[source_id]["status_counts"][status] != expected
        }
        for source_id, counts in totals.items()
    }
    mismatches = {
        source_id: detail for source_id, detail in mismatches.items() if detail
    }
    if mismatches:
        payload = _progress_payload(
            plan_sha256=plan_doc["plan_sha256"],
            status="reconciliation_failed",
            partitions_completed=completed,
            partitions_total=plan.partition_count,
            sources=source_progress,
            output_bytes=output_bytes,
            error=json.dumps(mismatches, sort_keys=True),
        )
        _write_json_atomic(args.output_dir / _PROGRESS_FILE, _canonical_json(payload))
        print(
            f"atlas-assemble: source label totals do not reconcile: {mismatches}",
            file=sys.stderr,
        )
        return EXIT_RECONCILIATION

    manifest = {
        "assembly_version": ASSEMBLY_VERSION,
        "schema_version": SCHEMA_VERSION,
        "data_version": DATA_VERSION,
        "implementation_revision": plan_doc["implementation_revision"],
        "plan_sha256": plan_doc["plan_sha256"],
        "status": "complete",
        "partitions_completed": completed,
        "partitions_total": plan.partition_count,
        "unique_source_bytes": plan.unique_source_bytes,
        "execution_transfer_bytes": plan.execution_transfer_bytes,
        "unique_output_bytes": output_bytes,
        "unique_output_row_counts": output_rows,
        "sources": source_progress,
    }
    assembly_payload = _canonical_json(manifest)
    assembly_path = args.output_dir / _MANIFEST_FILE
    if assembly_path.exists() and assembly_path.read_bytes() != assembly_payload:
        print(
            "atlas-assemble: existing assembly manifest conflicts with "
            "reconciled result",
            file=sys.stderr,
        )
        return EXIT_CONFLICT
    if not assembly_path.exists():
        _write_json_atomic(assembly_path, assembly_payload)
    complete = _progress_payload(
        plan_sha256=plan_doc["plan_sha256"],
        status="complete",
        partitions_completed=completed,
        partitions_total=plan.partition_count,
        sources=source_progress,
        output_bytes=output_bytes,
    )
    _write_json_atomic(args.output_dir / _PROGRESS_FILE, _canonical_json(complete))
    print(
        "ASSEMBLY_COMPLETE "
        + json.dumps(
            {
                "plan_sha256": plan_doc["plan_sha256"],
                "partitions_completed": completed,
                "unique_output_bytes": output_bytes,
                "sources": source_progress,
                "manifest": str(assembly_path),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
