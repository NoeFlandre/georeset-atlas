from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow.parquet as pq

from atlas_h3._errors import AtlasH3InputError

STATUSES = ("yes", "no", "failed", "skipped")
TABLES = (
    "labels",
    "text_units",
    "polygon_records",
    "documents",
    "polygon_document_links",
)
_SHA256 = re.compile(r"[0-9a-f]{64}")
_PLAN_METADATA_FIELDS = {
    "plan_sha256",
    "execution_identity",
    "execution_identity_sha256",
    "legacy_checkpoint",
}


@dataclass(frozen=True, slots=True)
class PartitionInput:
    source_id: str
    labels_path: str
    manifest_path: Path
    tables: dict[str, Path]


@dataclass(frozen=True, slots=True)
class VerifiedAssembly:
    root: Path
    partitions: tuple[PartitionInput, ...]
    source_ids: tuple[str, ...]
    status_counts: dict[str, dict[str, int]]
    plan_sha256: str
    implementation_revision: str


def load_verified_assembly(assembly_dir: Path) -> VerifiedAssembly:
    root = assembly_dir.expanduser().resolve()
    if not root.is_dir():
        raise AtlasH3InputError(f"assembly directory does not exist: {assembly_dir}")

    plan = _read_document(root / "assembly-plan.json")
    progress = _read_document(root / "assembly-progress.json")
    manifest = _read_document(root / "assembly-manifest.json")
    plan_sha256 = plan.get("plan_sha256")
    if not isinstance(plan_sha256, str) or not _SHA256.fullmatch(plan_sha256):
        raise AtlasH3InputError("assembly plan has a malformed plan_sha256")
    plan_body = {
        key: value for key, value in plan.items() if key not in _PLAN_METADATA_FIELDS
    }
    computed_plan_sha256 = hashlib.sha256(_canonical_json(plan_body)).hexdigest()
    if plan_sha256 != computed_plan_sha256:
        raise AtlasH3InputError("assembly plan sha256 does not match its contents")

    for label, document in (("progress", progress), ("manifest", manifest)):
        if document.get("status") != "complete":
            raise AtlasH3InputError(
                f"assembly is not complete: {label} status is not complete"
            )
        if document.get("plan_sha256") != plan_sha256:
            raise AtlasH3InputError(f"assembly {label} plan_sha256 does not match")

    plan_sources = plan.get("sources")
    plan_partitions = plan.get("partitions")
    if not isinstance(plan_sources, list) or not plan_sources:
        raise AtlasH3InputError("assembly plan has no sources")
    if not isinstance(plan_partitions, list) or not plan_partitions:
        raise AtlasH3InputError("assembly plan has no partitions")

    expected_status_counts: dict[str, dict[str, int]] = {}
    source_identity: dict[str, dict[str, Any]] = {}
    for source in plan_sources:
        if not isinstance(source, dict):
            raise AtlasH3InputError("assembly plan contains a malformed source")
        source_id = source.get("source_id")
        counts = source.get("expected_status_counts")
        if (
            not isinstance(source_id, str)
            or not source_id
            or source_id in source_identity
        ):
            raise AtlasH3InputError("assembly plan contains an invalid source_id")
        expected_status_counts[source_id] = _status_counts(
            counts, f"plan source {source_id} expected_status_counts"
        )
        source_identity[source_id] = source

    partition_specs: list[tuple[str, str]] = []
    partitions_per_source = {source_id: 0 for source_id in source_identity}
    seen_specs: set[tuple[str, str]] = set()
    for spec in plan_partitions:
        if not isinstance(spec, dict):
            raise AtlasH3InputError("assembly plan contains a malformed partition")
        source_id = spec.get("source_id")
        labels_path = spec.get("labels_path")
        if not isinstance(source_id, str) or source_id not in source_identity:
            raise AtlasH3InputError("assembly plan partition source_id is malformed")
        if not isinstance(labels_path, str):
            raise AtlasH3InputError("assembly plan partition identity is malformed")
        _safe_relative_path(labels_path, "partition labels_path")
        identity = (source_id, labels_path)
        if identity in seen_specs:
            raise AtlasH3InputError("assembly plan contains a duplicate partition")
        seen_specs.add(identity)
        partition_specs.append(identity)
        partitions_per_source[source_id] += 1

    total_partitions = len(partition_specs)
    for label, document in (("progress", progress), ("manifest", manifest)):
        if (
            document.get("partitions_total") != total_partitions
            or document.get("partitions_completed") != total_partitions
        ):
            raise AtlasH3InputError(f"assembly {label} partition counts are incomplete")

    partitions: list[PartitionInput] = []
    observed_source_counts = {
        source_id: {status: 0 for status in STATUSES} for source_id in source_identity
    }
    unique_outputs: dict[str, tuple[str, str, int, int]] = {}
    for source_id, labels_path in partition_specs:
        manifest_path = _partition_manifest_path(
            root,
            source_id,
            labels_path,
            partitions_per_source[source_id],
        )
        partition_manifest = _read_document(manifest_path)
        _check_partition_identity(
            partition_manifest,
            source_identity[source_id],
            source_id,
            labels_path,
        )
        counts = _status_counts(
            partition_manifest.get("status_counts"),
            f"partition {source_id}/{labels_path} status_counts",
        )
        label_row_count = _count(
            partition_manifest.get("label_row_count"),
            f"partition {source_id}/{labels_path} label_row_count",
        )
        if sum(counts.values()) != label_row_count:
            raise AtlasH3InputError(
                f"partition {source_id}/{labels_path} label counts do not reconcile"
            )

        row_counts_doc = partition_manifest.get("row_counts")
        outputs_doc = partition_manifest.get("outputs")
        if not isinstance(row_counts_doc, dict) or set(row_counts_doc) != set(TABLES):
            raise AtlasH3InputError(
                f"partition {source_id}/{labels_path} row_counts are malformed"
            )
        if not isinstance(outputs_doc, dict) or set(outputs_doc) != set(TABLES):
            raise AtlasH3InputError(
                f"partition {source_id}/{labels_path} outputs are malformed"
            )

        table_paths: dict[str, Path] = {}
        for table in TABLES:
            row_count = _count(
                row_counts_doc[table],
                f"partition {source_id}/{labels_path} {table} row_count",
            )
            output = outputs_doc[table]
            if not isinstance(output, dict):
                raise AtlasH3InputError(
                    f"partition {source_id}/{labels_path} {table} output is malformed"
                )
            name = output.get("name")
            if not isinstance(name, str):
                raise AtlasH3InputError(
                    f"partition {source_id}/{labels_path} {table} output has no name"
                )
            output_path = _safe_file(root, name, "partition output")
            digest = output.get("sha256")
            if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
                raise AtlasH3InputError(
                    f"partition output {name} has a malformed sha256"
                )
            size_bytes = _count(output.get("size_bytes"), f"output {name} size_bytes")
            output_rows = _count(output.get("row_count"), f"output {name} row_count")
            if output_rows != row_count:
                raise AtlasH3InputError(
                    f"partition {source_id}/{labels_path} {table} row counts disagree"
                )
            actual_digest, actual_size = _file_sha256(output_path)
            if actual_digest != digest or actual_size != size_bytes:
                raise AtlasH3InputError(f"input {name} sha256 or size does not match")
            try:
                actual_rows = pq.ParquetFile(output_path).metadata.num_rows
            except Exception as error:
                raise AtlasH3InputError(
                    f"input {name} is not readable as Parquet"
                ) from error
            if actual_rows != output_rows:
                raise AtlasH3InputError(f"input {name} row count does not match")

            previous = unique_outputs.get(name)
            output_identity = (table, digest, size_bytes, output_rows)
            if previous is not None and previous != output_identity:
                raise AtlasH3InputError(f"partition output name {name} is inconsistent")
            unique_outputs[name] = output_identity
            table_paths[table] = output_path

        partitions.append(
            PartitionInput(
                source_id=source_id,
                labels_path=labels_path,
                manifest_path=manifest_path,
                tables=table_paths,
            )
        )
        for status, count in counts.items():
            observed_source_counts[source_id][status] += count

    for source_id, expected in expected_status_counts.items():
        if observed_source_counts[source_id] != expected:
            raise AtlasH3InputError(
                f"source {source_id} label status counts do not match the plan"
            )

    unique_output_bytes = sum(item[2] for item in unique_outputs.values())
    if (
        _count(manifest.get("unique_output_bytes"), "unique_output_bytes")
        != unique_output_bytes
    ):
        raise AtlasH3InputError(
            "assembly manifest unique_output_bytes do not reconcile"
        )
    if (
        _count(progress.get("unique_output_bytes"), "progress unique_output_bytes")
        != unique_output_bytes
    ):
        raise AtlasH3InputError(
            "assembly progress unique_output_bytes do not reconcile"
        )
    unique_row_counts = {table: 0 for table in TABLES}
    for table, _, _, row_count in unique_outputs.values():
        unique_row_counts[table] += row_count
    recorded_rows = manifest.get("unique_output_row_counts")
    if not isinstance(recorded_rows, dict) or set(recorded_rows) != set(TABLES):
        raise AtlasH3InputError("assembly manifest output row counts are malformed")
    if {
        table: _count(recorded_rows[table], f"unique_output_row_counts.{table}")
        for table in TABLES
    } != unique_row_counts:
        raise AtlasH3InputError("assembly manifest output row counts do not reconcile")

    _check_assembly_sources(
        "manifest",
        manifest.get("sources"),
        source_identity,
        observed_source_counts,
        partitions_per_source,
    )
    _check_assembly_sources(
        "progress",
        progress.get("sources"),
        source_identity,
        observed_source_counts,
        partitions_per_source,
    )

    implementation_revision = plan.get("implementation_revision")
    if not isinstance(implementation_revision, str):
        raise AtlasH3InputError("assembly plan has no implementation_revision")
    return VerifiedAssembly(
        root=root,
        partitions=tuple(partitions),
        source_ids=tuple(sorted(source_identity)),
        status_counts=observed_source_counts,
        plan_sha256=plan_sha256,
        implementation_revision=implementation_revision,
    )


def _check_assembly_sources(
    label: str,
    sources_doc: Any,
    source_identity: dict[str, dict[str, Any]],
    status_counts: dict[str, dict[str, int]],
    partitions_per_source: dict[str, int],
) -> None:
    if not isinstance(sources_doc, dict) or set(sources_doc) != set(source_identity):
        raise AtlasH3InputError(
            f"assembly {label} source inventory does not match the plan"
        )
    for source_id, source_doc in sources_doc.items():
        if not isinstance(source_doc, dict):
            raise AtlasH3InputError(f"assembly {label} source {source_id} is malformed")
        actual_counts = _status_counts(
            source_doc.get("status_counts"),
            f"assembly {label} {source_id} status_counts",
        )
        expected = _status_counts(
            source_identity[source_id].get("expected_status_counts"),
            f"plan source {source_id} expected_status_counts",
        )
        if actual_counts != status_counts[source_id] or actual_counts != expected:
            raise AtlasH3InputError(
                f"assembly {label} source {source_id} status counts do not reconcile"
            )
        label_rows = _count(
            source_doc.get("label_rows"), f"{label} {source_id} label_rows"
        )
        if label_rows != sum(actual_counts.values()):
            raise AtlasH3InputError(
                f"assembly {label} source {source_id} label_rows do not reconcile"
            )
        completed = _count(
            source_doc.get("partitions_completed"),
            f"{label} {source_id} partitions_completed",
        )
        if completed != partitions_per_source[source_id]:
            raise AtlasH3InputError(
                f"assembly {label} source {source_id} partition count "
                "does not reconcile"
            )
        if source_doc.get("expected_status_counts") != expected:
            raise AtlasH3InputError(
                f"assembly {label} source {source_id} expected status counts "
                "do not match"
            )


def _check_partition_identity(
    manifest: dict[str, Any],
    source: dict[str, Any],
    source_id: str,
    labels_path: str,
) -> None:
    if manifest.get("source_id") != source_id or manifest.get("path") != labels_path:
        raise AtlasH3InputError(
            f"partition manifest identity does not match {source_id}/{labels_path}"
        )
    for key in ("repo_id", "revision"):
        if key in source and manifest.get(key) != source[key]:
            raise AtlasH3InputError(
                f"partition manifest {key} does not match source {source_id}"
            )


def _partition_manifest_path(
    root: Path,
    source_id: str,
    labels_path: str,
    partitions_for_source: int,
) -> Path:
    stem = PurePosixPath(labels_path).with_suffix("").as_posix()
    expected = root / f"{_slug(source_id)}__{_slug(stem)}.manifest.json"
    if expected.is_file():
        return expected
    if partitions_for_source == 1:
        fixture_path = root / f"{_slug(source_id)}.manifest.json"
        if fixture_path.is_file():
            return fixture_path
    raise AtlasH3InputError(
        f"assembly partition manifest is missing for {source_id}/{labels_path}"
    )


def _read_document(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise AtlasH3InputError(f"cannot read {path.name}: {error}") from error
    if not isinstance(value, dict):
        raise AtlasH3InputError(f"{path.name} must contain a JSON object")
    return value


def _safe_relative_path(value: str, label: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in ("", ".", "..") for part in path.parts)
    ):
        raise AtlasH3InputError(f"{label} is not a safe relative path")
    return path


def _safe_file(root: Path, name: str, label: str) -> Path:
    if Path(name).name != name or name in ("", ".", ".."):
        raise AtlasH3InputError(f"{label} has an unsafe file name")
    path = root / name
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as error:
        raise AtlasH3InputError(
            f"{label} is missing or outside the assembly"
        ) from error
    if not resolved.is_file():
        raise AtlasH3InputError(f"{label} is not a file: {name}")
    return resolved


def _file_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
    except OSError as error:
        raise AtlasH3InputError(
            f"cannot hash assembly input {path.name}: {error}"
        ) from error
    return digest.hexdigest(), size


def _status_counts(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != set(STATUSES):
        raise AtlasH3InputError(f"{label} must contain every label status")
    return {status: _count(value[status], f"{label}.{status}") for status in STATUSES}


def _count(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        raise AtlasH3InputError(f"{label} must be a non-negative integer")
    return value


def _canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
