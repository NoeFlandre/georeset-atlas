import hashlib
import json
import os
import platform
import sqlite3
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from atlas_io.disk_joined import _BATCH_ROWS
from atlas_io.joined_partitions import (
    OUTPUT_BATCH_BYTES,
    OUTPUT_BATCH_ROWS,
    PARQUET_COMPRESSION,
)

_INPUT_PLAN_FIELDS = (
    "assembly_version",
    "schema_version",
    "data_version",
    "sources",
    "files",
    "partitions",
    "unique_source_bytes",
    "execution_transfer_bytes",
    "largest_partition",
)
_GENERATED_PARTS = {"__pycache__"}
_GENERATED_SUFFIXES = {".pyc", ".pyo"}


@dataclass(frozen=True, slots=True)
class ExecutionIdentity:
    identity_version: int
    assembly_version: int
    schema_version: int
    data_version: str
    historical_label_model_identity: str | None
    historical_label_model_status: str
    input_plan_sha256: str
    source_tree_sha256: str
    python_implementation: str
    python_version: str
    platform_system: str
    platform_release: str
    device_kind: str
    device_architecture: str
    device_processor: str
    logical_cpu_count: int | None
    pyarrow_version: str
    sqlite_version: str
    max_bytes: int
    sqlite_batch_rows: int
    output_batch_rows: int
    output_batch_bytes: int
    parquet_compression: str


def _canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def input_plan_sha256(plan_document: Mapping[str, Any]) -> str:
    body = {field: plan_document[field] for field in _INPUT_PLAN_FIELDS}
    return hashlib.sha256(_canonical_json(body)).hexdigest()


def _is_generated_source(path: Path) -> bool:
    return (
        path.name == ".DS_Store"
        or path.suffix in _GENERATED_SUFFIXES
        or any(part in _GENERATED_PARTS for part in path.parts)
        or any(part.endswith(".egg-info") for part in path.parts)
    )


def _source_files(repository: Path) -> list[Path]:
    files: list[Path] = []
    source_root = repository / "src"
    if source_root.is_dir():
        roots = (source_root,)
        config_files = (repository / "pyproject.toml", repository / "uv.lock")
    else:
        roots = (repository / "atlas_domain", repository / "atlas_io")
        if not all(root.is_dir() for root in roots):
            raise FileNotFoundError(
                f"cannot locate Atlas source packages under {repository}"
            )
        config_files = ()
    for root in roots:
        for path in root.rglob("*"):
            if path.is_file() and not _is_generated_source(path):
                files.append(path)
    for path in config_files:
        if path.is_file():
            files.append(path)
    return sorted(files, key=lambda item: item.relative_to(repository).as_posix())


def source_tree_sha256(repository: Path) -> str:
    digest = hashlib.sha256()
    for path in _source_files(repository):
        relative_path = path.relative_to(repository).as_posix().encode()
        content = path.read_bytes()
        digest.update(relative_path)
        digest.update(b"\x00")
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def create_execution_identity(
    plan_document: Mapping[str, Any],
    *,
    max_bytes: int,
    source_sha256: str,
) -> ExecutionIdentity:
    if not isinstance(pa.__version__, str):
        raise RuntimeError("PyArrow does not expose a string version")
    return ExecutionIdentity(
        identity_version=1,
        assembly_version=int(plan_document["assembly_version"]),
        schema_version=int(plan_document["schema_version"]),
        data_version=str(plan_document["data_version"]),
        historical_label_model_identity=None,
        historical_label_model_status="unknown_not_evidenced_in_provenance",
        input_plan_sha256=input_plan_sha256(plan_document),
        source_tree_sha256=source_sha256,
        python_implementation=platform.python_implementation(),
        python_version=platform.python_version(),
        platform_system=platform.system(),
        platform_release=platform.release(),
        device_kind="cpu",
        device_architecture=platform.machine(),
        device_processor=platform.processor(),
        logical_cpu_count=os.cpu_count(),
        pyarrow_version=pa.__version__,
        sqlite_version=sqlite3.sqlite_version,
        max_bytes=max_bytes,
        sqlite_batch_rows=_BATCH_ROWS,
        output_batch_rows=OUTPUT_BATCH_ROWS,
        output_batch_bytes=OUTPUT_BATCH_BYTES,
        parquet_compression=PARQUET_COMPRESSION,
    )


def execution_identity_sha256(identity: ExecutionIdentity | Mapping[str, Any]) -> str:
    document = (
        asdict(identity) if isinstance(identity, ExecutionIdentity) else dict(identity)
    )
    return hashlib.sha256(_canonical_json(document)).hexdigest()
