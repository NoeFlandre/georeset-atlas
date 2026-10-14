import json
import re
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from atlas_domain.joined_outputs import RAW_DECISIONS, STATUSES, JoinedPartition
from atlas_domain.source_catalog import SOURCE_CATALOG, select_input_file
from atlas_io.shard_increment import _stream_sha256, _write_atomically

MANIFEST_VERSION = 1
PARQUET_COMPRESSION = "zstd"
OUTPUT_TABLES = (
    "labels",
    "text_units",
    "polygon_records",
    "documents",
    "polygon_document_links",
)
_MANIFEST_KEYS = frozenset(
    {
        "manifest_version",
        "source_id",
        "repo_id",
        "revision",
        "path",
        "table_root",
        "label_row_count",
        "row_counts",
        "status_counts",
        "raw_decision_counts",
        "outputs",
    }
)
_OUTPUT_KEYS = frozenset({"name", "size_bytes", "sha256", "row_count"})
_ROW_KEY_FIELD = "text_unit_key"
_NON_SLUG = re.compile(r"[^a-z0-9]+")
_TABLE_TYPES: Mapping[str, Mapping[str, pa.DataType]] = {
    "labels": {
        "source_row_index": pa.int64(),
        "raw_decision": pa.string(),
        "status": pa.string(),
    },
    "text_units": {
        "text": pa.string(),
        "source_url": pa.string(),
        "raw_language_code": pa.string(),
        "polygon_ids": pa.list_(pa.string()),
    },
    "polygon_records": {
        "polygon_id": pa.string(),
        "geometry": pa.binary(),
    },
    "documents": {
        "document_id": pa.string(),
        "project": pa.string(),
        "title": pa.string(),
        "page_id": pa.int64(),
        "updated_at": pa.timestamp("us"),
        "source_url": pa.string(),
    },
    "polygon_document_links": {
        "document_id": pa.string(),
        "project": pa.string(),
        "polygon_id": pa.string(),
        "input_revision": pa.string(),
    },
}


class JoinedPartitionConflictError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class JoinedPartitionResult:
    status: Literal["written", "verified"]
    manifest: Mapping[str, Any]
    manifest_path: Path


def write_joined_partition(
    *,
    source_id: str,
    repo_id: str,
    revision: str,
    path: str,
    output_dir: Path,
    build: Callable[[], JoinedPartition],
) -> JoinedPartitionResult:
    source_dataset, table_root = select_input_file(
        SOURCE_CATALOG, source_id=source_id, table="labels", path=path
    )
    if (repo_id, revision) != (source_dataset.repo_id, source_dataset.revision):
        raise ValueError("repo_id and revision must match the frozen catalog")
    identity: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "source_id": source_id,
        "repo_id": repo_id,
        "revision": revision,
        "path": path,
        "table_root": table_root,
    }
    prefix = _output_prefix(source_id, path)
    manifest_path = output_dir / f"{prefix}.manifest.json"
    if manifest_path.exists():
        return _verify(output_dir, manifest_path, prefix, identity)
    _reject_orphans(output_dir, prefix, allowed=())
    partition = build()
    if not isinstance(partition, JoinedPartition):
        raise TypeError("build must return a JoinedPartition")
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        table: _publish_table(output_dir, prefix, table, getattr(partition, table))
        for table in OUTPUT_TABLES
    }
    manifest: dict[str, Any] = {
        **identity,
        "label_row_count": len(partition.labels),
        "row_counts": {
            table: len(getattr(partition, table)) for table in OUTPUT_TABLES
        },
        "status_counts": dict(partition.status_counts),
        "raw_decision_counts": dict(partition.raw_decision_counts),
        "outputs": outputs,
    }
    payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    _write_atomically(manifest_path, lambda handle: handle.write(payload))
    return JoinedPartitionResult("written", manifest, manifest_path)


def _verify(
    output_dir: Path,
    manifest_path: Path,
    prefix: str,
    identity: Mapping[str, Any],
) -> JoinedPartitionResult:
    manifest = _load_manifest(manifest_path)
    for key, expected in identity.items():
        recorded = manifest.get(key)
        if type(recorded) is not type(expected) or recorded != expected:
            raise _conflict(
                f"manifest {manifest_path.name} records {key}={recorded!r}, "
                f"not {expected!r}"
            )
    _check_manifest(manifest, manifest_path.name)
    names = {table: f"{prefix}.{table}.parquet" for table in OUTPUT_TABLES}
    for table in OUTPUT_TABLES:
        _verify_output(output_dir, manifest["outputs"][table], names[table])
    _reject_orphans(output_dir, prefix, allowed={manifest_path.name, *names.values()})
    return JoinedPartitionResult("verified", manifest, manifest_path)


def _check_manifest(manifest: Mapping[str, Any], name: str) -> None:
    if set(manifest) != _MANIFEST_KEYS:
        raise _conflict(f"manifest {name} has unexpected keys")
    label_row_count = manifest["label_row_count"]
    row_counts = manifest["row_counts"]
    outputs = manifest["outputs"]
    if not (
        isinstance(row_counts, dict)
        and set(row_counts) == set(OUTPUT_TABLES)
        and all(_is_count(count) for count in row_counts.values())
    ):
        raise _conflict(f"manifest {name} row_counts must name every output table")
    if not _is_count(label_row_count) or label_row_count != row_counts["labels"]:
        raise _conflict(f"manifest {name} label_row_count does not match row_counts")
    if not (isinstance(outputs, dict) and set(outputs) == set(OUTPUT_TABLES)):
        raise _conflict(f"manifest {name} outputs must name every output table")
    for table in OUTPUT_TABLES:
        output = outputs[table]
        if not (
            isinstance(output, dict)
            and set(output) == _OUTPUT_KEYS
            and _is_count(output["row_count"])
        ):
            raise _conflict(f"manifest {name} output {table} is malformed")
        if output["row_count"] != row_counts[table]:
            raise _conflict(f"manifest {name} row count for {table} disagrees")
    status_counts = manifest["status_counts"]
    if not (
        isinstance(status_counts, dict)
        and set(status_counts) == set(STATUSES)
        and all(_is_count(count) for count in status_counts.values())
        and sum(status_counts.values()) == label_row_count
    ):
        raise _conflict(f"manifest {name} status_counts do not reconcile")
    raw_decision_counts = manifest["raw_decision_counts"]
    if not (
        isinstance(raw_decision_counts, dict)
        and set(raw_decision_counts) <= set(RAW_DECISIONS)
        and all(_is_count(count) for count in raw_decision_counts.values())
        and sum(raw_decision_counts.values()) == label_row_count
    ):
        raise _conflict(f"manifest {name} raw_decision_counts do not reconcile")


def _verify_output(output_dir: Path, output: Mapping[str, Any], expected: str) -> None:
    name = output["name"]
    if name != expected:
        raise _conflict(f"manifest names unexpected output {name!r}")
    path = output_dir / name
    if not path.is_file():
        raise _conflict(f"manifest names missing output {name}")
    digest, size = _stream_sha256(path)
    if size != output["size_bytes"] or digest != output["sha256"]:
        raise _conflict(f"output {name} does not match its manifest size and sha256")
    try:
        row_count = pq.ParquetFile(path).metadata.num_rows
    except Exception as error:
        raise _conflict(f"output {name} is not readable as Parquet") from error
    if row_count != output["row_count"]:
        raise _conflict(f"output {name} row count does not match its manifest")


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise _conflict(f"manifest {path.name} is unreadable ({error})") from error
    if not isinstance(loaded, dict):
        raise _conflict(f"manifest {path.name} is not a JSON object")
    return loaded


def _publish_table(
    output_dir: Path,
    prefix: str,
    table: str,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    name = f"{prefix}.{table}.parquet"
    arrow_table = _arrow_table(rows, _TABLE_TYPES[table])
    columns = arrow_table.column_names
    digest, size = _write_atomically(
        output_dir / name,
        lambda handle: pq.write_table(
            arrow_table, handle, compression=PARQUET_COMPRESSION
        ),
        verify=lambda temporary: _verify_table(temporary, columns, len(rows)),
    )
    return {"name": name, "size_bytes": size, "sha256": digest, "row_count": len(rows)}


def _arrow_table(
    rows: Sequence[Mapping[str, Any]],
    declared: Mapping[str, pa.DataType],
) -> pa.Table:
    names: dict[str, None] = dict.fromkeys(declared) if not rows else {}
    for row in rows:
        for name in row:
            if name != _ROW_KEY_FIELD:
                names.setdefault(name)
    return pa.table(
        {
            name: pa.array(
                [row.get(name) for row in rows],
                type=(
                    None
                    if rows and name in {"geometry", "geometry_wkb"}
                    else declared.get(name)
                ),
            )
            for name in names
        }
    )


def _verify_table(path: Path, columns: Sequence[str], rows: int) -> None:
    try:
        parquet_file = pq.ParquetFile(path)
    except Exception as error:
        raise OSError("temporary table is not readable as Parquet") from error
    if (
        parquet_file.schema_arrow.names != list(columns)
        or parquet_file.metadata.num_rows != rows
    ):
        raise OSError("temporary table does not match the rows it was built from")


def _reject_orphans(output_dir: Path, prefix: str, *, allowed: Collection[str]) -> None:
    if not output_dir.is_dir():
        return
    orphans = sorted(
        entry.name
        for entry in output_dir.iterdir()
        if _owned(entry.name, prefix) and entry.name not in allowed
    )
    if orphans:
        raise _conflict(f"orphan outputs {orphans} have no complete manifest")


def _owned(name: str, prefix: str) -> bool:
    return name.startswith((f"{prefix}.", f".{prefix}."))


def _output_prefix(source_id: str, path: str) -> str:
    stem = PurePosixPath(path).with_suffix("").as_posix()
    return f"{_slug(source_id)}__{_slug(stem)}"


def _slug(value: str) -> str:
    return _NON_SLUG.sub("-", value.lower()).strip("-")


def _is_count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _conflict(detail: str) -> JoinedPartitionConflictError:
    return JoinedPartitionConflictError(f"{detail}; preserved all files")
