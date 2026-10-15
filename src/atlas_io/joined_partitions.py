import json
import os
import re
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from atlas_domain.joined_outputs import (
    RAW_DECISIONS,
    STATUSES,
    JoinedPartition,
    Row,
    RowSource,
    normalize_row_index,
)
from atlas_domain.source_catalog import SOURCE_CATALOG, select_input_file
from atlas_io.shard_increment import _fsync_directory, _stream_sha256, _write_atomically

MANIFEST_VERSION = 1
PARQUET_COMPRESSION = "zstd"
OUTPUT_BATCH_ROWS = 10_000
OUTPUT_BATCH_BYTES = 1024 * 1024
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
_GEOMETRY_FIELDS = frozenset({"geometry", "geometry_wkb"})
_NON_SLUG = re.compile(r"[^a-z0-9]+")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SHARED_SOURCE_ID = "wiki"
_SHARED_TABLE = "polygon_document_links"
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


SchemaProvider = Callable[[], Mapping[str, pa.Schema]]


class JoinedPartitionConflictError(RuntimeError):
    pass


class StagedRows:
    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def schema_arrow(self) -> pa.Schema:
        return pq.ParquetFile(self._path).schema_arrow

    def __len__(self) -> int:
        return pq.ParquetFile(self._path).metadata.num_rows

    def __getitem__(self, index: int) -> Row:
        parquet_file = pq.ParquetFile(self._path)
        metadata = parquet_file.metadata
        position = normalize_row_index(index, metadata.num_rows)
        for group in range(metadata.num_row_groups):
            group_rows = metadata.row_group(group).num_rows
            if position < group_rows:
                table = parquet_file.read_row_group(group)
                return table.slice(position, 1).to_pylist()[0]
            position -= group_rows
        raise IndexError(
            f"row index {index} is out of range for {metadata.num_rows} rows"
        )

    def __iter__(self) -> Iterator[Row]:
        parquet_file = pq.ParquetFile(self._path)
        for index in range(parquet_file.metadata.num_row_groups):
            yield from parquet_file.read_row_group(index).to_pylist()


def stage_table_groups(groups: Iterable[pa.Table], path: Path) -> StagedRows:
    writer: pq.ParquetWriter | None = None
    try:
        for group in groups:
            if writer is None:
                writer = pq.ParquetWriter(
                    path, group.schema, compression=PARQUET_COMPRESSION
                )
            writer.write_table(group)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise ValueError(f"{path.name} has no row groups to stage")
    return StagedRows(path)


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
    schema_provider: SchemaProvider | None = None,
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
    source_schemas = _source_schemas(schema_provider)
    output_dir.mkdir(parents=True, exist_ok=True)
    publication_order = sorted(
        OUTPUT_TABLES,
        key=lambda table: _is_shared(source_id, table),
        reverse=True,
    )
    outputs = {
        table: _publish_table(
            output_dir,
            prefix,
            table,
            getattr(partition, table),
            source_schemas.get(table),
            shared=_is_shared(source_id, table),
        )
        for table in publication_order
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


def _source_schemas(provider: SchemaProvider | None) -> Mapping[str, pa.Schema]:
    if provider is None:
        return {}
    schemas = provider()
    if not isinstance(schemas, Mapping):
        raise TypeError("schema_provider must return a mapping of output tables")
    unknown = sorted(set(schemas) - set(OUTPUT_TABLES))
    if unknown:
        raise ValueError(f"schema_provider names unknown output tables {unknown}")
    if not all(isinstance(schema, pa.Schema) for schema in schemas.values()):
        raise TypeError("schema_provider must map output tables to pyarrow schemas")
    return schemas


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
    outputs = manifest["outputs"]
    names = {
        table: _expected_name(prefix, identity["source_id"], table, outputs[table])
        for table in OUTPUT_TABLES
    }
    for table in OUTPUT_TABLES:
        _verify_output(output_dir, outputs[table], names[table])
    _reject_orphans(output_dir, prefix, allowed={manifest_path.name, *names.values()})
    return JoinedPartitionResult("verified", manifest, manifest_path)


def _expected_name(
    prefix: str,
    source_id: str,
    table: str,
    output: Mapping[str, Any],
) -> str:
    if not _is_shared(source_id, table):
        return f"{prefix}.{table}.parquet"
    digest = output["sha256"]
    if not (isinstance(digest, str) and _SHA256.fullmatch(digest)):
        raise _conflict(f"manifest output {table} names a malformed sha256")
    return _shared_name(digest)


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
    rows: RowSource,
    source: pa.Schema | None,
    *,
    shared: bool,
) -> dict[str, Any]:
    name = f"{prefix}.{table}.parquet"
    row_count = len(rows)
    schema = _table_schema(table, rows, source)
    columns = schema.names
    digest, size = _write_atomically(
        output_dir / name,
        lambda handle: _write_batches(handle, schema, _record_batches(rows, schema)),
        verify=lambda temporary: _verify_table(temporary, columns, row_count),
    )
    output = {
        "name": name,
        "size_bytes": size,
        "sha256": digest,
        "row_count": row_count,
    }
    if shared:
        return _share_table(output_dir, output)
    return output


def _share_table(output_dir: Path, staged: Mapping[str, Any]) -> dict[str, Any]:
    shared = {**staged, "name": _shared_name(staged["sha256"])}
    path = output_dir / shared["name"]
    if path.exists():
        try:
            _verify_output(output_dir, shared, shared["name"])
        except JoinedPartitionConflictError:
            (output_dir / staged["name"]).unlink()
            raise
        (output_dir / staged["name"]).unlink()
    else:
        os.replace(output_dir / staged["name"], path)
        _fsync_directory(output_dir)
    return shared


def _table_schema(table: str, rows: RowSource, source: pa.Schema | None) -> pa.Schema:
    declared = _TABLE_TYPES[table]
    source_types: dict[str, pa.DataType] = (
        {} if source is None else {field.name: field.type for field in source}
    )
    names: dict[str, None] = {} if len(rows) else dict.fromkeys(declared)
    for row in rows:
        for name in row:
            if name != _ROW_KEY_FIELD:
                names.setdefault(name)
    return pa.schema(
        [
            pa.field(name, _column_type(name, declared, source_types, rows))
            for name in names
        ]
    )


def _column_type(
    name: str,
    declared: Mapping[str, pa.DataType],
    source_types: Mapping[str, pa.DataType],
    rows: RowSource,
) -> pa.DataType:
    if name in source_types:
        return source_types[name]
    if name in declared and name not in _GEOMETRY_FIELDS:
        return declared[name]
    for row in rows:
        value = row.get(name)
        if value is not None:
            return pa.array([value]).type
    return pa.null()


def _record_batches(rows: RowSource, schema: pa.Schema) -> Iterator[pa.RecordBatch]:
    pending: list[Mapping[str, Any]] = []
    pending_bytes = 0
    for row in rows:
        pending.append(row)
        pending_bytes += _row_bytes(row)
        if len(pending) == OUTPUT_BATCH_ROWS or pending_bytes >= OUTPUT_BATCH_BYTES:
            yield _record_batch(pending, schema)
            pending = []
            pending_bytes = 0
    if pending:
        yield _record_batch(pending, schema)


def _record_batch(
    rows: Sequence[Mapping[str, Any]],
    schema: pa.Schema,
) -> pa.RecordBatch:
    arrays = [
        pa.array([row.get(field.name) for row in rows], type=field.type)
        for field in schema
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _row_bytes(row: Mapping[str, Any]) -> int:
    total = 0
    for value in row.values():
        if isinstance(value, (bytes, str)):
            total += len(value)
        elif isinstance(value, (list, tuple)):
            total += sum(len(item) for item in value if isinstance(item, (bytes, str)))
    return total


def _write_batches(
    handle: BinaryIO,
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
) -> None:
    with pq.ParquetWriter(handle, schema, compression=PARQUET_COMPRESSION) as writer:
        for batch in batches:
            writer.write_batch(batch)


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


def _is_shared(source_id: str, table: str) -> bool:
    return (source_id, table) == (_SHARED_SOURCE_ID, _SHARED_TABLE)


def _shared_name(digest: str) -> str:
    return f"{_SHARED_SOURCE_ID}__{_SHARED_TABLE}.{digest}.parquet"


def _is_count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _conflict(detail: str) -> JoinedPartitionConflictError:
    return JoinedPartitionConflictError(f"{detail}; preserved all files")
