import hashlib
import json
import os
import re
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Literal

import pyarrow.parquet as pq

from atlas_domain.source_catalog import SOURCE_CATALOG, select_input_file
from atlas_domain.sources import PinnedHubParquetSource
from atlas_io.hub_parquet import (
    HubOpener,
    read_parquet_row_group,
    resolve_hub_file_size,
)

CHECKPOINT_VERSION = 1
SHARD_COMPRESSION = "zstd"
_CHUNK_BYTES = 1024 * 1024
_NON_SLUG = re.compile(r"[^a-z0-9]+")


class ShardConflictError(RuntimeError):
    """Raised when existing shard files cannot be resumed; they are preserved."""


@dataclass(frozen=True, slots=True)
class ShardResult:
    status: Literal["written", "verified"]
    shard_path: Path
    checkpoint_path: Path
    checkpoint: Mapping[str, Any]


def write_shard_increment(
    *,
    source_id: str,
    repo_id: str,
    revision: str,
    path: str,
    table: str,
    table_root: str,
    row_group_index: int,
    projection: Sequence[str],
    max_bytes: int,
    output_dir: Path,
    opener: HubOpener | None = None,
) -> ShardResult:
    """Write one projected row-group shard, or verify and resume an existing one."""
    source_dataset, declared_root = select_input_file(
        SOURCE_CATALOG, source_id=source_id, table=table, path=path
    )
    if (repo_id, revision, table_root) != (
        source_dataset.repo_id,
        source_dataset.revision,
        declared_root,
    ):
        raise ValueError("repo, revision, and table root must match the frozen catalog")
    columns = _canonical_projection(projection)
    if isinstance(row_group_index, bool) or not isinstance(row_group_index, int):
        raise TypeError("row_group_index must be an integer")
    if row_group_index < 0:
        raise ValueError("row_group_index must be non-negative")
    output_dir.mkdir(parents=True, exist_ok=True)
    file_size = resolve_hub_file_size(repo_id, revision, path, opener=opener)
    identity: dict[str, Any] = {
        "checkpoint_version": CHECKPOINT_VERSION,
        "source_id": source_id,
        "repo_id": repo_id,
        "revision": revision,
        "path": path,
        "file_size_bytes": file_size,
        "table": table,
        "table_root": table_root,
        "compression": SHARD_COMPRESSION,
        "row_group_index": row_group_index,
        "projection": list(columns),
    }
    stem = _output_stem(
        source_id=source_id,
        table=table,
        repo_id=repo_id,
        revision=revision,
        path=path,
        row_group_index=row_group_index,
        projection=columns,
    )
    shard_path = output_dir / f"{stem}.parquet"
    checkpoint_path = output_dir / f"{stem}.checkpoint.json"

    existing = _load_checkpoint(checkpoint_path)
    if existing is not None:
        return _resume(identity, existing, shard_path, checkpoint_path)
    if shard_path.exists():
        raise ShardConflictError(
            f"orphan shard {shard_path.name} has no checkpoint; preserved it"
        )

    source = PinnedHubParquetSource(
        repo_id=repo_id, revision=revision, path=path, size_bytes=file_size
    )
    rows = read_parquet_row_group(
        source,
        row_group_index=row_group_index,
        columns=columns,
        max_bytes=max_bytes,
        opener=opener,
    )
    digest, output_size = _write_atomically(
        shard_path,
        lambda handle: pq.write_table(rows, handle, compression=SHARD_COMPRESSION),
        verify=lambda path: _verify_parquet_shard(
            path, expected_columns=rows.column_names, expected_rows=rows.num_rows
        ),
    )
    checkpoint = {
        **identity,
        "row_count": rows.num_rows,
        "output_name": shard_path.name,
        "output_size_bytes": output_size,
        "output_sha256": digest,
    }
    payload = (json.dumps(checkpoint, indent=2, sort_keys=True) + "\n").encode()
    _write_atomically(checkpoint_path, lambda handle: handle.write(payload))
    return ShardResult("written", shard_path, checkpoint_path, checkpoint)


def _resume(
    identity: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    shard_path: Path,
    checkpoint_path: Path,
) -> ShardResult:
    for key, expected in identity.items():
        if checkpoint.get(key) != expected:
            raise ShardConflictError(
                f"{checkpoint_path.name} records {key}={checkpoint.get(key)!r}, "
                f"not {expected!r}; preserved both files"
            )
    if not shard_path.is_file():
        raise ShardConflictError(
            f"{checkpoint_path.name} names missing shard {shard_path.name}; "
            "preserved the checkpoint"
        )
    if checkpoint.get("output_name") != shard_path.name:
        raise ShardConflictError(
            f"{checkpoint_path.name} names an unexpected shard; preserved both files"
        )
    digest, size = _stream_sha256(shard_path)
    if size != checkpoint.get("output_size_bytes") or digest != checkpoint.get(
        "output_sha256"
    ):
        raise ShardConflictError(
            f"shard {shard_path.name} does not match its checkpoint size and "
            "SHA-256; preserved both files"
        )
    try:
        parquet_file = pq.ParquetFile(shard_path)
    except Exception as error:
        raise ShardConflictError(
            f"shard {shard_path.name} is not readable as Parquet; preserved both files"
        ) from error
    if parquet_file.metadata.num_rows != checkpoint.get("row_count"):
        raise ShardConflictError(
            f"shard {shard_path.name} row count does not match its checkpoint; "
            "preserved both files"
        )
    if parquet_file.schema_arrow.names != list(identity["projection"]):
        raise ShardConflictError(
            f"shard {shard_path.name} schema does not match its projection; "
            "preserved both files"
        )
    expected_compression = str(identity["compression"]).upper()
    for row_group in range(parquet_file.metadata.num_row_groups):
        for column in range(parquet_file.metadata.num_columns):
            codec = (
                parquet_file.metadata.row_group(row_group).column(column).compression
            )
            if codec.upper() != expected_compression:
                raise ShardConflictError(
                    f"shard {shard_path.name} compression does not match its "
                    "checkpoint; preserved both files"
                )
    return ShardResult("verified", shard_path, checkpoint_path, checkpoint)


def _load_checkpoint(path: Path) -> Mapping[str, Any] | None:
    if not path.exists():
        return None
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ShardConflictError(
            f"checkpoint {path.name} is unreadable ({error}); preserved it"
        ) from error
    if not isinstance(loaded, dict):
        raise ShardConflictError(f"checkpoint {path.name} is not a JSON object")
    return loaded


def _canonical_projection(projection: Sequence[str]) -> tuple[str, ...]:
    if (
        not projection
        or any(not isinstance(name, str) or not name for name in projection)
        or len(set(projection)) != len(projection)
    ):
        raise ValueError("projection must name at least one unique column")
    return tuple(sorted(projection))


def _output_stem(
    *,
    source_id: str,
    table: str,
    repo_id: str,
    revision: str,
    path: str,
    row_group_index: int,
    projection: Sequence[str],
) -> str:
    parts = (source_id, table, PurePosixPath(path).stem)
    readable = "__".join(_slug(part) for part in parts)
    identity = json.dumps(
        {
            "source_id": source_id,
            "table": table,
            "repo_id": repo_id,
            "revision": revision,
            "path": path,
            "projection": sorted(projection),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(identity.encode()).hexdigest()[:12]
    return f"{readable}__{digest}__rg{row_group_index:05d}"


def _slug(value: str) -> str:
    return _NON_SLUG.sub("-", value.lower()).strip("-")


def _write_atomically(
    final_path: Path,
    write: Callable[[BinaryIO], object],
    *,
    verify: Callable[[Path], None] | None = None,
) -> tuple[str, int]:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{final_path.name}.", suffix=".tmp", dir=final_path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            write(handle)
            _fsync_file(handle)
        if verify is not None:
            verify(temporary)
        digest, size = _stream_sha256(temporary)
        os.replace(temporary, final_path)
        _fsync_directory(final_path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return digest, size


def _verify_parquet_shard(
    path: Path,
    *,
    expected_columns: Sequence[str],
    expected_rows: int,
) -> None:
    try:
        parquet_file = pq.ParquetFile(path)
    except Exception as error:
        raise OSError("temporary shard is not readable as Parquet") from error
    if parquet_file.schema_arrow.names != list(expected_columns):
        raise OSError("temporary shard schema does not match the requested projection")
    if parquet_file.metadata.num_rows != expected_rows:
        raise OSError("temporary shard row count does not match the source row group")


def _fsync_file(handle: BinaryIO) -> None:
    handle.flush()
    _full_fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        _full_fsync(descriptor)
    finally:
        os.close(descriptor)


def _full_fsync(descriptor: int) -> None:
    if sys.platform == "darwin":
        try:
            import errno
            import fcntl

            full_sync = getattr(fcntl, "F_FULLFSYNC", None)
            if full_sync is not None:
                try:
                    fcntl.fcntl(descriptor, full_sync)
                    return
                except OSError as error:
                    if error.errno not in {errno.EINVAL, errno.ENOTSUP, errno.ENOTTY}:
                        raise
        except ImportError:
            pass
    os.fsync(descriptor)


def _stream_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size
