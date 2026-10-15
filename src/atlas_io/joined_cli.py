import argparse
import http.client
import shutil
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from atlas_domain.joined_outputs import (
    JoinedPartition,
    RowSource,
)
from atlas_domain.source_catalog import (
    SOURCE_CATALOG,
    SourceDataset,
    companion_input_paths,
    select_input_file,
)
from atlas_domain.source_records import DataJoinError
from atlas_domain.sources import PinnedHubParquetSource
from atlas_io.disk_joined import (
    build_description_partition,
    build_website_partition,
    build_wiki_partition,
)
from atlas_io.hub_parquet import (
    ByteBudgetExceeded,
    HubOpener,
    ParquetReadStats,
    iter_parquet_file,
    resolve_hub_file_size,
)
from atlas_io.joined_partitions import (
    JoinedPartitionConflictError,
    StagedRows,
    stage_table_groups,
    write_joined_partition,
)

EXIT_OK = 0
EXIT_INPUT_ERROR = 1
EXIT_CONFLICT = 3
EXIT_BUDGET_EXCEEDED = 4
EXIT_JOIN_ERROR = 5
Rows = RowSource
_INPUT_ERRORS = (OSError, ValueError, pa.ArrowException, http.client.HTTPException)
_STAGING_SPACE_FACTOR = 2
_STAGING_SPACE_MARGIN_BYTES = 16 * 1024 * 1024


class InsufficientDiskSpace(OSError):
    pass


@dataclass(frozen=True, slots=True)
class _InputTotals:
    rows: int
    row_groups: int
    range_bytes: int


_SUPPORTED_SOURCE_IDS = frozenset({"description", "website", "wiki"})
_OUTPUTS_BY_INPUT: Mapping[str, Mapping[str, tuple[str, ...]]] = {
    "description": {
        "labels": ("labels",),
        "text_units": ("text_units",),
        "polygons": ("polygon_records",),
    },
    "website": {
        "labels": ("labels",),
        "polygons_and_text": ("text_units", "polygon_records"),
    },
    "wiki": {
        "labels": ("labels",),
        "text_units": ("text_units",),
        "documents": ("documents",),
        "polygon_document_links": ("polygon_document_links",),
        "polygons": ("polygon_records",),
    },
}


def _stage_rows(
    source: SourceDataset,
    table: str,
    path: str,
    *,
    size_bytes: int,
    max_bytes: int,
    opener: HubOpener | None,
    directory: Path,
) -> tuple[StagedRows, _InputTotals]:
    pinned = PinnedHubParquetSource(
        repo_id=source.repo_id,
        revision=source.revision,
        path=path,
        size_bytes=size_bytes,
    )
    stats = ParquetReadStats()
    try:
        staged = stage_table_groups(
            iter_parquet_file(
                pinned,
                max_bytes=max_bytes,
                opener=opener,
                stats=stats,
            ),
            directory / f"{table}.parquet",
        )
    except _INPUT_ERRORS as error:
        error.add_note(f"while reading {table} input {path}")
        raise
    return staged, _InputTotals(len(staged), stats.row_groups, stats.range_bytes)


def _output_schemas(
    source: SourceDataset,
    staged: Mapping[str, StagedRows],
) -> dict[str, pa.Schema]:
    schemas: dict[str, pa.Schema] = {}
    for input_table, outputs in _OUTPUTS_BY_INPUT[source.source_id].items():
        schema = staged[input_table].schema_arrow
        for output in outputs:
            schemas[output] = schema
    return schemas


def _require_staging_space(output_dir: Path, source_bytes: int) -> None:
    required = _STAGING_SPACE_FACTOR * source_bytes + _STAGING_SPACE_MARGIN_BYTES
    free = shutil.disk_usage(output_dir).free
    if free < required:
        raise InsufficientDiskSpace(
            f"staging needs {required} free bytes; {free} bytes are available"
        )


def _positive_int(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        message = f"expected an integer, got {text!r}"
        raise argparse.ArgumentTypeError(message) from None
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="atlas-join",
        description=(
            "Join one labels partition of a pinned source to its native companion "
            "tables and write a hash-verified joined partition. Each input is read "
            "by strict Range requests, one row group at a time. Rerunning the same "
            "command verifies existing outputs without rereading rows."
        ),
        epilog=(
            "exit codes: 0 partition written or verified; 1 Hub, Range, Parquet, or "
            "file I/O error; 2 usage, unknown source, or invalid path; 3 output "
            "conflict (preserved); 4 byte budget exceeded; 5 join error"
        ),
    )
    parser.add_argument(
        "--source", required=True, help="source ID declared in SOURCE_CATALOG"
    )
    parser.add_argument(
        "--labels",
        required=True,
        help="repository-relative labels .parquet path under a declared labels root",
    )
    parser.add_argument(
        "--max-bytes",
        type=_positive_int,
        required=True,
        help="cumulative Range byte budget for each input file, footer included",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="directory for the joined partition outputs and manifest",
    )
    parser.add_argument(
        "--resume-orphans",
        action="store_true",
        help=("reuse matching orphan outputs after an interrupted write"),
    )
    return parser


def _report_error(code: int, error: BaseException) -> int:
    details = [str(error), *getattr(error, "__notes__", [])]
    print(f"atlas-join: error: {'; '.join(details)}", file=sys.stderr)
    return code


def main(argv: Sequence[str] | None = None, *, opener: HubOpener | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        source, labels_root = select_input_file(
            SOURCE_CATALOG, source_id=args.source, table="labels", path=args.labels
        )
        if source.source_id not in _SUPPORTED_SOURCE_IDS:
            raise ValueError(f"no joined partition is defined for {args.source!r}")
        companions = companion_input_paths(source, labels_root, args.labels)
    except ValueError as error:
        parser.error(str(error))
    inputs = {"labels": args.labels, **companions}
    staging_dirs: list[Path] = []
    staged_tables: dict[str, StagedRows] = {}

    def build() -> JoinedPartition:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        sizes = {
            table: resolve_hub_file_size(
                source.repo_id, source.revision, path, opener=opener
            )
            for table, path in inputs.items()
        }
        _require_staging_space(args.output_dir, sum(sizes.values()))
        directory = Path(
            tempfile.mkdtemp(prefix=".atlas-join-staging-", dir=args.output_dir)
        )
        staging_dirs.append(directory)
        for table, path in inputs.items():
            staged, stats = _stage_rows(
                source,
                table,
                path,
                size_bytes=sizes[table],
                max_bytes=args.max_bytes,
                opener=opener,
                directory=directory,
            )
            staged_tables[table] = staged
            print(
                f"input {table}: rows={stats.rows} row_groups={stats.row_groups} "
                f"range_bytes={stats.range_bytes}"
            )
        try:
            if source.source_id == "description":
                return build_description_partition(
                    labels=staged_tables["labels"],
                    text_units=staged_tables["text_units"],
                    polygons=staged_tables["polygons"],
                    workspace_path=directory / "description.sqlite",
                )
            if source.source_id == "website":
                return build_website_partition(
                    labels=staged_tables["labels"],
                    polygons_and_text=staged_tables["polygons_and_text"],
                    workspace_path=directory / "website.sqlite",
                )
            if source.source_id == "wiki":
                return build_wiki_partition(
                    labels=staged_tables["labels"],
                    sentences=staged_tables["text_units"],
                    documents=staged_tables["documents"],
                    links=staged_tables["polygon_document_links"],
                    polygons=staged_tables["polygons"],
                    workspace_path=directory / "wiki.sqlite",
                )
            raise DataJoinError(
                f"no disk-backed join is defined for {source.source_id!r}"
            )
        except ValueError as error:
            raise DataJoinError(str(error)) from error

    def source_schemas() -> dict[str, pa.Schema]:
        return _output_schemas(source, staged_tables)

    try:
        try:
            result = write_joined_partition(
                source_id=source.source_id,
                repo_id=source.repo_id,
                revision=source.revision,
                path=args.labels,
                output_dir=args.output_dir,
                build=build,
                schema_provider=source_schemas,
                resume_orphans=args.resume_orphans,
            )
        except JoinedPartitionConflictError as error:
            return _report_error(EXIT_CONFLICT, error)
        except ByteBudgetExceeded as error:
            return _report_error(EXIT_BUDGET_EXCEEDED, error)
        except DataJoinError as error:
            return _report_error(EXIT_JOIN_ERROR, error)
        except _INPUT_ERRORS as error:
            return _report_error(EXIT_INPUT_ERROR, error)
    finally:
        for directory in staging_dirs:
            shutil.rmtree(directory, ignore_errors=True)
    label_rows = result.manifest["label_row_count"]
    print(result.status, result.manifest_path, f"label_rows={label_rows}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
