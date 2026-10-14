import argparse
import http.client
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow as pa

from atlas_domain.joined_outputs import (
    JoinedPartition,
    join_description_partition,
    join_website_partition,
    join_wiki_partition,
)
from atlas_domain.source_catalog import (
    SOURCE_CATALOG,
    SourceDataset,
    select_input_file,
)
from atlas_domain.source_records import DataJoinError
from atlas_domain.sources import PinnedHubParquetSource
from atlas_io.hub_parquet import (
    ByteBudgetExceeded,
    HubOpener,
    ParquetReadStats,
    iter_parquet_file,
    resolve_hub_file_size,
)
from atlas_io.joined_partitions import (
    JoinedPartitionConflictError,
    write_joined_partition,
)

EXIT_OK = 0
EXIT_INPUT_ERROR = 1
EXIT_CONFLICT = 3
EXIT_BUDGET_EXCEEDED = 4
EXIT_JOIN_ERROR = 5
Rows = Sequence[Mapping[str, Any]]
_INPUT_ERRORS = (OSError, ValueError, pa.ArrowException, http.client.HTTPException)


@dataclass(frozen=True, slots=True)
class _InputTotals:
    rows: int
    row_groups: int
    range_bytes: int


def _join_description(tables: Mapping[str, Rows]) -> JoinedPartition:
    return join_description_partition(
        label_rows=tables["labels"],
        text_unit_rows=tables["text_units"],
        polygon_rows=tables["polygons"],
    )


def _join_website(tables: Mapping[str, Rows]) -> JoinedPartition:
    return join_website_partition(
        label_rows=tables["labels"],
        polygon_text_rows=tables["polygons_and_text"],
    )


def _join_wiki(tables: Mapping[str, Rows]) -> JoinedPartition:
    return join_wiki_partition(
        label_rows=tables["labels"],
        sentence_rows=tables["text_units"],
        document_rows=tables["documents"],
        link_rows=tables["polygon_document_links"],
        polygon_rows=tables["polygons"],
    )


_JOINS: Mapping[str, Callable[[Mapping[str, Rows]], JoinedPartition]] = {
    "description": _join_description,
    "website": _join_website,
    "wiki": _join_wiki,
}


def _companion_root(table: str, roots: Sequence[str], project: str) -> str:
    if len(roots) == 1:
        return roots[0]
    matching = [root for root in roots if root.split("/")[0] == project]
    if len(matching) != 1:
        raise ValueError(f"no unique {table!r} root for project {project!r}")
    return matching[0]


def _companion_paths(
    source: SourceDataset,
    labels_root: str,
    labels_path: str,
) -> dict[str, str]:
    basename = PurePosixPath(labels_path).name
    project = labels_root.removeprefix("labels/").split("/")[0]
    return {
        table: f"{_companion_root(table, roots, project)}/{basename}"
        for table, roots in source.table_roots.items()
        if table != "labels"
    }


def _read_rows(
    source: SourceDataset,
    table: str,
    path: str,
    *,
    max_bytes: int,
    opener: HubOpener | None,
) -> tuple[list[dict[str, Any]], _InputTotals]:
    try:
        size_bytes = resolve_hub_file_size(
            source.repo_id, source.revision, path, opener=opener
        )
        pinned = PinnedHubParquetSource(
            repo_id=source.repo_id,
            revision=source.revision,
            path=path,
            size_bytes=size_bytes,
        )
        stats = ParquetReadStats()
        rows: list[dict[str, Any]] = []
        for group in iter_parquet_file(
            pinned,
            max_bytes=max_bytes,
            opener=opener,
            stats=stats,
        ):
            rows.extend(group.to_pylist())
            del group
    except _INPUT_ERRORS as error:
        error.add_note(f"while reading {table} input {path}")
        raise
    return rows, _InputTotals(len(rows), stats.row_groups, stats.range_bytes)


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
        if source.source_id not in _JOINS:
            raise ValueError(f"no joined partition is defined for {args.source!r}")
        companions = _companion_paths(source, labels_root, args.labels)
    except ValueError as error:
        parser.error(str(error))
    inputs = {"labels": args.labels, **companions}

    def build() -> JoinedPartition:
        tables: dict[str, Rows] = {}
        for table, path in inputs.items():
            rows, stats = _read_rows(
                source, table, path, max_bytes=args.max_bytes, opener=opener
            )
            tables[table] = rows
            print(
                f"input {table}: rows={stats.rows} row_groups={stats.row_groups} "
                f"range_bytes={stats.range_bytes}"
            )
        try:
            return _JOINS[source.source_id](tables)
        except ValueError as error:
            raise DataJoinError(str(error)) from error

    try:
        result = write_joined_partition(
            source_id=source.source_id,
            repo_id=source.repo_id,
            revision=source.revision,
            path=args.labels,
            output_dir=args.output_dir,
            build=build,
        )
    except JoinedPartitionConflictError as error:
        return _report_error(EXIT_CONFLICT, error)
    except ByteBudgetExceeded as error:
        return _report_error(EXIT_BUDGET_EXCEEDED, error)
    except DataJoinError as error:
        return _report_error(EXIT_JOIN_ERROR, error)
    except _INPUT_ERRORS as error:
        return _report_error(EXIT_INPUT_ERROR, error)
    label_rows = result.manifest["label_row_count"]
    print(result.status, result.manifest_path, f"label_rows={label_rows}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
