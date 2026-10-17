import argparse
import http.client
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

import pyarrow as pa

from atlas_domain.source_catalog import SOURCE_CATALOG, select_input_file
from atlas_io.hub_parquet import HubOpener
from atlas_io.shard_increment import ShardConflictError, write_shard_increment

EXIT_OK = 0
EXIT_INPUT_ERROR = 1
EXIT_CONFLICT = 3


def _bounded_int(minimum: int) -> Callable[[str], int]:
    def parse(text: str) -> int:
        try:
            value = int(text)
        except ValueError:
            message = f"expected an integer, got {text!r}"
            raise argparse.ArgumentTypeError(message) from None
        if value < minimum:
            raise argparse.ArgumentTypeError(f"must be at least {minimum}")
        return value

    return parse


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="atlas-shard",
        description=(
            "Write one deterministic, compressed Parquet shard from one projected "
            "row group of a pinned native input file, plus a JSON checkpoint. "
            "Rerunning the same command verifies and skips completed work."
        ),
        epilog=(
            "exit codes: 0 shard written or verified; 1 input, byte budget, or Hub "
            "error; 2 usage error; 3 existing files need inspection (preserved)"
        ),
    )
    parser.add_argument(
        "--source", required=True, help="source ID declared in SOURCE_CATALOG"
    )
    parser.add_argument(
        "--table", required=True, help="table name declared for that source"
    )
    parser.add_argument(
        "--path",
        required=True,
        help="repository-relative .parquet path under one declared table root",
    )
    parser.add_argument(
        "--row-group",
        type=_bounded_int(0),
        default=0,
        help="row group index to read (default: 0)",
    )
    parser.add_argument(
        "--column",
        action="append",
        required=True,
        help="column to project; repeat for several",
    )
    parser.add_argument(
        "--max-bytes",
        type=_bounded_int(1),
        required=True,
        help="cumulative Range byte budget for this input, footer included",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="directory for the shard and its checkpoint",
    )
    return parser


def main(argv: Sequence[str] | None = None, *, opener: HubOpener | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if len(set(args.column)) != len(args.column):
        parser.error("--column names must be unique")
    if any(not column for column in args.column):
        parser.error("--column names must be non-empty")
    try:
        source, table_root = select_input_file(
            SOURCE_CATALOG, source_id=args.source, table=args.table, path=args.path
        )
    except ValueError as error:
        parser.error(str(error))
    try:
        result = write_shard_increment(
            source_id=source.source_id,
            repo_id=source.repo_id,
            revision=source.revision,
            path=args.path,
            table=args.table,
            table_root=table_root,
            row_group_index=args.row_group,
            projection=args.column,
            max_bytes=args.max_bytes,
            output_dir=args.output_dir,
            opener=opener,
        )
    except ShardConflictError as error:
        print(f"atlas-shard: error: {error}", file=sys.stderr)
        return EXIT_CONFLICT
    except (OSError, IndexError, ValueError, pa.ArrowException) as error:
        print(f"atlas-shard: error: {error}", file=sys.stderr)
        return EXIT_INPUT_ERROR
    except http.client.HTTPException as error:
        print(
            f"atlas-shard: error: Hub HTTP protocol error: {error!r}",
            file=sys.stderr,
        )
        return EXIT_INPUT_ERROR
    checkpoint = result.checkpoint
    print(
        result.status,
        result.shard_path,
        f"rows={checkpoint['row_count']}",
        f"bytes={checkpoint['output_size_bytes']}",
        f"sha256={checkpoint['output_sha256']}",
    )
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
