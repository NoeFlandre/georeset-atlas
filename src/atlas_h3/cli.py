from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from atlas_h3 import AtlasH3InputError, AtlasH3OutputError, build_maps


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="atlas-h3",
        description=(
            "Build deterministic resolution-3 H3 map artifacts from a complete, "
            "hash-verified atlas assembly."
        ),
    )
    parser.add_argument("--assembly-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--work-dir", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        manifest_path = build_maps(
            args.assembly_dir, args.output_dir, work_dir=args.work_dir
        )
    except (AtlasH3InputError, AtlasH3OutputError) as error:
        print(f"atlas-h3: {error}", file=sys.stderr)
        return 1
    print(manifest_path)
    return 0
