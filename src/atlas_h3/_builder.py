from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from atlas_h3._aggregate import RESOLUTION, AggregatedMaps, aggregate_maps
from atlas_h3._assembly import VerifiedAssembly, load_verified_assembly
from atlas_h3._errors import AtlasH3InputError, AtlasH3OutputError
from atlas_h3._render import MAP_METRICS, render_map

_MANIFEST_NAME = "atlas-h3-manifest.json"
_WORK_DATABASE_NAME = "atlas-h3.sqlite3"
_SCHEMA = pa.schema(
    [
        ("h3_cell", pa.string()),
        ("distinct_yes_polygons", pa.int64()),
        ("association_occurrences", pa.int64()),
        ("distinct_text_units", pa.int64()),
        ("yes_label_count", pa.int64()),
        ("no_label_count", pa.int64()),
        ("yes_share_denominator", pa.int64()),
        ("yes_share", pa.float64()),
        ("failed_label_count", pa.int64()),
        ("skipped_label_count", pa.int64()),
        ("cell_area_km2", pa.float64()),
        ("distinct_text_units_per_km2", pa.float64()),
    ]
)


def build_maps(
    assembly_dir: str | Path,
    output_dir: str | Path,
    *,
    work_dir: str | Path | None = None,
) -> Path:
    assembly = load_verified_assembly(Path(assembly_dir))
    target = Path(output_dir).expanduser().resolve(strict=False)
    _check_output_target(assembly, target)
    if target.exists() and not target.is_dir():
        raise AtlasH3OutputError(f"output path is not a directory: {target}")

    scratch_dir = _prepare_work_dir(assembly, target, work_dir)
    try:
        aggregated = aggregate_maps(assembly, scratch_dir / _WORK_DATABASE_NAME)
    except (AtlasH3InputError, AtlasH3OutputError):
        raise
    except (OSError, sqlite3.Error) as error:
        raise AtlasH3OutputError(f"cannot use SQLite work database: {error}") from error
    view_slugs = _view_slugs(aggregated.rows_by_view)

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        stage = Path(
            tempfile.mkdtemp(prefix=f".{target.name or 'atlas-h3'}.", dir=target.parent)
        )
    except OSError as error:
        raise AtlasH3OutputError(f"cannot prepare output directory: {error}") from error

    try:
        _write_artifacts(stage, aggregated.rows_by_view, view_slugs)
        outputs = _output_inventory(stage)
        manifest = _manifest(assembly, aggregated, outputs)
        manifest_bytes = _json_bytes(manifest)
        (stage / _MANIFEST_NAME).write_bytes(manifest_bytes)
        if target.exists():
            _reuse_existing(target, manifest_bytes, outputs)
            return target / _MANIFEST_NAME
        os.replace(stage, target)
        return target / _MANIFEST_NAME
    except AtlasH3InputError:
        raise
    except AtlasH3OutputError:
        raise
    except Exception as error:
        raise AtlasH3OutputError(f"cannot publish map artifacts: {error}") from error
    finally:
        if stage.exists():
            shutil.rmtree(stage, ignore_errors=True)


def _check_output_target(assembly: VerifiedAssembly, target: Path) -> None:
    if _paths_overlap(target, assembly.root):
        raise AtlasH3OutputError(
            "output directory must not overlap the assembly directory"
        )


def _prepare_work_dir(
    assembly: VerifiedAssembly,
    target: Path,
    requested: str | Path | None,
) -> Path:
    if requested is None:
        if target == target.parent:
            raise AtlasH3OutputError(
                "default SQLite work directory would overlap the output directory"
            )
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            work_dir = Path(
                tempfile.mkdtemp(
                    prefix=f".{target.name or 'atlas-h3'}.work-",
                    dir=target.parent,
                )
            )
        except OSError as error:
            raise AtlasH3OutputError(
                f"cannot create SQLite work directory beside the output: {error}"
            ) from error
    else:
        work_dir = Path(requested).expanduser().resolve(strict=False)

    if _paths_overlap(work_dir, assembly.root):
        raise AtlasH3OutputError(
            "SQLite work directory must not overlap the assembly directory"
        )
    if _paths_overlap(work_dir, target):
        raise AtlasH3OutputError(
            "SQLite work directory must not overlap the output directory"
        )

    if requested is None:
        return work_dir
    try:
        if work_dir.exists():
            if not work_dir.is_dir():
                raise AtlasH3OutputError(
                    f"SQLite work path is not a directory: {work_dir}"
                )
            if next(work_dir.iterdir(), None) is not None:
                raise AtlasH3OutputError(
                    f"SQLite work directory is not empty: {work_dir}"
                )
        else:
            work_dir.mkdir(parents=True)
    except AtlasH3OutputError:
        raise
    except OSError as error:
        raise AtlasH3OutputError(
            f"cannot prepare SQLite work directory: {error}"
        ) from error
    return work_dir


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _view_slugs(rows_by_view: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    slugs: dict[str, str] = {}
    used: set[str] = set()
    for view_name in rows_by_view:
        slug = re.sub(r"[^a-z0-9]+", "-", view_name.lower()).strip("-")
        if not slug or slug in used:
            raise AtlasH3InputError(
                f"source IDs do not produce unique map paths: {view_name}"
            )
        slugs[view_name] = slug
        used.add(slug)
    return slugs


def _write_artifacts(
    root: Path,
    rows_by_view: dict[str, list[dict[str, Any]]],
    view_slugs: dict[str, str],
) -> None:
    for view_name, rows in rows_by_view.items():
        slug = view_slugs[view_name]
        aggregate_dir = root / "aggregates"
        map_dir = root / "maps" / slug
        aggregate_dir.mkdir(parents=True, exist_ok=True)
        map_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows, schema=_SCHEMA)
        pq.write_table(
            table,
            aggregate_dir / f"{slug}.parquet",
            compression="zstd",
            version="2.6",
            use_dictionary=False,
            write_statistics=True,
            data_page_version="1.0",
            row_group_size=65_536,
        )
        for metric in MAP_METRICS:
            svg = render_map(rows, view_name=view_name, metric=metric)
            (map_dir / metric.file_name).write_text(svg, encoding="utf-8", newline="\n")


def _output_inventory(root: Path) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        digest, size_bytes = _sha256(path)
        outputs.append({"path": relative, "sha256": digest, "size_bytes": size_bytes})
    return outputs


def _manifest(
    assembly: VerifiedAssembly,
    aggregated: AggregatedMaps,
    outputs: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "manifest_version": 1,
        "assembly": {
            "plan_sha256": assembly.plan_sha256,
            "implementation_revision": assembly.implementation_revision,
        },
        "views": list(aggregated.rows_by_view),
        "resolution": RESOLUTION,
        "crs": "OGC:CRS84",
        "representative_point_rule": (
            "Shapely representative_point (point on surface) in CRS84"
        ),
        "yes_share_denominator": "yes_label_count + no_label_count",
        "density_scale": "logarithmic in SVG maps using log1p(value)",
        "geometry_quality": aggregated.geometry_quality,
        "label_reconciliation": aggregated.label_reconciliation,
        "outputs": outputs,
    }


def _reuse_existing(
    target: Path,
    expected_manifest: bytes,
    outputs: list[dict[str, Any]],
) -> None:
    manifest_path = target / _MANIFEST_NAME
    try:
        existing_manifest = manifest_path.read_bytes()
    except OSError as error:
        raise AtlasH3OutputError(
            f"output directory already exists without a readable manifest: {target}"
        ) from error
    if existing_manifest != expected_manifest:
        raise AtlasH3OutputError(
            f"output directory already contains different artifacts: {target}"
        )
    for item in outputs:
        path = target / item["path"]
        try:
            path.resolve(strict=True).relative_to(target)
        except (OSError, ValueError) as error:
            raise AtlasH3OutputError(
                f"existing output is missing: {item['path']}"
            ) from error
        digest, size = _sha256(path)
        if digest != item["sha256"] or size != item["size_bytes"]:
            raise AtlasH3OutputError(
                f"existing output does not match its manifest: {item['path']}"
            )


def _sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
