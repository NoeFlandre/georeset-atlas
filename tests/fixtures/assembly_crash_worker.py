import json
import os
import sys
from pathlib import Path
from typing import Any

import atlas_io.assembly_cli as assembly_cli
from atlas_domain.source_catalog import SourceDataset
from atlas_io.hub_inventory import HubParquetFile
from atlas_io.joined_partitions import joined_manifest_path


def _source() -> SourceDataset:
    return SourceDataset(
        source_id="fixture",
        repo_id="owner/repo",
        revision="0" * 40,
        table_roots={
            "labels": ("labels/data",),
            "text_units": ("texts",),
            "polygons": ("polygons",),
        },
        expected_status_counts={"yes": 1, "no": 1, "failed": 1, "skipped": 1},
    )


def _files() -> tuple[HubParquetFile, ...]:
    return (
        HubParquetFile("fixture", "labels", "labels/data/a.parquet", 100),
        HubParquetFile("fixture", "labels", "labels/data/b.parquet", 100),
        HubParquetFile("fixture", "text_units", "texts/a.parquet", 50),
        HubParquetFile("fixture", "text_units", "texts/b.parquet", 50),
        HubParquetFile("fixture", "polygons", "polygons/a.parquet", 30),
        HubParquetFile("fixture", "polygons", "polygons/b.parquet", 30),
    )


def _write_manifest(output_dir: Path, labels_path: str) -> None:
    status_counts = (
        {"yes": 1, "no": 0, "failed": 1, "skipped": 0}
        if labels_path.endswith("a.parquet")
        else {"yes": 0, "no": 1, "failed": 0, "skipped": 1}
    )
    row_count = sum(status_counts.values())
    stem = Path(labels_path).stem
    outputs = {
        table: {"name": f"{stem}.{table}.parquet", "size_bytes": 10, "row_count": rows}
        for table, rows in {
            "labels": row_count,
            "text_units": 1,
            "polygon_records": 1,
            "documents": 0,
            "polygon_document_links": 0,
        }.items()
    }
    manifest = {
        "source_id": "fixture",
        "repo_id": "owner/repo",
        "revision": "0" * 40,
        "path": labels_path,
        "label_row_count": row_count,
        "status_counts": status_counts,
        "row_counts": {table: output["row_count"] for table, output in outputs.items()},
        "outputs": outputs,
    }
    path = joined_manifest_path(output_dir, "fixture", labels_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest))


def main(output_dir_text: str) -> int:
    output_dir = Path(output_dir_text)
    source = _source()
    assembly_cli.SOURCE_CATALOG = (source,)
    setattr(assembly_cli, "fetch_pinned_files", lambda *_args, **_kwargs: _files())

    def runner(
        argv: list[str],
        *,
        opener: Any = None,
        progress_callback: Any = None,
    ) -> int:
        labels_path = argv[argv.index("--labels") + 1]
        if labels_path.endswith("b.parquet"):
            os._exit(73)
        _write_manifest(Path(argv[argv.index("--output-dir") + 1]), labels_path)
        return 0

    setattr(assembly_cli, "run_joined_partition", runner)
    return assembly_cli.main(["--output-dir", str(output_dir)])


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
