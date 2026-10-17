from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import sqlite3
import struct
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

STATUSES = ("yes", "no", "failed", "skipped")
OUTPUT_TABLES = (
    "labels",
    "text_units",
    "polygon_records",
    "documents",
    "polygon_document_links",
)
REPO_ROOT = Path(__file__).resolve().parents[1]


def test_package_exposes_build_maps() -> None:
    package = importlib.util.find_spec("atlas_h3")

    assert package is not None
    module = importlib.import_module("atlas_h3")
    assert callable(getattr(module, "build_maps", None))


@pytest.mark.parametrize(
    ("source_id", "fields", "values", "expected"),
    [
        (
            "description",
            ("description_identity", "tag_key", "sentence_index"),
            ("way/1", "name", 2),
            '["way/1","name",2]',
        ),
        (
            "website",
            ("polygon_id", "field", "sentence_index"),
            ("way/1", "contact_website", 1),
            '["way/1","contact_website",1]',
        ),
        ("wiki", ("sentence_id",), ("enwiki:42",), "enwiki:42"),
    ],
)
def test_h3_reconstructs_native_occurrence_keys(
    source_id: str,
    fields: tuple[str, ...],
    values: tuple[str | int, ...],
    expected: str,
) -> None:
    from atlas_h3._aggregate import (
        _text_unit_key_columns,
        _text_unit_key_from_values,
    )

    assert _text_unit_key_columns(source_id, list(fields)) == fields
    assert _text_unit_key_from_values(source_id, fields, values) == expected


def test_disk_joined_description_output_feeds_h3_and_keeps_language_provenance(
    tmp_path: Path,
) -> None:
    from atlas_domain.source_catalog import SOURCE_CATALOG
    from atlas_h3._aggregate import aggregate_maps
    from atlas_h3._assembly import PartitionInput, VerifiedAssembly
    from atlas_io.disk_joined import build_description_partition
    from atlas_io.joined_partitions import stage_table_groups, write_joined_partition

    source = next(item for item in SOURCE_CATALOG if item.source_id == "description")
    label_path = f"{source.table_roots['labels'][0]}/region-a.parquet"
    labels = stage_table_groups(
        [
            pa.Table.from_pylist(
                [
                    {
                        "description_identity": "way/1",
                        "tag_key": "name",
                        "sentence_index": 0,
                        "decision": "yes",
                    }
                ]
            )
        ],
        tmp_path / "labels.parquet",
    )
    text_units = stage_table_groups(
        [
            pa.Table.from_pylist(
                [
                    {
                        "description_identity": "way/1",
                        "tag_key": "name",
                        "source_pbf": "region.osm.pbf",
                        "osm_type": "way",
                        "osm_id": "1",
                        "split_status": "split",
                        "original_text": "A mapped place.",
                        "sentences": ["A mapped place."],
                        "source_url": "https://example.test/way/1",
                        "raw_language_code": "eng",
                        "reason": "detected",
                    }
                ]
            )
        ],
        tmp_path / "text-units.parquet",
    )
    polygons = stage_table_groups(
        [
            pa.Table.from_pylist(
                [
                    {
                        "polygon_id": "way/1",
                        "source_pbf": "region.osm.pbf",
                        "osm_type": "way",
                        "osm_id": "1",
                        "geometry_wkb": _rectangle_wkb(-0.1, -0.1, 0.1, 0.1),
                    }
                ]
            )
        ],
        tmp_path / "polygons.parquet",
    )
    output_dir = tmp_path / "joined"
    output_dir.mkdir()
    result = write_joined_partition(
        source_id=source.source_id,
        repo_id=source.repo_id,
        revision=source.revision,
        path=label_path,
        output_dir=output_dir,
        build=lambda: build_description_partition(
            labels=labels,
            text_units=text_units,
            polygons=polygons,
            workspace_path=tmp_path / "description.sqlite",
        ),
    )
    output_paths = {
        table: output_dir / item["name"]
        for table, item in result.manifest["outputs"].items()
    }
    joined_labels = pq.read_table(output_paths["labels"])
    joined_text_units = pq.read_table(output_paths["text_units"])

    assert "text_unit_key" not in joined_labels.schema.names
    assert "text_unit_key" not in joined_text_units.schema.names
    assert joined_text_units.schema.field("language_scheme").type == pa.string()
    assert (
        joined_text_units.schema.field("language_mapping_version").type == pa.string()
    )
    text_unit = joined_text_units.to_pylist()[0]
    assert text_unit["language_code"] == "en"
    assert text_unit["raw_language_code"] == "eng"
    assert text_unit["language_scheme"] == "lingua_iso639_3"
    assert text_unit["language_mapping_version"] == "atlas-language-crosswalk-1.0.0"
    assert text_unit["language_script"] is None
    assert text_unit["language_mapping_status"] == "resolved"

    partition = PartitionInput(
        source_id=source.source_id,
        labels_path=label_path,
        manifest_path=result.manifest_path,
        tables=output_paths,
    )
    status_counts = {"yes": 1, "no": 0, "failed": 0, "skipped": 0}
    assembly = VerifiedAssembly(
        root=tmp_path,
        partitions=(partition,),
        source_ids=(source.source_id,),
        status_counts={source.source_id: status_counts},
        plan_sha256="a" * 64,
        implementation_revision="b" * 40,
    )

    aggregate = aggregate_maps(assembly, tmp_path / "atlas-h3.sqlite3")

    assert aggregate.rows_by_view["description"][0]["distinct_text_units"] == 1
    assert aggregate.rows_by_view["description"][0]["yes_label_count"] == 1


def test_build_maps_aggregates_duplicate_associations_and_all_statuses(
    tmp_path: Path,
) -> None:
    assembly = _write_complete_assembly(tmp_path / "assembly")
    output_dir = tmp_path / "maps"

    package = importlib.import_module("atlas_h3")
    manifest_path = package.build_maps(assembly, output_dir)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    global_rows = pq.read_table(output_dir / "aggregates/global.parquet").to_pylist()
    alpha_rows = pq.read_table(output_dir / "aggregates/alpha.parquet").to_pylist()
    assert len(global_rows) == 1
    assert len(alpha_rows) == 1
    global_cell = global_rows[0]
    alpha_cell = alpha_rows[0]
    assert global_cell["distinct_yes_polygons"] == 3
    assert global_cell["association_occurrences"] == 9
    assert global_cell["distinct_text_units"] == 7
    assert global_cell["yes_label_count"] == 2
    assert global_cell["no_label_count"] == 2
    assert global_cell["yes_share_denominator"] == 4
    assert global_cell["yes_share"] == 0.5
    assert global_cell["failed_label_count"] == 2
    assert global_cell["skipped_label_count"] == 1
    assert alpha_cell["distinct_yes_polygons"] == 2
    assert alpha_cell["association_occurrences"] == 5
    assert alpha_cell["distinct_text_units"] == 3
    assert alpha_cell["yes_label_count"] == 1
    assert alpha_cell["no_label_count"] == 1
    assert alpha_cell["yes_share_denominator"] == 2
    assert alpha_cell["failed_label_count"] == 1
    assert alpha_cell["skipped_label_count"] == 0
    assert alpha_cell["cell_area_km2"] > 0
    assert alpha_cell["distinct_text_units_per_km2"] == pytest.approx(
        3 / alpha_cell["cell_area_km2"]
    )
    assert manifest["geometry_quality"]["alpha"]["missing_polygon_count"] == 1
    assert manifest["geometry_quality"]["alpha"]["invalid_polygon_count"] == 1
    assert (
        manifest["geometry_quality"]["alpha"][
            "missing_geometry_association_occurrences"
        ]
        == 1
    )
    assert (
        manifest["geometry_quality"]["alpha"][
            "invalid_geometry_association_occurrences"
        ]
        == 1
    )
    assert manifest["resolution"] == 3
    assert manifest["representative_point_rule"] == (
        "Shapely representative_point (point on surface) in CRS84"
    )
    assert manifest["yes_share_denominator"] == "yes_label_count + no_label_count"
    assert len(manifest["outputs"]) == 15
    assert all(
        (output_dir / item["path"]).is_file()
        and _sha256(output_dir / item["path"]) == item["sha256"]
        for item in manifest["outputs"]
    )
    svg = (output_dir / "maps/global/yes_share.svg").read_text(encoding="utf-8")
    assert "#f3f2ee" in svg
    assert "Cividis" in svg
    assert "yes / (yes + no)" in svg
    density_svg = (
        output_dir / "maps/global/distinct_text_units_per_km2.svg"
    ).read_text(encoding="utf-8")
    assert "logarithmic density" in density_svg


def test_build_maps_uses_an_interior_representative_point(tmp_path: Path) -> None:
    assembly = _write_complete_assembly(tmp_path / "assembly")
    output_dir = tmp_path / "maps"

    package = importlib.import_module("atlas_h3")
    package.build_maps(assembly, output_dir)

    from h3 import latlng_to_cell
    from shapely import from_wkb

    shape = from_wkb(_rectangle_wkb(-0.1, -0.1, 0.1, 0.1))
    point = shape.representative_point()
    cell = pq.read_table(output_dir / "aggregates/alpha.parquet").to_pylist()[0][
        "h3_cell"
    ]
    assert shape.contains(point)
    assert cell == latlng_to_cell(point.y, point.x, 3)


def test_build_maps_rejects_incomplete_assembly_without_partial_output(
    tmp_path: Path,
) -> None:
    assembly = _write_complete_assembly(tmp_path / "assembly")
    progress_path = assembly / "assembly-progress.json"
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    progress["status"] = "running"
    progress_path.write_text(_json_bytes(progress).decode(), encoding="utf-8")
    output_dir = tmp_path / "maps"
    package = importlib.import_module("atlas_h3")

    with pytest.raises(package.AtlasH3InputError, match="not complete"):
        package.build_maps(assembly, output_dir)

    assert not output_dir.exists()


def test_build_maps_rejects_corrupt_input_without_partial_output(
    tmp_path: Path,
) -> None:
    assembly = _write_complete_assembly(tmp_path / "assembly")
    labels_path = next(assembly.glob("*.labels.parquet"))
    labels_path.write_bytes(labels_path.read_bytes() + b"corruption")
    output_dir = tmp_path / "maps"
    package = importlib.import_module("atlas_h3")

    with pytest.raises(package.AtlasH3InputError, match="sha256"):
        package.build_maps(assembly, output_dir)

    assert not output_dir.exists()


def test_build_maps_is_reproducible_and_idempotent(tmp_path: Path) -> None:
    assembly = _write_complete_assembly(tmp_path / "assembly")
    package = importlib.import_module("atlas_h3")
    first_output = tmp_path / "maps-one"
    second_output = tmp_path / "maps-two"
    first = package.build_maps(assembly, first_output)
    second = package.build_maps(assembly, second_output)
    first_doc = json.loads(first.read_text(encoding="utf-8"))
    second_doc = json.loads(second.read_text(encoding="utf-8"))
    assert first_doc["outputs"] == second_doc["outputs"]
    first_work_dirs = list(tmp_path.glob(".maps-one.work-*"))
    second_work_dirs = list(tmp_path.glob(".maps-two.work-*"))
    assert len(first_work_dirs) == len(second_work_dirs) == 1
    assert first_work_dirs[0].parent == second_work_dirs[0].parent == tmp_path
    assert (first_work_dirs[0] / "atlas-h3.sqlite3").is_file()
    assert (second_work_dirs[0] / "atlas-h3.sqlite3").is_file()
    assert package.build_maps(assembly, first_output) == first
    assert len(list(tmp_path.glob(".maps-one.work-*"))) == 2


def test_command_line_builds_maps_outside_the_assembly(tmp_path: Path) -> None:
    assembly = _write_complete_assembly(tmp_path / "assembly")
    output_dir = tmp_path / "maps"
    work_dir = tmp_path / "cli-work"
    env = os.environ.copy()
    python_path = str(REPO_ROOT / "src")
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (python_path, env.get("PYTHONPATH", "")) if part
    )

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "atlas_h3",
            "--assembly-dir",
            str(assembly),
            "--output-dir",
            str(output_dir),
            "--work-dir",
            str(work_dir),
        ],
        cwd=REPO_ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert (output_dir / "atlas-h3-manifest.json").is_file()
    assert (output_dir / "maps/global/yes_share.svg").is_file()
    assert (work_dir / "atlas-h3.sqlite3").is_file()


@settings(
    max_examples=8,
    derandomize=True,
    deadline=None,
    suppress_health_check=(HealthCheck.function_scoped_fixture,),
)
@given(polygon_ids=st.lists(st.sampled_from(("p-a", "p-b")), min_size=1, max_size=8))
def test_duplicate_associations_change_link_count_only(
    tmp_path: Path, polygon_ids: list[str]
) -> None:
    with tempfile.TemporaryDirectory(dir=tmp_path) as directory:
        case_dir = Path(directory)
        assembly = _write_complete_assembly(
            case_dir / "assembly",
            alpha_yes_polygon_ids=polygon_ids,
            beta=False,
        )
        output_dir = case_dir / "maps"
        package = importlib.import_module("atlas_h3")

        package.build_maps(assembly, output_dir)

        row = pq.read_table(output_dir / "aggregates/alpha.parquet").to_pylist()[0]
        assert row["association_occurrences"] == len(polygon_ids) + 2
        assert row["distinct_text_units"] == 3
        assert row["yes_label_count"] == 1
        assert row["no_label_count"] == 1
        assert row["yes_share_denominator"] == 2


def test_label_occurrences_change_label_counts_without_inflating_associations(
    tmp_path: Path,
) -> None:
    assembly = _write_complete_assembly(
        tmp_path / "assembly",
        alpha_extra_labels=[
            {"text_unit_key": "yes-alpha", "source_row_index": 4, "status": "yes"},
            {"text_unit_key": "yes-alpha", "source_row_index": 5, "status": "no"},
        ],
    )
    output_dir = tmp_path / "maps"
    package = importlib.import_module("atlas_h3")

    package.build_maps(assembly, output_dir)

    alpha_rows = pq.read_table(output_dir / "aggregates/alpha.parquet").to_pylist()
    assert len(alpha_rows) == 1
    alpha_cell = alpha_rows[0]
    assert alpha_cell["yes_label_count"] == 2
    assert alpha_cell["no_label_count"] == 2
    assert alpha_cell["yes_share_denominator"] == 4
    assert alpha_cell["distinct_text_units"] == 3
    assert alpha_cell["association_occurrences"] == 5
    assert alpha_cell["distinct_yes_polygons"] == 2


def test_build_maps_uses_explicit_disk_backed_aggregation_work_dir(
    tmp_path: Path,
) -> None:
    assembly = _write_complete_assembly(tmp_path / "assembly")
    output_dir = tmp_path / "maps"
    work_dir = tmp_path / "aggregation-work"
    package = importlib.import_module("atlas_h3")

    manifest_path = package.build_maps(assembly, output_dir, work_dir=work_dir)

    database_path = work_dir / "atlas-h3.sqlite3"
    assert database_path.is_file()
    with sqlite3.connect(database_path) as connection:
        tables = {
            name
            for (name,) in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert {
            "labels",
            "polygon_locations",
            "text_polygon_associations",
            "cell_counts",
            "cell_label_occurrences",
        } <= tables
        association_columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(text_polygon_associations)"
            )
        }
        assert {
            "source_id",
            "association_id",
            "text_unit_key",
            "polygon_id",
            "location_state",
            "h3_cell",
        } <= association_columns
        assert "label_status" not in association_columns
        label_occurrence_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(cell_label_occurrences)")
        }
        assert {
            "source_id",
            "h3_cell",
            "label_id",
            "status",
        } <= label_occurrence_columns
        association_indexes = {
            row[1]
            for row in connection.execute(
                "PRAGMA index_list(text_polygon_associations)"
            )
        }
        assert {
            "text_polygon_associations_polygon",
            "text_polygon_associations_unit",
        } <= association_indexes
        assert connection.execute("PRAGMA default_cache_size").fetchone()[0] == 8_192
        assert connection.execute("SELECT COUNT(*) FROM labels").fetchone()[0] == 8
        assert (
            connection.execute("SELECT COUNT(*) FROM polygon_locations").fetchone()[0]
            == 5
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM text_polygon_associations"
            ).fetchone()[0]
            == 11
        )
        assert connection.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT polygon_id)
            FROM text_polygon_associations
            WHERE source_id = 'alpha' AND text_unit_key = 'yes-alpha'
            """
        ).fetchone() == (3, 2)
        assert dict(
            connection.execute(
                """
                SELECT location_state, COUNT(*)
                FROM text_polygon_associations GROUP BY location_state
                """
            )
        ) == {"invalid": 1, "missing": 1, "valid": 9}
        cell_rows = connection.execute(
            """
            SELECT source_id, distinct_yes_polygons, association_occurrences,
                   distinct_text_units, yes_label_count, no_label_count,
                   failed_label_count, skipped_label_count
            FROM cell_counts ORDER BY source_id
            """
        ).fetchall()
    assert cell_rows == [
        ("alpha", 2, 5, 3, 1, 1, 1, 0),
        ("beta", 1, 4, 4, 1, 1, 1, 1),
    ]
    global_rows = pq.read_table(output_dir / "aggregates/global.parquet").to_pylist()
    assert global_rows[0]["distinct_yes_polygons"] == 3
    assert global_rows[0]["association_occurrences"] == 9
    assert global_rows[0]["distinct_text_units"] == 7
    assert (
        json.loads(manifest_path.read_text(encoding="utf-8"))["geometry_quality"][
            "global"
        ]["unmatched_polygon_association_occurrences"]
        == 0
    )


@pytest.mark.parametrize(
    ("assembly_relative", "output_relative", "work_relative"),
    [
        ("assembly", "maps", "assembly"),
        ("assembly", "maps", "assembly/work"),
        ("assembly", "maps", "maps"),
        ("assembly", "maps", "maps/work"),
        ("input/assembly", "maps", "input"),
        ("assembly", "output/maps", "output"),
    ],
)
def test_build_maps_rejects_work_dir_overlapping_assembly_or_output(
    tmp_path: Path,
    assembly_relative: str,
    output_relative: str,
    work_relative: str,
) -> None:
    assembly = _write_complete_assembly(tmp_path / assembly_relative)
    output_dir = tmp_path / output_relative
    work_dir = tmp_path / work_relative
    package = importlib.import_module("atlas_h3")

    with pytest.raises(package.AtlasH3OutputError, match="overlap"):
        package.build_maps(assembly, output_dir, work_dir=work_dir)

    assert not output_dir.exists()


def test_build_maps_rejects_nonempty_work_dir_without_changing_it(
    tmp_path: Path,
) -> None:
    assembly = _write_complete_assembly(tmp_path / "assembly")
    output_dir = tmp_path / "maps"
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    marker = work_dir / "keep.txt"
    marker.write_text("retained", encoding="utf-8")
    package = importlib.import_module("atlas_h3")

    with pytest.raises(package.AtlasH3OutputError, match="not empty"):
        package.build_maps(assembly, output_dir, work_dir=work_dir)

    assert marker.read_text(encoding="utf-8") == "retained"
    assert not output_dir.exists()


def _write_complete_assembly(
    root: Path,
    *,
    alpha_yes_polygon_ids: list[str] | None = None,
    alpha_extra_labels: list[dict[str, Any]] | None = None,
    beta: bool = True,
) -> Path:
    root.mkdir(parents=True)
    alpha_ids = alpha_yes_polygon_ids or ["p-a", "p-a", "p-b"]
    source_specs = {"alpha": _alpha_spec(alpha_ids, alpha_extra_labels)}
    if beta:
        source_specs["beta"] = _beta_spec()

    source_manifest_rows: list[dict[str, Any]] = []
    plan_partitions: list[dict[str, Any]] = []
    source_progress: dict[str, dict[str, Any]] = {}
    output_rows = {table: 0 for table in OUTPUT_TABLES}
    unique_output_bytes = 0
    for source_id, tables in source_specs.items():
        labels = tables["labels"].to_pylist()
        status_counts = {
            status: sum(row["status"] == status for row in labels)
            for status in STATUSES
        }
        source_revision = (source_id[0] * 40)[:40]
        repo_id = f"synthetic/{source_id}"
        labels_path = f"labels/{source_id}.parquet"
        output_entries: dict[str, dict[str, Any]] = {}
        row_counts: dict[str, int] = {}
        for table in OUTPUT_TABLES:
            filename = f"{source_id}.{table}.parquet"
            path = root / filename
            pq.write_table(tables[table], path, compression="zstd")
            row_count = tables[table].num_rows
            digest = _sha256(path)
            size = path.stat().st_size
            output_entries[table] = {
                "name": filename,
                "size_bytes": size,
                "sha256": digest,
                "row_count": row_count,
            }
            row_counts[table] = row_count
            output_rows[table] += row_count
            unique_output_bytes += size
        partition_manifest = {
            "manifest_version": 1,
            "source_id": source_id,
            "repo_id": repo_id,
            "revision": source_revision,
            "path": labels_path,
            "table_root": "labels",
            "label_row_count": len(labels),
            "row_counts": row_counts,
            "status_counts": status_counts,
            "raw_decision_counts": status_counts,
            "outputs": output_entries,
        }
        manifest_name = f"{source_id}.manifest.json"
        (root / manifest_name).write_bytes(_json_bytes(partition_manifest))
        source_manifest_rows.append(
            {
                "source_id": source_id,
                "repo_id": repo_id,
                "revision": source_revision,
                "expected_status_counts": status_counts,
            }
        )
        plan_partitions.append(
            {"source_id": source_id, "labels_path": labels_path, "input_bytes": 1}
        )
        source_progress[source_id] = {
            "status_counts": status_counts,
            "label_rows": len(labels),
            "partitions_completed": 1,
            "expected_status_counts": status_counts,
        }

    plan_body = {
        "assembly_version": 1,
        "schema_version": 1,
        "data_version": "v0",
        "implementation_revision": "a" * 40,
        "sources": source_manifest_rows,
        "files": [],
        "partitions": plan_partitions,
        "unique_source_bytes": 1,
        "execution_transfer_bytes": 1,
        "largest_partition": plan_partitions[0],
        "storage_budget": {
            "output_allowance_bytes": 1,
            "contingency_bytes": 1,
            "workspace_allowance_bytes": 1,
            "safety_margin_bytes": 1,
        },
    }
    plan_sha256 = hashlib.sha256(_canonical_json(plan_body) + b"\n").hexdigest()
    plan = {**plan_body, "plan_sha256": plan_sha256}
    (root / "assembly-plan.json").write_bytes(_json_bytes(plan))
    assembly_manifest = {
        "assembly_version": 1,
        "schema_version": 1,
        "data_version": "v0",
        "implementation_revision": "a" * 40,
        "plan_sha256": plan_sha256,
        "status": "complete",
        "partitions_completed": len(plan_partitions),
        "partitions_total": len(plan_partitions),
        "unique_source_bytes": 1,
        "execution_transfer_bytes": 1,
        "unique_output_bytes": unique_output_bytes,
        "unique_output_row_counts": output_rows,
        "sources": source_progress,
    }
    (root / "assembly-manifest.json").write_bytes(_json_bytes(assembly_manifest))
    progress = {
        "assembly_version": 1,
        "plan_sha256": plan_sha256,
        "status": "complete",
        "partitions_completed": len(plan_partitions),
        "partitions_total": len(plan_partitions),
        "sources": source_progress,
        "unique_output_bytes": unique_output_bytes,
    }
    (root / "assembly-progress.json").write_bytes(_json_bytes(progress))
    return root


def test_verified_assembly_plan_digest_ignores_current_metadata(
    tmp_path: Path,
) -> None:
    from atlas_h3._assembly import load_verified_assembly

    root = _write_complete_assembly(tmp_path / "assembly-current-plan")
    plan_path = root / "assembly-plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan_body = {key: value for key, value in plan.items() if key != "plan_sha256"}
    plan_body.update(
        execution_identity={"identity_version": 1, "source_tree_sha256": "b" * 64},
        execution_identity_sha256="c" * 64,
        legacy_checkpoint={"prior_plan_sha256": "d" * 64},
    )
    metadata = {
        "plan_sha256",
        "execution_identity",
        "execution_identity_sha256",
        "legacy_checkpoint",
    }
    digest_body = {
        key: value for key, value in plan_body.items() if key not in metadata
    }
    plan_sha256 = hashlib.sha256(_canonical_json(digest_body) + b"\n").hexdigest()
    plan_path.write_bytes(_json_bytes({**plan_body, "plan_sha256": plan_sha256}))
    for name in ("assembly-progress.json", "assembly-manifest.json"):
        path = root / name
        document = json.loads(path.read_text(encoding="utf-8"))
        document["plan_sha256"] = plan_sha256
        path.write_bytes(_json_bytes(document))

    assembly = load_verified_assembly(root)

    assert assembly.plan_sha256 == plan_sha256


def _alpha_spec(
    yes_polygon_ids: list[str],
    extra_labels: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    labels = [
        {"text_unit_key": "yes-alpha", "source_row_index": 0, "status": "yes"},
        {"text_unit_key": "no-alpha", "source_row_index": 1, "status": "no"},
        {"text_unit_key": "failed-alpha", "source_row_index": 2, "status": "failed"},
        {"text_unit_key": "skipped-alpha", "source_row_index": 3, "status": "skipped"},
    ]
    labels.extend(extra_labels or [])
    text_units = [
        {"text_unit_key": "yes-alpha", "polygon_ids": yes_polygon_ids},
        {"text_unit_key": "no-alpha", "polygon_ids": ["p-a"]},
        {"text_unit_key": "failed-alpha", "polygon_ids": ["p-a"]},
        {"text_unit_key": "skipped-alpha", "polygon_ids": ["p-missing", "p-invalid"]},
    ]
    polygons = [
        {"polygon_id": "p-a", "geometry_wkb": _rectangle_wkb(-0.1, -0.1, 0.1, 0.1)},
        {"polygon_id": "p-b", "geometry_wkb": _rectangle_wkb(-0.1, -0.1, 0.1, 0.1)},
        {"polygon_id": "p-missing", "geometry_wkb": None},
        {"polygon_id": "p-invalid", "geometry_wkb": b"not-wkb"},
    ]
    return _tables(labels, text_units, polygons, geometry_field="geometry_wkb")


def _beta_spec() -> dict[str, Any]:
    labels = [
        {"text_unit_key": f"{status}-beta", "source_row_index": i, "status": status}
        for i, status in enumerate(STATUSES)
    ]
    text_units = [
        {"text_unit_key": f"{status}-beta", "polygon_ids": ["p-a"]}
        for status in STATUSES
    ]
    geometry = json.dumps(
        {
            "type": "Polygon",
            "coordinates": [
                [[-0.1, -0.1], [0.1, -0.1], [0.1, 0.1], [-0.1, 0.1], [-0.1, -0.1]]
            ],
        },
        separators=(",", ":"),
    )
    polygons = [{"polygon_id": "p-a", "geometry": geometry}]
    return _tables(labels, text_units, polygons, geometry_field="geometry")


def _tables(
    labels: list[dict[str, Any]],
    text_units: list[dict[str, Any]],
    polygons: list[dict[str, Any]],
    *,
    geometry_field: str,
) -> dict[str, pa.Table]:
    label_table = pa.table(
        {
            "text_unit_key": pa.array(
                [row["text_unit_key"] for row in labels], type=pa.string()
            ),
            "source_row_index": pa.array(
                [row["source_row_index"] for row in labels], type=pa.int64()
            ),
            "status": pa.array([row["status"] for row in labels], type=pa.string()),
        }
    )
    text_table = pa.table(
        {
            "text_unit_key": pa.array(
                [row["text_unit_key"] for row in text_units], type=pa.string()
            ),
            "polygon_ids": pa.array(
                [row["polygon_ids"] for row in text_units],
                type=pa.list_(pa.string()),
            ),
        }
    )
    geometry_type = pa.binary() if geometry_field == "geometry_wkb" else pa.string()
    polygon_table = pa.table(
        {
            "polygon_id": pa.array(
                [row["polygon_id"] for row in polygons], type=pa.string()
            ),
            geometry_field: pa.array(
                [row[geometry_field] for row in polygons], type=geometry_type
            ),
        }
    )
    return {
        "labels": label_table,
        "text_units": text_table,
        "polygon_records": polygon_table,
        "documents": pa.table({"document_id": pa.array([], type=pa.string())}),
        "polygon_document_links": pa.table(
            {
                "document_id": pa.array([], type=pa.string()),
                "project": pa.array([], type=pa.string()),
                "polygon_id": pa.array([], type=pa.string()),
            }
        ),
    }


def _rectangle_wkb(xmin: float, ymin: float, xmax: float, ymax: float) -> bytes:
    coordinates = (
        (xmin, ymin),
        (xmax, ymin),
        (xmax, ymax),
        (xmin, ymax),
        (xmin, ymin),
    )
    ring = struct.pack("<I", len(coordinates)) + b"".join(
        struct.pack("<dd", x, y) for x, y in coordinates
    )
    return struct.pack("<BII", 1, 3, 1) + ring


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _json_bytes(value: Any) -> bytes:
    return _canonical_json(value) + b"\n"
