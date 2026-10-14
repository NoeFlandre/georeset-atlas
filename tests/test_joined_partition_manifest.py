import hashlib
import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from atlas_domain.joined_outputs import (
    JoinedPartition,
    join_description_partition,
    join_wiki_partition,
)
from atlas_domain.source_records import DataJoinError
from atlas_io.joined_partitions import (
    JoinedPartitionConflictError,
    JoinedPartitionResult,
    write_joined_partition,
)

SOURCE_ID = "description"
REPO_ID = "NoeFlandre/osm-polygon-description-tag-landuse"
REVISION = "ce98b328f3c6543800a3d931877b51eb3f0cf768"
OTHER_REVISION = "e6524c711ee0033c3cd5990e5fdbbe4b89992dfb"
TABLE_ROOT = "labels/language-v1/data"
PATH = f"{TABLE_ROOT}/region-a.parquet"
OUTPUT_TABLES = {
    "labels",
    "text_units",
    "polygon_records",
    "documents",
    "polygon_document_links",
}
MANIFEST_KEYS = {
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
LABELS: list[dict[str, Any]] = [
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "yes",
    },
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
        "decision": "skipped_unsplit",
    },
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 1,
        "decision": "failed",
    },
]
TEXTS: list[dict[str, Any]] = [
    {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "split",
        "original_text": "First. Second.",
        "sentences": ["First.", "Second."],
        "source_url": "https://www.openstreetmap.org/way/1",
        "raw_language_code": "en-GB",
    },
]
POLYGONS: list[dict[str, Any]] = [
    {
        "polygon_id": "polygon-1",
        "description_identity": "description-1",
        "geometry_wkb": b"geometry-1",
    },
]


def _partition() -> JoinedPartition:
    return join_description_partition(
        label_rows=LABELS, text_unit_rows=TEXTS, polygon_rows=POLYGONS
    )


class CountingBuild:
    """Stands in for the source reader; each call is one round of new reads."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> JoinedPartition:
        self.calls += 1
        return _partition()


def _write(
    output_dir: Path,
    build: Callable[[], JoinedPartition],
    *,
    revision: str = REVISION,
    path: str = PATH,
) -> JoinedPartitionResult:
    return write_joined_partition(
        source_id=SOURCE_ID,
        repo_id=REPO_ID,
        revision=revision,
        path=path,
        output_dir=output_dir,
        build=build,
    )


def _outputs(output_dir: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in sorted(output_dir.iterdir())}


def _manifest_path(output_dir: Path) -> Path:
    (manifest,) = output_dir.glob("*.manifest.json")
    return manifest


def test_first_run_writes_every_table_and_a_complete_manifest(tmp_path: Path) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()

    result = _write(output_dir, build)

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert result.status == "written"
    assert build.calls == 1
    assert set(manifest) == MANIFEST_KEYS
    assert manifest["manifest_version"] == 1
    assert (manifest["source_id"], manifest["repo_id"]) == (SOURCE_ID, REPO_ID)
    assert manifest["revision"] == REVISION
    assert (manifest["path"], manifest["table_root"]) == (PATH, TABLE_ROOT)
    assert manifest["label_row_count"] == len(LABELS)
    assert manifest["row_counts"] == {
        "labels": 3,
        "text_units": 2,
        "polygon_records": 1,
        "documents": 0,
        "polygon_document_links": 0,
    }
    assert manifest["status_counts"] == {"yes": 1, "no": 0, "failed": 1, "skipped": 1}
    assert manifest["raw_decision_counts"] == {
        "yes": 1,
        "skipped_unsplit": 1,
        "failed": 1,
    }
    assert set(manifest["outputs"]) == OUTPUT_TABLES
    for output in manifest["outputs"].values():
        file = output_dir / output["name"]
        assert file.is_file()
        assert output["size_bytes"] == file.stat().st_size
        assert output["sha256"] == hashlib.sha256(file.read_bytes()).hexdigest()
        assert output["row_count"] == pq.ParquetFile(file).metadata.num_rows


def test_identical_rerun_verifies_the_manifest_and_skips_new_reads(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()
    first = _write(output_dir, build)
    written = _outputs(output_dir)

    second = _write(output_dir, build)

    assert second.status == "verified"
    assert second.manifest == first.manifest
    assert build.calls == 1
    assert _outputs(output_dir) == written


@pytest.mark.parametrize(
    ("revision", "path"),
    [
        (OTHER_REVISION, PATH),
        (REVISION, "polygons/region-a.parquet"),
        (REVISION, f"{TABLE_ROOT}/../region-a.parquet"),
        (REVISION, f"{TABLE_ROOT}/region-a.csv"),
    ],
)
def test_writer_rejects_unpinned_or_undeclared_inputs_before_any_read(
    tmp_path: Path,
    revision: str,
    path: str,
) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()

    with pytest.raises(ValueError):
        _write(output_dir, build, revision=revision, path=path)

    assert build.calls == 0
    assert not output_dir.exists()


def test_a_failed_join_publishes_no_outputs_or_manifest(tmp_path: Path) -> None:
    output_dir = tmp_path / "joined"

    def failing_build() -> JoinedPartition:
        raise DataJoinError("label has no text occurrence")

    with pytest.raises(DataJoinError):
        _write(output_dir, failing_build)

    assert not output_dir.exists() or list(output_dir.iterdir()) == []


def test_missing_output_with_a_manifest_is_a_conflict_and_nothing_is_rewritten(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()
    _write(output_dir, build)
    next(output_dir.glob("*.text_units.parquet")).unlink()
    preserved = _outputs(output_dir)

    with pytest.raises(JoinedPartitionConflictError, match="missing"):
        _write(output_dir, build)

    assert _outputs(output_dir) == preserved
    assert build.calls == 1


def test_corrupt_output_is_a_conflict_and_is_preserved(tmp_path: Path) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()
    _write(output_dir, build)
    labels = next(output_dir.glob("*.labels.parquet"))
    corrupted = bytearray(labels.read_bytes())
    corrupted[len(corrupted) // 2] ^= 0x01
    labels.write_bytes(bytes(corrupted))
    preserved = _outputs(output_dir)

    with pytest.raises(JoinedPartitionConflictError, match="sha256"):
        _write(output_dir, build)

    assert _outputs(output_dir) == preserved
    assert build.calls == 1


def test_outputs_without_a_manifest_are_an_orphan_conflict(tmp_path: Path) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()
    _write(output_dir, build)
    _manifest_path(output_dir).unlink()
    preserved = _outputs(output_dir)

    with pytest.raises(JoinedPartitionConflictError, match="orphan"):
        _write(output_dir, build)

    assert _outputs(output_dir) == preserved
    assert build.calls == 1


@pytest.mark.parametrize("contents", ["{", "[]"])
def test_corrupt_manifest_is_a_conflict_and_is_preserved(
    tmp_path: Path,
    contents: str,
) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()
    _write(output_dir, build)
    _manifest_path(output_dir).write_text(contents, encoding="utf-8")
    preserved = _outputs(output_dir)

    with pytest.raises(JoinedPartitionConflictError, match="manifest"):
        _write(output_dir, build)

    assert _outputs(output_dir) == preserved
    assert build.calls == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("manifest_version", 2),
        ("source_id", "website"),
        ("repo_id", "NoeFlandre/other-dataset"),
        ("revision", OTHER_REVISION),
        ("path", f"{TABLE_ROOT}/region-b.parquet"),
        ("table_root", "labels/other/root"),
        ("label_row_count", 99),
    ],
)
def test_manifest_identity_mismatch_is_a_conflict(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()
    _write(output_dir, build)
    manifest = json.loads(_manifest_path(output_dir).read_text(encoding="utf-8"))
    manifest[field] = value
    _manifest_path(output_dir).write_text(json.dumps(manifest), encoding="utf-8")
    preserved = _outputs(output_dir)

    with pytest.raises(JoinedPartitionConflictError):
        _write(output_dir, build)

    assert _outputs(output_dir) == preserved
    assert build.calls == 1


def test_manifest_row_count_that_disagrees_with_its_parquet_is_a_conflict(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()
    _write(output_dir, build)
    manifest = json.loads(_manifest_path(output_dir).read_text(encoding="utf-8"))
    manifest["outputs"]["labels"]["row_count"] = 99
    _manifest_path(output_dir).write_text(json.dumps(manifest), encoding="utf-8")
    preserved = _outputs(output_dir)

    with pytest.raises(JoinedPartitionConflictError, match="row count"):
        _write(output_dir, build)

    assert _outputs(output_dir) == preserved
    assert build.calls == 1


def test_interrupted_publish_leaves_an_orphan_that_is_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()
    original_write_table = pq.write_table
    writes: list[str] = []

    def fail_on_second_table(table: pa.Table, handle: Any, **options: Any) -> None:
        writes.append("table")
        if len(writes) == 2:
            raise OSError("disk full")
        original_write_table(table, handle, **options)

    monkeypatch.setattr(pq, "write_table", fail_on_second_table)

    with pytest.raises(OSError, match="disk full"):
        _write(output_dir, build)

    assert list(output_dir.glob("*.manifest.json")) == []
    assert list(output_dir.glob("*.tmp")) == []
    preserved = _outputs(output_dir)
    with pytest.raises(JoinedPartitionConflictError, match="orphan"):
        _write(output_dir, build)
    assert _outputs(output_dir) == preserved


WIKI_SOURCE_ID = "wiki"
WIKI_REPO_ID = "NoeFlandre/osm-polygon-wikidata-and-wikipedia-landuse"
WIKI_REVISION = "49307aeb05b6dcc8a5d393f77f59a1e6d13a967a"
WIKI_PATH = "labels/wikipedia/sentences/region-a.parquet"


def _wiki_partition() -> JoinedPartition:
    return join_wiki_partition(
        label_rows=[{"sentence_id": "sentence-1", "decision": "yes"}],
        sentence_rows=[
            {
                "sentence_id": "sentence-1",
                "document_id": "document-1",
                "project": "project-a",
                "sentence_text": "Alpha.",
                "raw_language_code": "en",
            }
        ],
        document_rows=[
            {
                "document_id": "document-1",
                "project": "project-a",
                "title": "Alpha",
                "page_id": 42,
                "updated_at": datetime(2026, 1, 2, 3, 4, 5),
                "source_url": "https://en.wikipedia.org/wiki/Alpha",
            }
        ],
        link_rows=[
            {
                "document_id": "document-1",
                "project": "project-a",
                "polygon_id": "polygon-1",
            }
        ],
        polygon_rows=[{"polygon_id": "polygon-1", "geometry_wkb": b"geometry-1"}],
    )


def test_documents_are_written_with_native_typed_columns(tmp_path: Path) -> None:
    result = write_joined_partition(
        source_id=WIKI_SOURCE_ID,
        repo_id=WIKI_REPO_ID,
        revision=WIKI_REVISION,
        path=WIKI_PATH,
        output_dir=tmp_path / "joined",
        build=_wiki_partition,
    )

    documents = result.manifest["outputs"]["documents"]
    file = result.manifest_path.parent / documents["name"]
    schema = pq.ParquetFile(file).schema_arrow
    assert schema.field("page_id").type == pa.int64()
    assert schema.field("updated_at").type == pa.timestamp("us")
    assert pq.read_table(file).column("source_url").to_pylist() == [
        "https://en.wikipedia.org/wiki/Alpha"
    ]


def _native_wiki_partition() -> JoinedPartition:
    return join_wiki_partition(
        label_rows=[
            {"sentence_id": "sentence-1", "decision": "yes", "language": "en"},
        ],
        sentence_rows=[
            {
                "sentence_id": "sentence-1",
                "document_id": "document-1",
                "project": "wikipedia",
                "language": "en",
                "text": "Alpha.",
                "license": "CC BY-SA 4.0",
                "attribution": "Wikipedia contributors",
            }
        ],
        document_rows=[
            {
                "document_id": "document-1",
                "project": "wikipedia",
                "title": "Alpha",
                "url": "https://en.wikipedia.org/wiki/Alpha",
                "page_id": 42,
                "revision_id": 1234567,
                "license": "CC BY-SA 4.0",
                "attribution": "Wikipedia contributors",
            }
        ],
        link_rows=[
            {
                "document_id": "document-1",
                "project": "wikipedia",
                "polygon_id": "polygon-1",
                "input_revision": "rev-1",
            },
            {
                "document_id": "document-1",
                "project": "wikipedia",
                "polygon_id": "polygon-2",
                "input_revision": "rev-1",
            },
            {
                "document_id": "document-1",
                "project": "wikipedia",
                "polygon_id": "polygon-1",
                "input_revision": "rev-2",
            },
        ],
        polygon_rows=[
            {
                "polygon_id": "polygon-1",
                "geometry": '{"type":"Point","coordinates":[69.2,34.5]}',
            },
            {
                "polygon_id": "polygon-2",
                "geometry": '{"type":"Point","coordinates":[69.1,34.4]}',
            },
        ],
    )


def _write_native_wiki(tmp_path: Path) -> JoinedPartitionResult:
    return write_joined_partition(
        source_id=WIKI_SOURCE_ID,
        repo_id=WIKI_REPO_ID,
        revision=WIKI_REVISION,
        path=WIKI_PATH,
        output_dir=tmp_path / "joined",
        build=_native_wiki_partition,
    )


def test_polygon_document_links_are_persisted_for_every_source_occurrence(
    tmp_path: Path,
) -> None:
    result = _write_native_wiki(tmp_path)

    links = result.manifest["outputs"]["polygon_document_links"]
    table = pq.read_table(result.manifest_path.parent / links["name"])
    polygon_ids = table.column("polygon_id").to_pylist()
    revisions = table.column("input_revision").to_pylist()
    assert result.manifest["row_counts"]["polygon_document_links"] == 3
    assert links["row_count"] == 3
    assert sorted(zip(polygon_ids, revisions, strict=True)) == [
        ("polygon-1", "rev-1"),
        ("polygon-1", "rev-2"),
        ("polygon-2", "rev-1"),
    ]


def test_native_geometry_strings_are_persisted_unchanged(tmp_path: Path) -> None:
    result = _write_native_wiki(tmp_path)

    polygons = result.manifest["outputs"]["polygon_records"]
    table = pq.read_table(result.manifest_path.parent / polygons["name"])
    geometries = table.column("geometry").to_pylist()
    assert sorted(geometries) == [
        '{"type":"Point","coordinates":[69.1,34.4]}',
        '{"type":"Point","coordinates":[69.2,34.5]}',
    ]
