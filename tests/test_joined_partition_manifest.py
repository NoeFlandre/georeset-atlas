import hashlib
import json
from collections.abc import Callable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import atlas_io.joined_partitions as joined_partitions
from atlas_domain.joined_outputs import (
    JoinedPartition,
    join_description_partition,
    join_wiki_partition,
)
from atlas_domain.source_records import DataJoinError
from atlas_io.disk_joined import (
    _DescriptionLabels,
    _WebsitePolygons,
    _WikiSelectedRows,
)
from atlas_io.joined_partitions import (
    JoinedPartitionConflictError,
    JoinedPartitionResult,
    _table_schema,
    stage_table_groups,
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
    schema_provider: Callable[[], dict[str, pa.Schema]] | None = None,
) -> JoinedPartitionResult:
    return write_joined_partition(
        source_id=SOURCE_ID,
        repo_id=REPO_ID,
        revision=revision,
        path=path,
        output_dir=output_dir,
        build=build,
        schema_provider=schema_provider,
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


def test_resume_orphans_reuses_only_identical_partition_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "joined"
    first = _write(output_dir, CountingBuild())
    expected = _outputs(output_dir)
    first.manifest_path.unlink()
    build = CountingBuild()

    result = write_joined_partition(
        source_id=SOURCE_ID,
        repo_id=REPO_ID,
        revision=REVISION,
        path=PATH,
        output_dir=output_dir,
        build=build,
        resume_orphans=True,
    )

    assert result.status == "written"
    assert build.calls == 1
    assert _outputs(output_dir) == expected | {
        result.manifest_path.name: result.manifest_path.read_bytes()
    }


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


class CountingSchemas:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> dict[str, pa.Schema]:
        self.calls += 1
        return {}


def test_schema_provider_runs_after_build_on_a_new_write(tmp_path: Path) -> None:
    events: list[str] = []

    def build() -> JoinedPartition:
        events.append("build")
        return _partition()

    def schemas() -> dict[str, pa.Schema]:
        events.append("schemas")
        return {}

    _write(tmp_path / "joined", build, schema_provider=schemas)

    assert events == ["build", "schemas"]


def test_identical_rerun_does_not_call_the_schema_provider(tmp_path: Path) -> None:
    output_dir = tmp_path / "joined"
    build = CountingBuild()
    schemas = CountingSchemas()
    first = _write(output_dir, build, schema_provider=schemas)
    written = _outputs(output_dir)

    second = _write(output_dir, build, schema_provider=schemas)

    assert first.status == "written"
    assert second.status == "verified"
    assert schemas.calls == 1
    assert build.calls == 1
    assert _outputs(output_dir) == written


def test_null_only_list_elements_keep_the_exact_source_list_type(
    tmp_path: Path,
) -> None:
    list_type = pa.list_(
        pa.struct(
            [
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.string()),
            ]
        )
    )
    polygon_rows = [{**POLYGONS[0], "localized_descriptions": [None, None]}]
    result = _write(
        tmp_path / "joined",
        lambda: join_description_partition(
            label_rows=LABELS, text_unit_rows=TEXTS, polygon_rows=polygon_rows
        ),
        schema_provider=lambda: {
            "polygon_records": pa.schema([("localized_descriptions", list_type)])
        },
    )
    outputs = result.manifest["outputs"]
    polygon_records = result.manifest_path.parent / outputs["polygon_records"]["name"]
    schema = pq.ParquetFile(polygon_records).schema_arrow
    assert schema.field("localized_descriptions").type == list_type
    column = pq.read_table(polygon_records).column("localized_descriptions")
    assert column.to_pylist() == [[None, None]]


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
    original_write_batch = pq.ParquetWriter.write_batch
    writes: list[pa.RecordBatch] = []

    def fail_on_second_batch(writer: pq.ParquetWriter, batch: pa.RecordBatch) -> None:
        writes.append(batch)
        if len(writes) == 2:
            raise OSError("disk full")
        original_write_batch(writer, batch)

    monkeypatch.setattr(pq.ParquetWriter, "write_batch", fail_on_second_batch)

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
WIKIVOYAGE_PATH = "labels/wikivoyage/sentences/region-a.parquet"
NATIVE_WIKI_LINKS: list[dict[str, Any]] = [
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
]


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


def _native_wiki_partition(
    link_rows: list[dict[str, Any]] = NATIVE_WIKI_LINKS,
) -> JoinedPartition:
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
        link_rows=link_rows,
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


def _write_wiki_partition(
    output_dir: Path,
    path: str,
    build: Callable[[], JoinedPartition],
) -> JoinedPartitionResult:
    return write_joined_partition(
        source_id=WIKI_SOURCE_ID,
        repo_id=WIKI_REPO_ID,
        revision=WIKI_REVISION,
        path=path,
        output_dir=output_dir,
        build=build,
    )


def _link_files(output_dir: Path) -> list[str]:
    return sorted(
        path.name
        for path in output_dir.glob("*.parquet")
        if "polygon_document_links" in path.name
    )


def _unexpected_build() -> JoinedPartition:
    raise AssertionError("a verified rerun must not rebuild the partition")


def test_identical_wiki_links_share_one_shard_across_label_paths(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "joined"
    wikipedia = _write_wiki_partition(output_dir, WIKI_PATH, _native_wiki_partition)
    wikivoyage = _write_wiki_partition(
        output_dir, WIKIVOYAGE_PATH, _native_wiki_partition
    )
    written = _outputs(output_dir)

    assert len(_link_files(output_dir)) == 1
    names = {
        result.manifest["outputs"]["polygon_document_links"]["name"]
        for result in (wikipedia, wikivoyage)
    }
    assert names == set(_link_files(output_dir))

    rerun_wikipedia = _write_wiki_partition(output_dir, WIKI_PATH, _unexpected_build)
    rerun_wikivoyage = _write_wiki_partition(
        output_dir, WIKIVOYAGE_PATH, _unexpected_build
    )
    statuses = (rerun_wikipedia.status, rerun_wikivoyage.status)

    assert statuses == ("verified", "verified")
    assert _outputs(output_dir) == written


def test_distinct_wiki_links_get_distinct_content_addressed_shards(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "joined"
    wikipedia = _write_wiki_partition(output_dir, WIKI_PATH, _native_wiki_partition)
    preserved = _outputs(output_dir)

    wikivoyage = _write_wiki_partition(
        output_dir,
        WIKIVOYAGE_PATH,
        lambda: _native_wiki_partition(NATIVE_WIKI_LINKS[:2]),
    )

    links = [
        result.manifest["outputs"]["polygon_document_links"]
        for result in (wikipedia, wikivoyage)
    ]
    assert links[0]["name"] != links[1]["name"]
    assert sorted(link["name"] for link in links) == _link_files(output_dir)
    for link in links:
        assert link["sha256"] in link["name"]
    after = _outputs(output_dir)
    assert {name: after[name] for name in preserved} == preserved


def test_conflicting_shared_shard_is_preserved_and_no_manifest_is_written(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "joined"
    wikipedia = _write_wiki_partition(output_dir, WIKI_PATH, _native_wiki_partition)
    links = wikipedia.manifest["outputs"]["polygon_document_links"]
    shard = output_dir / links["name"]
    corrupted = bytearray(shard.read_bytes())
    corrupted[len(corrupted) // 2] ^= 0x01
    shard.write_bytes(bytes(corrupted))
    preserved = _outputs(output_dir)

    with pytest.raises(JoinedPartitionConflictError, match="sha256"):
        _write_wiki_partition(output_dir, WIKIVOYAGE_PATH, _native_wiki_partition)

    after = _outputs(output_dir)
    assert after == preserved
    assert len(list(output_dir.glob("*.manifest.json"))) == 1


def test_non_wiki_links_keep_the_per_partition_name(tmp_path: Path) -> None:
    result = _write(tmp_path / "joined", CountingBuild())

    assert result.manifest["outputs"]["polygon_document_links"]["name"] == (
        "description__labels-language-v1-data-region-a.polygon_document_links.parquet"
    )


class ScanCounter:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.scanned = 0

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for row in self.rows:
            self.scanned += 1
            yield row


def _polygon_row(polygon_id: str, description_identity: str) -> dict[str, Any]:
    return {
        "polygon_id": polygon_id,
        "description_identity": description_identity,
        "geometry_wkb": polygon_id.encode(),
    }


def test_polygon_records_index_follows_selected_rows_in_source_order() -> None:
    partition = join_description_partition(
        label_rows=LABELS,
        text_unit_rows=TEXTS,
        polygon_rows=[
            _polygon_row("polygon-a", "description-1"),
            _polygon_row("polygon-b", "description-2"),
            _polygon_row("polygon-c", "description-1"),
        ],
    )
    records = partition.polygon_records

    assert len(records) == 2
    assert records[0]["polygon_id"] == "polygon-a"
    assert records[1]["polygon_id"] == "polygon-c"
    assert records[-1]["polygon_id"] == "polygon-c"
    assert records[-2]["polygon_id"] == "polygon-a"
    with pytest.raises(IndexError):
        records[2]
    with pytest.raises(IndexError):
        records[-3]


def test_polygon_records_index_stops_scanning_at_the_requested_row() -> None:
    source = ScanCounter(
        [
            _polygon_row("polygon-a", "description-1"),
            _polygon_row("polygon-b", "description-1"),
            _polygon_row("polygon-c", "description-1"),
        ]
    )
    partition = join_description_partition(
        label_rows=LABELS, text_unit_rows=TEXTS, polygon_rows=source
    )
    source.scanned = 0

    assert partition.polygon_records[0]["polygon_id"] == "polygon-a"
    assert source.scanned == 1
    source.scanned = 0
    assert partition.polygon_records[1]["polygon_id"] == "polygon-b"
    assert source.scanned == 2


def test_staged_polygon_index_reads_only_the_row_group_holding_the_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "polygons.parquet"
    staged = stage_table_groups(
        [
            pa.table({"polygon_id": ["a", "b"], "geometry": [b"1", b"2"]}),
            pa.table({"polygon_id": ["c"], "geometry": [b"3"]}),
            pa.table({"polygon_id": ["d", "e", "f"], "geometry": [b"4", b"5", b"6"]}),
        ],
        path,
    )
    assert pq.ParquetFile(path).metadata.num_row_groups == 3
    original_read_row_group = pq.ParquetFile.read_row_group
    read_groups: list[int] = []

    def record_read(
        parquet_file: pq.ParquetFile, index: int, **options: Any
    ) -> pa.Table:
        read_groups.append(index)
        return original_read_row_group(parquet_file, index, **options)

    monkeypatch.setattr(pq.ParquetFile, "read_row_group", record_read)

    assert staged[3]["polygon_id"] == "d"
    assert staged[-1]["polygon_id"] == "f"
    assert read_groups == [2, 2]
    with pytest.raises(IndexError):
        staged[6]
    with pytest.raises(IndexError):
        staged[-7]


def test_staged_rows_expose_the_exact_schema_of_their_groups(tmp_path: Path) -> None:
    list_type = pa.list_(pa.string())
    group = pa.table({"localized_descriptions": pa.array([[None, None]], list_type)})
    staged = stage_table_groups([group], tmp_path / "text_units.parquet")

    assert staged.schema_arrow.field("localized_descriptions").type == list_type


def test_staged_rows_read_many_groups_row_groups_once_and_preserves_request_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "polygons.parquet"
    staged = stage_table_groups(
        [
            pa.table({"polygon_id": ["a", "b"]}),
            pa.table({"polygon_id": ["c", "d"]}),
            pa.table({"polygon_id": ["e", "f"]}),
        ],
        path,
    )
    original_read_row_group = pq.ParquetFile.read_row_group
    read_groups: list[int] = []

    def record_read(
        parquet_file: pq.ParquetFile, index: int, **options: Any
    ) -> pa.Table:
        read_groups.append(index)
        return original_read_row_group(parquet_file, index, **options)

    monkeypatch.setattr(pq.ParquetFile, "read_row_group", record_read)

    rows = staged.read_many([4, 0, 4, 2, 3])

    assert [row["polygon_id"] for row in rows] == ["e", "a", "e", "c", "d"]
    assert read_groups == [0, 1, 2]
    assert staged.read_many([]) == []


@pytest.mark.parametrize(
    ("indices", "error"),
    [([6], IndexError), ([-7], IndexError), ([1.5], TypeError)],
)
def test_staged_rows_read_many_rejects_invalid_indices(
    tmp_path: Path,
    indices: list[Any],
    error: type[Exception],
) -> None:
    staged = stage_table_groups(
        [pa.table({"polygon_id": ["a", "b"]})], tmp_path / "polygons.parquet"
    )

    with pytest.raises(error):
        staged.read_many(indices)


def test_staged_rows_reuses_a_cached_row_group_across_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "polygons.parquet"
    staged = stage_table_groups([pa.table({"polygon_id": ["a", "b", "c"]})], path)
    original_read_row_group = pq.ParquetFile.read_row_group
    read_groups: list[int] = []

    def record_read(
        parquet_file: pq.ParquetFile, index: int, **options: Any
    ) -> pa.Table:
        read_groups.append(index)
        return original_read_row_group(parquet_file, index, **options)

    monkeypatch.setattr(pq.ParquetFile, "read_row_group", record_read)

    rows = [staged.read_many([index])[0]["polygon_id"] for index in (1, 0, 2, 1)]

    assert rows == ["b", "a", "c", "b"]
    assert read_groups == [0]


def test_staged_rows_releases_row_groups_above_its_byte_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        joined_partitions, "_MAX_CACHED_ROW_GROUP_BYTES", 0, raising=False
    )
    path = tmp_path / "polygons.parquet"
    staged = stage_table_groups([pa.table({"polygon_id": ["a", "b", "c"]})], path)
    original_read_row_group = pq.ParquetFile.read_row_group
    read_groups: list[int] = []

    def record_read(
        parquet_file: pq.ParquetFile, index: int, **options: Any
    ) -> pa.Table:
        read_groups.append(index)
        return original_read_row_group(parquet_file, index, **options)

    monkeypatch.setattr(pq.ParquetFile, "read_row_group", record_read)

    assert staged.read_many([1])[0]["polygon_id"] == "b"
    assert staged.read_many([1])[0]["polygon_id"] == "b"
    assert read_groups == [0, 0]


def test_staged_rows_materializes_group_requests_in_bounded_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class RecordingTable:
        def __init__(self, table: pa.Table) -> None:
            self._table = table
            self.take_calls: list[pa.Array] = []

        def get_total_buffer_size(self) -> int:
            return self._table.get_total_buffer_size()

        def take(self, offsets: pa.Array) -> pa.Table:
            self.take_calls.append(offsets)
            return self._table.take(offsets)

        def slice(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("read_many must not slice rows individually")

    path = tmp_path / "rows.parquet"
    row_count = 19
    staged = stage_table_groups([pa.table({"row_index": list(range(row_count))})], path)
    original_read_row_group = pq.ParquetFile.read_row_group
    wrapped_tables: list[RecordingTable] = []

    def record_read(parquet_file: pq.ParquetFile, index: int, **options: Any) -> Any:
        wrapped = RecordingTable(
            original_read_row_group(parquet_file, index, **options)
        )
        wrapped_tables.append(wrapped)
        return wrapped

    monkeypatch.setattr(pq.ParquetFile, "read_row_group", record_read)
    indices = [(index * 7) % row_count for index in range(257)]

    rows = staged.read_many(indices)

    assert [row["row_index"] for row in rows] == indices
    assert len(wrapped_tables) == 1
    take_calls = wrapped_tables[0].take_calls
    assert [offsets.type for offsets in take_calls] == [pa.int64()] * 3
    assert [offsets.to_pylist() for offsets in take_calls] == [
        indices[:128],
        indices[128:256],
        indices[256:],
    ]


def test_schema_for_a_generic_lazy_source_keeps_later_only_fields() -> None:
    rows = _SchemaSampleRows(
        [
            {"source_values": None, "text": "First."},
            {"source_values": ["later"], "text": "Second.", "late_field": "kept"},
        ]
    )

    schema = _table_schema(
        "text_units", rows, pa.schema([("source_values", pa.list_(pa.string()))])
    )

    assert "late_field" in schema.names
    assert schema.field("late_field").type == pa.string()


def test_schema_for_a_generic_lazy_source_infers_after_null_first_value() -> None:
    rows = _SchemaSampleRows(
        [
            {"optional_value": None},
            {"optional_value": "available later"},
        ]
    )

    schema = _table_schema(
        "text_units", rows, pa.schema([("unrelated_source_column", pa.string())])
    )

    assert schema.field("optional_value").type == pa.string()


def test_description_label_source_declares_native_and_derived_schema(
    tmp_path: Path,
) -> None:
    labels = stage_table_groups(
        [pa.table({"decision": ["yes"], "optional_native": [None]})],
        tmp_path / "labels.parquet",
    )
    rows = _DescriptionLabels(labels)

    schema = _table_schema("labels", rows, labels.schema_arrow)

    assert schema.names == [
        "optional_native",
        "source_row_index",
        "raw_decision",
        "status",
    ]
    assert schema.field("optional_native").type == pa.null()
    assert schema.field("source_row_index").type == pa.int64()
    assert schema.field("raw_decision").type == pa.string()
    assert schema.field("status").type == pa.string()


def test_passthrough_lazy_sources_exclude_the_internal_row_key(
    tmp_path: Path,
) -> None:
    staged = stage_table_groups(
        [pa.table({"text_unit_key": ["internal"], "native_field": ["kept"]})],
        tmp_path / "rows.parquet",
    )
    website_rows = _WebsitePolygons(tmp_path / "website.sqlite", staged, 1)
    wiki_rows = _WikiSelectedRows(tmp_path / "wiki.sqlite", staged, "selected_rows", 1)

    website_schema = _table_schema("polygon_records", website_rows, staged.schema_arrow)
    wiki_schema = _table_schema("documents", wiki_rows, staged.schema_arrow)

    assert website_schema.names == ["native_field"]
    assert wiki_schema.names == ["native_field"]


def test_schema_for_a_lazy_source_uses_explicit_typed_metadata() -> None:
    source_type = pa.list_(pa.string())
    rows = _SchemaSampleRows(
        [
            {
                "text_unit_key": ("identity", "name", 0),
                "source_values": None,
                "sentence_index": 0,
                "text": "First.",
                "full_text": None,
                "language": None,
                "sentence_status": "success",
                "polygon_ids": ["polygon-1"],
            },
            {
                "text_unit_key": ("identity", "name", 1),
                "source_values": ["later"],
                "sentence_index": 1,
                "text": "Second.",
                "full_text": "First. Second.",
                "language": "eng",
                "sentence_status": "success",
                "polygon_ids": ["polygon-1"],
            },
        ]
    )
    rows.output_schema = pa.schema(
        [
            pa.field("source_values", source_type),
            pa.field("sentence_index", pa.int64()),
            pa.field("text", pa.string()),
            pa.field("full_text", pa.string()),
            pa.field("language", pa.string()),
            pa.field("sentence_status", pa.string()),
            pa.field("polygon_ids", pa.list_(pa.string())),
        ]
    )

    schema = _table_schema(
        "text_units", rows, pa.schema([("source_values", source_type)])
    )

    assert rows.accessed == []
    assert schema.names == [
        "source_values",
        "sentence_index",
        "text",
        "full_text",
        "language",
        "sentence_status",
        "polygon_ids",
    ]
    assert schema.field("source_values").type == source_type
    assert schema.field("sentence_index").type == pa.int64()
    assert schema.field("text").type == pa.string()
    assert schema.field("full_text").type == pa.string()
    assert schema.field("language").type == pa.string()
    assert schema.field("sentence_status").type == pa.string()
    assert schema.field("polygon_ids").type == pa.list_(pa.string())


class _SchemaSampleRows:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.accessed: list[int] = []
        self.output_schema: pa.Schema | None = None

    def __len__(self) -> int:
        return len(self._rows)

    def __getitem__(self, index: int) -> dict[str, Any]:
        self.accessed.append(index)
        return self._rows[index]

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for index in range(len(self)):
            yield self[index]
