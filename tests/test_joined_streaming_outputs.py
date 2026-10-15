import json
import re
import tracemalloc
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.request import Request

import pyarrow as pa
import pyarrow.parquet as pq

import atlas_domain.joined_outputs as joined_outputs
from atlas_domain.joined_outputs import join_description_partition
from atlas_io.joined_cli import EXIT_JOIN_ERROR, main
from atlas_io.joined_partitions import write_joined_partition

REPO_ID = "NoeFlandre/osm-polygon-description-tag-landuse"
REVISION = "ce98b328f3c6543800a3d931877b51eb3f0cf768"
PATH = "labels/language-v1/data/region-a.parquet"
LABEL_ROWS = 25_000


def test_output_tables_stream_in_bounded_record_batches(tmp_path: Path) -> None:
    labels: list[dict[str, Any]] = [
        {
            "description_identity": "description-1",
            "tag_key": "name",
            "sentence_index": 0,
            "decision": "yes",
        }
        for _ in range(LABEL_ROWS)
    ]
    texts: list[dict[str, Any]] = [
        {
            "description_identity": "description-1",
            "tag_key": "name",
            "split_status": "unsplit",
            "original_text": "First.",
            "sentences": ["First."],
            "source_url": "https://www.openstreetmap.org/way/1",
            "raw_language_code": "en-GB",
        }
    ]
    polygons: list[dict[str, Any]] = [
        {
            "polygon_id": "polygon-1",
            "description_identity": "description-1",
            "geometry_wkb": b"geometry-1",
        }
    ]
    output_dir = tmp_path / "joined"
    result = write_joined_partition(
        source_id="description",
        repo_id=REPO_ID,
        revision=REVISION,
        path=PATH,
        output_dir=output_dir,
        build=lambda: join_description_partition(
            label_rows=labels, text_unit_rows=texts, polygon_rows=polygons
        ),
    )

    labels_path = output_dir / result.manifest["outputs"]["labels"]["name"]
    labels_file = pq.ParquetFile(labels_path)
    assert labels_file.metadata.num_rows == LABEL_ROWS
    assert labels_file.metadata.num_row_groups > 1


LABELS_PATH = "labels/language-v1/data/region-a.parquet"
TEXT_UNITS_PATH = "language-v1/data/region-a.parquet"
POLYGONS_PATH = "data/region-a.parquet"
WEBSITE_LABELS_PATH = "labels/polygons/region-a.parquet"
WEBSITE_POLYGONS_PATH = "polygons/region-a.parquet"
POLYGON_COUNT = 2_000
POLYGON_BYTES = 8_192
ROW_GROUP_ROWS = 250
MAX_BYTES = 50_000_000
TRACED_PEAK_LIMIT = 12 * 1024 * 1024


class FakeHeaders:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    def get(self, name: str, /) -> str | None:
        return self._values.get(name)


class FakeHubResponse:
    def __init__(self, body: bytes, status: int, headers: dict[str, str]) -> None:
        self._body = BytesIO(body)
        self.status = status
        self.headers = FakeHeaders(headers)

    def read(self, size: int = -1, /) -> bytes:
        return self._body.read(size)

    def close(self) -> None:
        self._body.close()


class FakeHub:
    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files

    def __call__(self, request: Request) -> FakeHubResponse:
        path = request.full_url.split("/resolve/", 1)[1].split("/", 1)[1]
        payload = self._files[path]
        if request.get_method() == "HEAD":
            return FakeHubResponse(b"", 200, {"Content-Length": str(len(payload))})
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", request.get_header("Range") or "")
        assert match is not None
        start, end = (int(value) for value in match.groups())
        body = payload[start : end + 1]
        return FakeHubResponse(
            body,
            206,
            {
                "Content-Range": f"bytes {start}-{end}/{len(payload)}",
                "Content-Length": str(len(body)),
            },
        )


def _parquet(table: pa.Table, *, row_group_size: int | None = None) -> bytes:
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="zstd", row_group_size=row_group_size)
    return buffer.getvalue()


def _labels_parquet(identity: str = "description-1") -> bytes:
    return _parquet(
        pa.table(
            {
                "description_identity": [identity] * 3,
                "tag_key": ["name"] * 3,
                "sentence_index": pa.array([0, 0, 0], pa.int64()),
                "decision": ["yes", "no", "skipped_unsplit"],
            }
        )
    )


def _text_units_parquet(*, localized: pa.Array | None = None) -> bytes:
    columns: dict[str, Any] = {
        "description_identity": ["description-1"],
        "source_pbf": ["afghanistan-latest.osm.pbf"],
        "osm_type": ["way"],
        "osm_id": pa.array([1], pa.int64()),
        "tag_key": ["name"],
        "split_status": ["split"],
        "original_text": ["First."],
        "language_code": ["en"],
        "sentences": [["First."]],
    }
    if localized is not None:
        columns["localized_descriptions"] = localized
    return _parquet(pa.table(columns))


def _polygons_parquet(*, localized: pa.Array | None = None) -> bytes:
    columns: dict[str, Any] = {
        "polygon_id": [f"polygon-{index}" for index in range(POLYGON_COUNT)],
        "source_pbf": ["afghanistan-latest.osm.pbf"] * POLYGON_COUNT,
        "osm_type": ["way"] * POLYGON_COUNT,
        "osm_id": pa.array([1] * POLYGON_COUNT, pa.int64()),
        "geometry": pa.array(
            [bytes(POLYGON_BYTES) for _ in range(POLYGON_COUNT)], pa.binary()
        ),
    }
    if localized is not None:
        columns["localized_descriptions"] = localized
    return _parquet(
        pa.table(columns),
        row_group_size=ROW_GROUP_ROWS,
    )


def _hub(
    *,
    labels: bytes | None = None,
    text_units: bytes | None = None,
    polygons: bytes | None = None,
) -> FakeHub:
    return FakeHub(
        {
            LABELS_PATH: labels if labels is not None else _labels_parquet(),
            TEXT_UNITS_PATH: (
                text_units if text_units is not None else _text_units_parquet()
            ),
            POLYGONS_PATH: polygons if polygons is not None else _polygons_parquet(),
        }
    )


def _argv(output_dir: Path) -> list[str]:
    return [
        "--source",
        "description",
        "--labels",
        LABELS_PATH,
        "--max-bytes",
        str(MAX_BYTES),
        "--output-dir",
        str(output_dir),
    ]


def _website_argv(output_dir: Path) -> list[str]:
    return [
        "--source",
        "website",
        "--labels",
        WEBSITE_LABELS_PATH,
        "--max-bytes",
        str(MAX_BYTES),
        "--output-dir",
        str(output_dir),
    ]


def test_join_working_set_stays_bounded_by_row_groups_and_output_batches(
    tmp_path: Path,
) -> None:
    hub = _hub()
    tracemalloc.start()
    try:
        exit_code = main(_argv(tmp_path / "joined"), opener=hub)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert exit_code == 0
    assert peak < TRACED_PEAK_LIMIT, f"traced peak was {peak} bytes"


def test_cli_description_joins_fixed_batches_and_preserves_partition_order(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    label_count = 1_027
    label_ids = [f"label-{index}" for index in range(label_count)]
    sentence_indices = [index % 2 for index in range(label_count)]
    decisions = [
        ("yes", "no", "failed", "skipped_unsplit")[index % 4]
        for index in range(label_count)
    ]
    labels = pa.table(
        {
            "label_id": label_ids,
            "description_identity": ["description-1"] * label_count,
            "tag_key": ["name"] * label_count,
            "sentence_index": pa.array(sentence_indices, pa.int32()),
            "decision": decisions,
            "confidence": pa.array([0.75] * label_count, pa.float32()),
        }
    )
    localized_type = pa.list_(
        pa.struct(
            [
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.string()),
            ]
        )
    )
    texts = pa.table(
        {
            "description_identity": ["description-1"],
            "source_pbf": ["afghanistan-latest.osm.pbf"],
            "osm_type": ["way"],
            "osm_id": pa.array([1], pa.int64()),
            "tag_key": ["name"],
            "split_status": ["split"],
            "original_text": ["First. Second."],
            "sentences": [["First.", "Second."]],
            "localized_descriptions": pa.array([[None, None]], localized_type),
        }
    )
    polygons = pa.table(
        {
            "polygon_id": ["polygon-a", "polygon-unused", "polygon-b"],
            "source_pbf": ["afghanistan-latest.osm.pbf"] * 3,
            "osm_type": ["way"] * 3,
            "osm_id": pa.array([1, 2, 1], pa.int64()),
            "geometry": [b"geometry-a", b"geometry-unused", b"geometry-b"],
            "localized_descriptions": pa.array(
                [[None], [None], [None]], localized_type
            ),
        }
    )
    call_sizes: list[int] = []
    original_join = joined_outputs.join_description_partition

    def tracked_join(**kwargs: Any) -> Any:
        call_sizes.append(len(kwargs["label_rows"]))
        return original_join(**kwargs)

    monkeypatch.setattr(joined_outputs, "join_description_partition", tracked_join)
    output_dir = tmp_path / "joined"

    assert (
        main(
            _argv(output_dir),
            opener=_hub(
                labels=_parquet(labels, row_group_size=127),
                text_units=_parquet(texts),
                polygons=_parquet(polygons, row_group_size=1),
            ),
        )
        == 0
    )

    assert len(call_sizes) > 1
    assert max(call_sizes) <= 1_024
    manifest = json.loads(next(output_dir.glob("*.manifest.json")).read_text())
    assert manifest["row_counts"] == {
        "labels": label_count,
        "text_units": 2,
        "polygon_records": 2,
        "documents": 0,
        "polygon_document_links": 0,
    }
    assert manifest["status_counts"] == {
        "yes": 257,
        "no": 257,
        "failed": 257,
        "skipped": 256,
    }
    assert manifest["raw_decision_counts"] == {
        "failed": 257,
        "no": 257,
        "skipped_unsplit": 256,
        "yes": 257,
    }
    output_labels = pq.read_table(output_dir / manifest["outputs"]["labels"]["name"])
    assert output_labels.column("label_id").to_pylist() == label_ids
    assert output_labels.column("source_row_index").to_pylist() == list(
        range(label_count)
    )
    assert output_labels.column("raw_decision").to_pylist()[:3] == [
        "yes",
        "no",
        "failed",
    ]
    assert output_labels.schema.field("sentence_index").type == pa.int32()
    assert output_labels.schema.field("confidence").type == pa.float32()
    output_texts = pq.read_table(output_dir / manifest["outputs"]["text_units"]["name"])
    assert output_texts.column("text").to_pylist() == ["First.", "Second."]
    assert output_texts.column("polygon_ids").to_pylist() == [
        ["polygon-a", "polygon-b"],
        ["polygon-a", "polygon-b"],
    ]
    assert output_texts.schema.field("localized_descriptions").type == localized_type
    output_polygons = pq.read_table(
        output_dir / manifest["outputs"]["polygon_records"]["name"]
    )
    assert output_polygons.column("polygon_id").to_pylist() == [
        "polygon-a",
        "polygon-b",
    ]
    assert output_polygons.column("geometry").to_pylist() == [
        b"geometry-a",
        b"geometry-b",
    ]
    assert output_polygons.schema.field("localized_descriptions").type == localized_type


def test_atlas_join_routes_website_through_bounded_disk_batches(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    label_count = 513
    label_ids = [f"website-label-{index}" for index in range(label_count)]
    decisions = [
        ("yes", "no", "failed", "skipped_unsplit")[index % 4]
        for index in range(label_count)
    ]
    labels = pa.table(
        {
            "label_id": label_ids,
            "polygon_id": ["website-way-1"] * label_count,
            "field": ["website"] * label_count,
            "sentence_index": pa.array([0] * label_count, pa.int32()),
            "decision": decisions,
            "language": ["en-GB"] * label_count,
            "reviewer": [f"reviewer-{index % 5}" for index in range(label_count)],
            "confidence": pa.array([0.75] * label_count, pa.float32()),
        }
    )
    website_text = "  Clinic hours. Call first.\n"
    geometry = '{"type":"Point","coordinates":[69.2075,34.5553]}'
    polygons = pa.table(
        {
            "polygon_id": ["website-way-1"],
            "source_pbf": ["afghanistan-latest.osm.pbf"],
            "osm_type": ["way"],
            "osm_id": pa.array([1], pa.int64()),
            "website_text": [website_text],
            "website_sentences": [["Clinic hours.", "Call first."]],
            "website_language": ["en-GB"],
            "website_sentence_status": ["split"],
            "contact_website_text": ["Contact by phone."],
            "contact_website_sentences": pa.array([None], pa.list_(pa.string())),
            "contact_website_language": ["fr"],
            "contact_website_sentence_status": ["unsupported_language"],
            "geometry": [geometry],
            "lat": pa.array([34.5553], pa.float64()),
            "lon": pa.array([69.2075], pa.float64()),
        }
    )
    call_sizes: list[int] = []
    original_join = joined_outputs.join_website_partition

    def tracked_join(**kwargs: Any) -> Any:
        batch_labels = list(kwargs["label_rows"])
        if batch_labels:
            call_sizes.append(len(batch_labels))
        return original_join(
            label_rows=batch_labels,
            polygon_text_rows=kwargs["polygon_text_rows"],
        )

    monkeypatch.setattr(joined_outputs, "join_website_partition", tracked_join)
    output_dir = tmp_path / "website-joined"
    hub = FakeHub(
        {
            WEBSITE_LABELS_PATH: _parquet(labels, row_group_size=73),
            WEBSITE_POLYGONS_PATH: _parquet(polygons),
        }
    )

    assert main(_website_argv(output_dir), opener=hub) == 0

    assert call_sizes[:2] == [512, 1]
    assert max(call_sizes) <= 512
    manifest = json.loads(next(output_dir.glob("*.manifest.json")).read_text())
    assert manifest["row_counts"] == {
        "labels": label_count,
        "text_units": 1,
        "polygon_records": 1,
        "documents": 0,
        "polygon_document_links": 0,
    }
    assert manifest["status_counts"] == {
        "yes": 129,
        "no": 128,
        "failed": 128,
        "skipped": 128,
    }
    output_labels = pq.read_table(output_dir / manifest["outputs"]["labels"]["name"])
    assert output_labels.column("label_id").to_pylist() == label_ids
    assert output_labels.column("source_row_index").to_pylist() == list(
        range(label_count)
    )
    assert (
        output_labels.column("polygon_id").to_pylist()
        == ["website-way-1"] * label_count
    )
    assert output_labels.column("field").to_pylist() == ["website"] * label_count
    assert output_labels.column("raw_decision").to_pylist() == decisions
    assert output_labels.column("language").to_pylist() == ["en-GB"] * label_count
    assert output_labels.column("reviewer").to_pylist() == [
        f"reviewer-{index % 5}" for index in range(label_count)
    ]
    assert output_labels.schema.field("confidence").type == pa.float32()
    output_texts = pq.read_table(output_dir / manifest["outputs"]["text_units"]["name"])
    assert output_texts.column("text").to_pylist() == ["Clinic hours."]
    assert output_texts.column("full_text").to_pylist() == [website_text]
    assert output_texts.column("sentence_status").to_pylist() == ["split"]
    output_polygons = pq.read_table(
        output_dir / manifest["outputs"]["polygon_records"]["name"]
    )
    assert output_polygons.column("polygon_id").to_pylist() == ["website-way-1"]
    assert output_polygons.column("website_text").to_pylist() == [website_text]
    assert output_polygons.column("website_sentences").to_pylist() == [
        ["Clinic hours.", "Call first."]
    ]
    assert output_polygons.column("contact_website_sentences").to_pylist() == [None]
    assert output_polygons.column("geometry").to_pylist() == [geometry]
    assert set(polygons.column_names) <= set(output_polygons.schema.names)
    for column in polygons.column_names:
        assert (
            output_polygons.column(column).to_pylist()
            == polygons.column(column).to_pylist()
        )


def test_cli_description_keeps_derived_polygon_ids(tmp_path: Path) -> None:
    polygons = pa.table(
        {
            "source_pbf": ["afghanistan-latest.osm.pbf"],
            "osm_type": ["way"],
            "osm_id": pa.array([1], pa.int64()),
            "geometry": [b"geometry-1"],
        }
    )
    output_dir = tmp_path / "joined"

    assert (
        main(
            _argv(output_dir),
            opener=_hub(polygons=_parquet(polygons)),
        )
        == 0
    )

    manifest = json.loads(next(output_dir.glob("*.manifest.json")).read_text())
    output = pq.read_table(
        output_dir / manifest["outputs"]["polygon_records"]["name"]
    ).to_pylist()
    assert output == [
        {
            "source_pbf": "afghanistan-latest.osm.pbf",
            "osm_type": "way",
            "osm_id": 1,
            "geometry": b"geometry-1",
            "polygon_id": json.dumps(
                ("osm", "afghanistan-latest.osm.pbf", "way", 1),
                ensure_ascii=True,
                separators=(",", ":"),
            ),
        }
    ]


def test_cli_description_rejects_text_without_associated_polygons(
    tmp_path: Path,
) -> None:
    polygons = pa.table(
        {
            "polygon_id": ["polygon-other"],
            "source_pbf": ["afghanistan-latest.osm.pbf"],
            "osm_type": ["way"],
            "osm_id": pa.array([2], pa.int64()),
            "geometry": [b"geometry-other"],
        }
    )

    assert (
        main(
            _argv(tmp_path / "joined"),
            opener=_hub(polygons=_parquet(polygons)),
        )
        == EXIT_JOIN_ERROR
    )


def test_successful_join_publishes_only_the_manifest_and_outputs(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "joined"

    assert main(_argv(output_dir), opener=_hub()) == 0

    (manifest_path,) = output_dir.glob("*.manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["row_counts"] == {
        "labels": 3,
        "text_units": 1,
        "polygon_records": POLYGON_COUNT,
        "documents": 0,
        "polygon_document_links": 0,
    }
    assert manifest["status_counts"] == {"yes": 1, "no": 1, "failed": 0, "skipped": 1}
    names = sorted(path.name for path in output_dir.iterdir())
    assert len(names) == 6
    assert all(not name.startswith(".") for name in names)


def test_failed_join_publishes_nothing_and_leaves_no_staging(tmp_path: Path) -> None:
    output_dir = tmp_path / "joined"
    hub = _hub(labels=_labels_parquet(identity="missing-description"))

    assert main(_argv(output_dir), opener=hub) == EXIT_JOIN_ERROR
    assert not output_dir.exists() or not any(output_dir.iterdir())


def test_cli_null_only_list_elements_keep_the_exact_source_list_type(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "joined"
    localized_type = pa.list_(
        pa.struct(
            [
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.string()),
            ]
        )
    )
    localized = pa.array([[None, None] for _ in range(POLYGON_COUNT)], localized_type)
    hub = _hub(polygons=_polygons_parquet(localized=localized))

    assert main(_argv(output_dir), opener=hub) == 0

    (manifest_path,) = output_dir.glob("*.manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    polygon_records = output_dir / manifest["outputs"]["polygon_records"]["name"]
    schema = pq.ParquetFile(polygon_records).schema_arrow
    assert schema.field("localized_descriptions").type == localized_type
    column = pq.read_table(polygon_records).column("localized_descriptions")
    assert column.to_pylist() == [[None, None]] * POLYGON_COUNT
