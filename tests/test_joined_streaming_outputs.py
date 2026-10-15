import json
import re
import tracemalloc
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.request import Request

import pyarrow as pa
import pyarrow.parquet as pq

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
