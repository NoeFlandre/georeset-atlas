from __future__ import annotations

import re
from dataclasses import FrozenInstanceError
from io import SEEK_END, BytesIO
from urllib.request import Request

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from atlas_domain.sources import PinnedHubParquetSource
from atlas_io.hub_parquet import (
    ByteBudgetExceeded,
    HubParquetRangeReader,
    ParquetReadStats,
    RangeProtocolError,
    iter_parquet_file,
    read_parquet_row_group,
)

SOURCE_REVISION = "ce98b328f3c6543800a3d931877b51eb3f0cf768"


class FakeResponse:
    def __init__(self, body: bytes, status: int, headers: FakeHeaders) -> None:
        self._body = BytesIO(body)
        self.status = status
        self.headers = headers
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def close(self) -> None:
        self.closed = True


class FakeHeaders:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    def set(self, name: str, value: str) -> None:
        self._values[name.casefold()] = value

    def get(self, name: str, default: str | None = None) -> str | None:
        return self._values.get(name.casefold(), default)


class FakeRangeServer:
    def __init__(
        self,
        payload: bytes,
        *,
        status: int = 206,
        header_overrides: dict[str, str] | None = None,
        body_adjustment: int = 0,
    ) -> None:
        self.payload = payload
        self.status = status
        self.header_overrides = header_overrides or {}
        self.body_adjustment = body_adjustment
        self.requests: list[Request] = []

    def __call__(self, request: Request) -> FakeResponse:
        self.requests.append(request)
        range_header = request.get_header("Range")
        assert range_header is not None
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", range_header)
        assert match is not None
        start, end = (int(value) for value in match.groups())
        body = self.payload[start : end + 1]
        if self.body_adjustment < 0:
            body = body[: self.body_adjustment]
        elif self.body_adjustment > 0:
            body += b"x" * self.body_adjustment

        headers = FakeHeaders()
        headers.set("Content-Range", f"bytes {start}-{end}/{len(self.payload)}")
        headers.set("Content-Length", str(end - start + 1))
        for name, value in self.header_overrides.items():
            headers.set(name, value)
        return FakeResponse(body, self.status, headers)


class TimeoutOnceResponse:
    def __init__(self, response: FakeResponse) -> None:
        self._response = response
        self.status = response.status
        self.headers = response.headers
        self.closed = False
        self._timed_out = False

    def read(self, size: int = -1) -> bytes:
        if not self._timed_out:
            self._timed_out = True
            raise TimeoutError("The read operation timed out")
        return self._response.read(size)

    def close(self) -> None:
        self.closed = True
        self._response.close()


def _source(payload: bytes) -> PinnedHubParquetSource:
    return PinnedHubParquetSource(
        repo_id="NoeFlandre/atlas-fixture",
        revision=SOURCE_REVISION,
        path="data/polygons.parquet",
        size_bytes=len(payload),
    )


def test_reader_requests_the_exact_pinned_range() -> None:
    payload = b"abcdefgh"
    source = _source(payload)
    server = FakeRangeServer(payload)

    with HubParquetRangeReader(source, max_bytes=4, opener=server) as reader:
        assert reader.read(4) == b"abcd"
        assert reader.bytes_read == 4

    request = server.requests[0]
    assert request.full_url == (
        "https://huggingface.co/datasets/NoeFlandre/atlas-fixture/resolve/"
        f"{SOURCE_REVISION}/data/polygons.parquet"
    )
    assert request.get_header("Range") == "bytes=0-3"


def test_reader_retries_transient_read_timeout_with_same_range_and_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"abcdefgh"
    source = _source(payload)
    base_server = FakeRangeServer(payload)
    responses: list[TimeoutOnceResponse] = []
    progress: list[str] = []

    def opener(request: Request) -> FakeResponse | TimeoutOnceResponse:
        response = base_server(request)
        if len(base_server.requests) == 1:
            flaky = TimeoutOnceResponse(response)
            responses.append(flaky)
            return flaky
        return response

    monkeypatch.setattr("atlas_io.hub_parquet.time.sleep", lambda _seconds: None)
    with HubParquetRangeReader(
        source,
        max_bytes=8,
        opener=opener,
        progress_callback=progress.append,
    ) as reader:
        assert reader.read(4) == b"abcd"
        assert reader.tell() == 4
        assert reader.bytes_read == 4
        assert reader.attempted_range_bytes == 8
        assert reader.range_retries == 1
        with pytest.raises(ByteBudgetExceeded):
            reader.read(1)

    assert responses[0].closed
    assert len(base_server.requests) == 2
    assert [request.get_header("Range") for request in base_server.requests] == [
        "bytes=0-3",
        "bytes=0-3",
    ]
    assert any("range_retry" in event and "TimeoutError" in event for event in progress)


def test_reader_bounds_transient_retries_and_charges_each_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(b"abcdefgh")
    requests: list[Request] = []
    progress: list[str] = []

    def opener(request: Request) -> FakeResponse:
        requests.append(request)
        raise TimeoutError("temporary timeout")

    monkeypatch.setattr("atlas_io.hub_parquet.time.sleep", lambda _seconds: None)
    with HubParquetRangeReader(
        source,
        max_bytes=12,
        opener=opener,
        progress_callback=progress.append,
    ) as reader:
        with pytest.raises(TimeoutError, match="temporary timeout"):
            reader.read(4)
        assert reader.tell() == 0
        assert reader.bytes_read == 0
        assert reader.attempted_range_bytes == 12
        assert reader.range_retries == 2

    assert len(requests) == 3
    assert {request.get_header("Range") for request in requests} == {"bytes=0-3"}
    assert any("range_failed" in event and "attempt=3" in event for event in progress)


def test_reader_rejects_a_non_partial_response() -> None:
    payload = b"abcdefgh"
    source = _source(payload)
    server = FakeRangeServer(payload, status=200)

    with HubParquetRangeReader(source, max_bytes=4, opener=server) as reader:
        with pytest.raises(RangeProtocolError):
            reader.read(4)

        assert reader.tell() == 0
        assert reader.bytes_read == 0

    assert len(server.requests) == 1


@pytest.mark.parametrize(
    ("header_overrides", "body_adjustment"),
    [
        ({"Content-Range": "bytes 1-4/8"}, 0),
        ({"Content-Range": "bytes 0-4/8"}, 0),
        ({"Content-Range": "bytes 0-3/9"}, 0),
        ({"Content-Range": ""}, 0),
        ({"Content-Length": "3"}, 0),
        ({"Content-Length": ""}, 0),
        ({"Content-Encoding": "gzip"}, 0),
        ({"Content-Length": "5"}, 1),
    ],
)
def test_reader_rejects_mismatched_range_or_response_size(
    header_overrides: dict[str, str],
    body_adjustment: int,
) -> None:
    payload = b"abcdefgh"
    source = _source(payload)
    server = FakeRangeServer(
        payload,
        header_overrides=header_overrides,
        body_adjustment=body_adjustment,
    )

    with HubParquetRangeReader(source, max_bytes=4, opener=server) as reader:
        with pytest.raises(RangeProtocolError):
            reader.read(4)

        assert reader.tell() == 0
        assert reader.bytes_read == 0


def test_reader_retries_a_short_range_body_then_succeeds() -> None:
    payload = b"abcdefgh"
    source = _source(payload)
    short_body_server = FakeRangeServer(payload, body_adjustment=-1)
    complete_body_server = FakeRangeServer(payload)
    requests: list[Request] = []

    def opener(request: Request) -> FakeResponse:
        requests.append(request)
        server = short_body_server if len(requests) == 1 else complete_body_server
        return server(request)

    progress: list[str] = []
    with HubParquetRangeReader(
        source, max_bytes=8, opener=opener, progress_callback=progress.append
    ) as reader:
        assert reader.read(4) == b"abcd"
        assert reader.tell() == 4
        assert reader.bytes_read == 4
        assert reader.attempted_range_bytes == 8
        assert reader.range_retries == 1

    assert len(requests) == 2
    assert {request.get_header("Range") for request in requests} == {"bytes=0-3"}
    assert any(
        "range_retry" in event and "ShortRangeBodyError" in event for event in progress
    )


def test_reader_bounds_retries_for_persistently_short_range_bodies() -> None:
    payload = b"abcdefgh"
    source = _source(payload)
    server = FakeRangeServer(payload, body_adjustment=-1)
    progress: list[str] = []

    with HubParquetRangeReader(
        source, max_bytes=12, opener=server, progress_callback=progress.append
    ) as reader:
        with pytest.raises(RangeProtocolError, match="response body"):
            reader.read(4)
        assert reader.tell() == 0
        assert reader.bytes_read == 0
        assert reader.attempted_range_bytes == 12
        assert reader.range_retries == 2

    assert len(server.requests) == 3
    assert any("range_failed" in event and "attempt=3" in event for event in progress)


def test_reader_enforces_a_cumulative_per_source_byte_budget() -> None:
    payload = b"abcdefgh"
    source = _source(payload)
    server = FakeRangeServer(payload)

    with HubParquetRangeReader(source, max_bytes=4, opener=server) as reader:
        assert reader.read(3) == b"abc"
        with pytest.raises(ByteBudgetExceeded):
            reader.read(2)

        assert reader.bytes_read == 3
        assert len(server.requests) == 1


def test_reader_allows_exact_budget_use_and_blocks_the_next_byte() -> None:
    payload = b"abcd"
    source = _source(payload)
    server = FakeRangeServer(payload)

    with HubParquetRangeReader(source, max_bytes=3, opener=server) as reader:
        assert reader.read(3) == payload[:3]
        with pytest.raises(ByteBudgetExceeded):
            reader.read(1)
        assert reader.bytes_read == 3

    assert len(server.requests) == 1


def test_reader_seek_and_read_stay_within_the_pinned_file_size() -> None:
    payload = b"abcdef"
    source = _source(payload)
    server = FakeRangeServer(payload)

    with HubParquetRangeReader(source, max_bytes=8, opener=server) as reader:
        assert reader.seek(2) == 2
        assert reader.read(2) == b"cd"
        assert reader.seek(-1, SEEK_END) == 5
        assert reader.read(8) == b"f"
        assert reader.tell() == len(payload)
        with pytest.raises(ValueError):
            reader.seek(-1)
        with pytest.raises(ValueError):
            reader.seek(len(payload) + 1)
        with pytest.raises(ValueError):
            reader.seek(0, 7)


@settings(database=None, max_examples=50)
@given(
    payload=st.binary(min_size=1, max_size=128),
    raw_offset=st.integers(min_value=0, max_value=128),
    length=st.integers(min_value=0, max_value=128),
)
def test_range_reader_matches_binary_seek_read_slices(
    payload: bytes,
    raw_offset: int,
    length: int,
) -> None:
    source = _source(payload)
    server = FakeRangeServer(payload)
    offset = min(raw_offset, len(payload))

    with HubParquetRangeReader(
        source,
        max_bytes=len(payload),
        opener=server,
    ) as reader:
        assert reader.seek(offset) == offset
        result = reader.read(length)

        assert result == payload[offset : offset + length]
        assert reader.tell() == offset + len(result)
        assert reader.bytes_read == len(result)


def test_pinned_source_rejects_mutation_and_mutable_revisions() -> None:
    source = _source(b"parquet")

    with pytest.raises(FrozenInstanceError):
        setattr(source, "revision", "e6524c711ee0033c3cd5990e5fdbbe4b89992dfb")

    with pytest.raises(ValueError):
        PinnedHubParquetSource(
            repo_id="NoeFlandre/atlas-fixture",
            revision="main",
            path="data/polygons.parquet",
            size_bytes=7,
        )

    with pytest.raises(ValueError, match="repo_id"):
        PinnedHubParquetSource(
            repo_id=r"NoeFlandre\atlas-fixture",
            revision=SOURCE_REVISION,
            path="data/polygons.parquet",
            size_bytes=7,
        )


def _chunk_span(column: pq.ColumnChunkMetaData) -> tuple[int, int]:
    start = (
        column.dictionary_page_offset
        if column.has_dictionary_page
        else column.data_page_offset
    )
    return start, start + column.total_compressed_size - 1


def _range_span(request: Request) -> tuple[int, int]:
    range_header = request.get_header("Range")
    assert range_header is not None
    match = re.fullmatch(r"bytes=(\d+)-(\d+)", range_header)
    assert match is not None
    start, end = (int(value) for value in match.groups())
    return start, end


def _overlaps(first: tuple[int, int], second: tuple[int, int]) -> bool:
    return first[0] <= second[1] and second[0] <= first[1]


def test_projection_returns_the_selected_column_of_the_requested_row_group() -> None:
    rows_per_group = 1_000
    row_count = 3 * rows_per_group
    buffer = BytesIO()
    pq.write_table(
        pa.table(
            {
                "selected": [f"id-{index:05d}" for index in range(row_count)],
                "unselected": [f"{index:08d}" * 128 for index in range(row_count)],
            }
        ),
        buffer,
        row_group_size=rows_per_group,
        compression="NONE",
        use_dictionary=False,
    )
    payload = buffer.getvalue()
    group = pq.ParquetFile(BytesIO(payload)).metadata.row_group(1)
    chunks = {
        group.column(index).path_in_schema: _chunk_span(group.column(index))
        for index in range(group.num_columns)
    }
    source = _source(payload)
    server = FakeRangeServer(payload)

    table = read_parquet_row_group(
        source,
        row_group_index=1,
        columns=("selected",),
        max_bytes=len(payload) * 4,
        opener=server,
    )

    assert table.schema.names == ["selected"]
    assert table.column("selected").to_pylist() == [
        f"id-{index:05d}" for index in range(rows_per_group, 2 * rows_per_group)
    ]
    fetched = [_range_span(request) for request in server.requests]
    assert any(_overlaps(span, chunks["selected"]) for span in fetched)
    assert not any(_overlaps(span, chunks["unselected"]) for span in fetched)


def test_file_iterator_reads_and_releases_one_row_group_at_a_time() -> None:
    rows_per_group = 100
    row_count = rows_per_group * 3
    buffer = BytesIO()
    pq.write_table(
        pa.table(
            {
                "selected": [f"id-{index:05d}" for index in range(row_count)],
                "unselected": [f"{index:08d}" * 128 for index in range(row_count)],
            }
        ),
        buffer,
        row_group_size=rows_per_group,
        compression="NONE",
        use_dictionary=False,
    )
    payload = buffer.getvalue()
    source = _source(payload)
    server = FakeRangeServer(payload)
    stats = ParquetReadStats()
    stream = iter_parquet_file(
        source,
        max_bytes=len(payload) * 4,
        opener=server,
        stats=stats,
    )

    first = next(stream)

    assert stats.row_groups == 3
    assert first.num_rows == rows_per_group
    assert first.column_names == ["selected", "unselected"]
    first_group = pq.ParquetFile(BytesIO(payload)).metadata.row_group(0)
    first_group_start = min(
        first_group.column(index).data_page_offset
        for index in range(first_group.num_columns)
    )
    assert stats.range_bytes > 0
    assert stats.range_bytes < len(payload)
    assert any(
        _overlaps(_range_span(request), (first_group_start, len(payload) - 1))
        for request in server.requests
    )

    second = next(stream)

    assert second.num_rows == rows_per_group
    assert stats.range_bytes > 0
    stream.close()


def test_file_iterator_reports_row_group_and_range_progress() -> None:
    buffer = BytesIO()
    pq.write_table(
        pa.table({"polygon_id": ["a", "b", "c", "d"]}),
        buffer,
        row_group_size=2,
        compression="NONE",
    )
    payload = buffer.getvalue()
    source = _source(payload)
    server = FakeRangeServer(payload)
    stats = ParquetReadStats()
    progress: list[str] = []

    tables = list(
        iter_parquet_file(
            source,
            max_bytes=len(payload) * 4,
            opener=server,
            stats=stats,
            progress_callback=progress.append,
        )
    )

    assert [table.num_rows for table in tables] == [2, 2]
    assert stats.row_groups == 2
    assert stats.row_groups_completed == 2
    assert stats.attempted_range_bytes == stats.range_bytes
    assert stats.range_retries == 0
    assert any(
        "row_group_start" in event and "index=1/2" in event for event in progress
    )
    assert any(
        "row_group_complete" in event and "index=2/2" in event for event in progress
    )
    assert any("range_start" in event for event in progress)
    assert any("range_complete" in event for event in progress)


def test_file_iterator_preserves_schema_for_a_file_with_no_row_groups() -> None:
    schema = pa.schema(
        [("polygon_id", pa.string()), ("geometry", pa.binary())],
        metadata={b"source": b"empty-fixture"},
    )
    buffer = BytesIO()
    with pq.ParquetWriter(buffer, schema, compression="NONE"):
        pass
    payload = buffer.getvalue()
    source = _source(payload)
    server = FakeRangeServer(payload)
    stats = ParquetReadStats()

    tables = list(
        iter_parquet_file(
            source,
            max_bytes=len(payload) * 4,
            opener=server,
            stats=stats,
        )
    )

    assert stats.row_groups == 0
    assert stats.range_bytes > 0
    assert len(tables) == 1
    assert tables[0].num_rows == 0
    assert tables[0].schema == schema


def test_projection_rejects_a_missing_source_column() -> None:
    buffer = BytesIO()
    pq.write_table(pa.table({"selected": ["a"]}), buffer, compression="NONE")
    payload = buffer.getvalue()
    source = _source(payload)
    server = FakeRangeServer(payload)

    with pytest.raises(ValueError, match="unknown projection"):
        read_parquet_row_group(
            source,
            row_group_index=0,
            columns=("missing",),
            max_bytes=len(payload) * 4,
            opener=server,
        )

    assert server.requests


def test_projection_rejects_an_ambiguous_duplicate_source_column() -> None:
    buffer = BytesIO()
    duplicate_names = pa.Table.from_arrays(
        [pa.array(["first"]), pa.array(["second"])], names=["key", "key"]
    )
    pq.write_table(duplicate_names, buffer, compression="NONE")
    payload = buffer.getvalue()
    source = _source(payload)

    with pytest.raises(ValueError, match="ambiguous projection"):
        read_parquet_row_group(
            source,
            row_group_index=0,
            columns=("key",),
            max_bytes=len(payload) * 4,
            opener=FakeRangeServer(payload),
        )
