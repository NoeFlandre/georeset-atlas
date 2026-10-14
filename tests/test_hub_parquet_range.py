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
    RangeProtocolError,
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


def test_reader_rejects_a_non_partial_response() -> None:
    payload = b"abcdefgh"
    source = _source(payload)
    server = FakeRangeServer(payload, status=200)

    with HubParquetRangeReader(source, max_bytes=4, opener=server) as reader:
        with pytest.raises(RangeProtocolError):
            reader.read(4)

        assert reader.tell() == 0
        assert reader.bytes_read == 0


@pytest.mark.parametrize(
    ("header_overrides", "body_adjustment"),
    [
        ({"Content-Range": "bytes 1-4/8"}, 0),
        ({"Content-Range": "bytes 0-4/8"}, 0),
        ({"Content-Range": "bytes 0-3/9"}, 0),
        ({"Content-Range": ""}, 0),
        ({"Content-Length": "3"}, 0),
        ({"Content-Length": "5"}, 1),
        ({}, -1),
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


def test_projection_reads_only_the_requested_column_and_row_group() -> None:
    buffer = BytesIO()
    pq.write_table(
        pa.table(
            {
                "selected": ["a", "b", "c", "d"],
                "unselected": [1, 2, 3, 4],
            }
        ),
        buffer,
        row_group_size=2,
        compression="NONE",
    )
    payload = buffer.getvalue()
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
    assert table.to_pylist() == [{"selected": "c"}, {"selected": "d"}]
    assert server.requests
