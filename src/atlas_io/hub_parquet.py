import operator
import re
from collections.abc import Buffer, Callable, Iterable
from io import SEEK_CUR, SEEK_END, SEEK_SET, RawIOBase
from typing import Protocol
from urllib.parse import quote
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

from atlas_domain.sources import PinnedHubParquetSource


class ByteBudgetExceeded(OSError):
    """Raised before a range request would exceed its source byte budget."""


class RangeProtocolError(OSError):
    """Raised when a server response does not match the requested byte range."""


class _Headers(Protocol):
    def get(self, name: str, default: str | None = None, /) -> str | None: ...


class _Response(Protocol):
    @property
    def status(self) -> int | None: ...

    @property
    def headers(self) -> _Headers: ...

    def read(self, size: int = -1) -> bytes: ...

    def close(self) -> None: ...


_Opener = Callable[[Request], _Response]


def _source_url(source: PinnedHubParquetSource) -> str:
    repo_id = quote(source.repo_id, safe="/")
    path = quote(source.path, safe="/")
    return f"https://huggingface.co/datasets/{repo_id}/resolve/{source.revision}/{path}"


class HubParquetRangeReader(RawIOBase):
    def __init__(
        self,
        source: PinnedHubParquetSource,
        *,
        max_bytes: int,
        opener: _Opener = urlopen,
    ) -> None:
        super().__init__()
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or max_bytes < 0
        ):
            raise ValueError("max_bytes must be a non-negative integer")
        self.source = source
        self.max_bytes = max_bytes
        self._opener = opener
        self._position = 0
        self._budget_used = 0
        self._bytes_read = 0

    @property
    def bytes_read(self) -> int:
        return self._bytes_read

    def readable(self) -> bool:
        return not self.closed

    def seekable(self) -> bool:
        return not self.closed

    def tell(self) -> int:
        self._check_open()
        return self._position

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        self._check_open()
        offset = operator.index(offset)
        whence = operator.index(whence)
        if whence == SEEK_SET:
            base = 0
        elif whence == SEEK_CUR:
            base = self._position
        elif whence == SEEK_END:
            base = self.source.size_bytes
        else:
            raise ValueError(f"invalid whence value: {whence}")

        position = base + offset
        if not 0 <= position <= self.source.size_bytes:
            raise ValueError("seek position is outside the pinned file size")
        self._position = position
        return position

    def read(self, size: int = -1) -> bytes:
        self._check_open()
        size = operator.index(size)
        available = self.source.size_bytes - self._position
        requested = available if size < 0 else min(size, available)
        if requested == 0:
            return b""
        if requested > self.max_bytes - self._budget_used:
            raise ByteBudgetExceeded(
                f"range needs {requested} bytes with "
                f"{self.max_bytes - self._budget_used} bytes remaining"
            )

        start = self._position
        end = start + requested - 1
        self._budget_used += requested
        request = Request(
            _source_url(self.source),
            headers={
                "Accept-Encoding": "identity",
                "Range": f"bytes={start}-{end}",
            },
        )
        response = self._opener(request)
        try:
            self._validate_response(response, start, end, requested)
            body = response.read(requested)
            if len(body) != requested:
                raise RangeProtocolError(
                    f"response body has {len(body)} bytes; expected {requested}"
                )
        finally:
            response.close()

        self._position += requested
        self._bytes_read += requested
        return body

    def readinto(self, buffer: Buffer) -> int:
        view = memoryview(buffer).cast("B")
        data = self.read(view.nbytes)
        view[: len(data)] = data
        return len(data)

    def _validate_response(
        self,
        response: _Response,
        start: int,
        end: int,
        requested: int,
    ) -> None:
        if response.status != 206:
            raise RangeProtocolError(
                f"range request returned HTTP {response.status}; expected 206"
            )

        content_encoding = response.headers.get("Content-Encoding")
        if content_encoding not in {None, "identity"}:
            raise RangeProtocolError("range response used a content encoding")

        content_range = response.headers.get("Content-Range")
        match = (
            re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
            if content_range is not None
            else None
        )
        actual_range = tuple(int(value) for value in match.groups()) if match else None
        expected_range = (start, end, self.source.size_bytes)
        if actual_range != expected_range:
            raise RangeProtocolError(
                f"Content-Range is {content_range!r}; expected "
                f"bytes {start}-{end}/{self.source.size_bytes}"
            )

        content_length = response.headers.get("Content-Length")
        if content_length is None or not content_length.isdecimal():
            raise RangeProtocolError("range response has no exact Content-Length")
        if int(content_length) != requested:
            raise RangeProtocolError(
                f"Content-Length is {content_length}; expected {requested}"
            )

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")


def read_parquet_row_group(
    source: PinnedHubParquetSource,
    *,
    row_group_index: int,
    columns: Iterable[str],
    max_bytes: int,
    opener: _Opener = urlopen,
) -> pa.Table:
    if isinstance(row_group_index, bool) or not isinstance(row_group_index, int):
        raise TypeError("row_group_index must be an integer")
    if row_group_index < 0:
        raise ValueError("row_group_index must be non-negative")
    column_names = tuple(columns)
    if not column_names or any(
        not isinstance(name, str) or not name for name in column_names
    ):
        raise ValueError("columns must contain at least one non-empty column name")

    with HubParquetRangeReader(source, max_bytes=max_bytes, opener=opener) as reader:
        parquet_file = pq.ParquetFile(reader)
        if row_group_index >= parquet_file.metadata.num_row_groups:
            raise IndexError(f"row group {row_group_index} does not exist")
        return parquet_file.read_row_group(row_group_index, columns=list(column_names))
