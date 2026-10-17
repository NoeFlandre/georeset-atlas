import operator
import re
import time
from collections.abc import Buffer, Callable, Generator, Iterable
from dataclasses import dataclass
from http.client import HTTPMessage, IncompleteRead, RemoteDisconnected
from io import SEEK_CUR, SEEK_END, SEEK_SET, RawIOBase
from ssl import SSLError
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

from atlas_domain.sources import PinnedHubParquetSource, validate_pinned_file


class ByteBudgetExceeded(OSError):
    """Raised before a range request would exceed its source byte budget."""


class RangeProtocolError(OSError):
    """Raised when a server response does not match the requested byte range."""


class ShortRangeBodyError(RangeProtocolError):
    """Raised when a range response body ends before the requested bytes arrive."""


class FileSizeProtocolError(OSError):
    """Raised when a HEAD response cannot pin the size of a Hub file."""


class _Headers(Protocol):
    def get(self, name: str, /) -> str | None: ...


class _Response(Protocol):
    @property
    def status(self) -> int | None: ...

    @property
    def headers(self) -> _Headers: ...

    def read(self, size: int = -1, /) -> bytes: ...

    def close(self) -> None: ...


HubOpener = Callable[[Request], _Response]
DEFAULT_HUB_TIMEOUT_SECONDS = 30.0
RANGE_MAX_ATTEMPTS = 3
_RANGE_RETRY_DELAYS_SECONDS = (0.25, 1.0)
_RETRYABLE_HTTP_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


@dataclass(slots=True)
class ParquetReadStats:
    row_groups: int = 0
    row_groups_completed: int = 0
    range_bytes: int = 0
    attempted_range_bytes: int = 0
    range_retries: int = 0


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: object,
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> None:
        return None


def _open_head_without_redirect(request: Request) -> _Response:
    opener = build_opener(_NoRedirect())
    try:
        return opener.open(request, timeout=DEFAULT_HUB_TIMEOUT_SECONDS)
    except HTTPError as response:
        return response


def _open_range(request: Request) -> _Response:
    return urlopen(request, timeout=DEFAULT_HUB_TIMEOUT_SECONDS)


def _hub_file_url(repo_id: str, revision: str, path: str) -> str:
    repo = quote(repo_id, safe="/")
    file_path = quote(path, safe="/")
    return f"https://huggingface.co/datasets/{repo}/resolve/{revision}/{file_path}"


def _source_url(source: PinnedHubParquetSource) -> str:
    return _hub_file_url(source.repo_id, source.revision, source.path)


def resolve_hub_file_size(
    repo_id: str,
    revision: str,
    path: str,
    *,
    opener: HubOpener | None = None,
) -> int:
    validate_pinned_file(repo_id, revision, path)
    request = Request(
        _hub_file_url(repo_id, revision, path),
        method="HEAD",
        headers={"Accept-Encoding": "identity"},
    )
    response = (opener or _open_head_without_redirect)(request)
    try:
        if response.status in {301, 302, 303, 307, 308}:
            linked_size = response.headers.get("X-Linked-Size")
            if linked_size is None or not linked_size.isdecimal():
                raise FileSizeProtocolError("HEAD redirect has no exact X-Linked-Size")
            size = int(linked_size)
            if size <= 0:
                raise FileSizeProtocolError("HEAD redirect has an empty file size")
            return size
        if response.status != 200:
            raise FileSizeProtocolError(
                f"HEAD request returned HTTP {response.status}; expected 200"
            )
        content_encoding = response.headers.get("Content-Encoding")
        if content_encoding not in {None, "identity"}:
            raise FileSizeProtocolError("HEAD response used a content encoding")
        content_length = response.headers.get("Content-Length")
        if content_length is None or not content_length.isdecimal():
            raise FileSizeProtocolError("HEAD response has no exact Content-Length")
        size = int(content_length)
        if size <= 0:
            raise FileSizeProtocolError("pinned Hub file is empty")
        return size
    finally:
        response.close()


class HubParquetRangeReader(RawIOBase):
    def __init__(
        self,
        source: PinnedHubParquetSource,
        *,
        max_bytes: int,
        opener: HubOpener | None = None,
        progress_callback: Callable[[str], None] | None = None,
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
        self._progress_callback = progress_callback
        self._position = 0
        self._budget_used = 0
        self._bytes_read = 0
        self._attempted_range_bytes = 0
        self._range_retries = 0

    @property
    def bytes_read(self) -> int:
        return self._bytes_read

    @property
    def attempted_range_bytes(self) -> int:
        return self._attempted_range_bytes

    @property
    def range_retries(self) -> int:
        return self._range_retries

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
        open_request = self._opener or _open_range
        body: bytes | None = None

        for attempt in range(1, RANGE_MAX_ATTEMPTS + 1):
            remaining = self.max_bytes - self._budget_used
            if requested > remaining:
                raise ByteBudgetExceeded(
                    f"range needs {requested} bytes with {remaining} bytes remaining"
                )
            # Charge each attempt conservatively: a failed response may have
            # transferred bytes before its stream timed out.
            self._budget_used += requested
            self._attempted_range_bytes += requested
            request = Request(
                _source_url(self.source),
                headers={
                    "Accept-Encoding": "identity",
                    "Range": f"bytes={start}-{end}",
                },
            )
            self._report_progress(
                f"range_start path={self.source.path} bytes={start}-{end} "
                f"attempt={attempt}/{RANGE_MAX_ATTEMPTS}"
            )
            response: _Response | None = None
            try:
                response = open_request(request)
                self._validate_response(response, start, end, requested)
                body = response.read(requested)
                if len(body) != requested:
                    raise ShortRangeBodyError(
                        f"response body has {len(body)} bytes; expected {requested}"
                    )
            except HTTPError as error:
                error.close()
                if (
                    error.code not in _RETRYABLE_HTTP_STATUS_CODES
                    or attempt == RANGE_MAX_ATTEMPTS
                ):
                    self._report_progress(
                        f"range_failed path={self.source.path} attempt={attempt} "
                        f"http={error.code}"
                    )
                    raise
                self._range_retries += 1
                self._report_progress(
                    f"range_retry path={self.source.path} attempt={attempt} "
                    f"http={error.code}"
                )
                time.sleep(_RANGE_RETRY_DELAYS_SECONDS[attempt - 1])
            except (
                ShortRangeBodyError,
                ConnectionError,
                IncompleteRead,
                RemoteDisconnected,
                SSLError,
                TimeoutError,
                URLError,
            ) as error:
                body = None
                if response is not None:
                    response.close()
                    response = None
                if attempt == RANGE_MAX_ATTEMPTS:
                    self._report_progress(
                        f"range_failed path={self.source.path} attempt={attempt} "
                        f"error={type(error).__name__}"
                    )
                    raise
                self._range_retries += 1
                self._report_progress(
                    f"range_retry path={self.source.path} attempt={attempt} "
                    f"error={type(error).__name__}"
                )
                time.sleep(_RANGE_RETRY_DELAYS_SECONDS[attempt - 1])
            finally:
                if response is not None:
                    response.close()
            if body is not None:
                break

        if body is None:
            raise RuntimeError("range request ended without data or an error")
        self._position += requested
        self._bytes_read += requested
        self._report_progress(
            f"range_complete path={self.source.path} bytes={start}-{end} "
            f"attempted_bytes={self._attempted_range_bytes} "
            f"retries={self._range_retries}"
        )
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

    def _report_progress(self, message: str) -> None:
        if self._progress_callback is not None:
            self._progress_callback(message)

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file")


def read_parquet_row_group(
    source: PinnedHubParquetSource,
    *,
    row_group_index: int,
    columns: Iterable[str],
    max_bytes: int,
    opener: HubOpener | None = None,
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
        available_names = parquet_file.schema_arrow.names
        unknown = sorted(set(column_names) - set(available_names))
        if unknown:
            raise ValueError(f"unknown projection column(s): {', '.join(unknown)}")
        ambiguous = sorted(
            name for name in set(column_names) if available_names.count(name) > 1
        )
        if ambiguous:
            raise ValueError(f"ambiguous projection column(s): {', '.join(ambiguous)}")
        table = parquet_file.read_row_group(row_group_index, columns=list(column_names))
        if table.column_names != list(column_names):
            raise ValueError("projected schema does not match requested columns")
        return table


def read_parquet_file(
    source: PinnedHubParquetSource,
    *,
    max_bytes: int,
    opener: HubOpener | None = None,
) -> tuple[tuple[pa.Table, ...], int]:
    stats = ParquetReadStats()
    tables = tuple(
        iter_parquet_file(
            source,
            max_bytes=max_bytes,
            opener=opener,
            stats=stats,
        )
    )
    return tables, stats.range_bytes


def iter_parquet_file(
    source: PinnedHubParquetSource,
    *,
    max_bytes: int,
    opener: HubOpener | None = None,
    stats: ParquetReadStats | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> Generator[pa.Table, None, None]:
    read_stats = stats if stats is not None else ParquetReadStats()
    read_stats.row_groups = 0
    read_stats.row_groups_completed = 0
    read_stats.range_bytes = 0
    read_stats.attempted_range_bytes = 0
    read_stats.range_retries = 0
    with HubParquetRangeReader(
        source,
        max_bytes=max_bytes,
        opener=opener,
        progress_callback=progress_callback,
    ) as reader:
        try:
            parquet_file = pq.ParquetFile(reader)
            names = parquet_file.schema_arrow.names
            ambiguous = sorted({name for name in names if names.count(name) > 1})
            if ambiguous:
                raise ValueError(f"ambiguous column(s): {', '.join(ambiguous)}")
            read_stats.row_groups = parquet_file.metadata.num_row_groups
            if read_stats.row_groups == 0:
                read_stats.range_bytes = reader.bytes_read
                yield pa.Table.from_batches([], schema=parquet_file.schema_arrow)
                return
            for index in range(read_stats.row_groups):
                if progress_callback is not None:
                    progress_callback(
                        f"row_group_start path={source.path} "
                        f"index={index + 1}/{read_stats.row_groups}"
                    )
                table = parquet_file.read_row_group(index, columns=names)
                read_stats.row_groups_completed += 1
                read_stats.range_bytes = reader.bytes_read
                read_stats.attempted_range_bytes = reader.attempted_range_bytes
                read_stats.range_retries = reader.range_retries
                if progress_callback is not None:
                    progress_callback(
                        f"row_group_complete path={source.path} "
                        f"index={index + 1}/{read_stats.row_groups} "
                        f"rows={table.num_rows} range_bytes={reader.bytes_read} "
                        f"attempted_range_bytes={reader.attempted_range_bytes} "
                        f"retries={reader.range_retries}"
                    )
                yield table
        finally:
            read_stats.range_bytes = reader.bytes_read
            read_stats.attempted_range_bytes = reader.attempted_range_bytes
            read_stats.range_retries = reader.range_retries
