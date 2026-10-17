import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from http.client import BadStatusLine, HTTPException, HTTPMessage, IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from threading import Barrier, Thread
from types import SimpleNamespace
from typing import Any, BinaryIO, NoReturn
from urllib.request import Request

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_bdd import given, scenarios, then, when

from atlas_domain.source_catalog import SOURCE_CATALOG, select_input_file
from atlas_domain.sources import PinnedHubParquetSource
from atlas_io import hub_parquet, shard_increment
from atlas_io.hub_parquet import (
    DEFAULT_HUB_TIMEOUT_SECONDS,
    ByteBudgetExceeded,
    FileSizeProtocolError,
    HubParquetRangeReader,
    RangeProtocolError,
    resolve_hub_file_size,
)
from atlas_io.shard_cli import main
from atlas_io.shard_increment import (
    ShardConflictError,
    ShardResult,
    _write_atomically,
    write_shard_increment,
)

scenarios("../features/bounded_shard_increment.feature")

SOURCE_ID = "description"
REPO_ID = "NoeFlandre/osm-polygon-description-tag-landuse"
REVISION = "ce98b328f3c6543800a3d931877b51eb3f0cf768"
OTHER_REVISION = "e6524c711ee0033c3cd5990e5fdbbe4b89992dfb"
TABLE = "labels"
TABLE_ROOT = "labels/language-v1/data"
PATH = f"{TABLE_ROOT}/part-00000.parquet"


def _label_parquet() -> bytes:
    buffer = BytesIO()
    pq.write_table(
        pa.table(
            {
                "label": ["yes", "no", "failed", "skipped", "yes", "no"],
                "text": ["a", "b", "c", "d", "e", "f"],
            }
        ),
        buffer,
        row_group_size=3,
        compression="NONE",
    )
    return buffer.getvalue()


PAYLOAD = _label_parquet()
MAX_BYTES = 4 * len(PAYLOAD)


class FakeHeaders:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = {name.casefold(): value for name, value in values.items()}

    def get(self, name: str, default: str | None = None) -> str | None:
        return self._values.get(name.casefold(), default)


class FakeResponse:
    def __init__(self, body: bytes, *, status: int, headers: dict[str, str]) -> None:
        self._body = BytesIO(body)
        self.status = status
        self.headers = FakeHeaders(headers)

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def close(self) -> None:
        self._body.close()


class FakeHub:
    """Serves one pinned payload to HEAD and Range requests without the network."""

    def __init__(
        self,
        payload: bytes = PAYLOAD,
        *,
        range_status: int = 206,
        head_status: int = 200,
        head_header_overrides: dict[str, str] | None = None,
        range_header_overrides: dict[str, str] | None = None,
        missing_range_headers: tuple[str, ...] = (),
        short_range_body: bool = False,
    ) -> None:
        self.payload = payload
        self.range_status = range_status
        self.head_status = head_status
        self.head_header_overrides = head_header_overrides or {}
        self.range_header_overrides = range_header_overrides or {}
        self.missing_range_headers = {name.casefold() for name in missing_range_headers}
        self.short_range_body = short_range_body
        self.requests: list[Request] = []

    @property
    def range_requests(self) -> list[Request]:
        return [request for request in self.requests if request.get_method() == "GET"]

    def __call__(self, request: Request) -> FakeResponse:
        self.requests.append(request)
        total = len(self.payload)
        if request.get_method() == "HEAD":
            headers = {"Content-Length": str(total)}
            headers.update(self.head_header_overrides)
            return FakeResponse(b"", status=self.head_status, headers=headers)
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", request.get_header("Range") or "")
        assert match is not None
        start, end = (int(value) for value in match.groups())
        body = self.payload[start : end + 1]
        if self.short_range_body and body:
            body = body[:-1]
        headers = {
            "Content-Range": f"bytes {start}-{end}/{total}",
            "Content-Length": str(end - start + 1),
        }
        headers.update(self.range_header_overrides)
        headers = {
            name: value
            for name, value in headers.items()
            if name.casefold() not in self.missing_range_headers
        }
        return FakeResponse(
            body,
            status=self.range_status,
            headers=headers,
        )


def _increment(
    hub: FakeHub,
    output_dir: Path,
    *,
    repo_id: str = REPO_ID,
    path: str = PATH,
    revision: str = REVISION,
    table_root: str = TABLE_ROOT,
    projection: tuple[str, ...] = ("label",),
    max_bytes: int = MAX_BYTES,
) -> ShardResult:
    return write_shard_increment(
        source_id=SOURCE_ID,
        repo_id=repo_id,
        revision=revision,
        path=path,
        table=TABLE,
        table_root=table_root,
        row_group_index=1,
        projection=projection,
        max_bytes=max_bytes,
        output_dir=output_dir,
        opener=hub,
    )


def _argv(output_dir: Path) -> list[str]:
    pairs = (
        ("--source", SOURCE_ID),
        ("--table", TABLE),
        ("--path", PATH),
        ("--row-group", "1"),
        ("--column", "label"),
        ("--max-bytes", str(MAX_BYTES)),
        ("--output-dir", str(output_dir)),
    )
    return [token for pair in pairs for token in pair]


def _single(output_dir: Path, pattern: str) -> Path:
    matches = sorted(output_dir.glob(pattern))
    assert len(matches) == 1, matches
    return matches[0]


def _range_size(request: Request) -> int:
    range_header = request.get_header("Range")
    assert range_header is not None
    match = re.fullmatch(r"bytes=(\d+)-(\d+)", range_header)
    assert match is not None
    start, end = (int(value) for value in match.groups())
    return end - start + 1


def test_selection_returns_the_declared_root_that_contains_the_file() -> None:
    source, table_root = select_input_file(
        SOURCE_CATALOG, source_id=SOURCE_ID, table=TABLE, path=PATH
    )

    assert (source.repo_id, source.revision) == (REPO_ID, REVISION)
    assert table_root == TABLE_ROOT


@pytest.mark.parametrize(
    ("source_id", "table", "path"),
    [
        ("missing", TABLE, PATH),
        (SOURCE_ID, "missing", PATH),
        (SOURCE_ID, TABLE, "data/part-00000.parquet"),
        (SOURCE_ID, TABLE, "labels/language-v1/data2/part-00000.parquet"),
        (SOURCE_ID, TABLE, f"{TABLE_ROOT}/../part-00000.parquet"),
        (SOURCE_ID, TABLE, f"/{PATH}"),
        (SOURCE_ID, TABLE, f"{TABLE_ROOT}/part-00000.csv"),
    ],
)
def test_selection_rejects_unknown_or_undeclared_inputs(
    source_id: str, table: str, path: str
) -> None:
    with pytest.raises(ValueError):
        select_input_file(SOURCE_CATALOG, source_id=source_id, table=table, path=path)


def test_head_resolves_the_pinned_size_without_a_range_request() -> None:
    hub = FakeHub()

    size = resolve_hub_file_size(REPO_ID, REVISION, PATH, opener=hub)

    assert size == len(PAYLOAD)
    (request,) = hub.requests
    assert request.get_method() == "HEAD"
    assert request.full_url == (
        f"https://huggingface.co/datasets/{REPO_ID}/resolve/{REVISION}/{PATH}"
    )


def test_head_redirect_uses_linked_size_without_following_to_a_get() -> None:
    hub = FakeHub(
        head_status=302,
        head_header_overrides={
            "X-Linked-Size": str(len(PAYLOAD)),
            "Location": "https://cdn.example.test/signed-object",
        },
    )

    size = resolve_hub_file_size(REPO_ID, REVISION, PATH, opener=hub)

    assert size == len(PAYLOAD)
    assert [request.get_method() for request in hub.requests] == ["HEAD"]


def test_head_redirect_without_linked_size_fails_without_following() -> None:
    hub = FakeHub(head_status=302)

    with pytest.raises(FileSizeProtocolError, match="X-Linked-Size"):
        resolve_hub_file_size(REPO_ID, REVISION, PATH, opener=hub)

    assert [request.get_method() for request in hub.requests] == ["HEAD"]


def test_head_redirect_over_loopback_http_returns_linked_size_without_a_get(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: list[str] = []

    class RedirectingHub(BaseHTTPRequestHandler):
        def do_HEAD(self) -> None:
            received.append(self.command)
            self.send_response(302)
            self.send_header("X-Linked-Size", str(len(PAYLOAD)))
            self.send_header(
                "Location", f"http://127.0.0.1:{server.server_port}/signed"
            )
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            received.append(self.command)
            self.send_error(404)

        def log_message(self, format: str, *args: Any) -> None:
            """Keep the loopback test output quiet."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), RedirectingHub)
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        monkeypatch.setattr(
            hub_parquet,
            "_hub_file_url",
            lambda repo_id, revision, path: (
                f"http://127.0.0.1:{server.server_port}/{repo_id}/{revision}/{path}"
            ),
        )
        # The real opener must not route loopback traffic through a proxy.
        monkeypatch.setattr(urllib.request, "getproxies", lambda: {})

        size = resolve_hub_file_size(REPO_ID, REVISION, PATH)
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)

    assert size == len(PAYLOAD)
    assert received == ["HEAD"]


def test_default_head_uses_a_non_redirecting_opener_and_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Request, float, object]] = []

    class HeadOpener:
        def open(self, request: Request, *, timeout: float) -> FakeResponse:
            calls.append((request, timeout, handlers[0]))
            return FakeResponse(
                b"",
                status=302,
                headers={"X-Linked-Size": str(len(PAYLOAD))},
            )

    handlers: list[object] = []

    def build_opener(handler: object) -> HeadOpener:
        handlers.append(handler)
        return HeadOpener()

    monkeypatch.setattr(hub_parquet, "build_opener", build_opener)

    assert resolve_hub_file_size(REPO_ID, REVISION, PATH) == len(PAYLOAD)
    (request, timeout, handler) = calls[0]
    assert request.get_method() == "HEAD"
    assert timeout == DEFAULT_HUB_TIMEOUT_SECONDS
    assert isinstance(handler, hub_parquet._NoRedirect)
    assert (
        handler.redirect_request(
            request,
            None,
            302,
            "Found",
            HTTPMessage(),
            "https://cdn.example.test/object",
        )
        is None
    )


def test_default_range_opener_uses_a_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"abc"
    source = PinnedHubParquetSource(
        repo_id=REPO_ID,
        revision=REVISION,
        path=PATH,
        size_bytes=len(payload),
    )
    calls: list[float] = []
    server = FakeHub(payload)

    def urlopen(request: Request, *, timeout: float) -> FakeResponse:
        calls.append(timeout)
        return server(request)

    monkeypatch.setattr(hub_parquet, "urlopen", urlopen)
    with HubParquetRangeReader(source, max_bytes=len(payload)) as reader:
        assert reader.read(3) == payload

    assert calls == [DEFAULT_HUB_TIMEOUT_SECONDS]


@pytest.mark.parametrize(
    ("status", "headers"),
    [
        (404, {"Content-Length": "8"}),
        (200, {}),
        (200, {"Content-Length": "eight"}),
        (200, {"Content-Length": "0"}),
        (200, {"Content-Length": "8", "Content-Encoding": "gzip"}),
    ],
)
def test_head_rejects_responses_that_cannot_pin_the_file_size(
    status: int, headers: dict[str, str]
) -> None:
    def opener(_request: Request) -> FakeResponse:
        return FakeResponse(b"", status=status, headers=headers)

    with pytest.raises(FileSizeProtocolError):
        resolve_hub_file_size(REPO_ID, REVISION, PATH, opener=opener)


def test_head_validates_the_pinned_path_before_any_request() -> None:
    hub = FakeHub()

    with pytest.raises(ValueError):
        resolve_hub_file_size(
            REPO_ID, REVISION, f"{TABLE_ROOT}/../part-00000.parquet", opener=hub
        )

    assert hub.requests == []


@pytest.mark.parametrize(
    ("path", "table_root"),
    [
        ("data/part-00000.parquet", TABLE_ROOT),
        (PATH, "labels/language-v1/data/archive"),
        (f"{TABLE_ROOT}/../part-00000.parquet", TABLE_ROOT),
    ],
)
def test_writer_rejects_unpinned_or_undeclared_path_before_network(
    tmp_path: Path, path: str, table_root: str
) -> None:
    hub = FakeHub()

    with pytest.raises(ValueError):
        _increment(hub, tmp_path / "shards", path=path, table_root=table_root)

    assert hub.requests == []


@pytest.mark.parametrize(
    ("repo_id", "revision"),
    [("NoeFlandre/other-dataset", REVISION), (REPO_ID, OTHER_REVISION)],
)
def test_writer_rejects_a_mismatched_repo_or_revision_before_any_head(
    tmp_path: Path, repo_id: str, revision: str
) -> None:
    hub = FakeHub()
    output_dir = tmp_path / "shards"

    with pytest.raises(ValueError, match="frozen catalog"):
        _increment(hub, output_dir, repo_id=repo_id, revision=revision)

    assert hub.requests == []
    assert not output_dir.exists() or list(output_dir.iterdir()) == []


def test_existing_file_as_output_dir_fails_before_any_hub_request(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "occupied"
    output_dir.write_bytes(b"not a directory")
    hub = FakeHub()

    with pytest.raises(FileExistsError):
        _increment(hub, output_dir)

    assert hub.requests == []
    assert output_dir.read_bytes() == b"not a directory"


def test_writer_rejects_a_projection_not_present_in_the_input_schema(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "shards"

    with pytest.raises(ValueError, match="unknown projection"):
        _increment(FakeHub(), output_dir, projection=("missing",))

    assert not output_dir.exists() or list(output_dir.iterdir()) == []


def test_first_run_writes_a_compressed_projected_shard_and_checkpoint(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "shards"

    result = _increment(FakeHub(), output_dir)

    shard = _single(output_dir, "*.parquet")
    checkpoint = _single(output_dir, "*.checkpoint.json")
    assert result.status == "written"
    assert result.shard_path == shard
    assert pq.ParquetFile(shard).schema_arrow.names == ["label"]
    assert pq.ParquetFile(shard).metadata.row_group(0).column(0).compression == "ZSTD"
    assert pq.read_table(shard).column("label").to_pylist() == ["skipped", "yes", "no"]
    assert json.loads(checkpoint.read_text(encoding="utf-8")) == {
        "checkpoint_version": 1,
        "source_id": SOURCE_ID,
        "repo_id": REPO_ID,
        "revision": REVISION,
        "path": PATH,
        "file_size_bytes": len(PAYLOAD),
        "table": TABLE,
        "table_root": TABLE_ROOT,
        "compression": "zstd",
        "row_group_index": 1,
        "projection": ["label"],
        "row_count": 3,
        "output_name": shard.name,
        "output_size_bytes": shard.stat().st_size,
        "output_sha256": hashlib.sha256(shard.read_bytes()).hexdigest(),
    }
    assert list(output_dir.glob("*.tmp")) == []


def test_identical_inputs_write_byte_identical_shards(tmp_path: Path) -> None:
    first = _increment(FakeHub(), tmp_path / "first")
    second = _increment(FakeHub(), tmp_path / "second")

    assert first.shard_path.name == second.shard_path.name
    assert first.shard_path.read_bytes() == second.shard_path.read_bytes()
    assert first.checkpoint == second.checkpoint


def test_output_names_are_path_safe_and_distinguish_same_named_files(
    tmp_path: Path,
) -> None:
    root_shard = _increment(FakeHub(), tmp_path / "root")
    archive_shard = _increment(
        FakeHub(),
        tmp_path / "archive",
        path=f"{TABLE_ROOT}/archive/part-00000.parquet",
    )

    assert re.fullmatch(r"[a-z0-9_-]+\.parquet", root_shard.shard_path.name)
    assert root_shard.shard_path.name != archive_shard.shard_path.name


def test_different_projections_write_distinct_shards_that_coexist_and_resume(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "shards"

    narrow = _increment(FakeHub(), output_dir, projection=("label",))
    wide = _increment(FakeHub(), output_dir, projection=("label", "text"))

    assert narrow.shard_path != wide.shard_path
    assert narrow.checkpoint_path != wide.checkpoint_path
    assert pq.ParquetFile(wide.shard_path).schema_arrow.names == ["label", "text"]
    assert len(list(output_dir.glob("*.parquet"))) == 2
    assert len(list(output_dir.glob("*.checkpoint.json"))) == 2
    narrow_rerun = _increment(FakeHub(), output_dir, projection=("label",))
    wide_rerun = _increment(FakeHub(), output_dir, projection=("label", "text"))
    assert (narrow_rerun.status, wide_rerun.status) == ("verified", "verified")


def test_projection_order_does_not_change_the_output_name(tmp_path: Path) -> None:
    output_dir = tmp_path / "shards"

    first = _increment(FakeHub(), output_dir, projection=("text", "label"))
    second = _increment(FakeHub(), output_dir, projection=("label", "text"))

    assert second.status == "verified"
    assert second.shard_path == first.shard_path


def test_different_revisions_of_the_same_path_write_distinct_shards(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "shards"
    source, _ = select_input_file(
        SOURCE_CATALOG, source_id=SOURCE_ID, table=TABLE, path=PATH
    )
    pinned = _increment(FakeHub(), output_dir)

    monkeypatch.setattr(
        shard_increment, "SOURCE_CATALOG", (replace(source, revision=OTHER_REVISION),)
    )
    other = _increment(FakeHub(), output_dir, revision=OTHER_REVISION)

    assert other.status == "written"
    assert other.shard_path != pinned.shard_path
    assert len(list(output_dir.glob("*.parquet"))) == 2

    monkeypatch.setattr(shard_increment, "SOURCE_CATALOG", (source,))
    assert _increment(FakeHub(), output_dir).status == "verified"


def test_rerun_verifies_the_shard_without_new_fake_range_requests(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "shards"
    hub = FakeHub()
    written = _increment(hub, output_dir)
    ranges_after_write = len(hub.range_requests)

    verified = _increment(hub, output_dir)

    assert verified.status == "verified"
    assert verified.checkpoint == written.checkpoint
    assert len(hub.range_requests) == ranges_after_write


def test_resume_preserves_a_shard_that_fails_sha256_verification(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "shards"
    written = _increment(FakeHub(), output_dir)
    corrupted = bytearray(written.shard_path.read_bytes())
    corrupted[len(corrupted) // 2] ^= 0x01
    written.shard_path.write_bytes(bytes(corrupted))
    shard_bytes = written.shard_path.read_bytes()
    checkpoint_bytes = written.checkpoint_path.read_bytes()

    with pytest.raises(ShardConflictError, match="SHA-256"):
        _increment(FakeHub(), output_dir)

    assert written.shard_path.read_bytes() == shard_bytes
    assert written.checkpoint_path.read_bytes() == checkpoint_bytes


def test_orphan_shard_without_checkpoint_is_preserved(tmp_path: Path) -> None:
    output_dir = tmp_path / "shards"
    written = _increment(FakeHub(), output_dir)
    written.checkpoint_path.unlink()
    shard_bytes = written.shard_path.read_bytes()
    hub = FakeHub()

    with pytest.raises(ShardConflictError, match="orphan"):
        _increment(hub, output_dir)

    assert written.shard_path.read_bytes() == shard_bytes
    assert not written.checkpoint_path.exists()
    assert hub.range_requests == []


def test_checkpoint_without_its_shard_is_a_conflict_not_a_refetch(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "shards"
    written = _increment(FakeHub(), output_dir)
    written.shard_path.unlink()
    hub = FakeHub()

    with pytest.raises(ShardConflictError, match="missing shard"):
        _increment(hub, output_dir)

    assert hub.range_requests == []
    assert written.checkpoint_path.exists()


@pytest.mark.parametrize("contents", ["{", "[]"])
def test_corrupt_checkpoint_is_a_conflict_and_preserves_both_files(
    tmp_path: Path, contents: str
) -> None:
    output_dir = tmp_path / "shards"
    written = _increment(FakeHub(), output_dir)
    written.checkpoint_path.write_text(contents, encoding="utf-8")
    shard_bytes = written.shard_path.read_bytes()
    checkpoint_bytes = written.checkpoint_path.read_bytes()

    with pytest.raises(ShardConflictError, match="checkpoint"):
        _increment(FakeHub(), output_dir)

    assert written.shard_path.read_bytes() == shard_bytes
    assert written.checkpoint_path.read_bytes() == checkpoint_bytes


@pytest.mark.parametrize(
    ("checkpoint_field", "value"),
    [
        ("row_count", 99),
        ("output_name", "elsewhere.parquet"),
        ("compression", "brotli"),
        ("checkpoint_version", 2),
        ("file_size_bytes", len(PAYLOAD) + 1),
        ("revision", OTHER_REVISION),
        ("table_root", "wrong/root"),
        ("projection", ["label", "text"]),
    ],
)
def test_resume_rejects_checkpoint_metadata_mismatch(
    tmp_path: Path, checkpoint_field: str, value: object
) -> None:
    output_dir = tmp_path / "shards"
    written = _increment(FakeHub(), output_dir)
    checkpoint = json.loads(written.checkpoint_path.read_text(encoding="utf-8"))
    checkpoint[checkpoint_field] = value
    written.checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    shard_bytes = written.shard_path.read_bytes()
    checkpoint_bytes = written.checkpoint_path.read_bytes()

    with pytest.raises(ShardConflictError):
        _increment(FakeHub(), output_dir)

    assert written.shard_path.read_bytes() == shard_bytes
    assert written.checkpoint_path.read_bytes() == checkpoint_bytes


def test_resume_rejects_a_valid_parquet_with_the_wrong_schema(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "shards"
    written = _increment(FakeHub(), output_dir)
    wrong_schema = pa.table({"text": ["not the projected label"]})
    pq.write_table(wrong_schema, written.shard_path, compression="zstd")
    checkpoint = json.loads(written.checkpoint_path.read_text(encoding="utf-8"))
    checkpoint["output_size_bytes"] = written.shard_path.stat().st_size
    checkpoint["output_sha256"] = hashlib.sha256(
        written.shard_path.read_bytes()
    ).hexdigest()
    checkpoint["row_count"] = wrong_schema.num_rows
    written.checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    shard_bytes = written.shard_path.read_bytes()
    checkpoint_bytes = written.checkpoint_path.read_bytes()

    with pytest.raises(ShardConflictError, match="schema"):
        _increment(FakeHub(), output_dir)

    assert written.shard_path.read_bytes() == shard_bytes
    assert written.checkpoint_path.read_bytes() == checkpoint_bytes


def test_byte_budget_failure_writes_no_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "shards"
    output_dir.mkdir()

    with pytest.raises(ByteBudgetExceeded):
        _increment(FakeHub(), output_dir, max_bytes=8)

    assert list(output_dir.iterdir()) == []


def test_range_budget_exact_fit_succeeds_and_one_byte_short_fails(
    tmp_path: Path,
) -> None:
    measured_hub = FakeHub()
    _increment(measured_hub, tmp_path / "measured", max_bytes=MAX_BYTES)
    required = sum(_range_size(request) for request in measured_hub.range_requests)
    assert required > 1

    exact_hub = FakeHub()
    _increment(exact_hub, tmp_path / "exact", max_bytes=required)
    assert sum(_range_size(request) for request in exact_hub.range_requests) == required

    too_small = tmp_path / "too-small"
    too_small.mkdir()
    with pytest.raises(ByteBudgetExceeded):
        _increment(FakeHub(), too_small, max_bytes=required - 1)
    assert list(too_small.iterdir()) == []


def test_non_partial_range_response_writes_no_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "shards"
    output_dir.mkdir()

    with pytest.raises(RangeProtocolError):
        _increment(FakeHub(range_status=200), output_dir)

    assert list(output_dir.iterdir()) == []


@pytest.mark.parametrize(
    ("hub", "message"),
    [
        (
            FakeHub(range_header_overrides={"Content-Range": "bytes 0-1/4"}),
            "Content-Range",
        ),
        (FakeHub(missing_range_headers=("Content-Length",)), "Content-Length"),
        (FakeHub(range_header_overrides={"Content-Length": "1"}), "Content-Length"),
        (
            FakeHub(range_header_overrides={"Content-Encoding": "gzip"}),
            "content encoding",
        ),
        (FakeHub(short_range_body=True), "response body"),
    ],
)
def test_malformed_range_responses_write_no_outputs(
    tmp_path: Path, hub: FakeHub, message: str
) -> None:
    output_dir = tmp_path / "shards"

    with pytest.raises(RangeProtocolError, match=message):
        _increment(hub, output_dir)

    assert not output_dir.exists() or list(output_dir.iterdir()) == []


def test_failed_shard_write_leaves_no_final_or_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "shards"

    def interrupted_write(table: pa.Table, handle: BinaryIO, **_: object) -> None:
        handle.write(b"PAR1 partial")
        raise OSError("disk full")

    monkeypatch.setattr(shard_increment.pq, "write_table", interrupted_write)

    with pytest.raises(OSError, match="disk full"):
        _increment(FakeHub(), output_dir)

    assert list(output_dir.iterdir()) == []


def test_shard_metadata_mismatch_is_rejected_before_atomic_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "shards"
    original_write_table = pq.write_table

    def write_wrong_schema(_table: pa.Table, handle: BinaryIO, **_: object) -> None:
        original_write_table(
            pa.table({"wrong": ["a", "b", "c"]}), handle, compression="zstd"
        )

    monkeypatch.setattr(shard_increment.pq, "write_table", write_wrong_schema)

    with pytest.raises(OSError, match="schema"):
        _increment(FakeHub(), output_dir)

    assert list(output_dir.iterdir()) == []


def test_shard_and_checkpoint_renames_sync_the_output_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "shards"
    synced: list[Path] = []
    sync_directory = shard_increment._fsync_directory

    def record_sync(path: Path) -> None:
        synced.append(path)
        sync_directory(path)

    monkeypatch.setattr(shard_increment, "_fsync_directory", record_sync)

    _increment(FakeHub(), output_dir)

    assert synced == [output_dir, output_dir]


FAKE_F_FULLFSYNC = 4242


def _record_darwin_directory_fsync(
    monkeypatch: pytest.MonkeyPatch,
    *,
    full_sync_error: OSError | None = None,
) -> tuple[list[tuple[bool, int]], list[int]]:
    full_syncs: list[tuple[bool, int]] = []
    plain_fsyncs: list[int] = []

    def fake_fcntl(descriptor: int, command: int, *_: object) -> int:
        is_directory = stat.S_ISDIR(os.fstat(descriptor).st_mode)
        full_syncs.append((is_directory, command))
        if full_sync_error is not None:
            raise full_sync_error
        return 0

    monkeypatch.setattr(shard_increment, "sys", SimpleNamespace(platform="darwin"))
    monkeypatch.setattr(fcntl, "F_FULLFSYNC", FAKE_F_FULLFSYNC, raising=False)
    monkeypatch.setattr(fcntl, "fcntl", fake_fcntl)
    monkeypatch.setattr(os, "fsync", plain_fsyncs.append)
    return full_syncs, plain_fsyncs


def test_directory_fsync_uses_full_fsync_on_darwin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    full_syncs, plain_fsyncs = _record_darwin_directory_fsync(monkeypatch)

    shard_increment._fsync_directory(tmp_path)

    assert full_syncs == [(True, FAKE_F_FULLFSYNC)]
    assert plain_fsyncs == []


@pytest.mark.parametrize("unsupported", [errno.EINVAL, errno.ENOTSUP, errno.ENOTTY])
def test_directory_fsync_falls_back_to_fsync_when_full_fsync_is_unsupported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unsupported: int
) -> None:
    full_syncs, plain_fsyncs = _record_darwin_directory_fsync(
        monkeypatch, full_sync_error=OSError(unsupported, "unsupported")
    )

    shard_increment._fsync_directory(tmp_path)

    assert full_syncs == [(True, FAKE_F_FULLFSYNC)]
    assert len(plain_fsyncs) == 1


def test_directory_fsync_does_not_hide_unexpected_full_fsync_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    full_syncs, plain_fsyncs = _record_darwin_directory_fsync(
        monkeypatch, full_sync_error=OSError(errno.EIO, "device I/O error")
    )

    with pytest.raises(OSError, match="device I/O error"):
        shard_increment._fsync_directory(tmp_path)

    assert full_syncs == [(True, FAKE_F_FULLFSYNC)]
    assert plain_fsyncs == []


def test_parallel_atomic_writes_use_distinct_temporary_files(tmp_path: Path) -> None:
    destination = tmp_path / "same.parquet"
    barrier = Barrier(2)
    payload = b"identical deterministic output"

    def write_same(handle: BinaryIO) -> None:
        barrier.wait(timeout=3)
        handle.write(payload)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(_write_atomically, destination, write_same) for _ in range(2)
        ]
        results = [future.result(timeout=5) for future in futures]

    digest = hashlib.sha256(payload).hexdigest()
    assert results == [(digest, len(payload)), (digest, len(payload))]
    assert destination.read_bytes() == payload
    assert list(tmp_path.glob("*.tmp")) == []


def test_concurrent_different_projection_writers_publish_both_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "shards"
    # Both writers pass their checkpoint and shard existence checks, then wait
    # here, so neither can publish until the other has finished those checks.
    both_checked = Barrier(2, timeout=5)
    original_write_table = pq.write_table

    def write_after_both_checked(
        table: pa.Table, handle: BinaryIO, **options: Any
    ) -> None:
        both_checked.wait()
        original_write_table(table, handle, **options)

    monkeypatch.setattr(shard_increment.pq, "write_table", write_after_both_checked)
    projections = (("label",), ("label", "text"))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(_increment, FakeHub(), output_dir, projection=projection)
            for projection in projections
        ]
        results = [future.result(timeout=10) for future in futures]

    assert [result.status for result in results] == ["written", "written"]
    assert len({result.shard_path for result in results}) == 2
    assert len(list(output_dir.glob("*.parquet"))) == 2
    assert len(list(output_dir.glob("*.checkpoint.json"))) == 2
    assert list(output_dir.glob("*.tmp")) == []
    for projection in projections:
        rerun = _increment(FakeHub(), output_dir, projection=projection)
        assert rerun.status == "verified"


def test_cli_help_documents_the_increment_options_and_exit_codes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["--help"], opener=FakeHub())

    assert exit_info.value.code == 0
    help_text = " ".join(capsys.readouterr().out.split())
    for option in ("--max-bytes", "--column", "--output-dir", "exit codes"):
        assert option in help_text


@pytest.mark.parametrize(
    "extra",
    [
        ["--max-bytes", "0"],
        ["--row-group", "-1"],
        ["--column", "label"],
        ["--source", "missing"],
        ["--path", "data/part-00000.parquet"],
        ["--column", ""],
    ],
)
def test_cli_usage_errors_exit_2_before_any_hub_request(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], extra: list[str]
) -> None:
    hub = FakeHub()

    with pytest.raises(SystemExit) as exit_info:
        main([*_argv(tmp_path / "shards"), *extra], opener=hub)

    assert exit_info.value.code == 2
    assert "error:" in capsys.readouterr().err
    assert hub.requests == []


def test_cli_maps_hub_protocol_error_to_exit_1(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    hub = FakeHub(range_status=200)

    assert main(_argv(tmp_path / "shards"), opener=hub) == 1

    assert "error:" in capsys.readouterr().err
    assert list((tmp_path / "shards").glob("*")) == []


@pytest.mark.parametrize(
    "protocol_error",
    [BadStatusLine("not an HTTP status line"), IncompleteRead(b"partial")],
)
def test_cli_maps_http_protocol_errors_to_exit_1_without_a_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], protocol_error: HTTPException
) -> None:
    def failing_opener(_request: Request) -> NoReturn:
        raise protocol_error

    assert main(_argv(tmp_path / "shards"), opener=failing_opener) == 1

    err = capsys.readouterr().err
    assert err.startswith("atlas-shard: error: ")
    assert "Traceback" not in err


def test_cli_existing_file_as_output_dir_exits_1_before_any_hub_request(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output_dir = tmp_path / "occupied"
    output_dir.write_bytes(b"not a directory")
    hub = FakeHub()

    assert main(_argv(output_dir), opener=hub) == 1

    assert "error:" in capsys.readouterr().err
    assert hub.requests == []


def test_cli_maps_write_resume_budget_and_conflict_to_exit_codes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output_dir = tmp_path / "shards"
    hub = FakeHub()

    assert main(_argv(output_dir), opener=hub) == 0
    assert capsys.readouterr().out.startswith("written ")
    assert main(_argv(output_dir), opener=hub) == 0
    assert capsys.readouterr().out.startswith("verified ")

    assert main([*_argv(tmp_path / "tight"), "--max-bytes", "8"], opener=hub) == 1
    assert "error:" in capsys.readouterr().err

    _single(output_dir, "*.checkpoint.json").unlink()
    assert main(_argv(output_dir), opener=hub) == 3
    assert "orphan" in capsys.readouterr().err


@dataclass
class ShardCommand:
    hub: FakeHub
    output_dir: Path
    exit_codes: list[int] = field(default_factory=list)
    range_counts: list[int] = field(default_factory=list)
    orphan_bytes: bytes = b""

    def run(self) -> int:
        exit_code = main(_argv(self.output_dir), opener=self.hub)
        self.exit_codes.append(exit_code)
        self.range_counts.append(len(self.hub.range_requests))
        return exit_code


@given(
    "a synthetic label Parquet served by FakeHub under the declared labels root",
    target_fixture="shard_command",
)
def synthetic_label_fixture(tmp_path: Path) -> ShardCommand:
    return ShardCommand(hub=FakeHub(), output_dir=tmp_path / "shards")


@given("a shard was already written but its checkpoint was lost")
def shard_with_lost_checkpoint(shard_command: ShardCommand) -> None:
    assert shard_command.run() == 0
    _single(shard_command.output_dir, "*.checkpoint.json").unlink()
    shard = _single(shard_command.output_dir, "*.parquet")
    shard_command.orphan_bytes = shard.read_bytes()


@when("the shard command writes row group 1 projecting the label column within budget")
def write_row_group(shard_command: ShardCommand) -> None:
    shard_command.run()


@when("the shard command runs again with the same arguments")
def run_again(shard_command: ShardCommand) -> None:
    shard_command.run()


@then("the rerun verifies the shard and issues no new FakeHub range requests")
def rerun_verifies_without_new_range_requests(shard_command: ShardCommand) -> None:
    assert shard_command.exit_codes == [0, 0]
    assert shard_command.range_counts[1] == shard_command.range_counts[0]


@then("the command exits with the conflict code and keeps the orphan shard unchanged")
def orphan_shard_is_preserved(shard_command: ShardCommand) -> None:
    assert shard_command.exit_codes[-1] == 3
    shard = _single(shard_command.output_dir, "*.parquet")
    assert shard.read_bytes() == shard_command.orphan_bytes
    assert list(shard_command.output_dir.glob("*.checkpoint.json")) == []
