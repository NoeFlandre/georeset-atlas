import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import quote
from urllib.request import Request, urlopen

from atlas_domain.source_catalog import (
    SourceDataset,
    companion_input_paths,
    select_input_file,
    validate_catalog,
)

TREE_PAGE_LIMIT = 1000
TREE_API = "https://huggingface.co/api/datasets/{repo}/tree/{revision}/{root}"


class HubInventoryError(ValueError):
    pass


class MetadataResponse(Protocol):
    @property
    def status(self) -> int | None: ...

    def read(self, size: int = -1, /) -> bytes: ...

    def close(self) -> None: ...


MetadataOpener = Callable[..., MetadataResponse]


@dataclass(frozen=True, slots=True)
class HubParquetFile:
    source_id: str
    table: str
    path: str
    size_bytes: int

    def __post_init__(self) -> None:
        if not self.source_id or not self.table or not self.path.endswith(".parquet"):
            raise ValueError("inventory entries must identify a Parquet file")
        if type(self.size_bytes) is not int or self.size_bytes <= 0:
            raise ValueError("inventory file size must be a positive integer")


@dataclass(frozen=True, slots=True)
class AssemblyPartition:
    source_id: str
    labels_path: str
    input_bytes: int


@dataclass(frozen=True, slots=True)
class AssemblyPlan:
    files: tuple[HubParquetFile, ...]
    partitions: tuple[AssemblyPartition, ...]
    unique_source_bytes: int
    execution_transfer_bytes: int
    largest_partition: AssemblyPartition

    @property
    def partition_count(self) -> int:
        return len(self.partitions)


@dataclass(frozen=True, slots=True)
class StorageBudget:
    output_allowance_bytes: int
    contingency_bytes: int
    workspace_allowance_bytes: int
    safety_margin_bytes: int

    @property
    def required_free_bytes(self) -> int:
        return (
            self.output_allowance_bytes
            + self.contingency_bytes
            + self.workspace_allowance_bytes
            + self.safety_margin_bytes
        )


def _tree_url(source: SourceDataset, root: str) -> str:
    repo = quote(source.repo_id, safe="/")
    file_root = quote(root, safe="/")
    return (
        TREE_API.format(repo=repo, revision=source.revision, root=file_root)
        + "?recursive=false&expand=false&limit="
        + str(TREE_PAGE_LIMIT)
    )


def _list_root(
    source: SourceDataset,
    table: str,
    root: str,
    opener: MetadataOpener,
) -> tuple[HubParquetFile, ...]:
    request = Request(
        _tree_url(source, root),
        headers={"Accept": "application/json", "User-Agent": "georeset-atlas/0"},
    )
    response = opener(request, timeout=30)
    try:
        if response.status not in {None, 200}:
            raise HubInventoryError(
                f"Hub tree request returned HTTP {response.status} for "
                f"{source.source_id}/{root}"
            )
        try:
            entries = json.loads(response.read())
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HubInventoryError(
                f"Hub returned invalid tree metadata for {source.source_id}/{root}"
            ) from error
    finally:
        response.close()
    if not isinstance(entries, list):
        raise HubInventoryError(
            f"Hub tree metadata is not a list for {source.source_id}/{root}"
        )
    if len(entries) >= TREE_PAGE_LIMIT:
        raise HubInventoryError(
            f"Hub tree page may be truncated for {source.source_id}/{root}; "
            "refusing an incomplete inventory"
        )

    files: list[HubParquetFile] = []
    seen_paths: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("type") != "file":
            continue
        path = entry.get("path")
        if not isinstance(path, str) or not path.endswith(".parquet"):
            continue
        if not path.startswith(f"{root}/"):
            raise HubInventoryError(
                f"Hub listed a Parquet path outside {root!r}: {path!r}"
            )
        size = entry.get("size")
        if type(size) is not int or size <= 0:
            raise HubInventoryError(f"Hub gave no positive size for {path!r}")
        if path in seen_paths:
            raise HubInventoryError(f"Hub listed {path!r} more than once")
        seen_paths.add(path)
        files.append(HubParquetFile(source.source_id, table, path, size))
    if not files:
        raise HubInventoryError(f"no Parquet files found for {source.source_id}/{root}")
    return tuple(sorted(files, key=lambda item: item.path))


def fetch_pinned_files(
    sources: Sequence[SourceDataset],
    *,
    opener: MetadataOpener | None = None,
) -> tuple[HubParquetFile, ...]:
    validate_catalog(sources)
    open_metadata = opener or urlopen
    files: list[HubParquetFile] = []
    for source in sources:
        for table, roots in source.table_roots.items():
            for root in roots:
                files.extend(_list_root(source, table, root, open_metadata))
    keys = [(file.source_id, file.table, file.path) for file in files]
    if len(keys) != len(set(keys)):
        raise HubInventoryError("the pinned inventory contains duplicate table paths")
    return tuple(
        sorted(files, key=lambda item: (item.source_id, item.table, item.path))
    )


def build_assembly_plan(
    sources: Sequence[SourceDataset],
    files: Sequence[HubParquetFile],
) -> AssemblyPlan:
    validate_catalog(sources)
    by_key = {(item.source_id, item.table, item.path): item for item in files}
    if len(by_key) != len(files):
        raise HubInventoryError("the pinned inventory contains duplicate table paths")
    for item in files:
        source, _ = select_input_file(
            sources, source_id=item.source_id, table=item.table, path=item.path
        )
        if not isinstance(source, SourceDataset):
            raise HubInventoryError("source catalog returned an invalid dataset")

    partitions: list[AssemblyPartition] = []
    for source in sources:
        for label_file in (
            item
            for item in files
            if item.source_id == source.source_id and item.table == "labels"
        ):
            _, labels_root = select_input_file(
                sources,
                source_id=source.source_id,
                table="labels",
                path=label_file.path,
            )
            companions = companion_input_paths(source, labels_root, label_file.path)
            input_paths = {"labels": label_file.path, **companions}
            input_size = 0
            for table, path in input_paths.items():
                item = by_key.get((source.source_id, table, path))
                if item is None:
                    raise HubInventoryError(
                        f"missing companion file {source.source_id}/{table}/{path}"
                    )
                input_size += item.size_bytes
            partitions.append(
                AssemblyPartition(source.source_id, label_file.path, input_size)
            )
    if not partitions:
        raise HubInventoryError("no label partitions were found")
    ordered_partitions = tuple(
        sorted(partitions, key=lambda item: (item.source_id, item.labels_path))
    )
    largest = max(
        ordered_partitions,
        key=lambda item: (item.input_bytes, item.source_id, item.labels_path),
    )
    return AssemblyPlan(
        files=tuple(
            sorted(files, key=lambda item: (item.source_id, item.table, item.path))
        ),
        partitions=ordered_partitions,
        unique_source_bytes=sum(item.size_bytes for item in files),
        execution_transfer_bytes=sum(item.input_bytes for item in ordered_partitions),
        largest_partition=largest,
    )


def estimate_storage_budget(plan: AssemblyPlan) -> StorageBudget:
    output_allowance = plan.unique_source_bytes * 4
    contingency = (output_allowance * 25 + 99) // 100
    workspace = plan.largest_partition.input_bytes * 4
    return StorageBudget(
        output_allowance_bytes=output_allowance,
        contingency_bytes=contingency,
        workspace_allowance_bytes=workspace,
        safety_margin_bytes=16 * 1024 * 1024,
    )
