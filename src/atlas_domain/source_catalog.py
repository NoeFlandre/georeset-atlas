import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, TypeAlias

from atlas_domain.sources import validate_pinned_reference

SourceStatus: TypeAlias = Literal["yes", "no", "failed", "skipped"]
EXPECTED_SOURCE_STATUSES = frozenset({"yes", "no", "failed", "skipped"})
_SOURCE_ID = re.compile(r"[a-z][a-z0-9_-]*")


@dataclass(frozen=True, slots=True)
class SourceDataset:
    source_id: str
    repo_id: str
    revision: str
    table_roots: Mapping[str, Sequence[str]]
    expected_status_counts: Mapping[SourceStatus, int]

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not _SOURCE_ID.fullmatch(
            self.source_id
        ):
            raise ValueError("source_id must be a lowercase identifier")
        validate_pinned_reference(self.repo_id, self.revision)
        object.__setattr__(self, "table_roots", _freeze_table_roots(self.table_roots))
        object.__setattr__(
            self,
            "expected_status_counts",
            _freeze_status_counts(self.expected_status_counts),
        )

    @property
    def expected_label_total(self) -> int:
        return sum(self.expected_status_counts.values())


def _safe_relative_root(root: object) -> str:
    if not isinstance(root, str) or not root or "\\" in root or root.startswith("/"):
        raise ValueError("table roots must be safe relative paths")
    if any(part in {"", ".", ".."} for part in root.split("/")):
        raise ValueError("table roots must not contain empty or traversal segments")
    return root


def _freeze_table_roots(
    table_roots: object,
) -> Mapping[str, tuple[str, ...]]:
    if not isinstance(table_roots, Mapping) or not table_roots:
        raise ValueError("table_roots must be a non-empty mapping")
    frozen: dict[str, tuple[str, ...]] = {}
    for table_name, raw_roots in table_roots.items():
        if not isinstance(table_name, str) or not table_name:
            raise ValueError("table names must be non-empty strings")
        if (
            isinstance(raw_roots, (str, bytes, bytearray))
            or not isinstance(raw_roots, Sequence)
            or not raw_roots
        ):
            raise ValueError(f"roots for table {table_name!r} must be a non-empty list")
        roots = tuple(_safe_relative_root(root) for root in raw_roots)
        if len(roots) != len(set(roots)):
            raise ValueError(f"roots for table {table_name!r} must be unique")
        frozen[table_name] = roots
    return MappingProxyType(frozen)


def _freeze_status_counts(
    status_counts: object,
) -> Mapping[SourceStatus, int]:
    if not isinstance(status_counts, Mapping):
        raise ValueError("expected_status_counts must be a mapping")
    if set(status_counts) != EXPECTED_SOURCE_STATUSES:
        raise ValueError("expected_status_counts must contain all four label statuses")
    frozen: dict[SourceStatus, int] = {}
    for status, count in status_counts.items():
        if (
            not isinstance(status, str)
            or isinstance(count, bool)
            or not isinstance(count, int)
        ):
            raise ValueError("label status counts must be positive integers")
        if count <= 0:
            raise ValueError("label status counts must be positive integers")
        frozen[status] = count
    return MappingProxyType(frozen)


def validate_catalog(catalog: Iterable[SourceDataset]) -> None:
    if isinstance(catalog, (str, bytes, bytearray)):
        raise ValueError("catalog must contain SourceDataset records")
    seen_source_ids: set[str] = set()
    seen_repo_ids: set[str] = set()
    count = 0
    for source in catalog:
        if not isinstance(source, SourceDataset):
            raise ValueError("catalog must contain SourceDataset records")
        if source.source_id in seen_source_ids or source.repo_id in seen_repo_ids:
            raise ValueError("source IDs and repositories must be unique")
        seen_source_ids.add(source.source_id)
        seen_repo_ids.add(source.repo_id)
        count += 1
    if count == 0:
        raise ValueError("catalog must contain at least one source")


def _safe_parquet_path(path: object) -> str:
    if (
        not isinstance(path, str)
        or not path.endswith(".parquet")
        or "\\" in path
        or path.startswith("/")
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
        raise ValueError("input path must be a safe relative .parquet file path")
    return path


def companion_input_paths(
    source: SourceDataset,
    labels_root: str,
    labels_path: str,
) -> dict[str, str]:
    safe_path = _safe_parquet_path(labels_path)
    if labels_root not in source.table_roots.get("labels", ()):
        raise ValueError("labels_root must be declared by the source catalog")
    project = labels_root.removeprefix("labels/").split("/")[0]
    basename = safe_path.rsplit("/", 1)[-1]
    paths: dict[str, str] = {}
    for table, roots in source.table_roots.items():
        if table == "labels":
            continue
        candidates = (
            tuple(root for root in roots if root.split("/")[0] == project)
            if len(roots) > 1
            else roots
        )
        if len(candidates) != 1:
            raise ValueError(f"no unique {table!r} root for project {project!r}")
        paths[table] = f"{candidates[0]}/{basename}"
    return paths


def select_input_file(
    catalog: Iterable[SourceDataset],
    *,
    source_id: str,
    table: str,
    path: str,
) -> tuple[SourceDataset, str]:
    sources = tuple(catalog)
    matches = [source for source in sources if source.source_id == source_id]
    if not matches:
        known = ", ".join(sorted(source.source_id for source in sources))
        raise ValueError(f"unknown source {source_id!r}; expected one of: {known}")
    (source,) = matches
    if table not in source.table_roots:
        known = ", ".join(sorted(source.table_roots))
        raise ValueError(
            f"source {source_id!r} has no table {table!r}; expected one of: {known}"
        )
    safe_path = _safe_parquet_path(path)
    roots = source.table_roots[table]
    matching = [root for root in roots if safe_path.startswith(f"{root}/")]
    if len(matching) != 1:
        declared = ", ".join(roots)
        raise ValueError(
            f"{safe_path!r} is not under one declared root of {table!r}: {declared}"
        )
    return source, matching[0]


SOURCE_CATALOG: tuple[SourceDataset, ...] = (
    SourceDataset(
        source_id="description",
        repo_id="NoeFlandre/osm-polygon-description-tag-landuse",
        revision="ce98b328f3c6543800a3d931877b51eb3f0cf768",
        table_roots={
            "polygons": ("data",),
            "text_units": ("language-v1/data",),
            "labels": ("labels/language-v1/data",),
        },
        expected_status_counts={
            "yes": 725_371,
            "no": 241_475,
            "failed": 11_927,
            "skipped": 78_229,
        },
    ),
    SourceDataset(
        source_id="website",
        repo_id="NoeFlandre/osm-polygon-website-tag-landuse",
        revision="e6524c711ee0033c3cd5990e5fdbbe4b89992dfb",
        table_roots={
            "polygons_and_text": ("polygons",),
            "labels": ("labels/polygons",),
        },
        expected_status_counts={
            "yes": 8_869_643,
            "no": 36_523_593,
            "failed": 430_211,
            "skipped": 37_473,
        },
    ),
    SourceDataset(
        source_id="wiki",
        repo_id="NoeFlandre/osm-polygon-wikidata-and-wikipedia-landuse",
        revision="49307aeb05b6dcc8a5d393f77f59a1e6d13a967a",
        table_roots={
            "polygons": ("polygons",),
            "text_units": ("wikipedia/sentences", "wikivoyage/sentences"),
            "documents": ("wikipedia/documents", "wikivoyage/documents"),
            "polygon_document_links": ("polygon_document_links",),
            "labels": (
                "labels/wikipedia/sentences",
                "labels/wikivoyage/sentences",
            ),
        },
        expected_status_counts={
            "yes": 25_868_341,
            "no": 46_068_954,
            "failed": 1_385_457,
            "skipped": 1_469_177,
        },
    ),
)

validate_catalog(SOURCE_CATALOG)
