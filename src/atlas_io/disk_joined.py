import sqlite3
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from itertools import islice
from pathlib import Path

import pyarrow as pa

from atlas_domain import joined_outputs
from atlas_domain.joined_outputs import (
    STATUSES,
    DescriptionAssociation,
    JoinedPartition,
    Row,
    _identifier,
    _label_row,
    description_association,
    description_polygon_identity,
    description_text_identity,
    normalize_row_index,
)
from atlas_domain.occurrences import (
    description_occurrence_key,
    website_occurrence_key,
    wiki_document_project_key,
    wiki_sentence_key,
)
from atlas_domain.source_records import DataJoinError, description_text_for_label
from atlas_io.disk_join_index import DiskJoinIndex, DiskMembershipSet, JoinKey
from atlas_io.joined_partitions import StagedRows, _schema_with_source

_BATCH_ROWS = 512
_TEXT_KEY = "description-text-key"
_POLYGON_ID = "description-polygon-id"
_POLYGON_ASSOCIATION = "description-polygon-association"
_WEBSITE_POLYGON_ID = "website-polygon-id"
_WIKI_SENTENCE_ID = "wiki-sentence-id"
_WIKI_DOCUMENT_PROJECT = "wiki-document-project"
_WIKI_LINK_OCCURRENCE = "wiki-link-occurrence"
_WIKI_POLYGON_ID = "wiki-polygon-id"


def _batches(rows: StagedRows) -> Iterator[tuple[int, list[Row]]]:
    for start in range(0, len(rows), _BATCH_ROWS):
        end = min(start + _BATCH_ROWS, len(rows))
        yield start, rows.read_many(range(start, end))


def _index_polygons(index: DiskJoinIndex, polygons: StagedRows) -> None:
    for start, rows in _batches(polygons):
        joined_outputs.join_description_partition(
            label_rows=(), text_unit_rows=(), polygon_rows=rows
        )
        for offset, polygon in enumerate(rows):
            association, polygon_id = description_polygon_identity(polygon, "polygon")
            try:
                index.add_unique((_POLYGON_ID, polygon_id), start + offset)
            except ValueError as error:
                raise DataJoinError(f"duplicate polygon_id {polygon_id!r}") from error
            index.add((_POLYGON_ASSOCIATION, association), start + offset)


def _index_text_units(index: DiskJoinIndex, text_units: StagedRows) -> None:
    for start, rows in _batches(text_units):
        joined_outputs.join_description_partition(
            label_rows=(), text_unit_rows=rows, polygon_rows=()
        )
        for offset, text_unit in enumerate(rows):
            text_key = description_text_identity(text_unit, "text_unit")
            try:
                index.add_unique((_TEXT_KEY, *text_key), start + offset)
            except ValueError as error:
                raise DataJoinError(
                    f"duplicate description text unit {text_key!r}"
                ) from error


class _AssociatedPolygonRows:
    def __init__(
        self,
        index: DiskJoinIndex,
        polygons: StagedRows,
        associations: set[DescriptionAssociation],
    ) -> None:
        self._index = index
        self._polygons = polygons
        self._associations = associations

    def __iter__(self) -> Iterator[Row]:
        occurrences = self._index.iter_row_occurrences_for_keys(
            ((_POLYGON_ASSOCIATION, association) for association in self._associations),
            max_unique_keys=len(self._associations),
        )
        while batch := list(islice(occurrences, _BATCH_ROWS)):
            yield from self._polygons.read_many(
                [row_ordinal for _, row_ordinal in batch]
            )


def _polygon_rows_for_associations(
    index: DiskJoinIndex,
    polygons: StagedRows,
    associations: set[DescriptionAssociation],
) -> _AssociatedPolygonRows:
    return _AssociatedPolygonRows(index, polygons, associations)


def _create_selection_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE selected_text_units ("
        "sequence INTEGER PRIMARY KEY, identity TEXT NOT NULL, "
        "tag_key TEXT NOT NULL, sentence_index INTEGER NOT NULL, "
        "text_ordinal INTEGER NOT NULL, label_ordinal INTEGER NOT NULL, "
        "UNIQUE(identity, tag_key, sentence_index))"
    )


def _record_label_batch(
    *,
    index: DiskJoinIndex,
    connection: sqlite3.Connection,
    text_units: StagedRows,
    polygons: StagedRows,
    start: int,
    batch: list[Row],
    status_counts: Counter[str],
    raw_decision_counts: Counter[str],
    selected_polygon_ids: DiskMembershipSet,
) -> int:
    text_ordinals_by_key: dict[tuple[str, str], int] = {}
    for label in batch:
        text_key = description_text_identity(label, "label")
        if text_key not in text_ordinals_by_key:
            ordinal = index.lookup_unique((_TEXT_KEY, *text_key))
            if ordinal is None:
                raise DataJoinError(
                    "label has no description text unit "
                    f"for {text_key[0]!r} {text_key[1]!r}"
                )
            text_ordinals_by_key[text_key] = ordinal

    unique_text_ordinals = list(dict.fromkeys(text_ordinals_by_key.values()))
    source_text_rows = text_units.read_many(unique_text_ordinals)
    associations = {
        description_association(text_unit, "text_unit")
        for text_unit in source_text_rows
    }
    polygon_rows = _polygon_rows_for_associations(index, polygons, associations)
    joined = joined_outputs.join_description_partition(
        label_rows=batch,
        text_unit_rows=source_text_rows,
        polygon_rows=polygon_rows,
    )
    status_counts.update(joined.status_counts)
    raw_decision_counts.update(joined.raw_decision_counts)

    first_label_by_key: dict[object, int] = {}
    for label in joined.labels:
        first_label_by_key.setdefault(
            label["text_unit_key"], start + label["source_row_index"]
        )

    new_text_units: list[tuple[str, str, int, int, int]] = []
    selected_polygon_count = 0
    for text_unit in joined.text_units:
        key = text_unit["text_unit_key"]
        identity, tag_key, sentence_index = key
        if connection.execute(
            "SELECT 1 FROM selected_text_units "
            "WHERE identity = ? AND tag_key = ? AND sentence_index = ?",
            (identity, tag_key, sentence_index),
        ).fetchone():
            continue
        text_ordinal = text_ordinals_by_key[(identity, tag_key)]
        new_text_units.append(
            (
                identity,
                tag_key,
                sentence_index,
                text_ordinal,
                first_label_by_key[key],
            )
        )
        for polygon_id in text_unit["polygon_ids"]:
            if not selected_polygon_ids.add(polygon_id):
                continue
            polygon_ordinal = index.lookup_unique((_POLYGON_ID, polygon_id))
            if polygon_ordinal is None:
                raise DataJoinError(
                    f"description polygon {polygon_id!r} is absent from its index"
                )
            selected_polygon_count += 1

    selected_polygon_ids.commit()
    connection.executemany(
        "INSERT INTO selected_text_units "
        "(identity, tag_key, sentence_index, text_ordinal, label_ordinal) "
        "VALUES (?, ?, ?, ?, ?)",
        new_text_units,
    )
    connection.commit()
    return selected_polygon_count


class _DescriptionLabels:
    def __init__(self, labels: StagedRows) -> None:
        self._labels = labels
        self.output_schema = _schema_with_source(
            labels.schema_arrow,
            {
                "source_row_index": pa.int64(),
                "raw_decision": pa.string(),
                "status": pa.string(),
            },
            excluded={"decision"},
        )

    def __len__(self) -> int:
        return len(self._labels)

    def __getitem__(self, index: int) -> Row:
        ordinal = normalize_row_index(index, len(self))
        return self._joined_row(self._labels.read_many([ordinal])[0], ordinal)

    def __iter__(self) -> Iterator[Row]:
        for start, rows in _batches(self._labels):
            for offset, label in enumerate(rows):
                yield self._joined_row(label, start + offset)

    @staticmethod
    def _joined_row(label: Row, ordinal: int) -> Row:
        key = description_occurrence_key(
            *description_text_identity(label, "label"),
            label["sentence_index"],
        )
        return _label_row(label, ordinal, key)


class _DescriptionTextUnits:
    def __init__(
        self,
        *,
        workspace_path: Path,
        text_units: StagedRows,
        labels: StagedRows,
        polygons: StagedRows,
        count: int,
    ) -> None:
        self._workspace_path = workspace_path
        self._text_units = text_units
        self._labels = labels
        self._polygons = polygons
        self._count = count
        self.output_schema = _schema_with_source(
            text_units.schema_arrow,
            {
                "sentence_index": pa.int64(),
                "text": pa.string(),
                "polygon_ids": pa.list_(pa.string()),
            },
        )

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, index: int) -> Row:
        position = normalize_row_index(index, len(self))
        return next(islice(iter(self), position, None))

    def __iter__(self) -> Iterator[Row]:
        connection = sqlite3.connect(self._workspace_path)
        index = DiskJoinIndex(self._workspace_path)
        try:
            cursor = connection.execute(
                "SELECT identity, tag_key, sentence_index, text_ordinal, "
                "label_ordinal FROM selected_text_units ORDER BY sequence"
            )
            while records := cursor.fetchmany(_BATCH_ROWS):
                text_rows = self._text_units.read_many(
                    [int(record[3]) for record in records]
                )
                label_rows = self._labels.read_many(
                    [int(record[4]) for record in records]
                )
                associations = {
                    description_association(text_unit, "text_unit")
                    for text_unit in text_rows
                }
                polygon_ids_by_association = _polygon_ids_for_associations(
                    index, self._polygons, associations
                )
                for record, text_unit, label in zip(
                    records, text_rows, label_rows, strict=True
                ):
                    key = (str(record[0]), str(record[1]), int(record[2]))
                    association = description_association(text_unit, "text_unit")
                    polygon_ids = polygon_ids_by_association[association]
                    yield {
                        **text_unit,
                        "text_unit_key": key,
                        "sentence_index": key[2],
                        "text": description_text_for_label(label, text_unit),
                        "polygon_ids": list(polygon_ids),
                    }
        finally:
            index.close()
            connection.close()


def _polygon_ids_for_associations(
    index: DiskJoinIndex,
    polygons: StagedRows,
    associations: set[DescriptionAssociation],
) -> dict[DescriptionAssociation, list[str]]:
    association_keys: dict[DescriptionAssociation, JoinKey] = {
        association: (_POLYGON_ASSOCIATION, association) for association in associations
    }
    polygon_ids_by_key: dict[JoinKey, list[str]] = {
        key: [] for key in association_keys.values()
    }
    occurrences = index.iter_row_occurrences_for_keys(
        association_keys.values(), max_unique_keys=len(association_keys)
    )
    while batch := list(islice(occurrences, _BATCH_ROWS)):
        polygon_rows = polygons.read_many([row_ordinal for _, row_ordinal in batch])
        for (key, _), polygon in zip(batch, polygon_rows, strict=True):
            polygon_ids_by_key[key].append(
                description_polygon_identity(polygon, "polygon")[1]
            )
    return {
        association: polygon_ids_by_key[key]
        for association, key in association_keys.items()
    }


class _DescriptionPolygons:
    def __init__(self, workspace_path: Path, polygons: StagedRows, count: int) -> None:
        self._workspace_path = workspace_path
        self._polygons = polygons
        self._count = count
        self.output_schema = _schema_with_source(
            polygons.schema_arrow, {"polygon_id": pa.string()}
        )

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, index: int) -> Row:
        position = normalize_row_index(index, len(self))
        return next(islice(iter(self), position, None))

    def __iter__(self) -> Iterator[Row]:
        selected_polygon_ids = DiskMembershipSet(self._workspace_path)
        try:
            for polygon in self._polygons:
                polygon_id = description_polygon_identity(polygon, "polygon")[1]
                if polygon_id in selected_polygon_ids:
                    yield {**polygon, "polygon_id": polygon_id}
        finally:
            selected_polygon_ids.close()


def build_description_partition(
    *,
    labels: StagedRows,
    text_units: StagedRows,
    polygons: StagedRows,
    workspace_path: str | Path,
) -> JoinedPartition:
    workspace = Path(workspace_path)
    if workspace.exists():
        raise FileExistsError(f"SQLite workspace already exists: {workspace}")

    index = DiskJoinIndex(workspace)
    connection: sqlite3.Connection | None = None
    selected_polygon_ids: DiskMembershipSet | None = None
    try:
        _index_polygons(index, polygons)
        _index_text_units(index, text_units)
        index.commit()
        selected_polygon_ids = DiskMembershipSet(workspace)
        connection = sqlite3.connect(workspace)
        _create_selection_tables(connection)
        connection.commit()

        status_counts: Counter[str] = Counter()
        raw_decision_counts: Counter[str] = Counter()
        polygon_count = 0
        for start, batch in _batches(labels):
            polygon_count += _record_label_batch(
                index=index,
                connection=connection,
                text_units=text_units,
                polygons=polygons,
                start=start,
                batch=batch,
                status_counts=status_counts,
                raw_decision_counts=raw_decision_counts,
                selected_polygon_ids=selected_polygon_ids,
            )
        text_count = int(
            connection.execute("SELECT COUNT(*) FROM selected_text_units").fetchone()[0]
        )
    finally:
        if connection is not None:
            connection.close()
        if selected_polygon_ids is not None:
            selected_polygon_ids.close()
        index.close()

    label_rows = _DescriptionLabels(labels)
    return JoinedPartition(
        labels=label_rows,
        text_units=_DescriptionTextUnits(
            workspace_path=workspace,
            text_units=text_units,
            labels=labels,
            polygons=polygons,
            count=text_count,
        ),
        polygon_records=_DescriptionPolygons(workspace, polygons, polygon_count),
        documents=(),
        status_counts={status: status_counts[status] for status in STATUSES},
        raw_decision_counts=dict(sorted(raw_decision_counts.items())),
    )


def _website_label_key(label: Row) -> tuple[str, str, int]:
    return website_occurrence_key(
        label["polygon_id"], label["field"], label["sentence_index"]
    )


def _index_website_polygons(index: DiskJoinIndex, polygons: StagedRows) -> None:
    for start, rows in _batches(polygons):
        joined_outputs.join_website_partition(label_rows=(), polygon_text_rows=rows)
        for offset, polygon in enumerate(rows):
            polygon_id = polygon["polygon_id"]
            try:
                index.add_unique((_WEBSITE_POLYGON_ID, polygon_id), start + offset)
            except ValueError as error:
                raise DataJoinError(f"duplicate polygon_id {polygon_id!r}") from error


def _create_website_selection_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE selected_website_text_units ("
        "sequence INTEGER PRIMARY KEY AUTOINCREMENT, polygon_id TEXT NOT NULL, "
        "field TEXT NOT NULL, sentence_index INTEGER NOT NULL, "
        "polygon_ordinal INTEGER NOT NULL, label_ordinal INTEGER NOT NULL, "
        "UNIQUE(polygon_id, field, sentence_index))"
    )
    connection.execute(
        "CREATE TABLE selected_website_polygon_rows ("
        "row_ordinal INTEGER PRIMARY KEY CHECK(row_ordinal >= 0))"
    )
    connection.execute(
        "CREATE TABLE website_counts ("
        "kind TEXT NOT NULL, value TEXT NOT NULL, row_count INTEGER NOT NULL, "
        "PRIMARY KEY(kind, value))"
    )


def _record_website_label_batch(
    *,
    index: DiskJoinIndex,
    connection: sqlite3.Connection,
    labels: StagedRows,
    polygons: StagedRows,
    start: int,
    batch: list[Row],
    selected_polygon_ids: DiskMembershipSet,
) -> None:
    polygon_ordinals_by_id: dict[str, int] = {}
    for label in batch:
        polygon_id, _, _ = _website_label_key(label)
        if polygon_id in polygon_ordinals_by_id:
            continue
        polygon_ordinal = index.lookup_unique((_WEBSITE_POLYGON_ID, polygon_id))
        if polygon_ordinal is None:
            raise DataJoinError(f"label has no polygon text row for {polygon_id!r}")
        polygon_ordinals_by_id[polygon_id] = polygon_ordinal

    polygon_rows = polygons.read_many(list(polygon_ordinals_by_id.values()))
    joined = joined_outputs.join_website_partition(
        label_rows=batch,
        polygon_text_rows=polygon_rows,
    )
    first_label_ordinal_by_key: dict[tuple[str, str, int], int] = {}
    count_rows: list[tuple[str, str]] = []
    for label in joined.labels:
        key = label["text_unit_key"]
        first_label_ordinal_by_key.setdefault(key, start + label["source_row_index"])
        count_rows.append(("status", label["status"]))
        count_rows.append(("raw_decision", label["raw_decision"]))

    new_text_units: list[tuple[str, str, int, int, int]] = []
    selected_polygon_ordinals: set[int] = set()
    for text_unit in joined.text_units:
        polygon_id, field, sentence_index = text_unit["text_unit_key"]
        polygon_ordinal = polygon_ordinals_by_id[polygon_id]
        new_text_units.append(
            (
                polygon_id,
                field,
                sentence_index,
                polygon_ordinal,
                first_label_ordinal_by_key[(polygon_id, field, sentence_index)],
            )
        )
        if selected_polygon_ids.add(polygon_id):
            selected_polygon_ordinals.add(polygon_ordinal)

    selected_polygon_ids.commit()
    connection.executemany(
        "INSERT INTO website_counts(kind, value, row_count) VALUES (?, ?, 1) "
        "ON CONFLICT(kind, value) DO UPDATE SET row_count = row_count + 1",
        count_rows,
    )
    connection.executemany(
        "INSERT OR IGNORE INTO selected_website_text_units "
        "(polygon_id, field, sentence_index, polygon_ordinal, label_ordinal) "
        "VALUES (?, ?, ?, ?, ?)",
        new_text_units,
    )
    connection.executemany(
        "INSERT OR IGNORE INTO selected_website_polygon_rows(row_ordinal) VALUES (?)",
        ((ordinal,) for ordinal in selected_polygon_ordinals),
    )
    connection.commit()


class _WebsiteLabels:
    def __init__(self, labels: StagedRows) -> None:
        self._labels = labels
        self.output_schema = _schema_with_source(
            labels.schema_arrow,
            {
                "source_row_index": pa.int64(),
                "raw_decision": pa.string(),
                "status": pa.string(),
            },
            excluded={"decision"},
        )

    def __len__(self) -> int:
        return len(self._labels)

    def __getitem__(self, index: int) -> Row:
        ordinal = normalize_row_index(index, len(self))
        return self._joined_row(self._labels.read_many([ordinal])[0], ordinal)

    def __iter__(self) -> Iterator[Row]:
        for start, rows in _batches(self._labels):
            for offset, label in enumerate(rows):
                yield self._joined_row(label, start + offset)

    @staticmethod
    def _joined_row(label: Row, ordinal: int) -> Row:
        return _label_row(label, ordinal, _website_label_key(label))


class _WebsiteTextUnits:
    def __init__(
        self,
        *,
        workspace_path: Path,
        labels: StagedRows,
        polygons: StagedRows,
        count: int,
    ) -> None:
        self._workspace_path = workspace_path
        self._labels = labels
        self._polygons = polygons
        self._count = count
        source_names = set(polygons.schema_arrow.names)
        extra_fields = {
            "polygon_id": pa.string(),
            "field": pa.string(),
            "sentence_index": pa.int64(),
            "text": pa.string(),
            "full_text": pa.string(),
            "polygon_ids": pa.list_(pa.string()),
        }
        if any(name.endswith("_sentence_status") for name in source_names):
            extra_fields.update(language=pa.string(), sentence_status=pa.string())
        if {"decision", "source_url"}.issubset(source_names):
            extra_fields.update(decision=pa.string(), source_url=pa.string())
        extra_fields.update(
            language_code=pa.string(),
            raw_language_code=pa.string(),
            language_scheme=pa.string(),
            language_mapping_version=pa.string(),
            language_script=pa.string(),
            language_mapping_status=pa.string(),
        )
        self.output_schema = pa.schema(
            [pa.field(name, data_type) for name, data_type in extra_fields.items()]
        )

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, index: int) -> Row:
        position = normalize_row_index(index, len(self))
        return next(islice(iter(self), position, None))

    def __iter__(self) -> Iterator[Row]:
        connection = sqlite3.connect(self._workspace_path)
        try:
            cursor = connection.execute(
                "SELECT polygon_id, field, sentence_index, polygon_ordinal, "
                "label_ordinal FROM selected_website_text_units ORDER BY sequence"
            )
            while records := cursor.fetchmany(_BATCH_ROWS):
                label_rows = self._labels.read_many(
                    [int(record[4]) for record in records]
                )
                polygon_ordinals = list(
                    dict.fromkeys(int(record[3]) for record in records)
                )
                polygon_rows = self._polygons.read_many(polygon_ordinals)
                joined = joined_outputs.join_website_partition(
                    label_rows=label_rows,
                    polygon_text_rows=polygon_rows,
                )
                yield from joined.text_units
        finally:
            connection.close()


class _WebsitePolygons:
    def __init__(self, workspace_path: Path, polygons: StagedRows, count: int) -> None:
        self._workspace_path = workspace_path
        self._polygons = polygons
        self._count = count
        self.output_schema = _schema_with_source(polygons.schema_arrow, {})

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, index: int) -> Row:
        position = normalize_row_index(index, len(self))
        return next(islice(iter(self), position, None))

    def __iter__(self) -> Iterator[Row]:
        connection = sqlite3.connect(self._workspace_path)
        try:
            cursor = connection.execute(
                "SELECT row_ordinal FROM selected_website_polygon_rows "
                "ORDER BY row_ordinal"
            )
            while records := cursor.fetchmany(_BATCH_ROWS):
                rows = self._polygons.read_many([int(record[0]) for record in records])
                yield from (dict(row) for row in rows)
        finally:
            connection.close()


def build_website_partition(
    *,
    labels: StagedRows,
    polygons_and_text: StagedRows,
    workspace_path: str | Path,
) -> JoinedPartition:
    workspace = Path(workspace_path)
    if workspace.exists():
        raise FileExistsError(f"SQLite workspace already exists: {workspace}")

    index = DiskJoinIndex(workspace)
    connection: sqlite3.Connection | None = None
    selected_polygon_ids: DiskMembershipSet | None = None
    status_counts: dict[str, int] = {status: 0 for status in STATUSES}
    raw_decision_counts: dict[str, int] = {}
    try:
        _index_website_polygons(index, polygons_and_text)
        index.commit()
        selected_polygon_ids = DiskMembershipSet(workspace)
        connection = sqlite3.connect(workspace)
        _create_website_selection_tables(connection)
        connection.commit()

        for start, batch in _batches(labels):
            _record_website_label_batch(
                index=index,
                connection=connection,
                labels=labels,
                polygons=polygons_and_text,
                start=start,
                batch=batch,
                selected_polygon_ids=selected_polygon_ids,
            )

        text_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM selected_website_text_units"
            ).fetchone()[0]
        )
        polygon_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM selected_website_polygon_rows"
            ).fetchone()[0]
        )
        for value, count in connection.execute(
            "SELECT value, row_count FROM website_counts WHERE kind = 'status'"
        ):
            status_counts[str(value)] = int(count)
        raw_decision_counts = {
            str(value): int(count)
            for value, count in connection.execute(
                "SELECT value, row_count FROM website_counts "
                "WHERE kind = 'raw_decision' ORDER BY value"
            )
        }
    finally:
        if connection is not None:
            connection.close()
        if selected_polygon_ids is not None:
            selected_polygon_ids.close()
        index.close()

    return JoinedPartition(
        labels=_WebsiteLabels(labels),
        text_units=_WebsiteTextUnits(
            workspace_path=workspace,
            labels=labels,
            polygons=polygons_and_text,
            count=text_count,
        ),
        polygon_records=_WebsitePolygons(workspace, polygons_and_text, polygon_count),
        documents=(),
        status_counts=status_counts,
        raw_decision_counts=raw_decision_counts,
    )


@dataclass(frozen=True, slots=True)
class _WikiJoinBatch:
    partition: JoinedPartition
    sentence_ordinals: Mapping[str, int]
    document_ordinals: Mapping[tuple[str, str], int]
    link_ordinals: Mapping[tuple[str, str], list[int]]
    link_rows: Mapping[tuple[str, str], list[Row]]
    polygon_ordinals: Mapping[str, int]


def _index_wiki_sources(
    *,
    index: DiskJoinIndex,
    sentences: StagedRows,
    documents: StagedRows,
    links: StagedRows,
    polygons: StagedRows,
) -> None:
    for start, rows in _batches(sentences):
        joined_outputs.join_wiki_partition(
            label_rows=(),
            sentence_rows=rows,
            document_rows=(),
            link_rows=(),
            polygon_rows=(),
        )
        for offset, sentence in enumerate(rows):
            sentence_id = wiki_sentence_key(sentence["sentence_id"])
            try:
                index.add_unique((_WIKI_SENTENCE_ID, sentence_id), start + offset)
            except ValueError as error:
                raise DataJoinError(
                    f"duplicate wiki sentence_id {sentence_id!r}"
                ) from error

    for start, rows in _batches(documents):
        joined_outputs.join_wiki_partition(
            label_rows=(),
            sentence_rows=(),
            document_rows=rows,
            link_rows=(),
            polygon_rows=(),
        )
        for offset, document in enumerate(rows):
            key = wiki_document_project_key(
                document["document_id"], document["project"]
            )
            try:
                index.add_unique((_WIKI_DOCUMENT_PROJECT, *key), start + offset)
            except ValueError as error:
                raise DataJoinError(f"duplicate wiki document {key!r}") from error

    for start, rows in _batches(links):
        joined_outputs.join_wiki_partition(
            label_rows=(),
            sentence_rows=(),
            document_rows=(),
            link_rows=rows,
            polygon_rows=(),
        )
        for offset, link in enumerate(rows):
            key = wiki_document_project_key(link["document_id"], link["project"])
            index.add((_WIKI_LINK_OCCURRENCE, *key), start + offset)

    for start, rows in _batches(polygons):
        joined_outputs.join_wiki_partition(
            label_rows=(),
            sentence_rows=(),
            document_rows=(),
            link_rows=(),
            polygon_rows=rows,
        )
        for offset, polygon in enumerate(rows):
            polygon_id = polygon["polygon_id"]
            try:
                index.add_unique((_WIKI_POLYGON_ID, polygon_id), start + offset)
            except ValueError as error:
                raise DataJoinError(f"duplicate polygon_id {polygon_id!r}") from error


def _create_wiki_selection_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE selected_wiki_text_units ("
        "sequence INTEGER PRIMARY KEY, sentence_id TEXT NOT NULL UNIQUE, "
        "sentence_ordinal INTEGER NOT NULL, label_ordinal INTEGER NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE selected_wiki_document_rows ("
        "row_ordinal INTEGER PRIMARY KEY CHECK(row_ordinal >= 0), "
        "document_id TEXT NOT NULL, project TEXT NOT NULL, "
        "UNIQUE(document_id, project))"
    )
    connection.execute(
        "CREATE TABLE selected_wiki_link_rows ("
        "row_ordinal INTEGER PRIMARY KEY CHECK(row_ordinal >= 0))"
    )
    connection.execute(
        "CREATE TABLE selected_wiki_polygon_rows ("
        "row_ordinal INTEGER PRIMARY KEY CHECK(row_ordinal >= 0), "
        "polygon_id TEXT NOT NULL UNIQUE)"
    )
    connection.execute(
        "CREATE TABLE wiki_counts ("
        "kind TEXT NOT NULL, value TEXT NOT NULL, row_count INTEGER NOT NULL, "
        "PRIMARY KEY(kind, value))"
    )


def _join_wiki_batch(
    *,
    index: DiskJoinIndex,
    labels: list[Row],
    sentences: StagedRows,
    documents: StagedRows,
    links: StagedRows,
    polygons: StagedRows,
    sentence_ordinals: Mapping[str, int] | None = None,
) -> _WikiJoinBatch:
    selected_sentence_ordinals: dict[str, int] = {}
    sentence_ids = list(
        dict.fromkeys(_identifier(label, "sentence_id", "label") for label in labels)
    )
    if sentence_ordinals is None:
        sentence_lookups = index.iter_lookup_unique(
            (_WIKI_SENTENCE_ID, sentence_id) for sentence_id in sentence_ids
        )
        for sentence_id, (_, ordinal) in zip(
            sentence_ids, sentence_lookups, strict=True
        ):
            if ordinal is None:
                raise DataJoinError(f"label has no wiki sentence {sentence_id!r}")
            selected_sentence_ordinals[sentence_id] = ordinal
    else:
        for sentence_id in sentence_ids:
            ordinal = sentence_ordinals.get(sentence_id)
            if ordinal is None:
                raise DataJoinError(f"label has no wiki sentence {sentence_id!r}")
            selected_sentence_ordinals[sentence_id] = ordinal

    sentence_rows = sentences.read_many(selected_sentence_ordinals.values())
    sentence_by_document: dict[tuple[str, str], str] = {}
    for sentence_id, sentence in zip(
        selected_sentence_ordinals, sentence_rows, strict=True
    ):
        key = wiki_document_project_key(
            _identifier(sentence, "document_id", "sentence"),
            _identifier(sentence, "project", "sentence"),
        )
        sentence_by_document.setdefault(key, sentence_id)

    document_lookup_keys = {
        (_WIKI_DOCUMENT_PROJECT, *key): key for key in sentence_by_document
    }
    document_ordinals: dict[tuple[str, str], int] = {}
    document_lookups = index.iter_lookup_unique(document_lookup_keys)
    for key, (_, ordinal) in zip(sentence_by_document, document_lookups, strict=True):
        if ordinal is None:
            raise DataJoinError(
                f"sentence {sentence_by_document[key]!r} has no wiki document"
            )
        document_ordinals[key] = ordinal

    document_rows = documents.read_many(document_ordinals.values())
    link_lookup_keys: dict[JoinKey, tuple[str, str]] = {
        (_WIKI_LINK_OCCURRENCE, *key): key for key in document_ordinals
    }
    link_ordinals: dict[tuple[str, str], list[int]] = {
        key: [] for key in document_ordinals
    }
    for indexed_key, ordinal in index.iter_row_occurrences_for_keys(
        link_lookup_keys, max_unique_keys=len(labels)
    ):
        link_ordinals[link_lookup_keys[indexed_key]].append(ordinal)

    link_rows: dict[tuple[str, str], list[Row]] = {}
    polygon_ordinals: dict[str, int] = {}
    polygon_lookup_keys: dict[JoinKey, str] = {}
    for key in document_ordinals:
        ordinals = link_ordinals[key]
        rows = links.read_many(ordinals)
        link_rows[key] = rows
        for link in rows:
            polygon_id = _identifier(link, "polygon_id", "polygon_link")
            polygon_lookup_keys.setdefault((_WIKI_POLYGON_ID, polygon_id), polygon_id)

    polygon_lookups = index.iter_lookup_unique(polygon_lookup_keys)
    for indexed_key, ordinal in polygon_lookups:
        polygon_id = polygon_lookup_keys[indexed_key]
        if ordinal is None:
            raise DataJoinError(f"wiki link names missing polygon {polygon_id!r}")
        polygon_ordinals[polygon_id] = ordinal

    polygon_rows = polygons.read_many(sorted(polygon_ordinals.values()))
    joined = joined_outputs.join_wiki_partition(
        label_rows=labels,
        sentence_rows=sentence_rows,
        document_rows=document_rows,
        link_rows=[row for key in document_ordinals for row in link_rows[key]],
        polygon_rows=polygon_rows,
    )
    return _WikiJoinBatch(
        partition=joined,
        sentence_ordinals=selected_sentence_ordinals,
        document_ordinals=document_ordinals,
        link_ordinals=link_ordinals,
        link_rows=link_rows,
        polygon_ordinals=polygon_ordinals,
    )


def _record_wiki_label_batch(
    *,
    index: DiskJoinIndex,
    connection: sqlite3.Connection,
    sentences: StagedRows,
    documents: StagedRows,
    links: StagedRows,
    polygons: StagedRows,
    start: int,
    batch: list[Row],
) -> int:
    joined_batch = _join_wiki_batch(
        index=index,
        labels=batch,
        sentences=sentences,
        documents=documents,
        links=links,
        polygons=polygons,
    )
    first_label_ordinal_by_sentence: dict[str, int] = {}
    count_rows: list[tuple[str, str]] = []
    for label in joined_batch.partition.labels:
        sentence_id = str(label["text_unit_key"])
        first_label_ordinal_by_sentence.setdefault(
            sentence_id, start + int(label["source_row_index"])
        )
        count_rows.extend(
            (
                ("status", str(label["status"])),
                ("raw_decision", str(label["raw_decision"])),
            )
        )

    for text_unit in joined_batch.partition.text_units:
        sentence_id = str(text_unit["text_unit_key"])
        inserted = connection.execute(
            "INSERT OR IGNORE INTO selected_wiki_text_units "
            "(sentence_id, sentence_ordinal, label_ordinal) VALUES (?, ?, ?)",
            (
                sentence_id,
                joined_batch.sentence_ordinals[sentence_id],
                first_label_ordinal_by_sentence[sentence_id],
            ),
        )
        if inserted.rowcount != 1:
            continue

        document_key = wiki_document_project_key(
            text_unit["document_id"], text_unit["project"]
        )
        document_inserted = connection.execute(
            "INSERT OR IGNORE INTO selected_wiki_document_rows "
            "(row_ordinal, document_id, project) VALUES (?, ?, ?)",
            (
                joined_batch.document_ordinals[document_key],
                document_key[0],
                document_key[1],
            ),
        )
        if document_inserted.rowcount != 1:
            continue

        connection.executemany(
            "INSERT OR IGNORE INTO selected_wiki_link_rows(row_ordinal) VALUES (?)",
            ((ordinal,) for ordinal in joined_batch.link_ordinals[document_key]),
        )
        for link in joined_batch.link_rows[document_key]:
            polygon_id = _identifier(link, "polygon_id", "polygon_link")
            polygon_ordinal = joined_batch.polygon_ordinals.get(polygon_id)
            if polygon_ordinal is None:
                raise DataJoinError(f"wiki link names missing polygon {polygon_id!r}")
            connection.execute(
                "INSERT OR IGNORE INTO selected_wiki_polygon_rows "
                "(row_ordinal, polygon_id) VALUES (?, ?)",
                (polygon_ordinal, polygon_id),
            )

    connection.executemany(
        "INSERT INTO wiki_counts(kind, value, row_count) VALUES (?, ?, 1) "
        "ON CONFLICT(kind, value) DO UPDATE SET row_count = row_count + 1",
        count_rows,
    )
    connection.commit()
    return len(joined_batch.partition.labels)


class _WikiLabels:
    def __init__(self, labels: StagedRows, count: int) -> None:
        self._labels = labels
        self._count = count
        self.output_schema = _schema_with_source(
            labels.schema_arrow,
            {
                "source_row_index": pa.int64(),
                "raw_decision": pa.string(),
                "status": pa.string(),
            },
            excluded={"decision"},
        )

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, index: int) -> Row:
        ordinal = normalize_row_index(index, len(self))
        label = self._labels.read_many([ordinal])[0]
        return self._joined_row(label, ordinal)

    def __iter__(self) -> Iterator[Row]:
        for start, rows in _batches(self._labels):
            for offset, label in enumerate(rows):
                yield self._joined_row(label, start + offset)

    @staticmethod
    def _joined_row(label: Row, ordinal: int) -> Row:
        sentence_id = _identifier(label, "sentence_id", "label")
        return _label_row(label, ordinal, wiki_sentence_key(sentence_id))


class _WikiTextUnits:
    def __init__(
        self,
        *,
        workspace_path: Path,
        labels: StagedRows,
        sentences: StagedRows,
        documents: StagedRows,
        links: StagedRows,
        polygons: StagedRows,
        count: int,
    ) -> None:
        self._workspace_path = workspace_path
        self._labels = labels
        self._sentences = sentences
        self._documents = documents
        self._links = links
        self._polygons = polygons
        self._count = count
        self.output_schema = _schema_with_source(
            sentences.schema_arrow,
            {
                "sentence_id": pa.string(),
                "document_id": pa.string(),
                "project": pa.string(),
                "text": pa.string(),
                "language": pa.string(),
                "source_url": pa.string(),
                "polygon_ids": pa.list_(pa.string()),
                "language_code": pa.string(),
                "raw_language_code": pa.string(),
                "language_scheme": pa.string(),
                "language_mapping_version": pa.string(),
                "language_script": pa.string(),
                "language_mapping_status": pa.string(),
            },
        )

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, index: int) -> Row:
        position = normalize_row_index(index, len(self))
        return next(islice(iter(self), position, None))

    def __iter__(self) -> Iterator[Row]:
        index = DiskJoinIndex(self._workspace_path)
        connection = sqlite3.connect(self._workspace_path)
        try:
            cursor = connection.execute(
                "SELECT sentence_id, sentence_ordinal, label_ordinal "
                "FROM selected_wiki_text_units ORDER BY sequence"
            )
            while records := cursor.fetchmany(_BATCH_ROWS):
                sentence_ordinals = {
                    str(record[0]): int(record[1]) for record in records
                }
                label_rows = self._labels.read_many(
                    [int(record[2]) for record in records]
                )
                batch = _join_wiki_batch(
                    index=index,
                    labels=label_rows,
                    sentences=self._sentences,
                    documents=self._documents,
                    links=self._links,
                    polygons=self._polygons,
                    sentence_ordinals=sentence_ordinals,
                )
                yield from batch.partition.text_units
        finally:
            index.close()
            connection.close()


class _WikiSelectedRows:
    def __init__(
        self,
        workspace_path: Path,
        source: StagedRows,
        selection_table: str,
        count: int,
    ) -> None:
        self._workspace_path = workspace_path
        self._source = source
        self._selection_table = selection_table
        self._count = count
        self.output_schema = _schema_with_source(source.schema_arrow, {})

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, index: int) -> Row:
        position = normalize_row_index(index, len(self))
        return next(islice(iter(self), position, None))

    def __iter__(self) -> Iterator[Row]:
        connection = sqlite3.connect(self._workspace_path)
        try:
            cursor = connection.execute(
                f"SELECT row_ordinal FROM {self._selection_table} ORDER BY row_ordinal"
            )
            while records := cursor.fetchmany(_BATCH_ROWS):
                rows = self._source.read_many([int(record[0]) for record in records])
                yield from (dict(row) for row in rows)
        finally:
            connection.close()


def build_wiki_partition(
    *,
    labels: StagedRows,
    sentences: StagedRows,
    documents: StagedRows,
    links: StagedRows,
    polygons: StagedRows,
    workspace_path: str | Path,
) -> JoinedPartition:
    workspace = Path(workspace_path)
    if workspace.exists():
        raise FileExistsError(f"SQLite workspace already exists: {workspace}")

    index = DiskJoinIndex(workspace)
    connection: sqlite3.Connection | None = None
    label_count = 0
    try:
        _index_wiki_sources(
            index=index,
            sentences=sentences,
            documents=documents,
            links=links,
            polygons=polygons,
        )
        index.commit()
        connection = sqlite3.connect(workspace)
        _create_wiki_selection_tables(connection)
        connection.commit()

        for start, batch in _batches(labels):
            label_count += _record_wiki_label_batch(
                index=index,
                connection=connection,
                sentences=sentences,
                documents=documents,
                links=links,
                polygons=polygons,
                start=start,
                batch=batch,
            )

        text_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM selected_wiki_text_units"
            ).fetchone()[0]
        )
        document_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM selected_wiki_document_rows"
            ).fetchone()[0]
        )
        link_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM selected_wiki_link_rows"
            ).fetchone()[0]
        )
        polygon_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM selected_wiki_polygon_rows"
            ).fetchone()[0]
        )
        status_counts = {status: 0 for status in STATUSES}
        for value, count in connection.execute(
            "SELECT value, row_count FROM wiki_counts WHERE kind = 'status'"
        ):
            status_counts[str(value)] = int(count)
        raw_decision_counts = {
            str(value): int(count)
            for value, count in connection.execute(
                "SELECT value, row_count FROM wiki_counts "
                "WHERE kind = 'raw_decision' ORDER BY value"
            )
        }
    finally:
        if connection is not None:
            connection.close()
        index.close()

    return JoinedPartition(
        labels=_WikiLabels(labels, label_count),
        text_units=_WikiTextUnits(
            workspace_path=workspace,
            labels=labels,
            sentences=sentences,
            documents=documents,
            links=links,
            polygons=polygons,
            count=text_count,
        ),
        polygon_records=_WikiSelectedRows(
            workspace,
            polygons,
            "selected_wiki_polygon_rows",
            polygon_count,
        ),
        documents=_WikiSelectedRows(
            workspace,
            documents,
            "selected_wiki_document_rows",
            document_count,
        ),
        polygon_document_links=_WikiSelectedRows(
            workspace,
            links,
            "selected_wiki_link_rows",
            link_count,
        ),
        status_counts=status_counts,
        raw_decision_counts=raw_decision_counts,
    )
