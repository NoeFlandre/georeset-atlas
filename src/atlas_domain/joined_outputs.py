import json
import operator
from collections import Counter
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from itertools import islice
from typing import Any, Protocol

from atlas_domain.occurrences import (
    description_occurrence_key,
    website_occurrence_key,
    wiki_document_project_key,
    wiki_sentence_key,
)
from atlas_domain.source_records import (
    DataJoinError,
    description_text_for_label,
    index_wiki_polygon_links,
    website_text_for_label,
    wiki_polygon_ids_for_sentence,
)

Row = dict[str, Any]


class RowSource(Protocol):
    def __len__(self) -> int: ...

    def __getitem__(self, index: int, /) -> Row: ...

    def __iter__(self) -> Iterator[Row]: ...


STATUSES = ("yes", "no", "failed", "skipped")
_STATUS_BY_DECISION = {
    "yes": "yes",
    "no": "no",
    "failed": "failed",
    "skipped_unsplit": "skipped",
}
RAW_DECISIONS = tuple(_STATUS_BY_DECISION)
_OSM_FIELDS = ("source_pbf", "osm_type", "osm_id")
_LABEL_RESERVED = frozenset(
    {"text_unit_key", "source_row_index", "raw_decision", "status"}
)
_TEXT_UNIT_RESERVED = frozenset(
    {"text_unit_key", "sentence_index", "text", "polygon_ids"}
)


@dataclass(frozen=True, slots=True)
class JoinedPartition:
    labels: tuple[Row, ...]
    text_units: tuple[Row, ...]
    polygon_records: RowSource
    documents: tuple[Row, ...]
    status_counts: Mapping[str, int]
    raw_decision_counts: Mapping[str, int]
    polygon_document_links: tuple[Row, ...] = ()


def normalize_label_decision(raw_decision: object) -> str:
    if not isinstance(raw_decision, str) or raw_decision not in _STATUS_BY_DECISION:
        raise DataJoinError(f"unsupported label decision {raw_decision!r}")
    return _STATUS_BY_DECISION[raw_decision]


def normalize_row_index(index: int, length: int) -> int:
    position = operator.index(index)
    if position < 0:
        position += length
    if not 0 <= position < length:
        raise IndexError(f"row index {index} is out of range for {length} rows")
    return position


def join_description_partition(
    *,
    label_rows: Iterable[Mapping[str, Any]],
    text_unit_rows: Iterable[Mapping[str, Any]],
    polygon_rows: Iterable[Mapping[str, Any]],
) -> JoinedPartition:
    if iter(polygon_rows) is polygon_rows:
        polygon_rows = tuple(polygon_rows)
    seen_polygon_ids: set[str] = set()
    polygon_ids_by_association: dict[tuple[object, ...], list[str]] = {}
    for polygon in polygon_rows:
        association = _description_association(polygon, "polygon")
        polygon_id = _description_polygon_identifier(polygon)
        _geometry(polygon, "polygon")
        if polygon_id in seen_polygon_ids:
            raise DataJoinError(f"duplicate polygon_id {polygon_id!r}")
        seen_polygon_ids.add(polygon_id)
        polygon_ids_by_association.setdefault(association, []).append(polygon_id)
    texts_by_identity: dict[tuple[str, str], Mapping[str, Any]] = {}
    for text in text_unit_rows:
        identity = (
            _identifier(text, "description_identity", "text_unit"),
            _identifier(text, "tag_key", "text_unit"),
        )
        _add_unique(texts_by_identity, identity, text, "description text unit")
    labels: list[Row] = []
    units: dict[tuple[str, str, int], Row] = {}
    for source_row_index, label in enumerate(label_rows):
        identity = _identifier(label, "description_identity", "label")
        tag_key = _identifier(label, "tag_key", "label")
        text_row = texts_by_identity.get((identity, tag_key))
        if text_row is None:
            raise DataJoinError(
                f"label has no description text unit for {identity!r} {tag_key!r}"
            )
        text = description_text_for_label(label, text_row)
        key = description_occurrence_key(identity, tag_key, label["sentence_index"])
        if key not in units:
            association = _description_association(text_row, "text_unit")
            polygon_ids = polygon_ids_by_association.get(association)
            if not polygon_ids:
                raise DataJoinError(f"description text unit {key!r} has no polygons")
            _reject_reserved(text_row, _TEXT_UNIT_RESERVED, "description text unit")
            units[key] = {
                **text_row,
                "text_unit_key": key,
                "sentence_index": key[2],
                "text": text,
                "polygon_ids": list(polygon_ids),
            }
        labels.append(_label_row(label, source_row_index, key))
    referenced = {
        polygon_id for unit in units.values() for polygon_id in unit["polygon_ids"]
    }
    return _partition(
        labels,
        units.values(),
        _SelectedRows(
            polygon_rows, key=_description_polygon_identifier, selected=referenced
        ),
        (),
    )


def join_website_partition(
    *,
    label_rows: Iterable[Mapping[str, Any]],
    polygon_text_rows: Iterable[Mapping[str, Any]],
) -> JoinedPartition:
    polygons_by_id: dict[str, Mapping[str, Any]] = {}
    for polygon in polygon_text_rows:
        polygon_id = _identifier(polygon, "polygon_id", "polygon_text")
        _add_unique(polygons_by_id, polygon_id, polygon, "polygon_id")
    labels: list[Row] = []
    units: dict[tuple[str, str, int], Row] = {}
    for source_row_index, label in enumerate(label_rows):
        polygon_id = _identifier(label, "polygon_id", "label")
        polygon = polygons_by_id.get(polygon_id)
        if polygon is None:
            raise DataJoinError(f"label has no polygon text row for {polygon_id!r}")
        field = _identifier(label, "field", "label")
        text = _website_text_for_label(label, polygon, field)
        key = website_occurrence_key(polygon_id, field, label["sentence_index"])
        if key not in units:
            unit = {
                "text_unit_key": key,
                "polygon_id": polygon_id,
                "field": field,
                "sentence_index": key[2],
                "text": text,
                "full_text": _website_full_text(polygon, field),
                "polygon_ids": [polygon_id],
            }
            if _is_native_website_row(polygon, field):
                unit["language"] = _string(polygon, f"{field}_language", "polygon_text")
                unit["sentence_status"] = _string(
                    polygon, f"{field}_sentence_status", "polygon_text"
                )
            else:
                unit.update(
                    decision=_string(polygon, "decision", "polygon_text"),
                    source_url=_string(polygon, "source_url", "polygon_text"),
                    raw_language_code=_string(
                        polygon, "raw_language_code", "polygon_text"
                    ),
                )
            units[key] = unit
        labels.append(_label_row(label, source_row_index, key))
    referenced = {unit["polygon_id"] for unit in units.values()}
    polygon_records = []
    for polygon_id, polygon in polygons_by_id.items():
        if polygon_id not in referenced:
            continue
        geometry = _geometry(polygon, "polygon_text")
        polygon_records.append(
            dict(polygon)
            if "geometry" in polygon
            else {"polygon_id": polygon_id, "geometry_wkb": geometry}
        )
    return _partition(
        labels,
        units.values(),
        tuple(polygon_records),
        (),
    )


def join_wiki_partition(
    *,
    label_rows: Iterable[Mapping[str, Any]],
    sentence_rows: Iterable[Mapping[str, Any]],
    document_rows: Iterable[Mapping[str, Any]],
    link_rows: Iterable[Mapping[str, Any]],
    polygon_rows: Iterable[Mapping[str, Any]],
) -> JoinedPartition:
    sentences_by_id: dict[str, Mapping[str, Any]] = {}
    for sentence in sentence_rows:
        sentence_id = _identifier(sentence, "sentence_id", "sentence")
        _identifier(sentence, "document_id", "sentence")
        _identifier(sentence, "project", "sentence")
        _string(
            sentence,
            "text" if "text" in sentence else "sentence_text",
            "sentence",
        )
        _string(
            sentence,
            "language" if "language" in sentence else "raw_language_code",
            "sentence",
        )
        _add_unique(
            sentences_by_id, wiki_sentence_key(sentence_id), sentence, "sentence_id"
        )
    documents_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for document in document_rows:
        key = wiki_document_project_key(
            _identifier(document, "document_id", "document"),
            _identifier(document, "project", "document"),
        )
        _add_unique(documents_by_key, key, document, "document")
    source_links = tuple(link_rows)
    link_index = index_wiki_polygon_links(source_links)
    polygons_by_id: dict[str, Mapping[str, Any]] = {}
    for polygon in polygon_rows:
        polygon_id = _identifier(polygon, "polygon_id", "polygon")
        _geometry(polygon, "polygon")
        _add_unique(polygons_by_id, polygon_id, polygon, "polygon_id")
    labels: list[Row] = []
    units: dict[str, Row] = {}
    for source_row_index, label in enumerate(label_rows):
        sentence_id = _identifier(label, "sentence_id", "label")
        sentence = sentences_by_id.get(sentence_id)
        if sentence is None:
            raise DataJoinError(f"label has no wiki sentence {sentence_id!r}")
        key = wiki_sentence_key(sentence_id)
        if key not in units:
            document_key = wiki_document_project_key(
                sentence["document_id"], sentence["project"]
            )
            document = documents_by_key.get(document_key)
            if document is None:
                raise DataJoinError(f"sentence {sentence_id!r} has no wiki document")
            polygon_ids = list(wiki_polygon_ids_for_sentence(sentence, link_index))
            for polygon_id in polygon_ids:
                if polygon_id not in polygons_by_id:
                    raise DataJoinError(
                        f"wiki link names missing polygon {polygon_id!r}"
                    )
            sentence_text = _string(
                sentence,
                "text" if "text" in sentence else "sentence_text",
                "sentence",
            )
            language = _string(
                sentence,
                "language" if "language" in sentence else "raw_language_code",
                "sentence",
            )
            source_url = _string(
                document,
                "url" if "url" in document else "source_url",
                "document",
            )
            units[key] = {
                **sentence,
                "text_unit_key": key,
                "sentence_id": sentence_id,
                "document_id": document_key[0],
                "project": document_key[1],
                "text": sentence_text,
                "language": language,
                "source_url": source_url,
                "polygon_ids": polygon_ids,
            }
        labels.append(_label_row(label, source_row_index, key))
    referenced_documents = {
        wiki_document_project_key(unit["document_id"], unit["project"])
        for unit in units.values()
    }
    referenced_polygons = {
        polygon_id for unit in units.values() for polygon_id in unit["polygon_ids"]
    }
    return _partition(
        labels,
        units.values(),
        tuple(
            dict(polygon)
            for polygon_id, polygon in polygons_by_id.items()
            if polygon_id in referenced_polygons
        ),
        [
            dict(document)
            for key, document in documents_by_key.items()
            if key in referenced_documents
        ],
        [
            dict(link)
            for link in source_links
            if wiki_document_project_key(link["document_id"], link["project"])
            in referenced_documents
        ],
    )


def _label_row(label: Mapping[str, Any], source_row_index: int, key: object) -> Row:
    raw_decision = _field(label, "decision", "label")
    status = normalize_label_decision(raw_decision)
    _reject_reserved(label, _LABEL_RESERVED, "label")
    native = {name: value for name, value in label.items() if name != "decision"}
    return {
        **native,
        "text_unit_key": key,
        "source_row_index": source_row_index,
        "raw_decision": raw_decision,
        "status": status,
    }


def _partition(
    labels: Sequence[Row],
    text_units: Iterable[Row],
    polygon_records: RowSource,
    documents: Sequence[Row],
    polygon_document_links: Sequence[Row] = (),
) -> JoinedPartition:
    statuses = Counter(row["status"] for row in labels)
    raw_decisions = Counter(row["raw_decision"] for row in labels)
    return JoinedPartition(
        labels=tuple(labels),
        text_units=tuple(text_units),
        polygon_records=polygon_records,
        documents=tuple(documents),
        polygon_document_links=tuple(polygon_document_links),
        status_counts={status: statuses[status] for status in STATUSES},
        raw_decision_counts=dict(sorted(raw_decisions.items())),
    )


def _description_polygon_identifier(polygon: Mapping[str, Any]) -> str:
    if "polygon_id" in polygon:
        return _identifier(polygon, "polygon_id", "polygon")
    return _description_polygon_id(_description_association(polygon, "polygon"))


def _description_polygon_id(association: tuple[object, ...]) -> str:
    return json.dumps(association, ensure_ascii=True, separators=(",", ":"))


class _SelectedRows:
    def __init__(
        self,
        source: Iterable[Mapping[str, Any]],
        *,
        key: Callable[[Mapping[str, Any]], str],
        selected: Collection[str],
    ) -> None:
        self._source = source
        self._key = key
        self._selected = selected

    def __len__(self) -> int:
        return len(self._selected)

    def __getitem__(self, index: int) -> Row:
        position = normalize_row_index(index, len(self))
        return next(islice(self, position, None))

    def __iter__(self) -> Iterator[Row]:
        for polygon in self._source:
            polygon_id = self._key(polygon)
            if polygon_id in self._selected:
                yield {**polygon, "polygon_id": polygon_id}


def _website_text_for_label(
    label: Mapping[str, Any],
    polygon: Mapping[str, Any],
    field: str,
) -> str:
    if not _is_native_website_row(polygon, field):
        return website_text_for_label(label, polygon)
    source = dict(polygon)
    source[f"{field}_text"] = _website_full_text(polygon, field)
    sentence_status = _string(polygon, f"{field}_sentence_status", "polygon_text")
    if sentence_status in {"unsupported", "unsupported_language", "not_detected"}:
        source.pop(f"{field}_sentences", None)
        source["decision"] = "skipped"
    else:
        source["decision"] = sentence_status
    return website_text_for_label(label, source)


def _is_native_website_row(polygon: Mapping[str, Any], field: str) -> bool:
    return f"{field}_sentence_status" in polygon


def _website_full_text(polygon: Mapping[str, Any], field: str) -> str:
    if field in polygon:
        return _string(polygon, field, "polygon_text")
    return _string(polygon, f"{field}_text", "polygon_text")


def _description_association(
    record: Mapping[str, Any],
    name: str,
) -> tuple[object, ...]:
    if not isinstance(record, Mapping):
        raise DataJoinError(f"{name} must be a mapping")
    present = [field for field in _OSM_FIELDS if field in record]
    if not present:
        return (
            "description_identity",
            _identifier(record, "description_identity", name),
        )
    if len(present) != len(_OSM_FIELDS):
        raise DataJoinError(f"{name} has partial OSM identity fields {present}")
    osm_id = _field(record, "osm_id", name)
    if isinstance(osm_id, bool) or not isinstance(osm_id, (int, str)) or osm_id == "":
        raise DataJoinError(f"{name}.osm_id must be an integer or non-empty string")
    return (
        "osm",
        _identifier(record, "source_pbf", name),
        _identifier(record, "osm_type", name),
        osm_id,
    )


def _reject_reserved(
    record: Mapping[str, Any],
    reserved: Collection[str],
    name: str,
) -> None:
    clashes = sorted(set(reserved).intersection(record))
    if clashes:
        raise DataJoinError(f"{name} already has reserved field(s) {clashes}")


def _add_unique(index: dict[Any, Any], key: Any, record: Any, name: str) -> None:
    if key in index:
        raise DataJoinError(f"duplicate {name} {key!r}")
    index[key] = record


def _field(record: object, field: str, name: str) -> Any:
    if not isinstance(record, Mapping):
        raise DataJoinError(f"{name} must be a mapping")
    if field not in record:
        raise DataJoinError(f"{name} is missing {field!r}")
    return record[field]


def _string(record: object, field: str, name: str) -> str:
    value = _field(record, field, name)
    if not isinstance(value, str):
        raise DataJoinError(f"{name}.{field} must be a string")
    return value


def _identifier(record: object, field: str, name: str) -> str:
    value = _string(record, field, name)
    if not value:
        raise DataJoinError(f"{name}.{field} must be a non-empty string")
    return value


def _geometry(record: object, name: str) -> bytes | str:
    field = (
        "geometry"
        if isinstance(record, Mapping) and "geometry" in record
        else "geometry_wkb"
    )
    value = _field(record, field, name)
    if not isinstance(value, (bytes, str)):
        raise DataJoinError(f"{name}.{field} must be bytes or a string")
    return value
