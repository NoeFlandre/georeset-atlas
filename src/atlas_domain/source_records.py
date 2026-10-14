from collections.abc import Iterable, Mapping, Sequence

from atlas_domain.occurrences import wiki_document_project_key


class DataJoinError(ValueError):
    pass


_Record = Mapping[str, object]
_WikiPolygonIndex = Mapping[tuple[str, str], tuple[str, ...]]


def _required_value(record: _Record, field: str, record_name: str) -> object:
    if not isinstance(record, Mapping):
        raise DataJoinError(f"{record_name} must be a mapping")
    if field not in record:
        raise DataJoinError(f"{record_name} is missing {field!r}")
    return record[field]


def _required_key(record: _Record, field: str, record_name: str) -> str:
    value = _required_value(record, field, record_name)
    if not isinstance(value, str) or not value:
        raise DataJoinError(f"{record_name}.{field} must be a non-empty string")
    return value


def _required_text(record: _Record, field: str, record_name: str) -> str:
    value = _required_value(record, field, record_name)
    if not isinstance(value, str):
        raise DataJoinError(f"{record_name}.{field} must be a string")
    return value


def _required_index(record: _Record, record_name: str) -> int:
    value = _required_value(record, "sentence_index", record_name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DataJoinError(
            f"{record_name}.sentence_index must be a non-negative integer"
        )
    return value


def _required_texts(record: _Record, field: str, record_name: str) -> Sequence[object]:
    value = _required_value(record, field, record_name)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise DataJoinError(f"{record_name}.{field} must be a text sequence")
    return value


def _selected_text(texts: Sequence[object], index: int, field: str) -> str:
    if index >= len(texts):
        raise DataJoinError(f"sentence_index is outside {field}")
    text = texts[index]
    if not isinstance(text, str):
        raise DataJoinError(f"{field}[{index}] must be a string")
    return text


def description_text_for_label(label: _Record, source_record: _Record) -> str:
    description_identity = _required_key(label, "description_identity", "label")
    tag_key = _required_key(label, "tag_key", "label")
    sentence_index = _required_index(label, "label")
    source_identity = _required_key(
        source_record, "description_identity", "source_record"
    )
    if source_identity != description_identity:
        raise DataJoinError("label.description_identity does not match source_record")
    if _required_key(source_record, "tag_key", "source_record") != tag_key:
        raise DataJoinError("label.tag_key does not match source_record")
    split_status = _required_value(source_record, "split_status", "source_record")
    if not isinstance(split_status, str):
        raise DataJoinError("source_record.split_status must be a string")
    if split_status in {"unsupported", "not_detected"}:
        if sentence_index != 0:
            raise DataJoinError(
                "unsplit description text only supports sentence_index 0"
            )
        return _required_text(source_record, "original_text", "source_record")
    sentences = _required_texts(source_record, "sentences", "source_record")
    return _selected_text(sentences, sentence_index, "source_record.sentences")


def website_text_for_label(label: _Record, source_record: _Record) -> str:
    polygon_id = _required_key(label, "polygon_id", "label")
    field = _required_key(label, "field", "label")
    if field not in {"website", "contact_website"}:
        raise DataJoinError(f"label.field has unsupported value {field!r}")
    sentence_index = _required_index(label, "label")
    if _required_key(source_record, "polygon_id", "source_record") != polygon_id:
        raise DataJoinError("label.polygon_id does not match source_record")
    decision = _required_value(source_record, "decision", "source_record")
    if not isinstance(decision, str):
        raise DataJoinError("source_record.decision must be a string")
    sentences_field = f"{field}_sentences"
    text_field = f"{field}_text"
    if sentences_field not in source_record:
        if decision == "skipped" and sentence_index == 0:
            return _required_text(source_record, text_field, "source_record")
        raise DataJoinError(f"source_record is missing {sentences_field!r}")
    sentences = _required_texts(source_record, sentences_field, "source_record")
    if not sentences and decision == "skipped" and sentence_index == 0:
        return _required_text(source_record, text_field, "source_record")
    return _selected_text(sentences, sentence_index, f"source_record.{sentences_field}")


def index_wiki_polygon_links(
    polygon_links: Iterable[_Record],
) -> dict[tuple[str, str], tuple[str, ...]]:
    grouped: dict[tuple[str, str], list[str]] = {}
    for link in polygon_links:
        document_id = _required_key(link, "document_id", "polygon_link")
        project = _required_key(link, "project", "polygon_link")
        polygon_id = _required_key(link, "polygon_id", "polygon_link")
        key = wiki_document_project_key(document_id, project)
        grouped.setdefault(key, []).append(polygon_id)
    return {key: tuple(polygon_ids) for key, polygon_ids in grouped.items()}


def wiki_polygon_ids_for_sentence(
    sentence: _Record,
    polygon_link_index: _WikiPolygonIndex,
) -> tuple[str, ...]:
    document_id = _required_key(sentence, "document_id", "sentence")
    project = _required_key(sentence, "project", "sentence")
    if not isinstance(polygon_link_index, Mapping):
        raise DataJoinError(
            "polygon_link_index must be a mapping from index_wiki_polygon_links"
        )
    key = wiki_document_project_key(document_id, project)
    return polygon_link_index.get(key, ())
