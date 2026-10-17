from collections.abc import Iterable
from typing import TypeVar

T = TypeVar("T")
WEBSITE_FIELDS = frozenset({"website", "contact_website"})


def _require_non_empty_text(value: object, name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")


def _require_sentence_index(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("sentence_index must be a non-negative integer")


def description_occurrence_key(
    description_identity: str,
    tag_key: str,
    sentence_index: int,
) -> tuple[str, str, int]:
    _require_non_empty_text(description_identity, "description_identity")
    _require_non_empty_text(tag_key, "tag_key")
    _require_sentence_index(sentence_index)
    return description_identity, tag_key, sentence_index


def website_occurrence_key(
    polygon_id: str,
    field: str,
    sentence_index: int,
) -> tuple[str, str, int]:
    _require_non_empty_text(polygon_id, "polygon_id")
    if not isinstance(field, str) or field not in WEBSITE_FIELDS:
        raise ValueError("field must be 'website' or 'contact_website'")
    _require_sentence_index(sentence_index)
    return polygon_id, field, sentence_index


def wiki_sentence_key(sentence_id: str) -> str:
    _require_non_empty_text(sentence_id, "sentence_id")
    return sentence_id


def wiki_document_project_key(document_id: str, project: str) -> tuple[str, str]:
    _require_non_empty_text(document_id, "document_id")
    _require_non_empty_text(project, "project")
    return document_id, project


def wiki_polygon_link_key(
    document_id: str,
    project: str,
    polygon_id: str,
) -> tuple[str, str, str]:
    _require_non_empty_text(document_id, "document_id")
    _require_non_empty_text(project, "project")
    _require_non_empty_text(polygon_id, "polygon_id")
    return document_id, project, polygon_id


def retain_occurrences(occurrences: Iterable[T]) -> tuple[T, ...]:
    return tuple(occurrences)
