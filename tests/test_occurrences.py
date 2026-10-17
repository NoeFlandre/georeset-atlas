from typing import Any

import pytest

from atlas_domain.occurrences import (
    description_occurrence_key,
    retain_occurrences,
    website_occurrence_key,
    wiki_document_project_key,
    wiki_polygon_link_key,
    wiki_sentence_key,
)
from atlas_domain.source_records import (
    index_wiki_polygon_links,
    wiki_polygon_ids_for_sentence,
)


def test_description_occurrence_key_uses_description_tag_and_sentence_index() -> None:
    assert description_occurrence_key(
        description_identity="description-1",
        tag_key="name",
        sentence_index=0,
    ) == ("description-1", "name", 0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"description_identity": "", "tag_key": "name", "sentence_index": 0},
        {"description_identity": "description-1", "tag_key": "", "sentence_index": 0},
        {
            "description_identity": "description-1",
            "tag_key": "name",
            "sentence_index": True,
        },
        {
            "description_identity": "description-1",
            "tag_key": "name",
            "sentence_index": "0",
        },
        {
            "description_identity": "description-1",
            "tag_key": "name",
            "sentence_index": -1,
        },
    ],
)
def test_description_occurrence_key_rejects_empty_identity_or_invalid_index(
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        description_occurrence_key(**kwargs)


def test_website_occurrence_key_uses_polygon_field_and_sentence_index() -> None:
    assert website_occurrence_key(
        polygon_id="polygon-1",
        field="website",
        sentence_index=2,
    ) == ("polygon-1", "website", 2)
    assert website_occurrence_key(
        polygon_id="polygon-1",
        field="contact_website",
        sentence_index=0,
    ) == ("polygon-1", "contact_website", 0)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"polygon_id": "", "field": "website", "sentence_index": 0},
        {"polygon_id": "polygon-1", "field": "", "sentence_index": 0},
        {"polygon_id": "polygon-1", "field": "description", "sentence_index": 0},
        {
            "polygon_id": "polygon-1",
            "field": "website:description",
            "sentence_index": 0,
        },
        {"polygon_id": "polygon-1", "field": "Website", "sentence_index": 0},
        {"polygon_id": "polygon-1", "field": "website", "sentence_index": True},
        {"polygon_id": "polygon-1", "field": "website", "sentence_index": "0"},
        {"polygon_id": "polygon-1", "field": "website", "sentence_index": -1},
    ],
)
def test_website_occurrence_key_rejects_invalid_polygon_field_or_index(
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        website_occurrence_key(**kwargs)


def test_wiki_label_key_is_the_sentence_id() -> None:
    assert wiki_sentence_key("sentence-1") == "sentence-1"


def test_wiki_links_are_keyed_by_document_project_and_polygon() -> None:
    assert wiki_document_project_key("document-1", "project-a") == (
        "document-1",
        "project-a",
    )
    assert wiki_polygon_link_key("document-1", "project-a", "polygon-1") == (
        "document-1",
        "project-a",
        "polygon-1",
    )
    assert wiki_polygon_link_key("document-1", "project-b", "polygon-1") != (
        wiki_polygon_link_key("document-1", "project-a", "polygon-1")
    )


@pytest.mark.parametrize("sentence_id", ["", None, 1])
def test_wiki_sentence_key_rejects_empty_or_non_string_id(sentence_id: Any) -> None:
    with pytest.raises(ValueError):
        wiki_sentence_key(sentence_id)


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("document_id", ""),
        ("document_id", None),
        ("document_id", 1),
        ("project", ""),
        ("project", None),
        ("project", 1),
    ],
)
def test_wiki_document_project_key_rejects_empty_or_non_string_parts(
    argument: str,
    value: Any,
) -> None:
    kwargs: dict[str, Any] = {"document_id": "document-1", "project": "project-a"}
    kwargs[argument] = value

    with pytest.raises(ValueError):
        wiki_document_project_key(**kwargs)


@pytest.mark.parametrize(
    ("argument", "value"),
    [
        ("document_id", ""),
        ("document_id", None),
        ("document_id", 1),
        ("project", ""),
        ("project", None),
        ("project", 1),
        ("polygon_id", ""),
        ("polygon_id", None),
        ("polygon_id", 1),
    ],
)
def test_wiki_polygon_link_key_rejects_empty_or_non_string_parts(
    argument: str,
    value: Any,
) -> None:
    kwargs: dict[str, Any] = {
        "document_id": "document-1",
        "project": "project-a",
        "polygon_id": "polygon-1",
    }
    kwargs[argument] = value

    with pytest.raises(ValueError):
        wiki_polygon_link_key(**kwargs)


def test_wiki_joins_keep_many_to_many_links_and_duplicate_rows() -> None:
    sentences = [
        {
            "sentence_id": "sentence-1",
            "document_id": "document-1",
            "project": "project-a",
        },
        {
            "sentence_id": "sentence-2",
            "document_id": "document-1",
            "project": "project-a",
        },
    ]
    polygon_links = [
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-1",
        },
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-1",
        },
        {
            "document_id": "document-1",
            "project": "project-b",
            "polygon_id": "polygon-1",
        },
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-2",
        },
    ]
    polygon_link_index = index_wiki_polygon_links(polygon_links)
    occurrences = []
    for sentence in sentences:
        for polygon_id in wiki_polygon_ids_for_sentence(sentence, polygon_link_index):
            occurrences.append(
                {
                    "sentence": wiki_sentence_key(sentence["sentence_id"]),
                    "key": wiki_polygon_link_key(
                        sentence["document_id"],
                        sentence["project"],
                        polygon_id,
                    ),
                }
            )

    assert [
        (row["sentence"], row["key"]) for row in retain_occurrences(occurrences)
    ] == [
        ("sentence-1", ("document-1", "project-a", "polygon-1")),
        ("sentence-1", ("document-1", "project-a", "polygon-1")),
        ("sentence-1", ("document-1", "project-a", "polygon-2")),
        ("sentence-2", ("document-1", "project-a", "polygon-1")),
        ("sentence-2", ("document-1", "project-a", "polygon-1")),
        ("sentence-2", ("document-1", "project-a", "polygon-2")),
    ]


def test_retain_occurrences_keeps_repeated_text_links_and_conflicting_labels() -> None:
    website_key = website_occurrence_key("polygon-1", "website", 0)
    wiki_link_key = wiki_polygon_link_key("document-1", "project-a", "polygon-1")
    occurrences = [
        {
            "key": description_occurrence_key("description-1", "name", 0),
            "text": "Repeated sentence.",
            "label": "place",
        },
        {
            "key": description_occurrence_key("description-1", "name", 1),
            "text": "Repeated sentence.",
            "label": "region",
        },
        {"key": website_key, "text": "Repeated sentence.", "label": "place"},
        {"key": website_key, "text": "Repeated sentence.", "label": "region"},
        {"key": wiki_link_key, "text": "Repeated sentence.", "label": "place"},
        {"key": wiki_link_key, "text": "Repeated sentence.", "label": "region"},
    ]

    retained = retain_occurrences(occurrences)

    assert retained == tuple(occurrences)
    assert len(retained) == 6
