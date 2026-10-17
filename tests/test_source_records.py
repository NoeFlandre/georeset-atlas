from copy import deepcopy

import pytest

from atlas_domain.occurrences import (
    description_occurrence_key,
    retain_occurrences,
    wiki_document_project_key,
)
from atlas_domain.source_records import (
    DataJoinError,
    description_text_for_label,
    index_wiki_polygon_links,
    website_text_for_label,
    wiki_polygon_ids_for_sentence,
)


def test_description_selects_sentence_at_label_index() -> None:
    label = {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 1,
    }
    source = {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "split",
        "original_text": "First. Second.",
        "sentences": ["First.", "Second."],
    }

    assert description_text_for_label(label, source) == "Second."


@pytest.mark.parametrize("split_status", ["unsupported", "not_detected"])
def test_description_uses_original_text_as_unsplit_index_zero(
    split_status: str,
) -> None:
    label = {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
    }
    source = {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": split_status,
        "original_text": " Full text, unchanged. ",
    }

    assert description_text_for_label(label, source) == " Full text, unchanged. "


@pytest.mark.parametrize(
    ("label_changes", "source_changes"),
    [
        ({"description_identity": "other"}, {}),
        ({"tag_key": "other"}, {}),
        ({"sentence_index": -1}, {}),
        ({"sentence_index": 2}, {}),
        ({"sentence_index": True}, {}),
        ({"sentence_index": "0"}, {}),
        ({}, {"sentences": [None]}),
    ],
)
def test_description_rejects_mismatched_or_invalid_records(
    label_changes: dict[str, object],
    source_changes: dict[str, object],
) -> None:
    label = {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
        **label_changes,
    }
    source = {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "split",
        "sentences": ["only one"],
        **source_changes,
    }

    with pytest.raises(DataJoinError):
        description_text_for_label(label, source)


def test_description_rejects_nonzero_index_for_unsplit_text() -> None:
    label = {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 1,
    }
    source = {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "unsupported",
        "original_text": "Unsplit text.",
    }

    with pytest.raises(DataJoinError):
        description_text_for_label(label, source)


@pytest.mark.parametrize(
    ("label_changes", "source_changes"),
    [
        ({"tag_key": None}, {}),
        ({"sentence_index": None}, {}),
        ({}, {"split_status": None}),
        ({}, {"sentences": None}),
    ],
)
def test_description_rejects_missing_or_invalid_required_fields(
    label_changes: dict[str, object],
    source_changes: dict[str, object],
) -> None:
    label = {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
        **label_changes,
    }
    source = {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "split",
        "sentences": ["Sentence."],
        **source_changes,
    }

    with pytest.raises(DataJoinError):
        description_text_for_label(label, source)


def test_description_skipped_fallback_requires_original_text() -> None:
    label = {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
    }
    source = {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "not_detected",
    }

    with pytest.raises(DataJoinError):
        description_text_for_label(label, source)


def test_description_rejects_absent_required_fields() -> None:
    label = {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
    }
    source = {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "split",
        "sentences": ["Sentence."],
    }
    label_without_index = label.copy()
    del label_without_index["sentence_index"]
    source_without_sentences = source.copy()
    del source_without_sentences["sentences"]
    source_without_status = source.copy()
    del source_without_status["split_status"]

    with pytest.raises(DataJoinError):
        description_text_for_label(label_without_index, source)
    with pytest.raises(DataJoinError):
        description_text_for_label(label, source_without_sentences)
    with pytest.raises(DataJoinError):
        description_text_for_label(label, source_without_status)


def test_website_selects_the_exact_field_sentence_array() -> None:
    source = {
        "polygon_id": "polygon-1",
        "decision": "split",
        "website_sentences": ["Website first.", "Website second."],
        "contact_website_sentences": ["Contact first.", "Contact second."],
    }
    website_label = {
        "polygon_id": "polygon-1",
        "field": "website",
        "sentence_index": 1,
    }
    contact_label = {
        "polygon_id": "polygon-1",
        "field": "contact_website",
        "sentence_index": 0,
    }

    assert website_text_for_label(website_label, source) == "Website second."
    assert website_text_for_label(contact_label, source) == "Contact first."


@pytest.mark.parametrize(
    ("field", "text_field", "array_field"),
    [
        ("website", "website_text", "website_sentences"),
        (
            "contact_website",
            "contact_website_text",
            "contact_website_sentences",
        ),
    ],
)
def test_skipped_website_uses_full_text_only_when_sentences_are_absent_or_empty(
    field: str,
    text_field: str,
    array_field: str,
) -> None:
    label = {"polygon_id": "polygon-1", "field": field, "sentence_index": 0}
    for source_array in ({}, {array_field: []}):
        source = {
            "polygon_id": "polygon-1",
            "decision": "skipped",
            text_field: " Full website text. ",
            **source_array,
        }

        assert website_text_for_label(label, source) == " Full website text. "


def test_skipped_website_prefers_selected_segment_without_concatenating() -> None:
    label = {"polygon_id": "polygon-1", "field": "website", "sentence_index": 0}
    source = {
        "polygon_id": "polygon-1",
        "decision": "skipped",
        "website_text": "Full site text.",
        "website_sentences": ["Part one.", "Part two."],
    }

    assert website_text_for_label(label, source) == "Part one."


@pytest.mark.parametrize(
    ("label_changes", "source_changes"),
    [
        ({"field": "description"}, {}),
        ({"polygon_id": "polygon-2"}, {}),
        ({"sentence_index": -1}, {}),
        ({"sentence_index": 2}, {}),
        ({"sentence_index": True}, {}),
        ({"sentence_index": "0"}, {}),
        ({}, {"website_sentences": "not an array"}),
        ({}, {"website_sentences": [None]}),
        ({}, {"website_sentences": []}),
    ],
)
def test_website_rejects_unknown_mismatched_or_invalid_records(
    label_changes: dict[str, object],
    source_changes: dict[str, object],
) -> None:
    label = {
        "polygon_id": "polygon-1",
        "field": "website",
        "sentence_index": 0,
        **label_changes,
    }
    source = {
        "polygon_id": "polygon-1",
        "decision": "split",
        "website_sentences": ["Website text."],
        **source_changes,
    }

    with pytest.raises(DataJoinError):
        website_text_for_label(label, source)


def test_website_skipped_fallback_requires_index_zero_and_string_text() -> None:
    label = {"polygon_id": "polygon-1", "field": "website", "sentence_index": 0}
    source = {
        "polygon_id": "polygon-1",
        "decision": "skipped",
        "website_text": None,
    }

    with pytest.raises(DataJoinError):
        website_text_for_label(label, source)

    with pytest.raises(DataJoinError):
        website_text_for_label(
            {**label, "sentence_index": 1},
            {**source, "website_text": "Website text."},
        )


@pytest.mark.parametrize(
    ("label_changes", "source_changes"),
    [
        ({"field": None}, {}),
        ({"sentence_index": None}, {}),
        ({}, {"decision": None}),
        ({}, {"website_sentences": None}),
    ],
)
def test_website_rejects_missing_or_invalid_required_fields(
    label_changes: dict[str, object],
    source_changes: dict[str, object],
) -> None:
    label = {
        "polygon_id": "polygon-1",
        "field": "website",
        "sentence_index": 0,
        **label_changes,
    }
    source = {
        "polygon_id": "polygon-1",
        "decision": "split",
        "website_sentences": ["Website text."],
        **source_changes,
    }

    with pytest.raises(DataJoinError):
        website_text_for_label(label, source)


def test_website_skipped_fallback_requires_full_text() -> None:
    label = {"polygon_id": "polygon-1", "field": "website", "sentence_index": 0}
    source = {"polygon_id": "polygon-1", "decision": "skipped"}

    with pytest.raises(DataJoinError):
        website_text_for_label(label, source)


def test_website_rejects_absent_required_fields() -> None:
    label = {"polygon_id": "polygon-1", "field": "website", "sentence_index": 0}
    source = {
        "polygon_id": "polygon-1",
        "decision": "split",
        "website_sentences": ["Website text."],
    }
    label_without_field = label.copy()
    del label_without_field["field"]
    source_without_sentences = source.copy()
    del source_without_sentences["website_sentences"]
    source_without_decision = source.copy()
    del source_without_decision["decision"]

    with pytest.raises(DataJoinError):
        website_text_for_label(label_without_field, source)
    with pytest.raises(DataJoinError):
        website_text_for_label(label, source_without_sentences)
    with pytest.raises(DataJoinError):
        website_text_for_label(label, source_without_decision)


def test_wiki_links_scope_by_document_and_project_and_keep_source_order() -> None:
    sentence = {"document_id": "document-1", "project": "project-a"}
    links = [
        {
            "document_id": "document-1",
            "project": "project-b",
            "polygon_id": "wrong-project",
        },
        {
            "document_id": "document-2",
            "project": "project-a",
            "polygon_id": "wrong-document",
        },
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-2",
        },
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-1",
        },
    ]
    wiki_index = index_wiki_polygon_links(links)

    assert wiki_polygon_ids_for_sentence(sentence, wiki_index) == (
        "polygon-2",
        "polygon-1",
    )


def test_wiki_links_keep_many_to_many_matches_and_duplicate_polygon_links() -> None:
    sentences = [
        {"document_id": "document-1", "project": "project-a"},
        {"document_id": "document-1", "project": "project-a"},
    ]
    links = [
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-2",
        },
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-1",
        },
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-2",
        },
    ]
    wiki_index = index_wiki_polygon_links(links)

    assert [wiki_polygon_ids_for_sentence(row, wiki_index) for row in sentences] == [
        ("polygon-2", "polygon-1", "polygon-2"),
        ("polygon-2", "polygon-1", "polygon-2"),
    ]


def test_wiki_index_is_keyed_by_document_project_with_ordered_tuples() -> None:
    links = [
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-2",
        },
        {
            "document_id": "document-1",
            "project": "project-b",
            "polygon_id": "polygon-3",
        },
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-1",
        },
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-2",
        },
    ]

    assert index_wiki_polygon_links(links) == {
        wiki_document_project_key("document-1", "project-a"): (
            "polygon-2",
            "polygon-1",
            "polygon-2",
        ),
        wiki_document_project_key("document-1", "project-b"): ("polygon-3",),
    }


@pytest.mark.parametrize(
    ("document_id", "project"),
    [
        ("document-1", "project-b"),
        ("document-2", "project-a"),
        ("document-3", "project-a"),
        ("document-1", "project-c"),
    ],
)
def test_wiki_lookup_returns_empty_tuple_for_absent_pair(
    document_id: str,
    project: str,
) -> None:
    links = [
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-1",
        },
        {
            "document_id": "document-2",
            "project": "project-b",
            "polygon_id": "polygon-2",
        },
    ]
    sentence = {"document_id": document_id, "project": project}
    wiki_index = index_wiki_polygon_links(links)

    assert wiki_polygon_ids_for_sentence(sentence, wiki_index) == ()
    assert wiki_polygon_ids_for_sentence(sentence, index_wiki_polygon_links([])) == ()


@pytest.mark.parametrize(
    ("sentence", "links"),
    [
        ({"project": "project-a"}, []),
        ({"document_id": "document-1"}, []),
        (
            {"document_id": "document-1", "project": "project-a"},
            [{"project": "project-a", "polygon_id": "polygon-1"}],
        ),
        (
            {"document_id": "document-1", "project": "project-a"},
            [{"document_id": "document-1", "polygon_id": "polygon-1"}],
        ),
    ],
)
def test_wiki_rejects_missing_join_keys(
    sentence: dict[str, object],
    links: list[dict[str, object]],
) -> None:
    with pytest.raises(DataJoinError):
        wiki_polygon_ids_for_sentence(sentence, index_wiki_polygon_links(links))


@pytest.mark.parametrize(
    "malformed_link",
    [
        {"document_id": "document-2", "project": "project-a"},
        {"document_id": "document-2", "project": "project-a", "polygon_id": ""},
        {"document_id": "document-2", "project": "project-a", "polygon_id": None},
        {"document_id": "", "project": "project-a", "polygon_id": "polygon-2"},
        {"document_id": "document-2", "project": "", "polygon_id": "polygon-2"},
    ],
)
def test_wiki_index_rejects_malformed_unrelated_link(
    malformed_link: dict[str, object],
) -> None:
    links: list[dict[str, object]] = [
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-1",
        },
        malformed_link,
    ]

    with pytest.raises(DataJoinError):
        index_wiki_polygon_links(links)


@pytest.mark.parametrize("field", ["document_id", "project", "polygon_id"])
@pytest.mark.parametrize("bad_value", ["", None, 7, ["document-1"]])
def test_wiki_index_rejects_empty_or_non_string_join_keys(
    field: str,
    bad_value: object,
) -> None:
    link = {
        "document_id": "document-1",
        "project": "project-a",
        "polygon_id": "polygon-1",
    }

    with pytest.raises(DataJoinError):
        index_wiki_polygon_links([{**link, field: bad_value}])


def test_repeated_text_and_conflicting_label_records_remain_separate() -> None:
    source = {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "split",
        "sentences": ["Repeated sentence.", "Repeated sentence."],
    }
    labels = [
        {
            "description_identity": "description-1",
            "tag_key": "name",
            "sentence_index": 0,
            "label": "place",
        },
        {
            "description_identity": "description-1",
            "tag_key": "name",
            "sentence_index": 1,
            "label": "place",
        },
        {
            "description_identity": "description-1",
            "tag_key": "name",
            "sentence_index": 0,
            "label": "region",
        },
    ]
    records = [
        {
            "key": description_occurrence_key(
                str(label["description_identity"]),
                str(label["tag_key"]),
                int(label["sentence_index"]),
            ),
            "text": description_text_for_label(label, source),
            "label": label["label"],
        }
        for label in labels
    ]

    assert retain_occurrences(records) == (
        {
            "key": ("description-1", "name", 0),
            "text": "Repeated sentence.",
            "label": "place",
        },
        {
            "key": ("description-1", "name", 1),
            "text": "Repeated sentence.",
            "label": "place",
        },
        {
            "key": ("description-1", "name", 0),
            "text": "Repeated sentence.",
            "label": "region",
        },
    )


def test_source_helpers_leave_source_mappings_unchanged() -> None:
    description_label = {
        "description_identity": "description-1",
        "tag_key": "name",
        "sentence_index": 0,
    }
    description_source = {
        "description_identity": "description-1",
        "tag_key": "name",
        "split_status": "split",
        "sentences": ["Description."],
    }
    website_label = {"polygon_id": "polygon-1", "field": "website", "sentence_index": 0}
    website_source = {
        "polygon_id": "polygon-1",
        "decision": "split",
        "website_sentences": ["Website."],
    }
    wiki_sentence = {"document_id": "document-1", "project": "project-a"}
    wiki_links = [
        {
            "document_id": "document-1",
            "project": "project-a",
            "polygon_id": "polygon-1",
        }
    ]
    source_rows = (description_source, website_source, wiki_sentence, wiki_links)
    before = deepcopy(source_rows)

    assert (
        description_text_for_label(description_label, description_source)
        == "Description."
    )
    assert website_text_for_label(website_label, website_source) == "Website."
    wiki_index = index_wiki_polygon_links(wiki_links)
    assert wiki_polygon_ids_for_sentence(wiki_sentence, wiki_index) == ("polygon-1",)
    assert source_rows == before
