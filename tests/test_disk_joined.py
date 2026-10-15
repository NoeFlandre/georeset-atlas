from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest

import atlas_domain.joined_outputs as joined_outputs
import atlas_io.disk_joined as disk_joined
import atlas_io.joined_cli as joined_cli
from atlas_domain.joined_outputs import JoinedPartition
from atlas_domain.source_catalog import SOURCE_CATALOG
from atlas_domain.source_records import DataJoinError
from atlas_io.joined_partitions import (
    StagedRows,
    stage_table_groups,
    write_joined_partition,
)


def _staged(rows: list[dict[str, Any]], path: Path) -> StagedRows:
    return stage_table_groups([pa.Table.from_pylist(rows)], path)


def test_description_indexed_join_keeps_cross_batch_duplicates_and_source_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    labels = [
        {
            "description_identity": "alpha",
            "tag_key": "name",
            "sentence_index": 0,
            "decision": "yes",
        },
        *[
            {
                "description_identity": "beta",
                "tag_key": "name",
                "sentence_index": 0,
                "decision": "no",
            }
            for _ in range(511)
        ],
        {
            "description_identity": "alpha",
            "tag_key": "name",
            "sentence_index": 0,
            "decision": "no",
        },
        {
            "description_identity": "gamma",
            "tag_key": "name",
            "sentence_index": 0,
            "decision": "skipped_unsplit",
        },
    ]
    text_units = [
        {
            "description_identity": identity,
            "source_pbf": "region.osm.pbf",
            "osm_type": "way",
            "osm_id": osm_id,
            "tag_key": "name",
            "split_status": "split",
            "original_text": text,
            "sentences": [text],
            "source_url": f"https://example.test/{identity}",
            "raw_language_code": "en",
        }
        for identity, osm_id, text in (
            ("gamma", 3, "Gamma."),
            ("beta", 2, "Beta."),
            ("alpha", 1, "Alpha."),
        )
    ]
    polygons = [
        {
            "polygon_id": polygon_id,
            "source_pbf": "region.osm.pbf",
            "osm_type": "way",
            "osm_id": osm_id,
            "geometry_wkb": polygon_id.encode(),
        }
        for polygon_id, osm_id in (
            ("beta", 2),
            ("unselected", 99),
            ("alpha-first", 1),
            ("alpha-second", 1),
            ("gamma", 3),
        )
    ]
    staged_labels = _staged(labels, tmp_path / "labels.parquet")
    staged_text_units = _staged(text_units, tmp_path / "text_units.parquet")
    staged_polygons = _staged(polygons, tmp_path / "polygons.parquet")
    original_join = joined_outputs.join_description_partition
    label_batch_sizes: list[int] = []

    def record_join(**kwargs: Any) -> JoinedPartition:
        batch_labels = list(kwargs["label_rows"])
        if batch_labels:
            label_batch_sizes.append(len(batch_labels))
        return original_join(
            label_rows=batch_labels,
            text_unit_rows=kwargs["text_unit_rows"],
            polygon_rows=kwargs["polygon_rows"],
        )

    monkeypatch.setattr(joined_outputs, "join_description_partition", record_join)

    partition = disk_joined.build_description_partition(
        labels=staged_labels,
        text_units=staged_text_units,
        polygons=staged_polygons,
        workspace_path=tmp_path / "description.sqlite",
    )

    joined_labels = list(partition.labels)
    assert label_batch_sizes == [512, 2]
    assert [row["source_row_index"] for row in joined_labels] == list(range(514))
    assert [row["raw_decision"] for row in joined_labels] == [
        "yes",
        *["no"] * 512,
        "skipped_unsplit",
    ]
    assert [joined_labels[index]["text_unit_key"] for index in (0, 512, 513)] == [
        ("alpha", "name", 0),
        ("alpha", "name", 0),
        ("gamma", "name", 0),
    ]
    assert [row["text_unit_key"] for row in partition.text_units] == [
        ("alpha", "name", 0),
        ("beta", "name", 0),
        ("gamma", "name", 0),
    ]
    assert [row["text"] for row in partition.text_units] == [
        "Alpha.",
        "Beta.",
        "Gamma.",
    ]
    assert list(partition.text_units) == list(partition.text_units)
    assert [row["polygon_id"] for row in partition.polygon_records] == [
        "beta",
        "alpha-first",
        "alpha-second",
        "gamma",
    ]
    assert partition.status_counts == {"yes": 1, "no": 512, "failed": 0, "skipped": 1}
    assert partition.raw_decision_counts == {
        "no": 512,
        "skipped_unsplit": 1,
        "yes": 1,
    }


def test_website_indexed_join_keeps_native_rows_and_cross_batch_decisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = getattr(disk_joined, "build_website_partition", None)
    assert callable(builder), "Website needs a disk-backed partition builder"

    labels: list[dict[str, Any]] = []

    def add_label(
        ordinal: int,
        polygon_id: str,
        field: str,
        sentence_index: int,
        decision: str,
    ) -> None:
        language = "en" if field == "website" else "fr"
        labels.append(
            {
                "label_id": f"label-{ordinal:04d}",
                "polygon_id": polygon_id,
                "field": field,
                "sentence_index": sentence_index,
                "decision": decision,
                "language": language,
                "reviewer": f"reviewer-{ordinal % 3}",
                "confidence": 0.75,
            }
        )

    add_label(0, "website-way-1", "website", 1, "yes")
    add_label(1, "website-way-1", "contact_website", 0, "no")
    decisions = ("yes", "no", "failed", "skipped_unsplit")
    for ordinal in range(2, 511):
        add_label(
            ordinal,
            "website-way-1",
            "website",
            1,
            decisions[(ordinal - 2) % len(decisions)],
        )
    add_label(511, "website-way-2", "website", 0, "skipped_unsplit")
    add_label(512, "website-way-2", "contact_website", 0, "failed")
    add_label(513, "website-way-1", "website", 1, "no")
    add_label(514, "website-way-1", "contact_website", 0, "yes")

    polygons = [
        {
            "polygon_id": "website-way-2",
            "source_pbf": "region.osm.pbf",
            "osm_type": "way",
            "osm_id": 2,
            "website_text": "  Bazaar website.  ",
            "website_sentences": [],
            "website_language": "und",
            "website_sentence_status": "skipped",
            "contact_website_text": "Contact details not detected.",
            "contact_website_sentences": None,
            "contact_website_language": "und",
            "contact_website_sentence_status": "not_detected",
            "geometry": '{"type":"Point","coordinates":[69.2,34.5]}',
            "lat": 34.5,
            "lon": 69.2,
        },
        {
            "polygon_id": "website-way-1",
            "source_pbf": "region.osm.pbf",
            "osm_type": "way",
            "osm_id": 1,
            "website_text": "  First sentence. Second sentence.\n",
            "website_sentences": ["First sentence.", "Second sentence."],
            "website_language": "en-GB",
            "website_sentence_status": "split",
            "contact_website_text": "\tCall first.\nOr send email.  ",
            "contact_website_sentences": None,
            "contact_website_language": "fr",
            "contact_website_sentence_status": "unsupported_language",
            "geometry": '{"type":"Point","coordinates":[69.1,34.4]}',
            "lat": 34.4,
            "lon": 69.1,
        },
    ]
    staged_labels = _staged(labels, tmp_path / "website-labels.parquet")
    staged_polygons = _staged(polygons, tmp_path / "website-polygons.parquet")
    call_sizes: list[int] = []
    original_join = joined_outputs.join_website_partition

    def tracked_join(*, label_rows: Any, polygon_text_rows: Any) -> JoinedPartition:
        batch_labels = list(label_rows)
        if batch_labels:
            call_sizes.append(len(batch_labels))
        return original_join(
            label_rows=batch_labels, polygon_text_rows=polygon_text_rows
        )

    monkeypatch.setattr(joined_outputs, "join_website_partition", tracked_join)
    partition = builder(
        labels=staged_labels,
        polygons_and_text=staged_polygons,
        workspace_path=tmp_path / "website.sqlite",
    )

    assert call_sizes == [512, 3]
    joined_labels = list(partition.labels)
    assert list(partition.labels) == joined_labels
    assert [row["label_id"] for row in joined_labels] == [
        f"label-{ordinal:04d}" for ordinal in range(515)
    ]
    assert [row["source_row_index"] for row in joined_labels] == list(range(515))
    assert [row["reviewer"] for row in joined_labels] == [
        f"reviewer-{ordinal % 3}" for ordinal in range(515)
    ]
    assert [row["confidence"] for row in joined_labels] == [0.75] * 515
    assert [row["raw_decision"] for row in joined_labels[510:515]] == [
        "yes",
        "skipped_unsplit",
        "failed",
        "no",
        "yes",
    ]
    assert [row["status"] for row in joined_labels[510:515]] == [
        "yes",
        "skipped",
        "failed",
        "no",
        "yes",
    ]

    text_units = list(partition.text_units)
    assert list(partition.text_units) == text_units
    assert [row["text_unit_key"] for row in text_units] == [
        ("website-way-1", "website", 1),
        ("website-way-1", "contact_website", 0),
        ("website-way-2", "website", 0),
        ("website-way-2", "contact_website", 0),
    ]
    assert [row["text"] for row in text_units] == [
        "Second sentence.",
        "\tCall first.\nOr send email.  ",
        "  Bazaar website.  ",
        "Contact details not detected.",
    ]
    assert [row["full_text"] for row in text_units] == [
        "  First sentence. Second sentence.\n",
        "\tCall first.\nOr send email.  ",
        "  Bazaar website.  ",
        "Contact details not detected.",
    ]
    assert [row["sentence_status"] for row in text_units] == [
        "split",
        "unsupported_language",
        "skipped",
        "not_detected",
    ]
    assert [row["language"] for row in text_units] == ["en-GB", "fr", "und", "und"]
    assert [row["polygon_ids"] for row in text_units] == [
        ["website-way-1"],
        ["website-way-1"],
        ["website-way-2"],
        ["website-way-2"],
    ]

    polygon_records = list(partition.polygon_records)
    assert list(partition.polygon_records) == polygon_records
    assert [row["polygon_id"] for row in polygon_records] == [
        "website-way-2",
        "website-way-1",
    ]
    assert polygon_records == polygons
    assert [row["geometry"] for row in polygon_records] == [
        '{"type":"Point","coordinates":[69.2,34.5]}',
        '{"type":"Point","coordinates":[69.1,34.4]}',
    ]
    assert [row["website_sentence_status"] for row in polygon_records] == [
        "skipped",
        "split",
    ]
    assert [row["contact_website_sentences"] for row in polygon_records] == [
        None,
        None,
    ]
    assert partition.status_counts == {
        "yes": 130,
        "no": 129,
        "failed": 128,
        "skipped": 128,
    }
    assert partition.raw_decision_counts == {
        "failed": 128,
        "no": 129,
        "skipped_unsplit": 128,
        "yes": 130,
    }

    unmatched_labels = _staged(
        [
            {
                "label_id": "unmatched",
                "polygon_id": "absent-polygon",
                "field": "website",
                "sentence_index": 0,
                "decision": "yes",
            }
        ],
        tmp_path / "unmatched-labels.parquet",
    )
    with pytest.raises(DataJoinError, match="label has no polygon text row"):
        builder(
            labels=unmatched_labels,
            polygons_and_text=staged_polygons,
            workspace_path=tmp_path / "unmatched.sqlite",
        )


def _wiki_rows() -> dict[str, list[dict[str, Any]]]:
    labels = [
        {
            "label_id": "wiki-label-0000",
            "sentence_id": "sentence-a",
            "decision": "yes",
            "reviewer": "reviewer-0",
            "confidence": 0.75,
        }
    ]
    decisions = ("yes", "no", "failed", "skipped_unsplit")
    labels.extend(
        {
            "label_id": f"wiki-label-{index:04d}",
            "sentence_id": "sentence-c" if index == 511 else "sentence-b",
            "decision": decisions[(index - 1) % len(decisions)],
            "reviewer": f"reviewer-{index % 3}",
            "confidence": 0.75,
        }
        for index in range(1, 512)
    )
    labels.extend(
        [
            {
                "label_id": "wiki-label-0512",
                "sentence_id": "sentence-a",
                "decision": "no",
                "reviewer": "reviewer-2",
                "confidence": 0.75,
            },
            {
                "label_id": "wiki-label-0513",
                "sentence_id": "sentence-b",
                "decision": "failed",
                "reviewer": "reviewer-0",
                "confidence": 0.75,
            },
        ]
    )
    first_link = {
        "document_id": "document-2",
        "project": "wikivoyage",
        "polygon_id": "polygon-2",
        "input_revision": "wiki-snapshot-4",
    }
    return {
        "labels": labels,
        "sentences": [
            {
                "sentence_id": "sentence-b",
                "document_id": "document-2",
                "project": "wikivoyage",
                "text": "Second document sentence.",
                "language": "fr",
                "sentence_revision": 7,
            },
            {
                "sentence_id": "sentence-a",
                "document_id": "document-1",
                "project": "wikipedia",
                "text": "First document sentence.",
                "language": "en-GB",
                "sentence_revision": 3,
            },
            {
                "sentence_id": "sentence-c",
                "document_id": "document-2",
                "project": "wikivoyage",
                "text": "Third sentence in the second document.",
                "language": "fr",
                "sentence_revision": 8,
            },
        ],
        "documents": [
            {
                "document_id": "document-2",
                "project": "wikivoyage",
                "title": "Second document",
                "page_id": 22,
                "url": "https://example.test/second",
            },
            {
                "document_id": "unused-document",
                "project": "wikipedia",
                "title": "Unmatched document",
                "page_id": 99,
                "url": "https://example.test/unmatched",
            },
            {
                "document_id": "document-1",
                "project": "wikipedia",
                "title": "First document",
                "page_id": 11,
                "url": "https://example.test/first",
            },
        ],
        "links": [
            first_link,
            {
                "document_id": "document-1",
                "project": "wikipedia",
                "polygon_id": "polygon-3",
                "input_revision": "wiki-snapshot-4",
            },
            {
                "document_id": "document-2",
                "project": "wikivoyage",
                "polygon_id": "polygon-3",
                "input_revision": "wiki-snapshot-4",
            },
            {
                "document_id": "document-1",
                "project": "wikipedia",
                "polygon_id": "polygon-1",
                "input_revision": "wiki-snapshot-4",
            },
            dict(first_link),
            {
                "document_id": "unused-document",
                "project": "wikipedia",
                "polygon_id": "missing-polygon",
                "input_revision": "unmatched-snapshot",
            },
        ],
        "polygons": [
            {
                "polygon_id": "polygon-3",
                "geometry": '{"type":"Point","coordinates":[3,3]}',
                "native_geometry_revision": 13,
            },
            {
                "polygon_id": "polygon-unused",
                "geometry": '{"type":"Point","coordinates":[9,9]}',
                "native_geometry_revision": 99,
            },
            {
                "polygon_id": "polygon-2",
                "geometry": '{"type":"Point","coordinates":[2,2]}',
                "native_geometry_revision": 12,
            },
            {
                "polygon_id": "polygon-1",
                "geometry": '{"type":"Point","coordinates":[1,1]}',
                "native_geometry_revision": 11,
            },
        ],
    }


def _staged_wiki_rows(
    rows: dict[str, list[dict[str, Any]]], path: Path
) -> dict[str, StagedRows]:
    path.mkdir(parents=True, exist_ok=True)
    return {
        name: _staged(records, path / f"{name}.parquet")
        for name, records in rows.items()
    }


def test_wiki_batch_carries_polygon_ordinals_into_selection_writes(
    tmp_path: Path,
) -> None:
    source_rows = _wiki_rows()
    staged = _staged_wiki_rows(source_rows, tmp_path / "inputs")
    index = disk_joined.DiskJoinIndex(tmp_path / "wiki.sqlite")
    try:
        disk_joined._index_wiki_sources(
            index=index,
            sentences=staged["sentences"],
            documents=staged["documents"],
            links=staged["links"],
            polygons=staged["polygons"],
        )
        index.commit()
        batch = disk_joined._join_wiki_batch(
            index=index,
            labels=source_rows["labels"][:2],
            sentences=staged["sentences"],
            documents=staged["documents"],
            links=staged["links"],
            polygons=staged["polygons"],
        )
    finally:
        index.close()

    assert batch.polygon_ordinals == {
        "polygon-2": 2,
        "polygon-3": 0,
        "polygon-1": 3,
    }


def test_wiki_indexed_join_preserves_batch_occurrences_and_native_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder = getattr(disk_joined, "build_wiki_partition", None)
    assert callable(builder), "Wiki needs a disk-backed partition builder"
    source_rows = _wiki_rows()
    staged = _staged_wiki_rows(source_rows, tmp_path / "inputs")
    label_batch_sizes: list[int] = []
    original_join = joined_outputs.join_wiki_partition

    def tracked_join(**kwargs: Any) -> JoinedPartition:
        batch_labels = list(kwargs["label_rows"])
        if batch_labels:
            label_batch_sizes.append(len(batch_labels))
        return original_join(
            label_rows=batch_labels,
            sentence_rows=kwargs["sentence_rows"],
            document_rows=kwargs["document_rows"],
            link_rows=kwargs["link_rows"],
            polygon_rows=kwargs["polygon_rows"],
        )

    monkeypatch.setattr(joined_outputs, "join_wiki_partition", tracked_join)
    partition = builder(
        labels=staged["labels"],
        sentences=staged["sentences"],
        documents=staged["documents"],
        links=staged["links"],
        polygons=staged["polygons"],
        workspace_path=tmp_path / "wiki.sqlite",
    )

    assert label_batch_sizes == [512, 2]
    labels = list(partition.labels)
    assert len(partition.labels) == 514
    assert [row["label_id"] for row in labels] == [
        f"wiki-label-{index:04d}" for index in range(514)
    ]
    assert [row["source_row_index"] for row in labels] == list(range(514))
    assert [row["raw_decision"] for row in labels[510:514]] == [
        "no",
        "failed",
        "no",
        "failed",
    ]
    assert [row["text_unit_key"] for row in labels[0:2] + labels[512:514]] == [
        "sentence-a",
        "sentence-b",
        "sentence-a",
        "sentence-b",
    ]
    assert labels[511]["text_unit_key"] == "sentence-c"
    assert [row["reviewer"] for row in labels[:3]] == [
        "reviewer-0",
        "reviewer-1",
        "reviewer-2",
    ]
    assert [row["confidence"] for row in labels[:3]] == [0.75, 0.75, 0.75]

    text_units = list(partition.text_units)
    assert len(partition.text_units) == 3
    assert [row["text_unit_key"] for row in text_units] == [
        "sentence-a",
        "sentence-b",
        "sentence-c",
    ]
    assert [row["text"] for row in text_units] == [
        "First document sentence.",
        "Second document sentence.",
        "Third sentence in the second document.",
    ]
    assert [row["polygon_ids"] for row in text_units] == [
        ["polygon-3", "polygon-1"],
        ["polygon-2", "polygon-3", "polygon-2"],
        ["polygon-2", "polygon-3", "polygon-2"],
    ]
    assert text_units[0] == {
        "sentence_id": "sentence-a",
        "document_id": "document-1",
        "project": "wikipedia",
        "text": "First document sentence.",
        "language": "en-GB",
        "sentence_revision": 3,
        "text_unit_key": "sentence-a",
        "source_url": "https://example.test/first",
        "polygon_ids": ["polygon-3", "polygon-1"],
        "raw_language_code": "en-GB",
        "language_code": None,
        "language_scheme": "wikimedia_project_code",
        "language_mapping_version": "atlas-language-crosswalk-1.0.0",
        "language_script": None,
        "language_mapping_status": "unresolved",
    }
    assert text_units[1]["sentence_revision"] == 7
    assert text_units[1]["source_url"] == "https://example.test/second"
    assert text_units[2]["sentence_revision"] == 8
    assert text_units[2]["source_url"] == "https://example.test/second"
    assert list(partition.text_units) == text_units

    documents = list(partition.documents)
    assert len(partition.documents) == 2
    assert [row["document_id"] for row in documents] == [
        "document-2",
        "document-1",
    ]
    assert documents == [source_rows["documents"][0], source_rows["documents"][2]]
    assert list(partition.documents) == documents

    links = list(partition.polygon_document_links)
    assert len(partition.polygon_document_links) == 5
    assert links == source_rows["links"][:5]
    assert links[0] == links[4]
    assert list(partition.polygon_document_links) == links

    polygons = list(partition.polygon_records)
    assert len(partition.polygon_records) == 3
    assert [row["polygon_id"] for row in polygons] == [
        "polygon-3",
        "polygon-2",
        "polygon-1",
    ]
    assert [row["geometry"] for row in polygons] == [
        '{"type":"Point","coordinates":[3,3]}',
        '{"type":"Point","coordinates":[2,2]}',
        '{"type":"Point","coordinates":[1,1]}',
    ]
    assert [row["native_geometry_revision"] for row in polygons] == [13, 12, 11]
    assert list(partition.polygon_records) == polygons
    assert partition.status_counts == {
        "yes": 129,
        "no": 129,
        "failed": 129,
        "skipped": 127,
    }
    assert partition.raw_decision_counts == {
        "failed": 129,
        "no": 129,
        "skipped_unsplit": 127,
        "yes": 129,
    }


def test_disk_wiki_partitions_share_identical_content_addressed_link_shard(
    tmp_path: Path,
) -> None:
    builder = getattr(disk_joined, "build_wiki_partition", None)
    assert callable(builder), "Wiki needs a disk-backed partition builder"
    source_rows = _wiki_rows()
    staged = _staged_wiki_rows(source_rows, tmp_path / "inputs")
    wiki_source = next(
        source for source in SOURCE_CATALOG if source.source_id == "wiki"
    )
    output_dir = tmp_path / "joined"
    partitions = [
        builder(
            labels=_staged(
                [source_rows["labels"][index] for index in order],
                tmp_path / f"labels-{number}.parquet",
            ),
            sentences=staged["sentences"],
            documents=staged["documents"],
            links=staged["links"],
            polygons=staged["polygons"],
            workspace_path=tmp_path / f"wiki-{number}.sqlite",
        )
        for number, order in enumerate(((0, 1), (1, 0)))
    ]
    paths = (
        "labels/wikipedia/sentences/first.parquet",
        "labels/wikipedia/sentences/second.parquet",
    )
    results = [
        write_joined_partition(
            source_id="wiki",
            repo_id=wiki_source.repo_id,
            revision=wiki_source.revision,
            path=path,
            output_dir=output_dir,
            build=lambda partition=partition: partition,
        )
        for path, partition in zip(paths, partitions, strict=True)
    ]

    link_names = {
        result.manifest["outputs"]["polygon_document_links"]["name"]
        for result in results
    }
    assert len(link_names) == 1
    (link_name,) = tuple(link_names)
    link_output = results[0].manifest["outputs"]["polygon_document_links"]
    assert link_name == f"wiki__polygon_document_links.{link_output['sha256']}.parquet"
    assert list(output_dir.glob("wiki__polygon_document_links.*.parquet")) == [
        output_dir / link_name
    ]


def test_atlas_join_routes_wiki_through_disk_builder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_rows = _wiki_rows()
    source_rows["labels"] = source_rows["labels"][:1]
    source_rows["sentences"] = [
        row for row in source_rows["sentences"] if row["sentence_id"] == "sentence-a"
    ]
    source_rows["documents"] = [source_rows["documents"][2]]
    source_rows["links"] = [source_rows["links"][1], source_rows["links"][3]]
    source_rows["polygons"] = [source_rows["polygons"][0], source_rows["polygons"][3]]
    staged = _staged_wiki_rows(source_rows, tmp_path / "cli-inputs")
    source = next(item for item in SOURCE_CATALOG if item.source_id == "wiki")
    labels_path = f"{source.table_roots['labels'][0]}/cli.parquet"
    captured: dict[str, Any] = {}
    original_join = joined_outputs.join_wiki_partition

    def build_disk(**kwargs: Any) -> JoinedPartition:
        captured["builder_arguments"] = kwargs
        return original_join(
            label_rows=kwargs["labels"],
            sentence_rows=kwargs["sentences"],
            document_rows=kwargs["documents"],
            link_rows=kwargs["links"],
            polygon_rows=kwargs["polygons"],
        )

    def stage_rows(
        _source: Any,
        table: str,
        _path: str,
        **_kwargs: Any,
    ) -> tuple[StagedRows, SimpleNamespace]:
        table_names = {
            "labels": "labels",
            "text_units": "sentences",
            "documents": "documents",
            "polygon_document_links": "links",
            "polygons": "polygons",
        }
        staged_rows = staged[table_names[table]]
        return staged_rows, SimpleNamespace(
            rows=len(staged_rows), row_groups=1, range_bytes=1
        )

    def write_partition(**kwargs: Any) -> SimpleNamespace:
        partition = kwargs["build"]()
        captured["partition"] = partition
        return SimpleNamespace(
            status="written",
            manifest={"label_row_count": len(partition.labels)},
            manifest_path=tmp_path / "cli-output" / "wiki.manifest.json",
        )

    def reject_in_memory_join(**_kwargs: Any) -> None:
        raise AssertionError("atlas-join used the in-memory Wiki join")

    monkeypatch.setattr(
        joined_cli, "resolve_hub_file_size", lambda *_args, **_kwargs: 1
    )
    monkeypatch.setattr(joined_cli, "_require_staging_space", lambda *_args: None)
    monkeypatch.setattr(joined_cli, "_stage_rows", stage_rows)
    monkeypatch.setattr(joined_cli, "write_joined_partition", write_partition)
    monkeypatch.setattr(joined_cli, "build_wiki_partition", build_disk, raising=False)
    monkeypatch.setattr(
        joined_cli, "join_wiki_partition", reject_in_memory_join, raising=False
    )

    exit_code = joined_cli.main(
        [
            "--source",
            "wiki",
            "--labels",
            labels_path,
            "--max-bytes",
            "100",
            "--output-dir",
            str(tmp_path / "cli-output"),
        ]
    )

    assert exit_code == joined_cli.EXIT_OK
    arguments = captured["builder_arguments"]
    assert arguments["labels"] is staged["labels"]
    assert arguments["sentences"] is staged["sentences"]
    assert arguments["documents"] is staged["documents"]
    assert arguments["links"] is staged["links"]
    assert arguments["polygons"] is staged["polygons"]
    assert arguments["workspace_path"].name == "wiki.sqlite"
    assert len(captured["partition"].labels) == 1
