from atlas_domain.joined_outputs import join_description_partition

PBF = "afghanistan-latest.osm.pbf"
ORIGINAL_TEXT = "Unsegmented description text, kept whole."


def test_unsupported_language_description_uses_original_text_at_index_zero() -> None:
    partition = join_description_partition(
        label_rows=[
            {
                "label_id": "label-1",
                "description_identity": "description-1",
                "tag_key": "name",
                "sentence_index": 0,
                "decision": "skipped_unsplit",
                "language": "fa",
            }
        ],
        text_unit_rows=[
            {
                "description_identity": "description-1",
                "source_pbf": PBF,
                "osm_type": "way",
                "osm_id": 1,
                "tag_key": "name",
                "original_text": ORIGINAL_TEXT,
                "language_code": "fa",
                "split_status": "unsupported_language",
                "sentence_count": 0,
                "sentences": [],
            }
        ],
        polygon_rows=[
            {
                "source_pbf": PBF,
                "osm_type": "way",
                "osm_id": 1,
                "geometry": b"\x00geometry-1\x01",
            }
        ],
    )

    [unit] = partition.text_units
    assert unit["text"] == ORIGINAL_TEXT
    assert unit["split_status"] == "unsupported_language"
