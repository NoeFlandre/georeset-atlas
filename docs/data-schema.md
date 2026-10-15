# Data schema

**Status: NOT RELEASE-READY.** This page is a draft. Table names come from the source catalog. Column lists are pending.

## Logical source tables

A logical table groups one or more table roots. Each table root is a relative path inside one pinned source.

| Source ID | Logical table | Table roots |
| --- | --- | --- |
| `description` | `polygons` | `data` |
| `description` | `text_units` | `language-v1/data` |
| `description` | `labels` | `labels/language-v1/data` |
| `website` | `polygons_and_text` | `polygons` |
| `website` | `labels` | `labels/polygons` |
| `wiki` | `polygons` | `polygons` |
| `wiki` | `text_units` | `wikipedia/sentences`, `wikivoyage/sentences` |
| `wiki` | `documents` | `wikipedia/documents`, `wikivoyage/documents` |
| `wiki` | `polygon_document_links` | `polygon_document_links` |
| `wiki` | `labels` | `labels/wikipedia/sentences`, `labels/wikivoyage/sentences` |

## Target tables

The planned Atlas output uses these target tables. Exact column lists are pending.

- `polygon_records`: stores polygons once. Source geometry forms differ. Description geometry is well-known binary (WKB) in GeoParquet 1.1, with CRS84. Website and Wiki geometry are GeoJSON strings. Source precision and provenance are preserved.
- `documents`: stores source documents.
- `text_units`: stores original text occurrences.
- `polygon_document_links`: stores occurrence associations between polygons and documents.
- `labels`: stores each source label row, including conflicts and failures or skips.
- Generation audit records: store frozen inputs, tool and config versions, row and status reconciliation, output hashes, and run measurements.

## Target configs

The configs below are intended. Implementation is pending.

- The default relevant view selects one original text unit labeled `yes`. It includes a `polygon_ids` list, the exact text, the source, the unified public language, IDs, the source URL, and provenance.
- The `all_labeled` view exposes `yes` and `no` labels, plus `failed` and `skipped` labels.
- Source-specific configs, the polygon companion, and the generation audit views share native Parquet shards partitioned by source and status. They do not copy the full data.

## Planned storage and preservation

- Outputs are Parquet files (a columnar file format), partitioned by source and status.
- Shared shards are stored once and referenced through configs. They are not copied into each partition.
- Occurrence duplicates are preserved, along with the conflicts between them.
- Original text is preserved.
- Geometry precision and provenance are preserved.
- Label status is preserved.

## Join keys

These join keys come from the approved contract.

- `source_id` and the pinned revision identify a source.

### Description

- Label-to-text joins use `description_identity`, `tag_key`, and `sentence_index`. `sentence_index` selects the entry in the original `sentences` array.
- Unsplit cases, with status `unsupported_language` or `not_detected`, use `original_text`. Their skipped label uses index 0.
- The identity then maps to polygons by `source_pbf`, `osm_type`, and `osm_id`.
- `osm_type` and `osm_id` form an entity key only. They do not authorize merging entities from different source snapshots.

### Website

- Label-to-text joins use `polygon_id`, `field`, and `sentence_index`. The join selects `website_sentences` or `contact_website_sentences`.
- Native statuses are field-specific. `unsupported_language` can have a null sentence array. Its skipped unit uses the full original field text.
- The full Website text is retained, because concatenating split sentences is lossy.

### Wiki

- The label `sentence_id` joins to the sentence row.
- The sentence `document_id` and `project` join to the document.
- `polygon_document_links` associate polygons and documents by `document_id`, `project`, and `polygon_id`. The association is many-to-many.
- The links table stores occurrences. Duplicate link occurrences and source order are preserved.
- Wiki labels cover Wikipedia and Wikivoyage sentences, not Wikidata facts.

The audit records these field names: `sentence_index`, `split_status`, `original_text`, and `sentences`. Exact column lists are pending.

## Known limitations

1. Some pinned Description text rows have empty `sentences` arrays. These rows still have `original_text`. Their skipped labels still point to index 0.
2. In the Afghanistan Description pilot, 31 of 215 labels have `split_status` `unsupported_language`. All 31 use index 0.
3. A joined row keeps its raw split status. The status `unsupported_language` is treated as unsplit.
4. The native Website polygon schema has field-specific statuses. It has no generic `decision` column.
5. Wiki pilot outputs copy the full regional `polygon_document_links` table into each project partition. The Wikipedia and Wikivoyage pilot manifests each list 1,806 links. Full assembly must store shared link shards once.
6. Wiki viewer language codes do not match the Wiki language-split directories. See [Methods](methods.md).
7. Pilot joins cover Afghanistan only. They are not full-source results.
