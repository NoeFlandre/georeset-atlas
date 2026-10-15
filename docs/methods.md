# Methods

**Status: NOT RELEASE-READY.** This page is a draft. Open gates are listed in [Release status](release-status.md).

## Source pins

The source pins are frozen in the source catalog. Each source uses one Hugging Face dataset repository and one 40-character revision.

| Source ID | Repository | Revision |
| --- | --- | --- |
| `description` | `NoeFlandre/osm-polygon-description-tag-landuse` | `ce98b328f3c6543800a3d931877b51eb3f0cf768` |
| `website` | `NoeFlandre/osm-polygon-website-tag-landuse` | `e6524c711ee0033c3cd5990e5fdbbe4b89992dfb` |
| `wiki` | `NoeFlandre/osm-polygon-wikidata-and-wikipedia-landuse` | `49307aeb05b6dcc8a5d393f77f59a1e6d13a967a` |

## Pipeline stages

These stages describe the intended historical pipeline. Details that this draft does not record are marked as pending.

1. **OSM polygon extraction.** The polygons are land-use polygons from OpenStreetMap (OSM). The three source names refer to three OSM tag groups: description, website, and Wikidata and Wikipedia.
2. **Source text collection.** Each source holds its own text. The Website source contains scraped text. The Wiki source includes Wikipedia and Wikivoyage data. The collection steps are not recorded in this draft.
3. **Language detection.** Lingua and GlotLID are language detection tools. The Description pipeline uses Lingua, with GlotLID as fallback. Row-level detector provenance is not proven. Language codes and their mapping are described below under Language crosswalk.
4. **Sentence splitting.** Description and Wiki text is split into sentences. Each Description source record has a `sentences` array. The `split_status` field records the result. Some Description text rows have an empty `sentences` array. The Website source stores sentences in `website_sentences` and `contact_website_sentences`. Website statuses are field-specific.
5. **Relevance labeling.** Each label records a relevance status. The statuses are defined under Label meanings. Join keys for each source are listed in [Data schema](data-schema.md). Wiki labels cover Wikipedia and Wikivoyage sentences, not Wikidata facts. The historical model details are listed under Not yet evidenced here.
6. **Preserved source lineage.** Each source is identified by its `source_id` and pinned revision. The release plan keeps original text occurrences, full Website field text, Wiki link occurrences (including duplicates, in source order), and label limitations. Joined rows keep the raw split status.
7. **Intermediate Hugging Face datasets.** The historical pipeline produced intermediate Hugging Face datasets. Links to them are not yet evidenced here and remain pending. The three pinned source repositories are listed under Source pins.

## Not yet evidenced here

These items are pending. This draft gives no values for them.

- The exact historical model, reference revision, prompt, and hash.
- Tool versions. Lingua and GlotLID are named, but their versions are not recorded.
- Links to the intermediate Hugging Face datasets.

## Label counts

These are expected label counts from the source catalog. They are not reconciled against full sources. Each total is the sum of the four status counts.

| Source ID | `yes` | `no` | `failed` | `skipped` | Expected total |
| --- | ---: | ---: | ---: | ---: | ---: |
| `description` | 725,371 | 241,475 | 11,927 | 78,229 | 1,057,002 |
| `website` | 8,869,643 | 36,523,593 | 430,211 | 37,473 | 45,860,920 |
| `wiki` | 25,868,341 | 46,068,954 | 1,385,457 | 1,469,177 | 74,791,929 |

Do not add totals across sources. Each source has its own label tables.

## Label meanings

- `yes`: the labeling step judged the text relevant to land use or land cover. The judgment uses the text only.
- `no`: the labeling step judged the text not relevant to land use or land cover.
- `failed`: the labeling step failed for this item. This is not a relevance judgment.
- `skipped`: the item was not labeled. This is not a relevance judgment.

A `yes` label is text-only relevance. It is not polygon-conditioned alignment. It does not show that the text matches the polygon it is linked to.

## Pilot joins (Afghanistan)

Pilot joins test the joins on Afghanistan partitions only. Pilot results are not full-source results.

| Source | Labels joined |
| --- | ---: |
| Description | 215 of 215 |
| Website | 1,136 of 1,136 |
| Wikipedia | 53,555 of 53,555 |
| Wikivoyage | 500 of 500 |

- Reads used a range budget of 128 MiB (mebibytes) for each file. Inputs were not downloaded in full.
- The pilot range transfer was 30,202,789 bytes. This is not a full network measurement.
- Pilot outputs were kept outside the repository checkout.
- An identical rerun verified all four output manifests.

## Language crosswalk

The versioned asset `atlas-language-crosswalk-1.0.0` has been generated. Its mapping version is 1.0.0. Row-level provenance, output integration, and inventory reconciliation remain pending.

### Scheme keys and lookup key

The asset uses three exact scheme keys:

- `lingua_iso639_3`: ISO 639-3 codes from Lingua.
- `glotlid_iso639_3_script`: ISO 639-3 codes with a script, from GlotLID.
- `wikimedia_project_code`: Wikimedia project codes.

The exact lookup key is `(scheme, raw_code, mapping_version)`. The script is separate. It is extracted only for GlotLID.

### Known explicit mappings

These are the known explicit mappings only.

- Lingua: `eng` maps to `en`, `cmn` to `cmn`, `pbt` to `pbt`, and `nrm` to `nrm`.
- GlotLID: `eng_Latn` maps to `en`, with script `Latn`.
- Wikimedia: `als` maps to `gsw`, `zh-min-nan` to `nan`, `zh-yue` to `yue`, `zh-classical` to `lzh`, and `simple` to `en`.
- Wikimedia `nrm`, `eml`, and `cbk-zam` remain unresolved.

### Code handling

- Explicit codes: a code maps only through an exact explicit mapping. The asset includes the supplied exact explicit mappings only.
- Special codes: `und`, `mul`, `zxx`, and `mis` are the only special codes. Each must match exactly.
- Missing codes: a null code means missing.
- Unresolved codes: every other exact key that is absent from the explicit mappings remains unresolved. A script-qualified variant without an exact mapping also stays unresolved.

### Sources and hashes

The official files match the supplied sizes and hashes. This check covers file bytes only. It does not show language coverage.

- IANA registry, SHA-256: `755fad43283be7b41ebe3c89ad054b6eaf928f404f9c0edb74799e0eab74beb1`
- LOC ISO-639-2, SHA-256: `42b71885e4dc885559fda5ad059fd81838cf4782cedb5fd08cc3431f7d067371`
- Meta-Wiki, SHA-1: `8073d6c8557f9ddaab5e17340adcc999e3a4fb42`

### Inventory

- The pinned Website file `analysis/languages.parquet` has 397 distinct codes across both tag fields. This is a source count. It is not a coverage claim.
- The Wiki language inventory is not reconciled. Wiki viewer codes do not match the Wiki language-split directories.
- The audit records viewer distinct-code counts of 278, 392, and 350. It does not tie each count to a source.
- The full source language inventory is still unreconciled.
- The full row-level scheme inventory and output integration are still pending.
- Row-level language provenance is pending. The Description pipeline uses Lingua, with GlotLID as fallback. Row-level detector provenance is not proven.

## Maps plan

Status: pending. Map and metric outputs are not recorded as done.

- Grid: H3 is a hexagonal grid. The plan uses resolution 3 for the global map and for each source map.
- Polygon position: each polygon is placed at one representative point. The same documented point rule is used for every source. The point rule is not recorded as implemented.
- Style: a neutral backdrop and a colorblind-safe color scale.
- Metrics:
  - Distinct relevant-text polygons: the number of different polygons linked to at least one text unit labeled `yes`.
  - Text-polygon association count: the number of links between text units and polygons.
  - Distinct text units per cell: the number of different text units linked to polygons in each cell.
  - Yes share: the share of labels that are `yes`, shown with the number of labels it is based on.
- Density: a count is divided by the actual area of its H3 cell. The legend labels the scale as logarithmic.
- Missing geometry: polygons with missing geometry are shown separately. They are not shown as zero.
- Aggregates: the aggregates behind each map are downloadable and can be reproduced.
- Scope: the maps have no semantic habitat labels.

## Historical GPU hours

GPU means graphics processing unit. The ledger lists historical GPU hours by scope. The ledger is not active GPU utilization. It is not lifetime compute.

| Scope | GPU hours |
| --- | ---: |
| Description only | 44.851388 |
| Website only | 1088.172500 |
| Wiki only | 3399.744165 |
| Description and Website, shared | 0.213611 |
| Website and Wiki, shared | 5.819444 |
| Total | 4538.801108 |

- The total is the sum of the five scope rows. Shared rows are counted once.
- The ledger includes setup time, idle time, and failed runs.
- Atlas adds zero model-inference GPU hours. The historical ledger is not Atlas model-inference time.
- Assembly CPU (processor), RAM (memory), storage, and network measurements are pending.
- The historical model, reference revision, prompt, and hash are pending. See Not yet evidenced here.
