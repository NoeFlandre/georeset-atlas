# Methods

**Status: NOT RELEASE-READY.** This page is a draft. Open gates are listed in [Release status](release-status.md).

## Source pins

The source pins are frozen in the source catalog. Each source uses one Hugging Face dataset repository and one 40-character revision.

| Source ID | Repository | Revision |
| --- | --- | --- |
| `description` | `NoeFlandre/osm-polygon-description-tag-landuse` | `ce98b328f3c6543800a3d931877b51eb3f0cf768` |
| `website` | `NoeFlandre/osm-polygon-website-tag-landuse` | `e6524c711ee0033c3cd5990e5fdbbe4b89992dfb` |
| `wiki` | `NoeFlandre/osm-polygon-wikidata-and-wikipedia-landuse` | `49307aeb05b6dcc8a5d393f77f59a1e6d13a967a` |

## Source lineage and historical method

The three label mirrors are the frozen input tables for Atlas. Each pinned card identifies the pre-label HF dataset that supplied its polygons and text. Those pre-label datasets are the documented intermediate HF artifacts; the pinned cards do not identify another intermediate HF dataset.

| Atlas source | Frozen label mirror | Pre-label HF dataset and revision |
| --- | --- | --- |
| `description` | [label card at the frozen revision](https://huggingface.co/datasets/NoeFlandre/osm-polygon-description-tag-landuse/blob/ce98b328f3c6543800a3d931877b51eb3f0cf768/README.md) | [`osm-polygon-description-tag` @ `b4706eb315c66a8a57135289e7612dca8e03caf8`](https://huggingface.co/datasets/NoeFlandre/osm-polygon-description-tag/tree/b4706eb315c66a8a57135289e7612dca8e03caf8) |
| `website` | [label card at the frozen revision](https://huggingface.co/datasets/NoeFlandre/osm-polygon-website-tag-landuse/blob/e6524c711ee0033c3cd5990e5fdbbe4b89992dfb/README.md) | [`osm-polygon-website-tag` @ `4cbddf95ce614e68d5ada8b94ca21bf9e48d39dd`](https://huggingface.co/datasets/NoeFlandre/osm-polygon-website-tag/tree/4cbddf95ce614e68d5ada8b94ca21bf9e48d39dd) |
| `wiki` | [label card at the frozen revision](https://huggingface.co/datasets/NoeFlandre/osm-polygon-wikidata-and-wikipedia-landuse/blob/49307aeb05b6dcc8a5d393f77f59a1e6d13a967a/README.md) | [`osm-polygon-wikidata-and-wikipedia` @ `d6fc058ccf4643bc5fe5bfd5c1c0bc403d3da801`](https://huggingface.co/datasets/NoeFlandre/osm-polygon-wikidata-and-wikipedia/tree/d6fc058ccf4643bc5fe5bfd5c1c0bc403d3da801) |

### Polygon and text processing

- **Description.** The pinned input card documents `osmium export` with standard OSM area handling. It retains qualifying polygon features with trimmed, non-empty base or localized description tags and preserves the OSM tags and geometry. Its `language-v1` annotations report Lingua 2.2.0 as primary and GlotLID v3 as fallback when Lingua is uncertain. The joined source schema also carries `status`, `reason`, `snapshot_id`, and `model_config_fingerprint`. In the completed Afghanistan pilot, `reason` distinguishes `detected` (171 rows), `fallback_glotlid_v3` (40 rows), and `too_short` (4 rows), so the detector scheme can be assigned from row provenance for those records. This is pilot-only evidence; global values still need audit. The card records the input snapshot and detector-configuration SHA-256 values and the GlotLID model revision. The split-tool name and version are not recorded in that card.
- **Website.** The pinned input card documents polygon assembly with libosmium and full-page text extraction with Trafilatura, without truncation. It reports GlotLID v3 language detection with script-aware codes and SaT `segment-any-text/sat-3l-sm` sentence segmentation for its supported languages. Full source text is retained because concatenated segments do not reconstruct it losslessly. The card does not pin the GlotLID model revision or runtime version.
- **Wiki.** The pinned input card describes a V2 dataset with multilingual Wikipedia and Wikivoyage documents linked to OSM polygons. Its split-coverage figures are over text sections: 11,197,076 of 12,666,253 eligible sections were split; 1,469,177 unsupported-language sections remain unsplit. The card does not name or pin the language detector or sentence-splitter versions.

These are claims from the frozen public input cards, not new processing by Atlas. Atlas performs no scraping, language detection, sentence splitting, annotation, or model inference.

### Relevance labeling and exact prompt reference

Each pinned label card reports the same generation setup: `LiquidAI/LFM2.5-2.6B` at revision [`654f9463ce32b05d0429d76fe1f580b27d4c1ac0`](https://huggingface.co/LiquidAI/LFM2.5-2.6B/tree/654f9463ce32b05d0429d76fe1f580b27d4c1ac0), with speculative-decoding draft [`LiquidAI/LFM2.5-2.6B-DSpark` at `458cedab07d0f7b2b05700c77e1aa463d43d6f04`](https://huggingface.co/LiquidAI/LFM2.5-2.6B-DSpark/tree/458cedab07d0f7b2b05700c77e1aa463d43d6f04). The cards specify SGLang, BF16, greedy decoding, thinking mode, and a maximum of 4,096 new tokens.

All three pinned label cards identify the prompt as the one from [the land-use relevance benchmark](https://huggingface.co/datasets/NoeFlandre/benchmark-llms-landuse-relevance). The benchmark's public `data/prompt.txt` has one recorded file-history commit, [`713ccaf6e15554ef20c241569438dec24868d35c`](https://github.com/NoeFlandre/benchmark-llms-landuse-relevance/blob/713ccaf6e15554ef20c241569438dec24868d35c/data/prompt.txt), dated 2026-09-13. Its exact file SHA-256 is `2fb48569c8bbb73fcf584fb7549b34ddd5e4cc365b7a5c75be7429906e312097`; the bytes at that commit match the current public prompt file. The three label mirror revisions are dated 2026-10-14. This is the immutable public prompt reference available in the source lineage. The label cards do not store a per-generation prompt hash, so no claim is made that each generation row independently attests to that hash. The SGLang version and generation-code revision are also not identified by these cards.

The published label tables retain a label row per labeled text unit and separate generation tables for unique generated texts. The source cards state that generation rows include raw model output, token counts, finish reason, and generation ID. Atlas preserves source label occurrences and does not deduplicate conflicting labels.

## Remaining historical evidence gaps

- The exact historical split-tool version for Description and detector/splitter versions for Wiki are not recorded in the pinned input cards.
- The Website GlotLID model revision and the generation runtime/code versions are not recorded in the pinned cards.
- The benchmark prompt artifact is pinned and hashed above, but source generation rows do not carry their own prompt hash.
- No other intermediate Hugging Face dataset is identified by the pinned label cards beyond the three linked pre-label datasets.
- Website's native language fields identify GlotLID v3, and the crosswalk encodes that scheme. Global output integration has not run. Wiki language fields are encoded as Wikimedia project codes. Their provenance still needs audit against the native columns and partitions.
- Full-source counts and the row-level language inventory remain subject to release reconciliation.

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

The versioned asset `atlas-language-crosswalk-1.0.0` is generated. Its mapping version is `atlas-language-crosswalk-1.0.0`. The file is `src/atlas_domain/language-crosswalk-1.0.0.json`, and the package includes it as package data. The asset has 184 ISO 639-1 codes, 7,922 ISO 639-3 codes, and 183 ISO 639-3 to ISO 639-1 mappings. The loader rejects a count that does not match its table. Joined text-unit code integration is implemented in the current work in progress. Global output integration and inventory reconciliation are pending.

### Scheme keys and lookup key

The asset uses three exact scheme keys:

- `lingua_iso639_3`: ISO 639-3 codes from Lingua.
- `glotlid_iso639_3_script`: ISO 639-3 codes with a script, from GlotLID.
- `wikimedia_project_code`: Wikimedia project codes.

The lookup key is `(scheme, raw_code, mapping_version)`. Each part must match exactly. The script is extracted separately, and only for GlotLID.

### Scheme assignment by source

- Description: `reason` sets the scheme. `detected` gives `lingua_iso639_3`, and `fallback_glotlid_v3` gives `glotlid_iso639_3_script`. Any other value, including `too_short`, gives a null scheme. The Afghanistan pilot is the only evidence for these `reason` values. Global values still need audit.
- Website: every text unit uses `glotlid_iso639_3_script`.
- Wiki: every text unit uses `wikimedia_project_code`.

### Common text-unit fields

Each joined text unit has these six fields.

| Field | Meaning |
| --- | --- |
| `language_code` | The canonical code from the scheme's mapping. Null unless the status is `resolved`. |
| `raw_language_code` | The source code, unchanged. Null when the source has no code. |
| `language_scheme` | The scheme the source assigns. Null when the source does not identify one. |
| `language_script` | The script subtag of a GlotLID code with valid syntax. Null otherwise. |
| `language_mapping_version` | `atlas-language-crosswalk-1.0.0` for every row. |
| `language_mapping_status` | `resolved`, `unresolved`, `missing`, or `special`. |

### Mapping states

- `resolved`: the code maps to `language_code`, through an explicit entry or the scheme's ISO fallback.
- `unresolved`: the code has no explicit entry and no fallback match. A code with a null scheme is `unresolved`. So are the Wikimedia codes `nrm`, `eml`, and `cbk-zam`.
- `missing`: the source has no code. A missing code keeps the scheme that the source assigns.
- `special`: the code is `und`, `mul`, `zxx`, or `mis`, in any scheme. Special codes never map.

### Code resolution

The asset has 11 explicit entries. They apply before the fallback:

- Lingua: `eng` to `en`, `cmn` to `cmn`, `pbt` to `pbt`, and `nrm` to `nrm`.
- GlotLID: `eng_Latn` to `en`.
- Wikimedia: `en` to `en`, `als` to `gsw`, `zh-min-nan` to `nan`, `zh-yue` to `yue`, `zh-classical` to `lzh`, and `simple` to `en`.

If no explicit entry applies, the scheme's ISO fallback applies:

- Lingua: a code in the ISO 639-3 table maps to its ISO 639-1 code when the asset has one. Otherwise, the code keeps its ISO 639-3 form, so `cmn` and `pbt` stay distinct. A code outside the ISO 639-3 table stays `unresolved`.
- GlotLID: the same rule applies to the ISO 639-3 part of a code with GlotLID syntax. That syntax is three lowercase letters, optionally followed by `-` or `_` and a four-letter script. A script-qualified code without an explicit entry still resolves this way when its ISO 639-3 part is in the table.
- Wikimedia: a code in the ISO 639-1 table maps to itself. A code in the ISO 639-3 table follows the Lingua rule. The codes `nrm`, `eml`, and `cbk-zam` stay `unresolved`.

There is no other alias guessing. A code that matches no rule stays `unresolved`.

### Sources and hashes

The official files match the supplied sizes and hashes. This check covers file bytes only. It does not show language coverage.

- IANA registry, SHA-256: `755fad43283be7b41ebe3c89ad054b6eaf928f404f9c0edb74799e0eab74beb1`
- LOC ISO-639-2, SHA-256: `42b71885e4dc885559fda5ad059fd81838cf4782cedb5fd08cc3431f7d067371`
- Meta-Wiki, SHA-1: `8073d6c8557f9ddaab5e17340adcc999e3a4fb42`

### Inventory

- The pinned Website file `analysis/languages.parquet` is 4,302 bytes. It has 566 tag/language rows and 397 distinct codes across both tag fields. A strict projected Range read transferred 8,141 bytes, including Parquet footer and re-read overhead. The 397 codes are a pinned aggregate inventory, not final full-output coverage. The resolution count for them under the current asset is not recorded.
- The Wiki language inventory is not reconciled. Wiki viewer codes do not match the Wiki language-split directories.
- The audit records viewer distinct-code counts of 278, 392, and 350. It does not tie each count to a source.
- The full source language inventory is still unreconciled.
- Joined text-unit code integration is implemented in the current work in progress. No global assembled output or inventory reconciliation exists.
- Description `reason` identifies the detector in the Afghanistan pilot only. The Description pipeline uses Lingua, with GlotLID as fallback. Global detector provenance still needs audit.

## Maps plan

Status: pending. The H3 builder and its tests exist. No full Atlas assembly maps have been built, so no map or metric output is recorded as done. See [H3 map artifacts](atlas-h3.md).

- Grid: H3 is a hexagonal grid. The maps use resolution 3 for the global view and for each source.
- Polygon position: each polygon is placed at its Shapely `representative_point()` in CRS84. The builder applies the same rule to every source.
- Style: a neutral `#f3f2ee` backdrop and a colorblind-safe Cividis color scale.
- Metrics:
  - Distinct yes polygons: the number of different polygons in a cell linked to at least one text unit with a `yes` label occurrence.
  - Association occurrences: the number of text-to-polygon links in a cell. Duplicate links count each time.
  - Distinct text units: the number of different text units with a link to a polygon in a cell.
  - Distinct text units per km²: distinct text units divided by the cell area.
  - Yes share: `yes` label occurrences divided by `yes` plus `no` label occurrences. Failed and skipped occurrences are excluded. The Parquet aggregate records the denominator in `yes_share_denominator`. The SVG does not print it.
- Density: the density map uses a logarithmic color scale, and its legend says so.
- Missing geometry: polygons with missing or invalid geometry create no cell. They do not appear as zero on the maps. Their counts appear in `geometry_quality` in the manifest.
- Aggregates: the Parquet aggregates behind each map are written with the maps and can be reproduced.
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

- The five rows sum to 4,538.801108 hours. Shared rows are counted once.
- These are scheduler-reserved allocation hours, including setup, idle time, and failed runs. They are not active GPU utilization or guaranteed lifetime compute.
- Atlas adds zero model-inference GPU hours. Assembly uses CPU only; measured assembly CPU time, RAM, storage, and network totals are pending.
- The source label cards record excluded/unmapped allocations and shared work; see each frozen source card linked above for the audit scope.
