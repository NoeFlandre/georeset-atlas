# Release status

**Status: NOT RELEASE-READY.** The dataset is not release-ready yet. This page is a draft.

A gate is a required input or check. A pending gate is not complete. Every gate in the table below is pending.

## Current state

| Area | State |
| --- | --- |
| Source pins | Frozen in the source catalog |
| Test suite | The current work in progress passed 436 tests with 16 pytest-bdd deprecation warnings. This is not the final gauntlet. |
| Pilot joins | Done for Afghanistan only |
| Full assembly | Not recorded as done |
| Language crosswalk asset | Generated as `atlas-language-crosswalk-1.0.0`. Row-level provenance, output integration, and inventory reconciliation are pending. |
| Release | Not release-ready |

## Pending gates

| Gate | State | Detail |
| --- | --- | --- |
| Full build and reconciliation | Pending | Full assembly is not recorded as done. Catalog counts are not checked against full sources. |
| Historical model, reference revision, prompt, and hash | Pending | Not yet evidenced in this draft. |
| Upstream tools and versions | Pending | Lingua and GlotLID are named. Their versions are not recorded. |
| Row-level language provenance | Pending | Row-level detector provenance is not proven. |
| Crosswalk output integration | Pending | Output integration is not recorded as done. |
| Final language inventory | Pending | The full source language inventory is not reconciled, including the Wiki inventory. |
| Website scraped-text redistribution basis | Pending | Redistribution evidence is explicitly missing. |
| Geometry rights statement | Pending | Separate from text rights. |
| Text rights for Description and Wiki text | Pending | No blanket text rights are claimed in this draft. |
| Representative point implementation | Pending | Not recorded as implemented. |
| Maps and metrics | Pending | The plan uses H3 resolution 3. Map and metric outputs are not recorded as done. |
| Assembly CPU, RAM, storage, and network measurements | Pending | The pilot range transfer is not a full network measurement. |
| Docker availability and smoke test | Pending | Not recorded. |

## Rights

- Geometry rights and scraped narrative text rights are separate. Do not read one as covering the other.
- The geometry rights statement is pending.
- This draft does not say that the Open Database License (ODbL) covers scraped narrative text.
- This draft makes no blanket claim about text rights. Text rights for Description and Wiki text are pending.
- The Website source contains scraped text. Redistribution evidence for that text is explicitly missing.

## Evidence

- Source catalog: `src/atlas_domain/source_catalog.py`
- Audit log: `.audit/georeset-atlas.tsv`
