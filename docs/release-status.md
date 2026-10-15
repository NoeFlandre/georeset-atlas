# Release status

**Status: NOT RELEASE-READY.** The dataset is not release-ready yet. This page is a draft.

A gate is a required input or check. Partially evidenced gates still need their remaining checks. The table below records current evidence and blockers.

## Current state

| Area | State |
| --- | --- |
| Source pins | Frozen in the source catalog; upstream HF lineage linked in Methods |
| Test suite | The current work in progress passed 519 tests with 16 existing pytest-bdd deprecation warnings. Ruff, ty, and an offline `uv lock` check also passed. This is not the final gauntlet. |
| Pilot joins | Done for Afghanistan only |
| Full assembly | Not recorded as done |
| Language crosswalk asset | Generated as `atlas-language-crosswalk-1.0.0` with 184 ISO 639-1 codes, 7,922 ISO 639-3 codes, and 183 ISO 639-3 to ISO 639-1 mappings. Joined text-unit code integration is implemented in the current work in progress. Global output and inventory reconciliation are pending. See [Language crosswalk](methods.md#language-crosswalk). |
| H3 map builder | Implemented with tests. No maps have been built from a full Atlas assembly. See [H3 map artifacts](atlas-h3.md). |
| Release | Not release-ready |

## Pending gates

| Gate | State | Detail |
| --- | --- | --- |
| Full build and reconciliation | Pending | Full assembly is not recorded as done. Catalog counts are not checked against full sources. Release blocker. |
| Architecture and dependency-cycle checks | Pending | No automated dependency or import-cycle check is recorded. |
| CRAP `< 6` and complexity | Pending | Read-only Radon baseline has functions up to D25/C15; no function-level CRAP coverage report is recorded. |
| Mutation testing | Pending | No mutation report is recorded for the release candidate. |
| Acceptance and executable smoke tests | Pending | Gherkin coverage exists in the suite, but the exact final gauntlet and real CLI/Docker smoke have not run. |
| Historical model and prompt reference | Partially evidenced | Frozen label cards identify the model revisions and benchmark prompt; the public prompt file is pinned and hashed in Methods. The generation rows do not carry a per-run prompt hash, and runtime/code versions remain unknown. |
| Upstream tools and versions | Partially evidenced | Description Lingua is 2.2.0; Website names GlotLID v3 and SaT `sat-3l-sm`; Description splitter, Wiki detector/splitter, Website GlotLID revision, and runtime versions are not recorded. |
| Row-level language provenance | Partially evidenced | Description `reason` identifies the detector in the Afghanistan pilot only. Website GlotLID and Wiki Wikimedia field provenance are encoded. Global audit remains pending. |
| Crosswalk output integration | Pending | Joined text-unit code integration is implemented in the current work in progress. No global assembled output or inventory reconciliation exists. |
| Final language inventory | Pending | The full source language inventory is not reconciled, including the Wiki inventory. The 397 distinct Website codes are a pinned aggregate inventory, not final full-output coverage. |
| Website scraped-text redistribution basis | Pending | Redistribution evidence is explicitly missing. Release blocker. |
| Geometry rights statement | Pending | Separate from text rights. |
| Text rights and required attribution | Pending | Frozen input cards distinguish ODbL OSM material, CC0 Wikidata, and CC BY-SA 4.0 Wikipedia/Wikivoyage text. Atlas attribution/share-alike compliance still needs review. No blanket rights claim is made. |
| Representative point implementation | Partially evidenced | The H3 builder uses Shapely `representative_point()` in CRS84, with a unit test. Full-assembly use is not recorded. |
| Maps and metrics | Pending | The H3 builder and its tests exist. No full Atlas assembly maps have been built, and metrics are not reconciled against a full assembly. |
| Assembly CPU, RAM, storage, and network measurements | Pending | The pilot range transfer is not a full network measurement. |
| Docker route and smoke test | Blocked | Docker 29.2.1 client is installed, but the checkout has no Dockerfile or Compose file and the Engine API is unavailable on the default, Docker Desktop, and OrbStack contexts. No container build or smoke test has run. |

## Rights

- Geometry rights and scraped narrative text rights are separate. Do not read one as covering the other.
- The geometry rights statement is pending.
- This draft does not say that the Open Database License (ODbL) covers scraped narrative text.
- The frozen Description input card identifies its OSM-derived data as ODbL. The frozen Wiki input card identifies OSM polygons as ODbL, Wikidata as CC0, and Wikipedia/Wikivoyage text as CC BY-SA 4.0. Atlas must carry the required attribution and comply with share-alike terms; the release review of those obligations is pending.
- The frozen Website input card explicitly states that the website text is not covered by ODbL, is third-party content, and remains subject to each source site's rights. No source-site permission or general redistribution basis is recorded. Website text blocks public upload of the complete joined Atlas until this is resolved. Geometry and text rights remain separate.

## Evidence

- Source catalog: `src/atlas_domain/source_catalog.py`
- Audit log: `.audit/georeset-atlas.tsv`
- Language crosswalk asset: `src/atlas_domain/language-crosswalk-1.0.0.json`
- H3 builder and tests: `src/atlas_h3/` and `tests/test_atlas_h3.py`
