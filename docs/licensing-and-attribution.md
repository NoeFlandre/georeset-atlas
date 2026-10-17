# Licensing and attribution

**Status: mixed source terms.** This page records the license declarations and attribution fields identified in the pinned source cards. It does not grant rights or verify permissions that are absent from those sources.

## Component terms

| Component | Source-declared terms | Atlas handling |
| --- | --- | --- |
| OSM-derived geometry and labeled-source tables | ODbL 1.0, as declared by the pinned input cards | Keep ODbL attribution and notices scoped to the OSM-derived geometry and labeled-source components. |
| Wikidata structured facts | CC0 1.0 Universal | Keep this separate from the Wikipedia/Wikivoyage text units used for sentence labeling. |
| Wikipedia and Wikivoyage text | CC BY-SA terms recorded by the Wiki source; each document's `license` and `attribution` fields must stay attached | Preserve source document values without flattening versions. Verify the assembled export before claiming those fields were retained. |
| Website text and sentence copies | The Website source card explicitly says this text is not covered by ODbL. Rights remain with the original sites; no general permission is recorded. | Preserve `source_url` and source attribution. Disclose the unresolved rights status. Do not label this text ODbL or imply Atlas grants permission. |

The repository root contains `LICENSE.md`, `NOTICE.md`, and the machine-readable `component-rights.json`. These files record source-declared terms and attribution without granting new rights.

## Publication decision

The local assembly plan includes Description, Website, and Wiki. Public release of the complete dataset is blocked until the source-site redistribution basis for Website text is documented and approved. Attribution and source URLs do not clear this gate.

The public Hub card uses a component-specific `other` license name and links to `LICENSE.md`. It must state that the Website text's source-site terms are unverified and must list the independent ODbL, CC0, and CC BY-SA components. It must not present one license as governing all Atlas content.

## Attribution and export checks

- Keep © OpenStreetMap contributors with OSM-derived geometry and notices.
- Keep Wiki document `license` and `attribution` values at document granularity. Final exported values still need readback verification.
- Keep Website `source_url` values with Website text. A source URL is traceability and attribution context; it is not a redistribution grant.
- Carry source revisions, file identities, and processing history from the [source catalog](methods.md#source-pins) into the release notice.

## Official license texts

- [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/)
- [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/legalcode.en)
- [CC BY-SA 4.0 International](https://creativecommons.org/licenses/by-sa/4.0/legalcode.en)
- [CC BY-SA 3.0 Unported](https://creativecommons.org/licenses/by-sa/3.0/legalcode)
