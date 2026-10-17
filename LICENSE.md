# GeoReSeT Atlas component-license notice

GeoReSeT Atlas contains components with different inherited terms. This notice identifies the source-declared terms and the components they cover. It is not a new Atlas license and does not grant permissions that a source has not granted.

The Hub repository card should use `license: other`, `license_name: georeset-atlas-component-licenses`, and `license_link: LICENSE.md`. Do not describe the complete merged dataset as ODbL.

## OSM-derived geometry and labeled-source tables

The three pinned input cards declare ODbL for their OSM-derived geometry and labeled-source datasets. The declared component is ODbL 1.0. Keep the attribution and license notices with that component. The ODbL text distinguishes database rights from rights in individual contents, so this notice does not extend ODbL to text that the Website source card excludes.

- Official [Open Database License 1.0 text](https://opendatacommons.org/licenses/odbl/1-0/).
- Required attribution: © OpenStreetMap contributors. See the [OpenStreetMap copyright and license page](https://www.openstreetmap.org/copyright).

## Wikidata structured facts

The pinned Wiki source describes its Wikidata structured-fact component as CC0 1.0 Universal. This component is separate from Wikipedia and Wikivoyage sentence text. Atlas label rows refer to the Wiki sentence sources, not Wikidata facts.

- Official [CC0 1.0 Universal legal code](https://creativecommons.org/publicdomain/zero/1.0/legalcode.en).

## Wikipedia and Wikivoyage text

The pinned Wiki source identifies Wikipedia and Wikivoyage text under Creative Commons Attribution-ShareAlike terms. Keep the source document's exact `license` and `attribution` values with its document record. Do not replace per-document versions with a single assumed version. The completed export still needs a readback audit of these fields before preservation is claimed.

- Official [CC BY-SA 4.0 legal code](https://creativecommons.org/licenses/by-sa/4.0/legalcode.en).
- Official [CC BY-SA 3.0 legal code](https://creativecommons.org/licenses/by-sa/3.0/legalcode).

## Website text

The pinned Website source card says the website text is not covered by ODbL. Rights remain with the original sites, and no general site permission is recorded. Atlas retains the source URL for attribution and traceability. Atlas grants no license to this text and does not label it ODbL.

Public release of Website text remains blocked while source-site redistribution rights are unresolved. Do not upload the text until the applicable rights basis is documented and approved. A source URL supports traceability but does not grant redistribution rights.

## Source evidence and release notice

The immutable source revisions, component scopes, expected fields, attribution, and unresolved Website text status are listed in [component-rights.json](component-rights.json) and [NOTICE.md](NOTICE.md). Source pins and lineage are recorded in [Methods](docs/methods.md). The source-by-source release decision is recorded in [Release status](docs/release-status.md).
