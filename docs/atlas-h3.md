# H3 map artifacts

`atlas-h3` builds resolution-3 H3 aggregates and SVG maps from a complete assembly.
It reads the assembly and writes a separate output directory.

Status: implemented, with tests. The tests use synthetic assemblies. No maps have been built from a full Atlas assembly, and no run of `atlas-h3` on a real assembly is recorded.

## Run the builder

```sh
atlas-h3 --assembly-dir /path/to/atlas-v0 --output-dir /path/to/atlas-h3 \
  --work-dir /path/to/atlas-h3-work
```

The command prints the path to `atlas-h3-manifest.json` when the build succeeds.
On an input or output error, it prints `atlas-h3:` and the error to standard
error, then exits with status 1. A failed build does not create or change the
output directory.

The `--work-dir` option is optional. The output directory must not overlap the
assembly directory. The builder stages all files beside the target directory
and publishes them together. A repeated build reuses an existing directory only
when its manifest and file hashes match.

## SQLite work directory

Pass `--work-dir` to choose where the builder stores `atlas-h3.sqlite3`. The path
must be new or empty. It must not overlap or be inside the assembly or output
directory. The builder leaves the database in place after success or failure so
you can inspect its state before restarting. Use a new empty work directory for
each run.

If you omit `--work-dir`, the builder creates a unique sibling directory beside
the output target. Its name starts with `.<output-name>.work-`. The database
stays on the output volume instead of using the Mac's internal temporary
directory. You can find it by listing matching sibling directories.

The database stores source-qualified label occurrences and polygon locations,
every text-to-polygon association occurrence, per-cell uniqueness keys, and cell
totals. Every key includes the source ID, so the same polygon or text-unit ID in
two sources stays two records. The builder reads at most 65,536 Arrow rows per
batch and limits the SQLite page cache to 32 MiB. Scratch space grows with the
distinct label and polygon keys, association occurrences, and indexes. For the
expected 121.7 million label rows and a comparable number of associations, plan
for tens of GB of scratch space. The tens of GB figure is an estimate. No full
run has measured the database size. Key lengths and duplicate rates change the
actual size.

## Input checks

The assembly must contain `assembly-plan.json`, `assembly-progress.json`, and
`assembly-manifest.json`. Both completion files must report `complete`, their
plan hashes must match, and the plan hash must verify against the plan contents.
The builder verifies every partition output's SHA-256, byte size, and Parquet row
count. It also reconciles source status counts, total output bytes, and output row
counts with the assembly manifests. The builder also counts each source's label occurrences in the `labels` tables and compares the totals with the partition manifests.

The builder reads `labels`, `text_units`, and `polygon_records`. Polygon geometry
can be WKB or GeoJSON in CRS84. A valid Polygon or MultiPolygon maps to the H3
resolution-3 cell that contains its Shapely `representative_point()`. The builder
treats an empty geometry, another geometry type, or a geometry that fails Shapely's
validity check as invalid. Missing geometry, invalid geometry, and associations
without a polygon record are reported separately in the manifest.

## Aggregate fields

Each cell row reports these count fields.

- `distinct_yes_polygons`: polygons in the cell with a valid link to a text unit that has a `yes` label occurrence.
- `association_occurrences`: valid text-to-polygon link rows in the cell.
- `distinct_text_units`: text units with a valid link to a polygon in the cell.
- `yes_label_count`, `no_label_count`, `failed_label_count`, and `skipped_label_count`: label occurrences counted in the cell.

The other fields are derived, as described below.

Missing and invalid geometries do not create cell rows or zero-valued map
metrics. Their polygon counts and association occurrences appear in
`geometry_quality` in the manifest. Unmatched polygon associations have a
separate occurrence count and also do not create cell rows.

### Label occurrences and text units

A label occurrence is one row in a source's `labels` table. Each label occurrence
counts once in each cell that its text unit touches. A text unit touches a cell
when at least one of its valid polygon links falls in that cell. Two links into
one cell still count the label occurrence once in that cell.

Duplicate and conflicting labels stay separate. Each label occurrence adds to its
own status count. A `yes` occurrence and a `no` occurrence for the same text unit
both count.

For example, one text unit has two `yes` label occurrences. It has three valid
links, two to polygon `p-1` and one to polygon `p-2`, all in one cell. That cell
gets 2 in `yes_label_count`, 3 in `association_occurrences`, 1 in
`distinct_text_units`, and 2 in `distinct_yes_polygons`.

Each count uses its own unit. `distinct_text_units` counts each text unit once per
cell. `distinct_yes_polygons` counts each polygon once per cell.
`association_occurrences` counts every valid link row, so a repeated link adds
again.

### Totals across cells

Each polygon has one cell. Summing `association_occurrences` and
`distinct_yes_polygons` over a source's cells gives that source's total. A text
unit can touch several cells, and a label occurrence counts in each of them.
Summing `distinct_text_units` or the label counts over cells therefore counts
those items more than once.

The global row adds the source rows cell by cell. Keys stay source-local, so the
same ID in two sources counts twice in the global row.

The manifest's `label_reconciliation` counts every label occurrence by status.
Cell label counts cover only occurrences whose text unit has a valid link. Cell
totals can be lower than `label_reconciliation`.

### Ratios and densities

`yes_share_denominator` is `yes_label_count + no_label_count`. `yes_share` is
`yes_label_count / yes_share_denominator`. Failed and skipped label occurrences do
not enter the denominator. A cell with no `yes` or `no` label occurrence has a null
`yes_share`.

`cell_area_km2` comes from H3's cell area calculation in km². `distinct_text_units_per_km2`
is `distinct_text_units / cell_area_km2`.

## Output files

The builder writes one Parquet aggregate for the global view and one for each
source under `aggregates/`. It writes four SVG maps for each view under `maps/`:
yes share, density, distinct yes polygons, and association occurrences.

The yes share map uses a fixed scale from 0 to 1. Cells with no `yes` or `no`
label occurrence use a separate gray fill on that map. The density map uses a
logarithmic scale and maps `log1p(distinct_text_units_per_km2)` to color. The
other two maps scale linearly from 0 to the largest cell value. All maps use the
Cividis palette on a neutral `#f3f2ee` background.

The manifest, `atlas-h3-manifest.json`, records the assembly plan hash, the
resolution, and the CRS. It lists the SHA-256 and byte size of every Parquet and
SVG file. It also records `geometry_quality` and `label_reconciliation`.
