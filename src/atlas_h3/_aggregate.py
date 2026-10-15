from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import h3
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import MultiPolygon, Polygon, shape

from atlas_h3._assembly import STATUSES, PartitionInput, VerifiedAssembly
from atlas_h3._errors import AtlasH3InputError

RESOLUTION = 3
_BATCH_ROWS = 65_536
_SQL_BATCH_ROWS = 4_096
_SQLITE_CACHE_KIB = 32_768
_SQLITE_CACHE_PAGES = 8_192
_COUNT_FIELDS = (
    "distinct_yes_polygons",
    "association_occurrences",
    "distinct_text_units",
    "yes_label_count",
    "no_label_count",
    "failed_label_count",
    "skipped_label_count",
)
_COUNT_INDEX = {field: index for index, field in enumerate(_COUNT_FIELDS)}
_STATUS_COUNT_FIELDS = {status: f"{status}_label_count" for status in STATUSES}
_QUALITY_FIELDS = (
    "polygon_count",
    "valid_polygon_count",
    "missing_polygon_count",
    "invalid_polygon_count",
    "missing_geometry_association_occurrences",
    "invalid_geometry_association_occurrences",
    "unmatched_polygon_association_occurrences",
)


@dataclass(frozen=True, slots=True)
class PolygonLocation:
    state: Literal["valid", "missing", "invalid"]
    cell: str | None = None


@dataclass(frozen=True, slots=True)
class AggregatedMaps:
    rows_by_view: dict[str, list[dict[str, Any]]]
    geometry_quality: dict[str, dict[str, int]]
    label_reconciliation: dict[str, dict[str, int]]


def aggregate_maps(
    assembly: VerifiedAssembly, work_database: str | Path
) -> AggregatedMaps:
    if "global" in assembly.source_ids:
        raise AtlasH3InputError("source_id 'global' is reserved for the global view")
    partitions_by_source: dict[str, list[PartitionInput]] = {
        source_id: [] for source_id in assembly.source_ids
    }
    for partition in assembly.partitions:
        partitions_by_source[partition.source_id].append(partition)

    connection = sqlite3.connect(work_database)
    try:
        _configure_database(connection)
        _create_schema(connection)
        source_rows: dict[str, list[dict[str, Any]]] = {}
        source_quality: dict[str, dict[str, int]] = {}
        global_counts: dict[str, dict[str, int]] = {}
        global_status_counts = {status: 0 for status in STATUSES}

        for source_id in assembly.source_ids:
            partitions = partitions_by_source[source_id]
            label_counts = _load_labels(connection, source_id, partitions)
            if label_counts != assembly.status_counts[source_id]:
                raise AtlasH3InputError(
                    f"source {source_id} label table does not match the "
                    "assembly manifest"
                )
            _load_polygon_locations(connection, source_id, partitions)
            _load_associations(connection, source_id, partitions)
            source_quality[source_id] = _quality_counts(connection, source_id)
            source_rows[source_id] = _source_rows(connection, source_id, global_counts)
            for status, count in label_counts.items():
                global_status_counts[status] += count

        global_quality = {
            field: sum(source[field] for source in source_quality.values())
            for field in _QUALITY_FIELDS
        }
        rows_by_view = {"global": _rows_from_counts(global_counts)}
        rows_by_view.update(source_rows)
        geometry_quality = {"global": global_quality, **source_quality}
        label_reconciliation = {"global": global_status_counts}
        label_reconciliation.update(
            {
                source_id: dict(assembly.status_counts[source_id])
                for source_id in assembly.source_ids
            }
        )
        return AggregatedMaps(rows_by_view, geometry_quality, label_reconciliation)
    finally:
        connection.close()


def _configure_database(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA page_size = 4096")
    connection.execute(f"PRAGMA default_cache_size = {_SQLITE_CACHE_PAGES}")
    connection.execute(f"PRAGMA cache_size = -{_SQLITE_CACHE_KIB}")
    connection.execute("PRAGMA mmap_size = 0")
    connection.execute("PRAGMA temp_store = FILE")
    connection.execute("PRAGMA journal_mode = DELETE")


def _create_schema(connection: sqlite3.Connection) -> None:
    statuses = ", ".join(f"'{status}'" for status in STATUSES)
    connection.executescript(
        f"""
        CREATE TABLE labels (
            source_id TEXT NOT NULL,
            label_id INTEGER NOT NULL,
            text_unit_key TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ({statuses})),
            PRIMARY KEY (source_id, label_id)
        ) WITHOUT ROWID;

        CREATE INDEX labels_unit
            ON labels (source_id, text_unit_key);

        CREATE TABLE polygon_locations (
            source_id TEXT NOT NULL,
            polygon_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('valid', 'missing', 'invalid')),
            h3_cell TEXT,
            CHECK ((state = 'valid' AND h3_cell IS NOT NULL) OR
                   (state != 'valid' AND h3_cell IS NULL)),
            PRIMARY KEY (source_id, polygon_id)
        ) WITHOUT ROWID;

        CREATE TABLE polygon_batch (
            source_id TEXT NOT NULL,
            polygon_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('valid', 'missing', 'invalid')),
            h3_cell TEXT,
            mapping_key TEXT NOT NULL,
            PRIMARY KEY (source_id, polygon_id, mapping_key)
        ) WITHOUT ROWID;

        CREATE TABLE association_batch (
            source_id TEXT NOT NULL,
            association_id INTEGER NOT NULL,
            text_unit_key TEXT NOT NULL,
            polygon_id TEXT NOT NULL,
            PRIMARY KEY (source_id, association_id)
        ) WITHOUT ROWID;

        CREATE TABLE text_polygon_associations (
            source_id TEXT NOT NULL,
            association_id INTEGER NOT NULL,
            text_unit_key TEXT NOT NULL,
            polygon_id TEXT NOT NULL,
            location_state TEXT NOT NULL
                CHECK (location_state IN ('valid', 'missing', 'invalid', 'unmatched')),
            h3_cell TEXT,
            CHECK ((location_state = 'valid' AND h3_cell IS NOT NULL) OR
                   (location_state != 'valid' AND h3_cell IS NULL)),
            PRIMARY KEY (source_id, association_id)
        ) WITHOUT ROWID;

        CREATE INDEX text_polygon_associations_polygon
            ON text_polygon_associations (source_id, polygon_id);
        CREATE INDEX text_polygon_associations_unit
            ON text_polygon_associations (source_id, text_unit_key);

        CREATE TABLE cell_text_units (
            source_id TEXT NOT NULL,
            h3_cell TEXT NOT NULL,
            text_unit_key TEXT NOT NULL,
            PRIMARY KEY (source_id, h3_cell, text_unit_key)
        ) WITHOUT ROWID;

        CREATE TABLE cell_label_occurrences (
            source_id TEXT NOT NULL,
            h3_cell TEXT NOT NULL,
            label_id INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ({statuses})),
            PRIMARY KEY (source_id, h3_cell, label_id)
        ) WITHOUT ROWID;

        CREATE TABLE cell_yes_polygons (
            source_id TEXT NOT NULL,
            h3_cell TEXT NOT NULL,
            polygon_id TEXT NOT NULL,
            PRIMARY KEY (source_id, h3_cell, polygon_id)
        ) WITHOUT ROWID;

        CREATE TABLE cell_counts (
            source_id TEXT NOT NULL,
            h3_cell TEXT NOT NULL,
            distinct_yes_polygons INTEGER NOT NULL,
            association_occurrences INTEGER NOT NULL,
            distinct_text_units INTEGER NOT NULL,
            yes_label_count INTEGER NOT NULL,
            no_label_count INTEGER NOT NULL,
            failed_label_count INTEGER NOT NULL,
            skipped_label_count INTEGER NOT NULL,
            PRIMARY KEY (source_id, h3_cell)
        ) WITHOUT ROWID;
        """
    )


def _load_labels(
    connection: sqlite3.Connection,
    source_id: str,
    partitions: list[PartitionInput],
) -> dict[str, int]:
    counts = {status: 0 for status in STATUSES}
    label_id = 0
    for partition in partitions:
        parquet_file = pq.ParquetFile(partition.tables["labels"])
        for batch in _iter_batches(parquet_file, ("text_unit_key", "status")):
            unit_keys = batch.column(0)
            statuses = batch.column(1)
            batch_rows: list[tuple[str, int, str, str]] = []
            for row_index in range(batch.num_rows):
                unit_key = _required_string(
                    unit_keys[row_index].as_py(), "text_unit_key"
                )
                status = statuses[row_index].as_py()
                if status not in STATUSES:
                    raise AtlasH3InputError(
                        f"source {source_id} contains an unknown label status"
                    )
                label_id += 1
                counts[status] += 1
                batch_rows.append((source_id, label_id, unit_key, status))
            with connection:
                connection.executemany(
                    "INSERT INTO labels VALUES (?, ?, ?, ?)", batch_rows
                )
    return counts


def _load_polygon_locations(
    connection: sqlite3.Connection,
    source_id: str,
    partitions: list[PartitionInput],
) -> None:
    for partition in partitions:
        parquet_file = pq.ParquetFile(partition.tables["polygon_records"])
        names = set(parquet_file.schema_arrow.names)
        geometry_field = next(
            (name for name in ("geometry_wkb", "geometry") if name in names), None
        )
        columns = ("polygon_id",) + ((geometry_field,) if geometry_field else ())
        for batch in _iter_batches(parquet_file, columns):
            polygon_ids = batch.column(0)
            geometries = batch.column(1) if geometry_field else None
            rows: list[tuple[str, str, str, str | None, str]] = []
            for row_index in range(batch.num_rows):
                polygon_id = _required_string(
                    polygon_ids[row_index].as_py(), "polygon_id"
                )
                geometry = (
                    geometries[row_index].as_py() if geometries is not None else None
                )
                location = _polygon_location(geometry)
                mapping_key = (
                    f"{location.state}:{location.cell}"
                    if location.cell is not None
                    else location.state
                )
                rows.append(
                    (
                        source_id,
                        polygon_id,
                        location.state,
                        location.cell,
                        mapping_key,
                    )
                )
            _store_polygon_batch(connection, source_id, rows)


def _store_polygon_batch(
    connection: sqlite3.Connection,
    source_id: str,
    rows: list[tuple[str, str, str, str | None, str]],
) -> None:
    if not rows:
        return
    with connection:
        connection.executemany(
            "INSERT OR IGNORE INTO polygon_batch VALUES (?, ?, ?, ?, ?)", rows
        )
        conflict = connection.execute(
            """
            SELECT polygon_id FROM polygon_batch
            WHERE source_id = ?
            GROUP BY source_id, polygon_id
            HAVING COUNT(*) > 1
            LIMIT 1
            """,
            (source_id,),
        ).fetchone()
        if conflict is None:
            conflict = connection.execute(
                """
                SELECT batch.polygon_id
                FROM polygon_batch AS batch
                JOIN polygon_locations AS existing
                  ON existing.source_id = batch.source_id
                 AND existing.polygon_id = batch.polygon_id
                WHERE batch.source_id = ?
                  AND (existing.state != batch.state OR
                       existing.h3_cell IS NOT batch.h3_cell)
                LIMIT 1
                """,
                (source_id,),
            ).fetchone()
        if conflict is not None:
            raise AtlasH3InputError(
                f"source {source_id} has conflicting geometry for polygon {conflict[0]}"
            )
        connection.execute(
            """
            INSERT OR IGNORE INTO polygon_locations
                (source_id, polygon_id, state, h3_cell)
            SELECT source_id, polygon_id, state, h3_cell
            FROM polygon_batch WHERE source_id = ?
            """,
            (source_id,),
        )
        connection.execute(
            "DELETE FROM polygon_batch WHERE source_id = ?", (source_id,)
        )


def _load_associations(
    connection: sqlite3.Connection,
    source_id: str,
    partitions: list[PartitionInput],
) -> None:
    association_id = 0
    association_batch: list[tuple[str, int, str, str]] = []
    for partition in partitions:
        parquet_file = pq.ParquetFile(partition.tables["text_units"])
        for batch in _iter_batches(parquet_file, ("text_unit_key", "polygon_ids")):
            unit_keys = batch.column(0)
            polygon_lists = batch.column(1)
            for row_index in range(batch.num_rows):
                unit_key = _required_string(
                    unit_keys[row_index].as_py(), "text_unit_key"
                )
                polygon_list = polygon_lists[row_index]
                if not polygon_list.is_valid:
                    continue
                if not (
                    pa.types.is_list(polygon_list.type)
                    or pa.types.is_large_list(polygon_list.type)
                    or pa.types.is_fixed_size_list(polygon_list.type)
                ):
                    raise AtlasH3InputError(
                        f"source {source_id} has malformed polygon_ids for {unit_key}"
                    )
                for polygon_scalar in polygon_list.values:
                    polygon_id = _required_string(
                        polygon_scalar.as_py(), "polygon_id association"
                    )
                    association_id += 1
                    association_batch.append(
                        (source_id, association_id, unit_key, polygon_id)
                    )
                    if len(association_batch) == _SQL_BATCH_ROWS:
                        _flush_association_batch(connection, association_batch)
                        association_batch.clear()
    _flush_association_batch(connection, association_batch)


def _flush_association_batch(
    connection: sqlite3.Connection,
    rows: list[tuple[str, int, str, str]],
) -> None:
    if not rows:
        return
    with connection:
        connection.executemany(
            "INSERT INTO association_batch VALUES (?, ?, ?, ?)", rows
        )
        connection.execute(
            """
            INSERT INTO text_polygon_associations
                (source_id, association_id, text_unit_key, polygon_id,
                 location_state, h3_cell)
            SELECT batch.source_id, batch.association_id, batch.text_unit_key,
                   batch.polygon_id, COALESCE(location.state, 'unmatched'),
                   location.h3_cell
            FROM association_batch AS batch
            LEFT JOIN polygon_locations AS location
              ON location.source_id = batch.source_id
             AND location.polygon_id = batch.polygon_id
            """
        )

        deltas: dict[str, list[int]] = {}
        for cell, count in connection.execute(
            """
            SELECT association.h3_cell, COUNT(*)
            FROM text_polygon_associations AS association
            JOIN association_batch AS batch
              ON batch.source_id = association.source_id
             AND batch.association_id = association.association_id
            WHERE association.location_state = 'valid'
            GROUP BY association.h3_cell
            """
        ):
            deltas.setdefault(cell, [0] * len(_COUNT_FIELDS))[
                _COUNT_INDEX["association_occurrences"]
            ] += count

        for (cell,) in connection.execute(
            """
            INSERT OR IGNORE INTO cell_text_units
                (source_id, h3_cell, text_unit_key)
            SELECT association.source_id, association.h3_cell,
                   association.text_unit_key
            FROM text_polygon_associations AS association
            JOIN association_batch AS batch
              ON batch.source_id = association.source_id
             AND batch.association_id = association.association_id
            WHERE association.location_state = 'valid'
            RETURNING h3_cell
            """
        ):
            counts = deltas.setdefault(cell, [0] * len(_COUNT_FIELDS))
            counts[_COUNT_INDEX["distinct_text_units"]] += 1

        for cell, status in connection.execute(
            """
            INSERT OR IGNORE INTO cell_label_occurrences
                (source_id, h3_cell, label_id, status)
            SELECT pair.source_id, pair.h3_cell, labels.label_id, labels.status
            FROM (
                SELECT DISTINCT association.source_id, association.h3_cell,
                       association.text_unit_key
                FROM text_polygon_associations AS association
                JOIN association_batch AS batch
                  ON batch.source_id = association.source_id
                 AND batch.association_id = association.association_id
                WHERE association.location_state = 'valid'
            ) AS pair
            JOIN labels
              ON labels.source_id = pair.source_id
             AND labels.text_unit_key = pair.text_unit_key
            RETURNING h3_cell, status
            """
        ):
            counts = deltas.setdefault(cell, [0] * len(_COUNT_FIELDS))
            counts[_COUNT_INDEX[_STATUS_COUNT_FIELDS[status]]] += 1

        for (cell,) in connection.execute(
            """
            INSERT OR IGNORE INTO cell_yes_polygons
                (source_id, h3_cell, polygon_id)
            SELECT association.source_id, association.h3_cell, association.polygon_id
            FROM text_polygon_associations AS association
            JOIN association_batch AS batch
              ON batch.source_id = association.source_id
             AND batch.association_id = association.association_id
            WHERE association.location_state = 'valid'
              AND EXISTS (
                  SELECT 1 FROM labels
                  WHERE labels.source_id = association.source_id
                    AND labels.text_unit_key = association.text_unit_key
                    AND labels.status = 'yes'
              )
            RETURNING h3_cell
            """
        ):
            deltas.setdefault(cell, [0] * len(_COUNT_FIELDS))[
                _COUNT_INDEX["distinct_yes_polygons"]
            ] += 1

        if deltas:
            columns = ", ".join(_COUNT_FIELDS)
            placeholders = ", ".join("?" for _ in range(len(_COUNT_FIELDS) + 2))
            updates = ", ".join(
                f"{field} = cell_counts.{field} + excluded.{field}"
                for field in _COUNT_FIELDS
            )
            connection.executemany(
                f"""
                INSERT INTO cell_counts (source_id, h3_cell, {columns})
                VALUES ({placeholders})
                ON CONFLICT (source_id, h3_cell) DO UPDATE SET {updates}
                """,
                ((rows[0][0], cell, *counts) for cell, counts in deltas.items()),
            )
        connection.execute("DELETE FROM association_batch")


def _quality_counts(connection: sqlite3.Connection, source_id: str) -> dict[str, int]:
    polygon_counts = {state: 0 for state in ("valid", "missing", "invalid")}
    for state, count in connection.execute(
        """
        SELECT state, COUNT(*) FROM polygon_locations
        WHERE source_id = ? GROUP BY state
        """,
        (source_id,),
    ):
        polygon_counts[state] = count

    association_counts = {
        state: 0 for state in ("valid", "missing", "invalid", "unmatched")
    }
    for state, count in connection.execute(
        """
        SELECT location_state, COUNT(*) FROM text_polygon_associations
        WHERE source_id = ? GROUP BY location_state
        """,
        (source_id,),
    ):
        association_counts[state] = count

    return {
        "polygon_count": sum(polygon_counts.values()),
        "valid_polygon_count": polygon_counts["valid"],
        "missing_polygon_count": polygon_counts["missing"],
        "invalid_polygon_count": polygon_counts["invalid"],
        "missing_geometry_association_occurrences": association_counts["missing"],
        "invalid_geometry_association_occurrences": association_counts["invalid"],
        "unmatched_polygon_association_occurrences": association_counts["unmatched"],
    }


def _source_rows(
    connection: sqlite3.Connection,
    source_id: str,
    global_counts: dict[str, dict[str, int]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    columns = ", ".join(_COUNT_FIELDS)
    for record in connection.execute(
        f"SELECT h3_cell, {columns} FROM cell_counts "
        "WHERE source_id = ? ORDER BY h3_cell",
        (source_id,),
    ):
        cell = record[0]
        counts = dict(zip(_COUNT_FIELDS, record[1:], strict=True))
        rows.append(_row_from_counts(cell, counts))
        global_cell = global_counts.setdefault(
            cell, {field: 0 for field in _COUNT_FIELDS}
        )
        for field in _COUNT_FIELDS:
            global_cell[field] += counts[field]
    return rows


def _rows_from_counts(
    cells: dict[str, dict[str, int]],
) -> list[dict[str, Any]]:
    return [_row_from_counts(cell, cells[cell]) for cell in sorted(cells)]


def _row_from_counts(cell: str, counts: dict[str, int]) -> dict[str, Any]:
    yes_no_denominator = counts["yes_label_count"] + counts["no_label_count"]
    area = h3.cell_area(cell, unit="km^2")
    return {
        "h3_cell": cell,
        **counts,
        "yes_share_denominator": yes_no_denominator,
        "yes_share": (
            counts["yes_label_count"] / yes_no_denominator
            if yes_no_denominator
            else None
        ),
        "cell_area_km2": area,
        "distinct_text_units_per_km2": counts["distinct_text_units"] / area,
    }


def _iter_batches(
    parquet_file: pq.ParquetFile,
    columns: tuple[str, ...],
) -> Iterator[pa.RecordBatch]:
    available = set(parquet_file.schema_arrow.names)
    if any(column not in available for column in columns):
        raise AtlasH3InputError("assembly Parquet table is missing a required column")
    yield from parquet_file.iter_batches(batch_size=_BATCH_ROWS, columns=list(columns))


def _polygon_location(value: Any) -> PolygonLocation:
    if value is None:
        return PolygonLocation("missing")
    try:
        if isinstance(value, str):
            geometry = shape(json.loads(value))
        elif isinstance(value, (bytes, bytearray, memoryview)):
            geometry = from_wkb(bytes(value))
        else:
            return PolygonLocation("invalid")
        if (
            not isinstance(geometry, (Polygon, MultiPolygon))
            or geometry.is_empty
            or not geometry.is_valid
        ):
            return PolygonLocation("invalid")
        point = geometry.representative_point()
        if (
            not math.isfinite(point.x)
            or not math.isfinite(point.y)
            or not -180 <= point.x <= 180
            or not -90 <= point.y <= 90
        ):
            return PolygonLocation("invalid")
        cell = h3.latlng_to_cell(point.y, point.x, RESOLUTION)
    except Exception:
        return PolygonLocation("invalid")
    return PolygonLocation("valid", cell)


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise AtlasH3InputError(
            f"assembly field {field_name} must be a non-empty string"
        )
    return value
