from __future__ import annotations

import html
import math
from dataclasses import dataclass
from typing import Any

import h3

_WIDTH = 1000
_HEIGHT = 700
_BACKGROUND = "#f3f2ee"
_NO_DATA = "#d7d5ce"
_CIVIDIS = ("#00204c", "#414e6b", "#7d7c78", "#bcae6c", "#fee838")


@dataclass(frozen=True, slots=True)
class MapMetric:
    file_name: str
    field_name: str
    title: str
    legend: str
    logarithmic: bool = False
    fixed_max: float | None = None


MAP_METRICS = (
    MapMetric(
        "yes_share.svg",
        "yes_share",
        "Yes share",
        "yes / (yes + no)",
        fixed_max=1.0,
    ),
    MapMetric(
        "distinct_text_units_per_km2.svg",
        "distinct_text_units_per_km2",
        "Distinct text units per km²",
        "logarithmic density · log1p(distinct text units/km²)",
        logarithmic=True,
    ),
    MapMetric(
        "distinct_yes_polygons.svg",
        "distinct_yes_polygons",
        "Distinct yes polygons",
        "distinct yes polygons",
    ),
    MapMetric(
        "association_occurrences.svg",
        "association_occurrences",
        "Polygon association occurrences",
        "polygon association occurrences",
    ),
)


def render_map(
    rows: list[dict[str, Any]],
    *,
    view_name: str,
    metric: MapMetric,
) -> str:
    values = [
        float(row[metric.field_name])
        for row in rows
        if row[metric.field_name] is not None
    ]
    color_values = [_color_value(value, metric) for value in values]
    maximum = metric.fixed_max
    if maximum is None:
        maximum = max(color_values, default=0.0)
    if maximum <= 0:
        maximum = 1.0

    boundaries = _boundaries(rows)
    projection = _projection(boundaries)
    title_text = f"{view_name} · {metric.title}"
    pieces = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{_WIDTH}" '
        f'height="{_HEIGHT}" viewBox="0 0 {_WIDTH} {_HEIGHT}" role="img" '
        f'aria-label="{html.escape(title_text, quote=True)}">',
        "<defs>",
        '<linearGradient id="cividis" x1="0%" y1="0%" x2="100%" y2="0%">',
    ]
    for index, color in enumerate(_CIVIDIS):
        offset = index / (len(_CIVIDIS) - 1) * 100
        pieces.append(f'<stop offset="{offset:.1f}%" stop-color="{color}"/>')
    pieces.extend(
        [
            "</linearGradient>",
            "</defs>",
            f'<rect width="{_WIDTH}" height="{_HEIGHT}" fill="{_BACKGROUND}"/>',
            f'<text x="38" y="35" fill="#20201d" font-size="20" '
            f'font-family="sans-serif">{html.escape(title_text)}</text>',
            '<g fill="none" stroke="#484843" stroke-width="0.8" '
            'stroke-linejoin="round">',
        ]
    )

    if boundaries and projection is not None:
        for row in rows:
            cell = row["h3_cell"]
            points = _project_ring(boundaries[cell], projection)
            value = row[metric.field_name]
            if value is None:
                fill = _NO_DATA
                value_label = "no yes/no labels"
            else:
                ratio = _color_value(float(value), metric) / maximum
                fill = _palette_color(min(1.0, max(0.0, ratio)))
                value_label = _display_value(float(value), metric)
            path_data = " ".join(
                f"{command}{x:.2f},{y:.2f}"
                for command, (x, y) in zip(("M", *(["L"] * (len(points) - 1))), points)
            )
            path_data += " Z"
            cell_title = html.escape(f"{cell}: {value_label}")
            pieces.append(
                f'<path d="{path_data}" fill="{fill}">'
                f"<title>{cell_title}</title></path>"
            )
    else:
        pieces.append(
            '<text x="500" y="300" text-anchor="middle" fill="#484843" '
            'font-size="18" font-family="sans-serif">'
            "No mapped polygon associations</text>"
        )

    pieces.extend(
        [
            "</g>",
            f'<text x="{_WIDTH / 2:.0f}" y="575" text-anchor="middle" '
            'fill="#20201d" font-size="14" font-family="sans-serif">'
            f"{html.escape(metric.legend)}</text>",
            '<rect x="330" y="590" width="340" height="16" '
            'fill="url(#cividis)" stroke="#484843" stroke-width="0.6"/>',
            f'<text x="330" y="626" fill="#484843" font-size="12" '
            f'font-family="sans-serif">{html.escape(_legend_min())}</text>',
            f'<text x="670" y="626" text-anchor="end" fill="#484843" '
            f'font-size="12" font-family="sans-serif">'
            f"{html.escape(_legend_max(metric, values))}</text>",
            '<text x="500" y="660" text-anchor="middle" fill="#484843" '
            'font-size="12" font-family="sans-serif">Cividis</text>',
            "</svg>",
        ]
    )
    return "\n".join(pieces) + "\n"


def _boundaries(rows: list[dict[str, Any]]) -> dict[str, list[tuple[float, float]]]:
    if not rows:
        return {}
    centers = [h3.cell_to_latlng(row["h3_cell"])[1] for row in rows]
    sine = sum(math.sin(math.radians(value)) for value in centers)
    cosine = sum(math.cos(math.radians(value)) for value in centers)
    center = math.degrees(math.atan2(sine, cosine)) if sine or cosine else 0.0
    boundaries: dict[str, list[tuple[float, float]]] = {}
    for row in rows:
        cell = row["h3_cell"]
        points: list[tuple[float, float]] = []
        previous_longitude: float | None = None
        for latitude, longitude in h3.cell_to_boundary(cell):
            unwrapped = center + ((longitude - center + 180) % 360) - 180
            if previous_longitude is not None:
                if unwrapped - previous_longitude > 180:
                    unwrapped -= 360
                elif unwrapped - previous_longitude < -180:
                    unwrapped += 360
            points.append((unwrapped, latitude))
            previous_longitude = unwrapped
        boundaries[cell] = points
    return boundaries


def _projection(
    boundaries: dict[str, list[tuple[float, float]]],
) -> tuple[float, float, float] | None:
    points = [point for ring in boundaries.values() for point in ring]
    if not points:
        return None
    min_x = min(point[0] for point in points)
    max_x = max(point[0] for point in points)
    min_y = min(point[1] for point in points)
    max_y = max(point[1] for point in points)
    plot_x, plot_y = 50.0, 60.0
    plot_width, plot_height = 900.0, 460.0
    x_span = max(max_x - min_x, 0.01)
    y_span = max(max_y - min_y, 0.01)
    scale = min(plot_width / x_span, plot_height / y_span)
    left = plot_x + (plot_width - x_span * scale) / 2 - min_x * scale
    top = plot_y + (plot_height - y_span * scale) / 2 + max_y * scale
    return left, top, scale


def _project_ring(
    ring: list[tuple[float, float]],
    projection: tuple[float, float, float],
) -> list[tuple[float, float]]:
    left, top, scale = projection
    return [
        (left + longitude * scale, top - latitude * scale)
        for longitude, latitude in ring
    ]


def _color_value(value: float, metric: MapMetric) -> float:
    return math.log1p(value) if metric.logarithmic else value


def _palette_color(ratio: float) -> str:
    scaled = ratio * (len(_CIVIDIS) - 1)
    index = min(int(scaled), len(_CIVIDIS) - 2)
    fraction = scaled - index
    start = _rgb(_CIVIDIS[index])
    end = _rgb(_CIVIDIS[index + 1])
    components = tuple(
        round(first + (last - first) * fraction) for first, last in zip(start, end)
    )
    return "#" + "".join(f"{component:02x}" for component in components)


def _rgb(color: str) -> tuple[int, int, int]:
    red, green, blue = (int(color[index : index + 2], 16) for index in (1, 3, 5))
    return red, green, blue


def _display_value(value: float, metric: MapMetric) -> str:
    if metric.logarithmic:
        return f"{value:.4g} text units/km²"
    if metric.field_name == "yes_share":
        return f"{value:.3f}"
    return f"{value:,.0f}"


def _legend_min() -> str:
    return "0"


def _legend_max(metric: MapMetric, values: list[float]) -> str:
    if metric.fixed_max is not None:
        return f"{metric.fixed_max:g}"
    maximum = max(values, default=0.0)
    return _display_value(maximum, metric)
