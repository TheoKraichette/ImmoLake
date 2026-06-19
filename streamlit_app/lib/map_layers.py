"""Map data preparation and PyDeck layer helpers."""
from __future__ import annotations

import json
from typing import Any

import pandas as pd


def metric_range(values: pd.Series) -> tuple[float, float]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return 0.0, 1.0
    low = float(numeric.min())
    high = float(numeric.max())
    if low == high:
        return low, low + 1.0
    return low, high


def metric_color(value: object, low: float, high: float, *, palette: str) -> list[int]:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return [160, 166, 176, 150]
    ratio = max(0.0, min(1.0, (float(numeric) - low) / (high - low)))
    if palette == "passoires":
        start, end = (43, 131, 186), (215, 48, 39)
    else:
        start, end = (69, 117, 180), (244, 109, 67)
    return [round(start[i] + (end[i] - start[i]) * ratio) for i in range(3)] + [185]


def build_geojson_features(df: pd.DataFrame, metric: str) -> list[dict[str, Any]]:
    low, high = metric_range(df[metric])
    palette = "passoires" if metric == "pct_passoires" else "prix"
    features = []
    for row in df.itertuples(index=False):
        geometry_json = getattr(row, "geometry_json", None)
        # geometry_json peut être None / pd.NA (centroïdes sans contour) : tester `not x` sur pd.NA
        # lève "boolean value of NA is ambiguous" -> on ne garde que les vraies chaînes JSON.
        if not isinstance(geometry_json, str) or not geometry_json:
            continue
        try:
            geometry = json.loads(geometry_json)
        except (TypeError, json.JSONDecodeError):
            continue
        properties = row._asdict()
        properties["fill_color"] = metric_color(properties.get(metric), low, high, palette=palette)
        features.append({"type": "Feature", "geometry": geometry, "properties": properties})
    return features


def build_map_layers(df: pd.DataFrame, metric: str):
    import pydeck as pdk

    features = build_geojson_features(df, metric)
    if features:
        return [
            pdk.Layer(
                "GeoJsonLayer",
                data={"type": "FeatureCollection", "features": features},
                get_fill_color="properties.fill_color",
                get_line_color=[255, 255, 255, 180],
                line_width_min_pixels=1,
                pickable=True,
                auto_highlight=True,
            )
        ]

    low, high = metric_range(df[metric])
    palette = "passoires" if metric == "pct_passoires" else "prix"
    point_df = df.copy()
    point_df["fill_color"] = point_df[metric].map(lambda value: metric_color(value, low, high, palette=palette))
    return [
        pdk.Layer(
            "ScatterplotLayer",
            data=point_df,
            get_position="[longitude, latitude]",
            get_radius=9500,
            get_fill_color="fill_color",
            get_line_color=[255, 255, 255],
            line_width_min_pixels=1,
            pickable=True,
            auto_highlight=True,
        )
    ]


def initial_view_state(df: pd.DataFrame):
    import pydeck as pdk

    if df.empty:
        return pdk.ViewState(latitude=46.8, longitude=2.4, zoom=5)
    return pdk.ViewState(
        latitude=float(df["latitude"].mean()),
        longitude=float(df["longitude"].mean()),
        zoom=5,
        min_zoom=4,
        max_zoom=12,
    )
