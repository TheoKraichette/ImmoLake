import pandas as pd

from streamlit_app.lib.map_layers import build_geojson_features, metric_color, metric_range


def test_metric_color_returns_rgba():
    color = metric_color(50, 0, 100, palette="passoires")

    assert len(color) == 4
    assert all(0 <= channel <= 255 for channel in color)


def test_build_geojson_features_keeps_properties():
    df = pd.DataFrame(
        {
            "commune": ["Bordeaux"],
            "prix_m2": [4500],
            "pct_passoires": [12.5],
            "nb_dpe": [42],
            "latitude": [44.8],
            "longitude": [-0.57],
            "geometry_json": ['{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}'],
        }
    )

    features = build_geojson_features(df, "prix_m2")

    assert features[0]["properties"]["commune"] == "Bordeaux"
    assert features[0]["properties"]["fill_color"]


def test_metric_range_handles_constant_series():
    assert metric_range(pd.Series([3, 3])) == (3.0, 4.0)
