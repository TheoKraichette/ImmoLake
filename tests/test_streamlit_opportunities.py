import pandas as pd

from streamlit_app.lib.filter_state import Filters
from streamlit_app.lib import queries


def test_get_opportunities_fallback_scores_and_labels(monkeypatch):
    sample = pd.DataFrame(
        {
            "commune": ["A", "B"],
            "code_insee": ["1", "2"],
            "departement": ["01", "01"],
            "region": ["84", "84"],
            "type_bien": ["appartement", "maison"],
            "etiquette": ["F", "D"],
            "prix_m2": [2000, 3000],
            "surface": [50, 70],
            "nb_dpe": [40, 50],
            "pct_passoires": [25.0, 5.0],
            "conso_energie_med": [300.0, 180.0],
            "indice_sous_cotation": [-12.0, 4.0],
            "z": [-1.4, 0.2],
            "score_opportunite": [pd.NA, pd.NA],
            "latitude": [45.0, 46.0],
            "longitude": [2.0, 3.0],
            "geometry_json": [None, None],
        }
    )
    monkeypatch.setattr(queries, "_run_query", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(queries, "get_market_data", lambda filters: sample)

    opportunities = queries.get_opportunities(Filters(opportunite_k=1.0, nb_dpe_min=1))

    assert opportunities.iloc[0]["commune"] == "A"
    assert opportunities.iloc[0]["score_opportunite"] > 0
    assert opportunities.iloc[0]["etiquette_opportunite"] == "sous-cotee + parc passoires"


def test_get_comparison_data_filters_selected_communes(monkeypatch):
    sample = pd.DataFrame({"commune": ["A", "B"], "prix_m2": [1, 2]})
    monkeypatch.setattr(queries, "get_market_data", lambda filters: sample)

    comparison = queries.get_comparison_data(("B",), Filters())

    assert comparison["commune"].tolist() == ["B"]
