"""Unit tests for gold kpi_commune aggregation."""
import pandas as pd

from immolake_transform_daily import _build_kpi_commune


def test_build_kpi_commune():
    fact = pd.DataFrame(
        {
            "code_insee": ["75101"] * 4,
            "etiquette": ["A", "B", "F", "G"],
            "prix_m2": [12000, 10000, 8000, 6000],
        }
    )
    dim_dpe = pd.DataFrame(
        {
            "etiquette": ["A", "B", "C", "D", "E", "F", "G"],
            "label_passoire": [False, False, False, False, False, True, True],
        }
    )

    kpi = _build_kpi_commune(fact, dim_dpe, "2026-06-17")
    row = kpi.iloc[0]

    assert row["code_insee"] == "75101"
    assert row["nb_transactions"] == 4
    assert row["prix_m2_median"] == 9000           # médiane de [12000,10000,8000,6000]
    assert row["pct_passoires"] == 50.0            # F et G sur 4 biens
    # décote = 100 * (moy F/G 7000 - moy A/B 11000) / 11000
    assert round(float(row["decote_passoire_pct"]), 2) == -36.36
