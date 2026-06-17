"""Unit tests for raw to silver transformations."""
from io import BytesIO

import pandas as pd

from immolake_transform_daily import SILVER_COLUMNS, _clean_dpe_rows, _to_parquet_bytes


def test_clean_dpe_rows_types_and_deduplicates():
    rows = [
        {
            "numero_dpe": " DPE-1 ",
            "code_insee_ban": "11069",
            "code_postal_ban": "11000",
            "type_batiment": "Immeuble",
            "surface_habitable_immeuble": "1363,5",
            "etiquette_dpe": " c ",
            "conso_5_usages_par_m2_ep": "139",
            "date_etablissement_dpe": "2026-01-07",
        },
        {
            "numero_dpe": "DPE-1",
            "code_insee_ban": "11069",
            "code_postal_ban": "11000",
            "type_batiment": "Maison",
            "surface_habitable_logement": "95",
            "etiquette_dpe": "D",
            "conso_5_usages_par_m2_ep": "210",
            "date_etablissement_dpe": "2026-01-08",
        },
        {
            "numero_dpe": "DPE-2",
            "code_insee_ban": "",
            "etiquette_dpe": "A",
            "surface_habitable_logement": "50",
        },
    ]

    df = _clean_dpe_rows(rows, "2026-06-17")

    assert list(df.columns) == SILVER_COLUMNS
    assert len(df) == 1
    assert df.loc[0, "numero_dpe"] == "DPE-1"
    assert df.loc[0, "type_batiment"] == "maison"
    assert df.loc[0, "surface_habitable"] == 95
    assert df.loc[0, "etiquette_dpe"] == "D"
    assert df.loc[0, "conso_energie"] == 210
    assert df.loc[0, "date_etablissement"].isoformat() == "2026-01-08"


def test_to_parquet_bytes_round_trips():
    df = _clean_dpe_rows(
        [
            {
                "numero_dpe": "DPE-1",
                "code_insee_ban": "75056",
                "code_postal_brut": 75001,
                "surface_habitable_logement": 42.7,
                "etiquette_dpe": "B",
            }
        ],
        "2026-06-17",
    )

    parquet_bytes = _to_parquet_bytes(df)
    loaded = pd.read_parquet(BytesIO(parquet_bytes))

    assert loaded.loc[0, "numero_dpe"] == "DPE-1"
    assert loaded.loc[0, "code_postal"] == "75001"
