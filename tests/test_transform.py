"""Unit tests for raw to silver transformations."""
from io import BytesIO

import pandas as pd

from immolake_transform_daily import (
    SILVER_COLUMNS,
    _clean_dpe_rows,
    _clean_dvf_frame,
    _enrich_prices_from_dvf,
    _prepare_dvf_price_reference,
    _to_parquet_bytes,
)


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


def test_prepare_dvf_price_reference_computes_median_price_m2():
    dvf_df = pd.DataFrame(
        [
            {
                "code_commune": "75056",
                "type_local": "Appartement",
                "surface_reelle_bati": "40",
                "valeur_fonciere": "400000",
            },
            {
                "code_commune": "75056",
                "type_local": "Appartement",
                "surface_reelle_bati": "50",
                "valeur_fonciere": "450000",
            },
            {
                "code_commune": "75056",
                "type_local": "Maison",
                "surface_reelle_bati": "100",
                "valeur_fonciere": "700000",
            },
        ]
    )

    prices = _prepare_dvf_price_reference(dvf_df)

    apartment = prices[prices["type_bien"] == "appartement"].iloc[0]
    assert apartment["code_insee"] == "75056"
    assert apartment["dvf_prix_m2"] == 9500
    assert apartment["dvf_nb_transactions"] == 2


def test_clean_dvf_frame_normalizes_raw_dvf_columns():
    raw_dvf = pd.DataFrame(
        [
            {
                "code_commune": "75056",
                "type_local": "Appartement",
                "surface_reelle_bati": "40,5",
                "valeur_fonciere": "405000",
                "date_mutation": "17/06/2026",
            },
            {
                "code_commune": "13055",
                "type_local": "Maison",
                "surface_reelle_bati": "0",
                "valeur_fonciere": "300000",
                "date_mutation": "17/06/2026",
            },
        ]
    )

    cleaned = _clean_dvf_frame(raw_dvf)

    assert list(cleaned.columns) == ["code_insee", "type_bien", "surface", "prix", "prix_m2", "date_mutation"]
    assert len(cleaned) == 1
    assert cleaned.loc[0, "code_insee"] == "75056"
    assert cleaned.loc[0, "type_bien"] == "appartement"
    assert cleaned.loc[0, "surface"] == 40.5
    assert cleaned.loc[0, "prix"] == 405000
    assert cleaned.loc[0, "prix_m2"] == 10000
    assert cleaned.loc[0, "date_mutation"].isoformat() == "2026-06-17"


def test_enrich_prices_from_dvf_sets_price_and_price_m2():
    fact_df = pd.DataFrame(
        [
            {
                "numero_dpe": "DPE-1",
                "dt": pd.to_datetime("2026-06-17").date(),
                "code_insee": "75056",
                "etiquette": "C",
                "type_bien": "appartement",
                "surface": 42.0,
                "prix": pd.NA,
                "prix_m2": pd.NA,
                "conso_energie": 150,
            },
            {
                "numero_dpe": "DPE-2",
                "dt": pd.to_datetime("2026-06-17").date(),
                "code_insee": "13055",
                "etiquette": "D",
                "type_bien": "maison",
                "surface": 90.0,
                "prix": pd.NA,
                "prix_m2": pd.NA,
                "conso_energie": 220,
            },
        ]
    )
    dvf_prices = pd.DataFrame(
        [{"code_insee": "75056", "type_bien": "appartement", "dvf_prix_m2": 9500.0, "dvf_nb_transactions": 2}]
    )

    enriched = _enrich_prices_from_dvf(fact_df, dvf_prices)

    assert enriched.loc[0, "prix_m2"] == 9500
    assert enriched.loc[0, "prix"] == 399000
    assert pd.isna(enriched.loc[1, "prix_m2"])
