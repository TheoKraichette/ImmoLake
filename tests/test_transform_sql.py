"""Tests d'intégration des transforms DuckDB : SQL rendu + exécuté sur fixtures locales (sans S3)."""
import duckdb
import pandas as pd

from duckdb_lake import render


def test_silver_dpe_cleans_types_and_deduplicates(tmp_path):
    raw = pd.DataFrame(
        [
            {  # gardé : doublon numero_dpe, date la plus récente
                "numero_dpe": "DPE-1", "code_insee_ban": "33063", "code_postal_ban": "33000",
                "type_batiment": "maison", "surface_habitable_logement": "95", "etiquette_dpe": " d ",
                "conso_5_usages_par_m2_ep": "210", "date_etablissement_dpe": "2026-01-08",
            },
            {  # écarté par la dédup (date plus ancienne)
                "numero_dpe": "DPE-1", "code_insee_ban": "33063", "code_postal_ban": "33000",
                "type_batiment": "maison", "surface_habitable_logement": "88", "etiquette_dpe": "C",
                "conso_5_usages_par_m2_ep": "150", "date_etablissement_dpe": "2026-01-07",
            },
            {  # écarté : pas de code_insee
                "numero_dpe": "DPE-2", "code_insee_ban": None, "code_postal_ban": None,
                "type_batiment": None, "surface_habitable_logement": "50", "etiquette_dpe": "A",
                "conso_5_usages_par_m2_ep": None, "date_etablissement_dpe": None,
            },
        ]
    )
    raw_path = tmp_path / "raw.parquet"
    raw.to_parquet(raw_path)
    out_path = tmp_path / "silver.parquet"

    sql = render("silver_dpe.sql", ds="2026-06-18", raw_glob=str(raw_path), out=str(out_path))
    duckdb.connect().execute(sql)
    silver = pd.read_parquet(out_path)

    assert list(silver.columns) == [
        "numero_dpe", "dt", "code_insee", "code_postal", "type_batiment",
        "surface_habitable", "etiquette_dpe", "conso_energie", "date_etablissement",
    ]
    assert len(silver) == 1
    row = silver.iloc[0]
    assert row["numero_dpe"] == "DPE-1"
    assert row["etiquette_dpe"] == "D"          # trim + upper, date la plus récente conservée
    assert row["surface_habitable"] == 95.0


def test_gold_fact_biens_enriches_price_and_keeps_known_communes(tmp_path):
    pd.DataFrame(
        [
            {"code_insee": "33063", "etiquette_dpe": "C", "type_batiment": "appartement",
             "surface_habitable": 50.0, "conso_energie": 150.0},
            {"code_insee": "99999", "etiquette_dpe": "C", "type_batiment": "appartement",  # commune inconnue
             "surface_habitable": 40.0, "conso_energie": 120.0},
        ]
    ).to_parquet(tmp_path / "silver_dpe.parquet")
    pd.DataFrame(
        [
            {"code_insee": "33063", "type_bien": "appartement", "prix_m2": 4000.0},
            {"code_insee": "33063", "type_bien": "appartement", "prix_m2": 5000.0},
        ]
    ).to_parquet(tmp_path / "silver_dvf.parquet")
    pd.DataFrame([{"code_insee": "33063", "nom": "Bordeaux"}]).to_parquet(tmp_path / "dim_commune.parquet")
    pd.DataFrame([{"etiquette": "C"}]).to_parquet(tmp_path / "dim_dpe.parquet")
    out_path = tmp_path / "fact.parquet"

    sql = render(
        "gold_fact_biens.sql", ds="2026-06-18",
        silver_dpe=str(tmp_path / "silver_dpe.parquet"),
        silver_dvf=str(tmp_path / "silver_dvf.parquet"),
        dim_commune=str(tmp_path / "dim_commune.parquet"),
        dim_dpe=str(tmp_path / "dim_dpe.parquet"),
        out=str(out_path),
    )
    duckdb.connect().execute(sql)
    fact = pd.read_parquet(out_path)

    assert len(fact) == 1                        # commune 99999 écartée (absente du référentiel)
    row = fact.iloc[0]
    assert row["code_insee"] == "33063"
    assert row["prix_m2"] == 4500.0              # médiane DVF (4000, 5000)
    assert row["prix"] == 50.0 * 4500.0          # surface * prix/m²
    assert row["type_bien"] == "appartement"     # libellé conservé (pas d'id SERIAL)
