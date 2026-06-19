"""Tests d'intégration des transforms DuckDB : SQL rendu + exécuté sur fixtures locales (sans S3)."""
import duckdb
import pandas as pd

from duckdb_lake import render


def test_silver_dpe_cleans_types_and_deduplicates(tmp_path):
    raw = pd.DataFrame(
        [
            {  # gardé : doublon numero_dpe, date la plus récente
                "numero_dpe": "DPE-1", "code_insee_ban": "33063", "code_postal_ban": "33000",
                "nom_commune_ban": "Bordeaux", "type_batiment": "maison",
                "surface_habitable_logement": "95", "annee_construction": "1975",
                "etiquette_dpe": " d ", "etiquette_ges": " c ", "conso_5_usages_par_m2_ep": "210",
                "emission_ges_5_usages_par_m2": "30", "cout_total_5_usages": "1500",
                "type_energie_principale_chauffage": "Gaz naturel", "date_etablissement_dpe": "2026-01-08",
            },
            {  # écarté par la dédup (date plus ancienne)
                "numero_dpe": "DPE-1", "code_insee_ban": "33063", "code_postal_ban": "33000",
                "nom_commune_ban": "Bordeaux", "type_batiment": "maison",
                "surface_habitable_logement": "88", "annee_construction": "1960",
                "etiquette_dpe": "C", "etiquette_ges": "B", "conso_5_usages_par_m2_ep": "150",
                "emission_ges_5_usages_par_m2": "20", "cout_total_5_usages": "1100",
                "type_energie_principale_chauffage": "Electricite", "date_etablissement_dpe": "2026-01-07",
            },
            {  # écarté : pas de code_insee
                "numero_dpe": "DPE-2", "code_insee_ban": None, "code_postal_ban": None,
                "nom_commune_ban": None, "type_batiment": None, "surface_habitable_logement": "50",
                "annee_construction": None, "etiquette_dpe": "A", "etiquette_ges": None,
                "conso_5_usages_par_m2_ep": None, "emission_ges_5_usages_par_m2": None,
                "cout_total_5_usages": None, "type_energie_principale_chauffage": None,
                "date_etablissement_dpe": None,
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
        "numero_dpe", "dt", "code_insee", "code_postal", "nom_commune", "type_batiment",
        "surface_habitable", "annee_construction", "etiquette_dpe", "etiquette_ges",
        "conso_energie", "emission_ges", "cout_energie_annuel", "energie_chauffage", "date_etablissement",
    ]
    assert len(silver) == 1
    row = silver.iloc[0]
    assert row["numero_dpe"] == "DPE-1"
    assert row["etiquette_dpe"] == "D"          # trim + upper, date la plus récente conservée
    assert row["etiquette_ges"] == "C"          # GES normalisé (trim + upper)
    assert row["surface_habitable"] == 95.0
    assert row["annee_construction"] == 1975
    assert row["cout_energie_annuel"] == 1500.0
    assert row["energie_chauffage"] == "gaz naturel"


def test_gold_fact_biens_enriches_price_and_keeps_known_communes(tmp_path):
    pd.DataFrame(
        [
            {"code_insee": "33063", "etiquette_dpe": "C", "etiquette_ges": "D",
             "type_batiment": "appartement", "surface_habitable": 50.0, "annee_construction": 1980,
             "conso_energie": 150.0, "emission_ges": 25.0, "cout_energie_annuel": 1300.0,
             "energie_chauffage": "gaz naturel", "date_etablissement": "2026-01-01"},
            {"code_insee": "99999", "etiquette_dpe": "C", "etiquette_ges": "D",  # commune inconnue
             "type_batiment": "appartement", "surface_habitable": 40.0, "annee_construction": 1990,
             "conso_energie": 120.0, "emission_ges": 18.0, "cout_energie_annuel": 1000.0,
             "energie_chauffage": "electricite", "date_etablissement": "2026-01-01"},
        ]
    ).to_parquet(tmp_path / "silver_dpe.parquet")
    pd.DataFrame(
        [  # surfaces dans la tranche 40-70 (comme le DPE) -> médiane de tranche = médiane commune
            {"code_insee": "33063", "type_bien": "appartement", "prix_m2": 4000.0, "surface": 50.0},
            {"code_insee": "33063", "type_bien": "appartement", "prix_m2": 5000.0, "surface": 55.0},
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
    assert row["prix_m2"] == 4500.0              # médiane DVF de la tranche 40-70 (4000, 5000)
    assert row["prix"] == 50.0 * 4500.0          # surface * prix/m²
    assert row["type_bien"] == "appartement"     # libellé conservé (pas d'id SERIAL)
    assert row["tranche_surface"] == "40-70"     # maille surface (v2-11)
    assert "date_etablissement" in fact.columns  # axe temporel des tendances (v2-11)
    # Attributs énergie/bâti remontés au grain bien (enrichissement colonnes pertinentes).
    for col in ("etiquette_ges", "annee_construction", "emission_ges", "cout_energie_annuel", "energie_chauffage"):
        assert col in fact.columns
    assert row["cout_energie_annuel"] == 1300.0
    assert row["annee_construction"] == 1980


def test_silver_dvf_filters_outliers_and_parses_quoted_commas(tmp_path):
    # CSV DVF avec : 3 transactions valides (dont une adresse contenant une virgule entre guillemets),
    # + des lignes à exclure : type hors Appart/Maison, surface < 9 (garage), prix/m² < 200, prix/m² > 40000,
    # valeur foncière nulle.
    csv = tmp_path / "dvf.csv"
    csv.write_text(
        "code_commune,type_local,surface_reelle_bati,valeur_fonciere,date_mutation,longitude,latitude,adresse\n"
        "33063,Appartement,50,200000,2024-03-01,-0.57,44.84,RUE SIMPLE\n"        # valide -> 4000 €/m²
        "33063,Maison,100,300000,2024-04-01,-0.57,44.84,RUE SIMPLE\n"           # valide -> 3000 €/m²
        "33063,Dependance,12,50000,2024-04-01,-0.57,44.84,RUE SIMPLE\n"         # type exclu
        "33063,Appartement,2,400000,2024-04-01,-0.57,44.84,RUE SIMPLE\n"        # surface < 9 (garage)
        "33063,Appartement,80,100,2024-04-01,-0.57,44.84,RUE SIMPLE\n"          # prix/m² ~1 < 200
        "33063,Maison,50,5000000,2024-04-01,-0.57,44.84,RUE SIMPLE\n"           # prix/m² 100000 > 40000
        "33063,Maison,90,0,2024-04-01,-0.57,44.84,RUE SIMPLE\n"                 # valeur foncière nulle
        '44109,Appartement,60,240000,2024-05-01,-1.55,47.21,"ALL ROSA, ST GUILLAUME"\n',  # virgule entre quotes
        encoding="utf-8",
    )
    out_path = tmp_path / "silver_dvf.parquet"
    sql = render("silver_dvf.sql", dvf_csv=str(csv), out=str(out_path))
    duckdb.connect().execute(sql)
    silver = pd.read_parquet(out_path)

    # 3 survivants : seules les transactions Appart/Maison à prix/m² réaliste et surface >= 9.
    assert len(silver) == 3
    assert sorted(round(p) for p in silver["prix_m2"]) == [3000, 4000, 4000]
    # La ligne à adresse "ALL ROSA, ST GUILLAUME" (virgule entre guillemets) est bien parsée (quote='"').
    assert "44109" in set(silver["code_insee"])
    assert set(silver["type_bien"]) <= {"appartement", "maison"}
