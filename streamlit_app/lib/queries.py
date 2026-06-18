"""Couche analytique : requêtes DuckDB sur les marts/gold Parquet de MinIO, pour le front.

Lit UNIQUEMENT du Parquet (gold + ref), sans Postgres. Chaque helper public est caché
(`@st.cache_data`) et renvoie un DataFrame pandas prêt pour Streamlit.

Règle perf (15M) : on interroge des **marts pré-agrégés** ; le grain ligne (`fact_biens`)
n'est lu qu'avec un filtre commune/département (jamais un scan total sans WHERE).
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from lib.connection import BUCKET, get_con, gold, ref

# Marts (snapshot, un seul fichier) vs fact_biens (partitionné dt= -> **).
MART_COMMUNE = f"s3://{BUCKET}/gold/mart_commune/*.parquet"
MART_COMMUNE_TYPE = f"s3://{BUCKET}/gold/mart_commune_type/*.parquet"
MART_OPPORTUNITES = f"s3://{BUCKET}/gold/mart_opportunites/*.parquet"
FACT_BIENS = gold("fact_biens")
DIM_COMMUNE = ref("dim_commune")


def _df(sql: str, params: list | None = None) -> pd.DataFrame:
    """Exécute une requête (curseur dédié = sûr entre sessions) -> DataFrame pandas."""
    return get_con().cursor().execute(sql, params or []).fetch_df()


def _geo_where(prefix: str, params: list, region=None, departement=None, commune=None) -> list[str]:
    """Construit des clauses WHERE paramétrées (anti-injection) sur région/département/commune."""
    clauses = []
    if region:
        clauses.append(f"{prefix}region = ?"); params.append(region)
    if departement:
        clauses.append(f"{prefix}departement = ?"); params.append(departement)
    if commune:
        clauses.append(f"{prefix}code_insee = ?"); params.append(commune)
    return clauses


@st.cache_data(ttl=600)
def kpis_france() -> dict:
    """KPIs globaux (page d'accueil), depuis le mart commune."""
    row = _df(
        f"""
        SELECT count(*) AS nb_communes,
               COALESCE(sum(nb_dpe), 0) AS nb_biens,
               median(prix_m2_median) AS prix_m2_median_national
        FROM read_parquet('{MART_COMMUNE}')
        """
    ).iloc[0]
    median = row.prix_m2_median_national
    return {
        "nb_communes": int(row.nb_communes),
        "nb_biens": int(row.nb_biens),
        "prix_m2_median_national": None if pd.isna(median) else round(float(median)),
    }


@st.cache_data(ttl=600)
def options_filtres() -> dict:
    """Valeurs distinctes pour les filtres sidebar (régions, départements, types de bien)."""
    geo = _df(f"SELECT DISTINCT region, departement FROM read_parquet('{MART_COMMUNE}')")
    types = _df(f"SELECT DISTINCT type_bien FROM read_parquet('{MART_COMMUNE_TYPE}') ORDER BY 1")
    return {
        "regions": sorted(geo["region"].dropna().unique().tolist()),
        "departements": sorted(geo["departement"].dropna().unique().tolist()),
        "types_bien": types["type_bien"].dropna().tolist(),
    }


@st.cache_data(ttl=600)
def marche_communes(region=None, departement=None, seuil_nb_dpe: int = 30) -> pd.DataFrame:
    """Classement des communes : prix/m² médian + NOMS + dpt/région + indicateurs territoriaux."""
    params: list = [seuil_nb_dpe]
    where = ["nb_dpe >= ?"] + _geo_where("", params, region=region, departement=departement)
    return _df(
        f"""
        SELECT code_insee, nom, departement, region, population, prix_m2_median, nb_dpe,
               pct_passoires, conso_energie_moy, indice_sous_cotation, z_prix_dpt, rang_prix_dpt
        FROM read_parquet('{MART_COMMUNE}')
        WHERE {' AND '.join(where)}
        ORDER BY prix_m2_median DESC
        """,
        params,
    )


@st.cache_data(ttl=600)
def repartition_dpe(region=None, departement=None, commune=None) -> pd.DataFrame:
    """Répartition des étiquettes A→G (grain ligne `fact_biens`, joint à `ref` pour filtrer)."""
    params: list = []
    geo = _geo_where("c.", params, region=region, departement=departement, commune=commune)
    join_where = " AND ".join(["f.code_insee = c.code_insee"] + geo)
    return _df(
        f"""
        SELECT f.etiquette, count(*) AS nb
        FROM read_parquet('{FACT_BIENS}') f
        JOIN read_parquet('{DIM_COMMUNE}') c ON {join_where}
        GROUP BY f.etiquette
        ORDER BY f.etiquette
        """,
        params,
    )


@st.cache_data(ttl=600)
def opportunites(departement=None, type_bien=None) -> pd.DataFrame:
    """Communes sous-cotées (mart_opportunites, base détecteur commune vs département)."""
    params: list = []
    where = ["est_opportunite"]
    if departement:
        where.append("departement = ?"); params.append(departement)
    if type_bien:
        where.append("type_bien = ?"); params.append(type_bien)
    return _df(
        f"""
        SELECT code_insee, nom, departement, type_bien, prix_m2_median, prix_m2_median_dpt,
               ecart_pct, z, nb_dpe, pct_passoires
        FROM read_parquet('{MART_OPPORTUNITES}')
        WHERE {' AND '.join(where)}
        ORDER BY z
        """,
        params,
    )
