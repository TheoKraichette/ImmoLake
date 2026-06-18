"""Accès données DuckDB pour le front Streamlit, branché sur les VRAIS marts MinIO.

Interface (Filters / build_where / pages) conservée de v2-8 ; la source de données passe de
`kpi_commune` + placeholders + démo en dur aux marts réels produits en v2-4 :
- `get_market_data`   -> mart_commune_type (prix/m² par commune×type) × mart_commune (indice/z réels)
- `get_dpe_distribution` -> fact_biens (répartition A→G réelle, filtrée)
- `get_opportunities` -> mart_opportunites (sous-cotation commune vs département réelle)

Règle perf (15M) : marts pré-agrégés ; `fact_biens` n'est lu que filtré. WHERE paramétré
(anti-injection) via `build_where`. Plus de fallback démo : on affiche les vraies données.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

import pandas as pd
import streamlit as st

from lib.connection import bucket, get_con
from lib.filter_state import Filters
from lib.filters_sql import build_where


@dataclass(frozen=True)
class FilterOptions:
    regions: list[str]
    departements: list[str]
    communes: list[str]
    types_bien: list[str]
    etiquettes: list[str]
    prix_m2_min: int
    prix_m2_max: int
    surface_min: int
    surface_max: int


def _mart(name: str) -> str:
    """Chemin s3:// d'un mart (un seul fichier snapshot)."""
    return f"s3://{bucket()}/gold/{name}/*.parquet"


def _fact() -> str:
    return f"s3://{bucket()}/gold/fact_biens/**/*.parquet"


def _dim_commune() -> str:
    return f"s3://{bucket()}/ref/dim_commune/*.parquet"


def _run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Curseur DuckDB dédié (sûr entre sessions) ; DataFrame vide si la donnée manque."""
    try:
        return get_con().cursor().execute(sql, params).fetchdf()
    except Exception:
        return pd.DataFrame()


def _market_sql() -> str:
    """Marché au grain commune×type : prix/m² réel + indicateurs territoriaux (indice/z) de la commune."""
    return f"""
    SELECT
        m.nom                       AS commune,
        m.code_insee,
        m.departement,
        m.region,
        m.type_bien,
        NULL                        AS etiquette,
        m.prix_m2_median            AS prix_m2,
        NULL::DOUBLE                AS surface,
        m.nb_dpe,
        m.pct_passoires,
        mc.conso_energie_moy        AS conso_energie_med,
        mc.indice_sous_cotation,
        mc.z_prix_dpt               AS z,
        NULL::DOUBLE                AS score_opportunite,
        NULL::DOUBLE                AS latitude,
        NULL::DOUBLE                AS longitude
    FROM read_parquet('{_mart('mart_commune_type')}') m
    LEFT JOIN read_parquet('{_mart('mart_commune')}') mc ON mc.code_insee = m.code_insee
    """


@st.cache_data(ttl=300)
def get_market_data(filters: Filters | None = None) -> pd.DataFrame:
    """Marché par commune×type. Les filtres étiquette/surface/passoires (grain bien) sont ignorés ici."""
    filters = filters or Filters()
    market = replace(filters, etiquettes=(), passoires_only=False, surface_min=None, surface_max=None)
    where = build_where(market, alias="m")
    sql = f"SELECT * FROM ({_market_sql()}) m {where.sql} ORDER BY prix_m2 DESC"
    return _run_query(sql, where.params)


@st.cache_data(ttl=300)
def get_filter_options() -> FilterOptions:
    df = get_market_data(Filters(nb_dpe_min=1))
    if df.empty:
        return FilterOptions([], [], [], ["appartement", "maison"], list("ABCDEFG"), 0, 5000, 0, 150)
    return FilterOptions(
        regions=sorted(df["region"].dropna().astype(str).unique().tolist()),
        departements=sorted(df["departement"].dropna().astype(str).unique().tolist()),
        communes=sorted(df["commune"].dropna().astype(str).unique().tolist()),
        types_bien=sorted(df["type_bien"].dropna().astype(str).unique().tolist()),
        etiquettes=list("ABCDEFG"),
        prix_m2_min=max(0, int(df["prix_m2"].min() // 100 * 100)),
        prix_m2_max=int(df["prix_m2"].max() // 100 * 100 + 500),
        surface_min=0,
        surface_max=150,
    )


@st.cache_data(ttl=300)
def get_dpe_distribution(filters: Filters) -> pd.DataFrame:
    """Répartition A→G réelle par commune (grain `fact_biens`, joint à `ref`, filtré géo/type/étiquette)."""
    grain = replace(filters, prix_m2_min=None, prix_m2_max=None, surface_min=None, surface_max=None, nb_dpe_min=0)
    where = build_where(grain, alias="m")
    sql = f"""
        SELECT commune, etiquette, count(*) AS nb FROM (
            SELECT c.nom AS commune, c.departement, c.region, f.type_bien, f.etiquette
            FROM read_parquet('{_fact()}') f
            JOIN read_parquet('{_dim_commune()}') c USING (code_insee)
        ) m {where.sql}
        GROUP BY commune, etiquette
        ORDER BY commune, etiquette
    """
    return _run_query(sql, where.params)


def _opportunites_sql() -> str:
    return f"""
    SELECT
        nom                         AS commune,
        code_insee,
        departement,
        region,
        type_bien,
        NULL                        AS etiquette,
        prix_m2_median              AS prix_m2,
        NULL::DOUBLE                AS surface,
        nb_dpe,
        pct_passoires,
        NULL::DOUBLE                AS conso_energie_med,
        ecart_pct                   AS indice_sous_cotation,
        z,
        NULL::DOUBLE                AS score_opportunite,
        NULL::DOUBLE                AS latitude,
        NULL::DOUBLE                AS longitude
    FROM read_parquet('{_mart('mart_opportunites')}')
    WHERE est_opportunite
    """


@st.cache_data(ttl=300)
def get_opportunities(filters: Filters) -> pd.DataFrame:
    """Communes sous-cotées (mart_opportunites : commune vs département). Scoring fin -> v2-7."""
    opp = replace(filters, etiquettes=(), passoires_only=False, surface_min=None, surface_max=None)
    where = build_where(opp, alias="m")
    df = _run_query(f"SELECT * FROM ({_opportunites_sql()}) m {where.sql} ORDER BY z", where.params)
    if df.empty:
        return df
    df["etiquette_opportunite"] = df["pct_passoires"].apply(
        lambda p: "sous-cotee + parc passoires" if p >= 20 else "sous-cotee"
    )
    return df
