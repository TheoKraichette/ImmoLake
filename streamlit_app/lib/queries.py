"""DuckDB-backed data access for the Streamlit front."""
from __future__ import annotations

from dataclasses import dataclass

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


DEMO_ROWS = [
    {
        "commune": "Bordeaux",
        "code_insee": "33063",
        "departement": "33",
        "region": "75",
        "type_bien": "appartement",
        "etiquette": "D",
        "prix_m2": 4571,
        "surface": 61,
        "nb_dpe": 240,
        "pct_passoires": 14.2,
        "conso_energie_med": 216,
        "indice_sous_cotation": -8.4,
        "z": -1.35,
        "score_opportunite": 78,
        "latitude": 44.8378,
        "longitude": -0.5792,
    },
    {
        "commune": "Nantes",
        "code_insee": "44109",
        "departement": "44",
        "region": "52",
        "type_bien": "maison",
        "etiquette": "E",
        "prix_m2": 3545,
        "surface": 93,
        "nb_dpe": 180,
        "pct_passoires": 18.6,
        "conso_energie_med": 248,
        "indice_sous_cotation": -5.1,
        "z": -1.08,
        "score_opportunite": 64,
        "latitude": 47.2184,
        "longitude": -1.5536,
    },
    {
        "commune": "Rennes",
        "code_insee": "35238",
        "departement": "35",
        "region": "53",
        "type_bien": "appartement",
        "etiquette": "F",
        "prix_m2": 3684,
        "surface": 54,
        "nb_dpe": 120,
        "pct_passoires": 24.5,
        "conso_energie_med": 302,
        "indice_sous_cotation": -12.0,
        "z": -1.62,
        "score_opportunite": 86,
        "latitude": 48.1173,
        "longitude": -1.6778,
    },
    {
        "commune": "Montpellier",
        "code_insee": "34172",
        "departement": "34",
        "region": "76",
        "type_bien": "appartement",
        "etiquette": "C",
        "prix_m2": 3478,
        "surface": 58,
        "nb_dpe": 150,
        "pct_passoires": 10.1,
        "conso_energie_med": 184,
        "indice_sous_cotation": 2.2,
        "z": 0.22,
        "score_opportunite": 36,
        "latitude": 43.6108,
        "longitude": 3.8767,
    },
]


def _demo_frame() -> pd.DataFrame:
    return pd.DataFrame(DEMO_ROWS)


def _run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        return get_con().execute(sql, params).fetchdf()
    except Exception:
        return pd.DataFrame()


def _mart_sql() -> str:
    lake = bucket()
    return f"""
    SELECT
        coalesce(c.nom, k.code_insee) AS commune,
        k.code_insee,
        c.departement,
        c.region,
        'tous' AS type_bien,
        NULL AS etiquette,
        k.prix_m2_median AS prix_m2,
        NULL::DOUBLE AS surface,
        k.nb_transactions AS nb_dpe,
        k.pct_passoires,
        NULL::DOUBLE AS conso_energie_med,
        k.decote_passoire_pct AS indice_sous_cotation,
        NULL::DOUBLE AS z,
        NULL::DOUBLE AS score_opportunite,
        NULL::DOUBLE AS latitude,
        NULL::DOUBLE AS longitude
    FROM read_parquet('s3://{lake}/gold/kpi_commune/**/*.parquet') k
    LEFT JOIN read_parquet('s3://{lake}/ref/dim_commune/**/*.parquet') c USING (code_insee)
    """


@st.cache_data(ttl=300)
def get_market_data(filters: Filters | None = None) -> pd.DataFrame:
    filters = filters or Filters()
    where = build_where(filters, alias="m")
    sql = f"SELECT * FROM ({_mart_sql()}) m {where.sql} ORDER BY prix_m2 DESC"
    df = _run_query(sql, where.params)
    if df.empty:
        df = _demo_frame()
    return df


@st.cache_data(ttl=300)
def get_filter_options() -> FilterOptions:
    df = get_market_data(Filters(nb_dpe_min=1))
    return FilterOptions(
        regions=sorted(df["region"].dropna().astype(str).unique().tolist()),
        departements=sorted(df["departement"].dropna().astype(str).unique().tolist()),
        communes=sorted(df["commune"].dropna().astype(str).unique().tolist()),
        types_bien=sorted(df["type_bien"].dropna().astype(str).unique().tolist()),
        etiquettes=["A", "B", "C", "D", "E", "F", "G"],
        prix_m2_min=max(0, int(df["prix_m2"].min() // 100 * 100)),
        prix_m2_max=int(df["prix_m2"].max() // 100 * 100 + 500),
        surface_min=0,
        surface_max=max(150, int(df["surface"].dropna().max() // 10 * 10 + 20) if df["surface"].notna().any() else 150),
    )


@st.cache_data(ttl=300)
def get_dpe_distribution(filters: Filters) -> pd.DataFrame:
    df = get_market_data(filters)
    grouped = df.groupby(["commune", "etiquette"], dropna=True).agg(nb=("nb_dpe", "sum")).reset_index()
    if grouped.empty:
        return pd.DataFrame(columns=["commune", "etiquette", "nb"])
    return grouped


@st.cache_data(ttl=300)
def get_opportunities(filters: Filters) -> pd.DataFrame:
    df = get_market_data(filters).copy()
    df = df[(df["z"].fillna(0) <= -filters.opportunite_k) | (df["indice_sous_cotation"].fillna(0) < 0)]
    if df.empty:
        return df
    df["etiquette_opportunite"] = df.apply(
        lambda row: "sous-cotee + parc passoires"
        if row["pct_passoires"] >= 20
        else "sous-cotee",
        axis=1,
    )
    return df.sort_values(["score_opportunite", "indice_sous_cotation"], ascending=[False, True])
