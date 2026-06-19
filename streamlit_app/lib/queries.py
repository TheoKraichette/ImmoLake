"""DuckDB-backed data access for the Streamlit front."""
from __future__ import annotations

import logging
from dataclasses import dataclass, replace

import pandas as pd

LOGGER = logging.getLogger(__name__)

from lib.connection import gold, ref, get_con
from lib.filter_state import Filters
from lib.filters_sql import build_where
from lib.st_compat import st


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
        "emission_ges_moy": 31.0,
        "cout_energie_annuel_median": 1450,
        "annee_construction_mediane": 1962,
        "pct_ges_passoires": 12.8,
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
        "emission_ges_moy": 38.5,
        "cout_energie_annuel_median": 1620,
        "annee_construction_mediane": 1974,
        "pct_ges_passoires": 16.4,
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
        "emission_ges_moy": 47.2,
        "cout_energie_annuel_median": 1780,
        "annee_construction_mediane": 1968,
        "pct_ges_passoires": 23.1,
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
        "emission_ges_moy": 22.4,
        "cout_energie_annuel_median": 1190,
        "annee_construction_mediane": 1991,
        "pct_ges_passoires": 8.7,
        "indice_sous_cotation": 2.2,
        "z": 0.22,
        "score_opportunite": 36,
        "latitude": 43.6108,
        "longitude": 3.8767,
    },
]


def _demo_frame() -> pd.DataFrame:
    df = pd.DataFrame(DEMO_ROWS)
    df["geometry_json"] = None  # aligne le schéma démo sur les marts (centroïdes : pas de contour)
    return df


def _run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        return get_con().execute(sql, params).fetchdf()
    except Exception:
        # On logue (une vraie erreur SQL/MinIO ne doit pas se confondre avec un résultat vide légitime),
        # puis on renvoie un DataFrame vide pour laisser l'appelant décider du fallback.
        LOGGER.exception("Requête front en échec (fallback vide) : %s", sql[:200])
        return pd.DataFrame()


def _mart_sql() -> str:
    return f"""
    SELECT
        coalesce(m.nom, m.code_insee) AS commune,
        m.code_insee,
        m.departement,
        m.region,
        'tous' AS type_bien,
        NULL AS etiquette,
        m.prix_m2_median AS prix_m2,
        NULL::DOUBLE AS surface,
        m.nb_dpe,
        m.pct_passoires,
        m.conso_energie_moy AS conso_energie_med,
        m.emission_ges_moy,
        m.cout_energie_annuel_median,
        m.annee_construction_mediane,
        m.pct_ges_passoires,
        m.indice_sous_cotation,
        m.z_prix_dpt AS z,
        NULL::DOUBLE AS score_opportunite,
        g.latitude,
        g.longitude,
        g.geometry_json
    FROM read_parquet('{gold('mart_commune')}') m
    LEFT JOIN read_parquet('{ref('geo_commune')}') g USING (code_insee)
    """


def _opportunities_sql() -> str:
    return f"""
    SELECT
        coalesce(o.nom, o.code_insee) AS commune,
        o.code_insee,
        o.departement,
        o.region,
        o.type_bien,
        NULL AS etiquette,
        o.prix_m2_median AS prix_m2,
        NULL::DOUBLE AS surface,
        o.nb_dpe,
        o.pct_passoires,
        NULL::DOUBLE AS conso_energie_med,
        o.cout_energie_annuel_median,
        o.annee_construction_mediane,
        o.ecart_pct AS indice_sous_cotation,
        o.z,
        round(
            (0.6 * greatest(-coalesce(o.z, 0), 0) * 50)
            + (0.4 * coalesce(o.pct_passoires, 0)),
            1
        ) AS score_opportunite,
        g.latitude,
        g.longitude,
        g.geometry_json,
        o.est_opportunite
    FROM read_parquet('{gold('mart_opportunites')}') o
    LEFT JOIN read_parquet('{ref('geo_commune')}') g USING (code_insee)
    """


# Villes à arrondissements municipaux : les DPE/DVF sont au grain arrondissement
# (noms 'Paris 1er Arrondissement' … 'Paris 20e Arrondissement'). Sélectionner « Paris » doit
# donc englober tous ses arrondissements (+ la commune-ville elle-même).
_CITY_ARRONDISSEMENTS = {"Paris": 20, "Lyon": 9, "Marseille": 16}


def _expand_cities(communes: tuple[str, ...]) -> tuple[str, ...]:
    out = set(communes)
    for city, n_arr in _CITY_ARRONDISSEMENTS.items():
        if city in out:
            out.update(
                f"{city} {'1er' if n == 1 else f'{n}e'} Arrondissement" for n in range(1, n_arr + 1)
            )
    return tuple(out)


def _commune_grain(filters: Filters) -> Filters:
    """Normalise les filtres pour une requête au grain commune/marts.

    - `surface`/`etiquette` sont NULL dans les marts -> filtrer dessus viderait tout (neutralisés).
    - `'tous'` est un pseudo-type exposé par `_mart_sql` ; au grain opportunités (commune×type réel
      'appartement'/'maison') il ne matche rien et viderait `mart_opportunites` -> on le retire.
    - `Paris`/`Lyon`/`Marseille` -> étendus à tous leurs arrondissements.
    """
    types = tuple(t for t in filters.types_bien if t != "tous")
    return replace(
        filters,
        types_bien=types,
        communes=_expand_cities(filters.communes),
        surface_min=None,
        surface_max=None,
        etiquettes=(),
        passoires_only=False,
    )


@st.cache_data(ttl=300)
def get_market_data(filters: Filters | None = None) -> pd.DataFrame:
    filters = filters or Filters()
    where = build_where(_commune_grain(filters), alias="m")
    sql = f"SELECT * FROM ({_mart_sql()}) m {where.sql} ORDER BY prix_m2 DESC"
    df = _run_query(sql, where.params)
    if df.empty and _run_query(f"SELECT 1 FROM ({_mart_sql()}) m LIMIT 1").empty:
        # Lac réellement vide (pas de pipeline ni de snapshot) -> données de démonstration.
        # Si le mart a des données mais que le filtre exclut tout, on renvoie un df vide
        # (la page affiche "aucun résultat" plutôt que des villes de démo trompeuses).
        df = _demo_frame()
    return df


@st.cache_data(ttl=300)
def get_map_data(filters: Filters | None = None) -> pd.DataFrame:
    df = get_market_data(filters)
    if "geometry_json" not in df.columns:
        df["geometry_json"] = None
    return df.dropna(subset=["latitude", "longitude"])


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
def get_dpe_distribution(filters: Filters, top_n: int = 25) -> pd.DataFrame:
    # Répartition A→G réelle au grain bien (fact_biens). On filtre sur le `dt` courant (le glob `**`
    # lirait sinon toutes les partitions -> double comptage si plusieurs runs), et on neutralise les
    # filtres qui n'ont pas de sens ici (type_bien 'tous', prix/surface au grain commune, nb_dpe_min).
    grain = replace(
        filters,
        types_bien=(),
        communes=_expand_cities(filters.communes),
        prix_m2_min=None,
        prix_m2_max=None,
        surface_min=None,
        surface_max=None,
        nb_dpe_min=0,
    )
    where = build_where(grain, alias="m")
    fact = gold("fact_biens")
    sql = f"""
        SELECT commune, etiquette, count(*) AS nb FROM (
            SELECT c.nom AS commune, c.departement, c.region, f.type_bien, f.etiquette
            FROM read_parquet('{fact}') f
            JOIN read_parquet('{ref('dim_commune')}') c USING (code_insee)
            WHERE f.dt = (SELECT max(dt) FROM read_parquet('{fact}'))
        ) m {where.sql}
        GROUP BY commune, etiquette
    """
    df = _run_query(sql, where.params)
    if df.empty:
        return pd.DataFrame(columns=["commune", "etiquette", "nb"])
    # Un px.bar de ~10 000 communes est illisible : on garde les `top_n` communes les plus riches en DPE.
    top = df.groupby("commune")["nb"].sum().nlargest(top_n).index
    return df[df["commune"].isin(top)].sort_values(["commune", "etiquette"])


@st.cache_data(ttl=300)
def get_opportunities(filters: Filters) -> pd.DataFrame:
    where = build_where(_commune_grain(filters), alias="m")
    sql = f"SELECT * FROM ({_opportunities_sql()}) m {where.sql} ORDER BY score_opportunite DESC"
    df = _run_query(sql, where.params)
    if df.empty:
        df = get_market_data(filters).copy()
    df = df[(df["z"].fillna(0) <= -filters.opportunite_k) | (df["indice_sous_cotation"].fillna(0) < 0)]
    if df.empty:
        return df
    df["score_opportunite"] = pd.to_numeric(df["score_opportunite"], errors="coerce").fillna(
        (0.6 * df["z"].fillna(0).mul(-1).clip(lower=0) * 50) + (0.4 * df["pct_passoires"].fillna(0))
    ).round(1)
    df["etiquette_opportunite"] = df.apply(
        lambda row: "sous-cotee + parc passoires"
        if row["pct_passoires"] >= 20
        else "sous-cotee",
        axis=1,
    )
    return df.sort_values(["score_opportunite", "indice_sous_cotation"], ascending=[False, True])


@st.cache_data(ttl=300)
def get_comparison_data(communes: tuple[str, ...], filters: Filters | None = None) -> pd.DataFrame:
    df = get_market_data(filters or Filters(nb_dpe_min=1))
    if communes:
        df = df[df["commune"].isin(communes)]
    return df.sort_values("commune")
