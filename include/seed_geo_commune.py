"""Build a cached commune geography reference for the Streamlit map.

The script can enrich a list of commune codes through geo.api.gouv.fr when network
is available. For offline demos it keeps a small set of committed centre points,
which is enough for the application to render and for contributors to regenerate
the cache deterministically.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

INCLUDE_DIR = Path(__file__).resolve().parent
REF_DIR = INCLUDE_DIR / "ref"
GEO_PARQUET = REF_DIR / "geo_commune.parquet"

FALLBACK_CENTRES: dict[str, tuple[str, float, float]] = {
    "33063": ("Bordeaux", 44.8378, -0.5792),
    "44109": ("Nantes", 47.2184, -1.5536),
    "35238": ("Rennes", 48.1173, -1.6778),
    "34172": ("Montpellier", 43.6108, 3.8767),
    "31555": ("Toulouse", 43.6045, 1.4442),
    "69123": ("Lyon", 45.7578, 4.8320),
    "13055": ("Marseille", 43.2965, 5.3698),
    "75101": ("Paris 1er Arrondissement", 48.8626, 2.3363),
    "75102": ("Paris 2e Arrondissement", 48.8686, 2.3418),
}

# Départements couverts par défaut (grandes métropoles) — alignés sur `ADEME_DEPARTEMENTS` (.env).
DEFAULT_DEPARTEMENTS = ["75", "69", "13", "33", "31", "06", "44", "67", "34", "59", "35", "38"]

# Arrondissements municipaux (Paris 75101-75120, Lyon 69381-69389, Marseille 13201-13216) :
# les DPE/DVF utilisent ces codes, pas le code commune unique. L'endpoint /departements n'inclut
# pas toujours les arrondissements -> on les récupère explicitement par code.
ARRONDISSEMENTS = (
    [f"751{n:02d}" for n in range(1, 21)]
    + [f"6938{n}" for n in range(1, 10)]
    + [f"132{n:02d}" for n in range(1, 17)]
)


def _fields(with_contour: bool) -> str:
    # Le contour (polygone) est lourd : sur un large périmètre (national), on s'en passe et la carte
    # rend des points colorés. On le garde pour un petit périmètre (choroplèthe urbain).
    return "nom,centre,contour" if with_contour else "nom,centre"


def _fetch_commune(code_insee: str, timeout: float = 10.0, with_contour: bool = True) -> dict[str, Any] | None:
    response = requests.get(
        f"https://geo.api.gouv.fr/communes/{code_insee}",
        params={"fields": _fields(with_contour)},
        timeout=timeout,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def _fetch_departement_communes(
    departement: str, timeout: float = 30.0, with_contour: bool = True
) -> list[dict[str, Any]]:
    """Toutes les communes d'un département (centroïde + éventuellement contour) en un appel."""
    response = requests.get(
        f"https://geo.api.gouv.fr/departements/{departement}/communes",
        params={"fields": _fields(with_contour)},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json() or []


def _row_from_payload(code_insee: str, payload: dict[str, Any]) -> dict[str, Any]:
    centre = payload.get("centre") or {}
    coordinates = centre.get("coordinates") or [None, None]
    contour = payload.get("contour")
    return {
        "code_insee": code_insee,
        "nom": payload.get("nom"),
        "latitude": coordinates[1],
        "longitude": coordinates[0],
        "geometry_json": json.dumps(contour, ensure_ascii=False) if contour else None,
        "source": "geo.api.gouv.fr",
    }


def _fallback_row(code_insee: str) -> dict[str, Any] | None:
    centre = FALLBACK_CENTRES.get(code_insee)
    if centre is None:
        return None
    nom, latitude, longitude = centre
    return {
        "code_insee": code_insee,
        "nom": nom,
        "latitude": latitude,
        "longitude": longitude,
        "geometry_json": None,
        "source": "fallback",
    }


def _clean_geo_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["code_insee", "nom", "latitude", "longitude", "geometry_json", "source"])
    if df.empty:
        return df
    df["code_insee"] = df["code_insee"].astype("string").str.strip()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    return df.dropna(subset=["code_insee", "latitude", "longitude"]).drop_duplicates("code_insee")


def build_geo_commune(codes: list[str], *, fetch_remote: bool = True) -> pd.DataFrame:
    """Géo par liste de codes commune (un appel API par code)."""
    rows: list[dict[str, Any]] = []
    for code_insee in codes:
        payload = None
        if fetch_remote:
            try:
                payload = _fetch_commune(code_insee)
            except requests.RequestException:
                payload = None
        row = _row_from_payload(code_insee, payload) if payload else _fallback_row(code_insee)
        if row is not None:
            rows.append(row)
    return _clean_geo_df(rows)


def build_geo_departements(
    departements: list[str], *, arrondissements: list[str] = ARRONDISSEMENTS,
    fetch_remote: bool = True, with_contour: bool = True,
) -> pd.DataFrame:
    """Géo de toutes les communes des départements + arrondissements municipaux (carte multi-villes)."""
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    if fetch_remote:
        for departement in departements:
            try:
                items = _fetch_departement_communes(departement, with_contour=with_contour)
            except requests.RequestException:
                items = []
            for item in items:
                code = (item.get("code") or "").strip()
                if code and code not in seen:
                    rows.append(_row_from_payload(code, item))
                    seen.add(code)
    # Arrondissements (par code : l'endpoint /departements ne les liste pas), fallback hors-ligne.
    for code in arrondissements:
        if code in seen:
            continue
        payload = None
        if fetch_remote:
            try:
                payload = _fetch_commune(code, with_contour=with_contour)
            except requests.RequestException:
                payload = None
        row = _row_from_payload(code, payload) if payload else _fallback_row(code)
        if row is not None:
            rows.append(row)
            seen.add(code)
    return _clean_geo_df(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--departements", nargs="*", default=DEFAULT_DEPARTEMENTS,
                        help="Départements à couvrir (toutes leurs communes + arrondissements).")
    parser.add_argument("--codes", nargs="*", default=None,
                        help="Override : liste explicite de codes commune (ignore --departements).")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--no-contour", action="store_true",
                        help="N'embarque pas les contours (fichier léger ; carte en points). Recommandé en national.")
    args = parser.parse_args()

    REF_DIR.mkdir(exist_ok=True)
    if args.codes:
        df = build_geo_commune(args.codes, fetch_remote=not args.offline)
    else:
        df = build_geo_departements(
            args.departements, fetch_remote=not args.offline, with_contour=not args.no_contour
        )
    df.to_parquet(GEO_PARQUET, engine="pyarrow", index=False)
    print(f"geo_commune: {len(df)} lignes -> {GEO_PARQUET.relative_to(INCLUDE_DIR.parent)}")


if __name__ == "__main__":
    main()
