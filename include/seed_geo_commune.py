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


def _fetch_commune(code_insee: str, timeout: float = 10.0) -> dict[str, Any] | None:
    response = requests.get(
        f"https://geo.api.gouv.fr/communes/{code_insee}",
        params={"fields": "nom,centre,contour"},
        timeout=timeout,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


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


def build_geo_commune(codes: list[str], *, fetch_remote: bool = True) -> pd.DataFrame:
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

    df = pd.DataFrame(rows, columns=["code_insee", "nom", "latitude", "longitude", "geometry_json", "source"])
    if df.empty:
        return df
    df["code_insee"] = df["code_insee"].astype("string").str.strip()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    return df.dropna(subset=["code_insee", "latitude", "longitude"]).drop_duplicates("code_insee")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--codes", nargs="*", default=sorted(FALLBACK_CENTRES))
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()

    REF_DIR.mkdir(exist_ok=True)
    df = build_geo_commune(args.codes, fetch_remote=not args.offline)
    df.to_parquet(GEO_PARQUET, engine="pyarrow", index=False)
    print(f"geo_commune: {len(df)} lignes -> {GEO_PARQUET.relative_to(INCLUDE_DIR.parent)}")


if __name__ == "__main__":
    main()
