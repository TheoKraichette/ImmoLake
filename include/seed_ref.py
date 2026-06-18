"""Génère les dimensions de référence (`ref/`) en Parquet depuis les JSON committés.

Reloge les dimensions hors Postgres : DuckDB lit directement `ref/*.parquet` dans MinIO.
Réutilisé par le DAG `immolake_seed_ref` (upload MinIO) et par la régénération des Parquet
committés (`python include/seed_ref.py` → `include/ref/`, chargés au boot par `minio-init`).
"""
from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path

import pandas as pd

INCLUDE_DIR = Path(__file__).resolve().parent
COMMUNES_JSON = INCLUDE_DIR / "communes.json"
ARRONDISSEMENTS_JSON = INCLUDE_DIR / "arrondissements.json"
REF_DIR = INCLUDE_DIR / "ref"

# Échelle DPE (A→G) ; F et G = passoires thermiques (loi Climat & Résilience).
DIM_DPE = pd.DataFrame(
    [
        ("A", 0, 70, False),
        ("B", 71, 110, False),
        ("C", 111, 180, False),
        ("D", 181, 250, False),
        ("E", 251, 330, False),
        ("F", 331, 420, True),
        ("G", 421, 9999, True),
    ],
    columns=["etiquette", "conso_min", "conso_max", "label_passoire"],
)

# Types de bien (libellés normalisés par le transform) : jointure sur le libellé, pas d'id.
DIM_TYPE_BIEN = pd.DataFrame({"type_bien": ["appartement", "maison", "autre"]})


def _load_json(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_dim_commune(
    communes_path: Path = COMMUNES_JSON,
    arrondissements_path: Path = ARRONDISSEMENTS_JSON,
) -> pd.DataFrame:
    """dim_commune = communes INSEE + arrondissements municipaux (75/69/13).

    Mapping JSON → schéma : `code`→`code_insee`, `codeDepartement`→`departement`,
    `codeRegion`→`region` (les JSON ne portent pas ces noms cibles).
    """
    rows = _load_json(communes_path) + _load_json(arrondissements_path)
    df = pd.DataFrame(rows).rename(
        columns={"code": "code_insee", "codeDepartement": "departement", "codeRegion": "region"}
    )
    df = df[["code_insee", "nom", "departement", "region", "population"]].copy()
    df["code_insee"] = df["code_insee"].astype("string").str.strip()
    df["nom"] = df["nom"].astype("string")
    df["departement"] = df["departement"].astype("string").str.strip()
    df["region"] = df["region"].astype("string").str.strip()
    df["population"] = pd.to_numeric(df["population"], errors="coerce").astype("Int64")
    return df.drop_duplicates(subset=["code_insee"]).reset_index(drop=True)


def build_dimensions() -> dict[str, pd.DataFrame]:
    """Les 3 dimensions de référence, prêtes à écrire en Parquet."""
    return {
        "dim_commune": build_dim_commune(),
        "dim_dpe": DIM_DPE.copy(),
        "dim_type_bien": DIM_TYPE_BIEN.copy(),
    }


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    return buffer.getvalue()


def main() -> None:
    """Régénère les Parquet committés dans `include/ref/` (chargés au boot par minio-init)."""
    REF_DIR.mkdir(exist_ok=True)
    for name, df in build_dimensions().items():
        df.to_parquet(REF_DIR / f"{name}.parquet", engine="pyarrow", index=False)
        print(f"{name}: {len(df)} lignes -> include/ref/{name}.parquet")


if __name__ == "__main__":
    main()
