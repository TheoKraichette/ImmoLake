"""Connexion DuckDB pour les transforms Airflow : lit/écrit le Parquet dans MinIO (httpfs).

Streaming + mémoire bornée (`memory_limit` + spill-to-disk) → tient les gros volumes (15M)
sans OOM, là où pandas calait. Les requêtes vivent dans `include/sql/*.sql` (paramétrées par
chemins `${...}` pour être rejouables ET testables hors S3).
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

SQL_DIR = Path(__file__).resolve().parent / "sql"


def _lit(value: str) -> str:
    """Littéral SQL échappé (pour CREATE SECRET / SET, qui n'acceptent pas de paramètres liés)."""
    return "'" + str(value).replace("'", "''") + "'"


def connect() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB configurée pour MinIO + garde-fous mémoire."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        "CREATE OR REPLACE SECRET minio (TYPE S3, "
        f"KEY_ID {_lit(os.getenv('MINIO_ROOT_USER', 'minio_admin'))}, "
        f"SECRET {_lit(os.getenv('MINIO_ROOT_PASSWORD', 'minio_password_2026'))}, "
        f"ENDPOINT {_lit(os.getenv('MINIO_ENDPOINT', 'minio:9000'))}, "
        "URL_STYLE 'path', USE_SSL false)"
    )
    con.execute(f"SET memory_limit = {_lit(os.getenv('DUCKDB_MEMORY_LIMIT', '6GB'))};")
    con.execute("SET temp_directory = '/tmp/duckdb_spill';")
    con.execute("SET preserve_insertion_order = false;")
    return con


def render(name: str, **params: object) -> str:
    """Charge `include/sql/<name>` et substitue les jetons `${cle}` (pas de braces → pas de conflit SQL)."""
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    for key, value in params.items():
        sql = sql.replace("${" + key + "}", str(value))
    return sql


def run_sql(con: duckdb.DuckDBPyConnection, name: str, **params: object) -> None:
    con.execute(render(name, **params))
