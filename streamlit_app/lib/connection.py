"""Connexion DuckDB partagée vers le Data Lake MinIO (httpfs / S3).

DuckDB interroge directement le Parquet (`gold/`, `ref/`) dans MinIO : pas de Postgres
de serving. La connexion est mise en cache pour être réutilisée entre les reruns Streamlit.
"""
from __future__ import annotations

import os

import duckdb
import streamlit as st

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minio_password_2026")
BUCKET = os.getenv("MINIO_BUCKET", "immolake")


def _sql_literal(value: str) -> str:
    """Échappe une valeur pour l'injecter dans une commande DDL DuckDB (CREATE SECRET)."""
    return "'" + value.replace("'", "''") + "'"


@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB configurée pour lire MinIO (partagée entre reruns / sessions)."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio (
            TYPE S3,
            KEY_ID {_sql_literal(MINIO_KEY)},
            SECRET {_sql_literal(MINIO_SECRET)},
            ENDPOINT {_sql_literal(MINIO_ENDPOINT)},
            URL_STYLE 'path',
            USE_SSL false
        );
        """
    )
    # Garde-fous mémoire : la machine de dev n'alloue que ~9 Go à Docker.
    con.execute("SET memory_limit='6GB';")
    con.execute("SET temp_directory='/tmp/duckdb_spill';")
    con.execute("SET preserve_insertion_order=false;")
    return con


def gold(name: str) -> str:
    """Glob `s3://` vers une zone gold partitionnée, ex. `gold('fact_biens')`."""
    return f"s3://{BUCKET}/gold/{name}/**/*.parquet"


def ref(name: str) -> str:
    """Glob `s3://` vers le référentiel Parquet, ex. `ref('dim_commune')`."""
    return f"s3://{BUCKET}/ref/{name}/*.parquet"
