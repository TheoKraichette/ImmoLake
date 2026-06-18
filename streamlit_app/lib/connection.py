"""DuckDB connection helpers for MinIO-backed Parquet."""
from __future__ import annotations

import os

import duckdb
import streamlit as st

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minio_password_2026")
BUCKET = os.getenv("MINIO_BUCKET", "immolake")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    """Return a cached DuckDB connection configured for MinIO."""
    con = duckdb.connect(database=":memory:")
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


def bucket() -> str:
    return BUCKET


def gold(name: str) -> str:
    return f"s3://{BUCKET}/gold/{name}/**/*.parquet"


def ref(name: str) -> str:
    return f"s3://{BUCKET}/ref/{name}/*.parquet"
