"""Transformation raw DPE -> silver Parquet -> gold facts."""
from __future__ import annotations

import json
import os
from io import BytesIO
from typing import Any

import pandas as pd
import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, get_current_context, task

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "immolake")
RAW_PREFIX = "raw/dpe"
SILVER_PREFIX = "silver/dpe"
SILVER_COLUMNS = [
    "numero_dpe",
    "dt",
    "code_insee",
    "code_postal",
    "type_batiment",
    "surface_habitable",
    "etiquette_dpe",
    "conso_energie",
    "date_etablissement",
]


def _ds(ds: str | None) -> str:
    if ds:
        return ds
    return get_current_context()["ds"]


def _first_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _clean_dpe_rows(rows: list[dict[str, Any]], run_ds: str) -> pd.DataFrame:
    records = []
    for row in rows:
        records.append(
            {
                "numero_dpe": row.get("numero_dpe"),
                "dt": run_ds,
                "code_insee": _first_value(row, "code_insee_ban", "code_insee"),
                "code_postal": _first_value(row, "code_postal_ban", "code_postal_brut", "code_postal"),
                "type_batiment": row.get("type_batiment"),
                "surface_habitable": _first_value(
                    row,
                    "surface_habitable_logement",
                    "surface_habitable_immeuble",
                ),
                "etiquette_dpe": row.get("etiquette_dpe"),
                "conso_energie": _first_value(row, "conso_5_usages_par_m2_ep", "conso_5_usages_ep"),
                "date_etablissement": row.get("date_etablissement_dpe"),
            }
        )

    df = pd.DataFrame.from_records(records, columns=SILVER_COLUMNS)
    if df.empty:
        return df.astype(
            {
                "numero_dpe": "string",
                "dt": "string",
                "code_insee": "string",
                "code_postal": "string",
                "type_batiment": "string",
                "etiquette_dpe": "string",
            }
        )

    df["numero_dpe"] = df["numero_dpe"].astype("string").str.strip()
    df["dt"] = df["dt"].astype("string")
    df["code_insee"] = df["code_insee"].astype("string").str.strip()
    df["code_postal"] = df["code_postal"].astype("string").str.replace(r"\.0$", "", regex=True).str.strip()
    df["type_batiment"] = df["type_batiment"].astype("string").str.strip().str.lower()
    df["surface_habitable"] = pd.to_numeric(
        df["surface_habitable"].astype("string").str.replace(",", ".", regex=False),
        errors="coerce",
    )
    df["etiquette_dpe"] = df["etiquette_dpe"].astype("string").str.strip().str.upper()
    df["conso_energie"] = pd.to_numeric(
        df["conso_energie"].astype("string").str.replace(",", ".", regex=False),
        errors="coerce",
    )
    df["date_etablissement"] = pd.to_datetime(df["date_etablissement"], errors="coerce").dt.date

    df = df.dropna(subset=["numero_dpe", "code_insee", "etiquette_dpe"])
    df = df[df["surface_habitable"].isna() | (df["surface_habitable"] > 0)]
    df = df.drop_duplicates(subset=["numero_dpe"], keep="last")
    return df.reset_index(drop=True)


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    return buffer.getvalue()


@dag(
    dag_id="immolake_transform_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "transform"],
)
def immolake_transform_daily():
    @task
    def raw_to_silver(ds: str | None = None) -> str:
        run_ds = _ds(ds)
        raw_key = f"{RAW_PREFIX}/dt={run_ds}/data.json"
        silver_key = f"{SILVER_PREFIX}/dt={run_ds}/data.parquet"

        s3 = S3Hook(aws_conn_id="minio_default")
        payload = json.loads(s3.read_key(key=raw_key, bucket_name=MINIO_BUCKET))
        df = _clean_dpe_rows(payload.get("results", []), run_ds)
        s3.load_bytes(
            bytes_data=_to_parquet_bytes(df),
            key=silver_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        return silver_key

    @task
    def load_fact_biens(ds: str | None = None) -> None:
        # TODO: silver -> gold/fact_biens (Parquet), then serving load in later issue
        raise NotImplementedError

    raw_to_silver() >> load_fact_biens()


immolake_transform_daily()
