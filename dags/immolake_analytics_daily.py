"""Chargement quotidien du gold MinIO vers PostgreSQL serving."""
from __future__ import annotations

import logging
import os
from io import BytesIO
from typing import Any

import pandas as pd
import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "immolake")
GOLD_FACT_PREFIX = "gold/fact_biens"
GOLD_KPI_PREFIX = "gold/kpi_commune"
POSTGRES_CONN_ID = "dwh_postgres"
LOGGER = logging.getLogger(__name__)

FACT_COLUMNS = [
    "dt",
    "code_insee",
    "etiquette",
    "type_bien_id",
    "surface",
    "prix",
    "prix_m2",
    "conso_energie",
]
KPI_COLUMNS = [
    "dt",
    "code_insee",
    "prix_m2_median",
    "pct_passoires",
    "decote_passoire_pct",
    "nb_transactions",
]

LOAD_TARGETS = {
    "dwh.fact_biens": FACT_COLUMNS,
    "analytics.kpi_commune_mensuel": KPI_COLUMNS,
}


def _ds(ds: str | None) -> str:
    if ds:
        return ds
    return get_current_context()["ds"]


def _read_gold_partition(s3_hook: S3Hook, prefix: str, run_ds: str) -> pd.DataFrame:
    partition_prefix = f"{prefix}/dt={run_ds}/"
    keys = [
        key
        for key in s3_hook.list_keys(bucket_name=MINIO_BUCKET, prefix=partition_prefix) or []
        if key.endswith(".parquet")
    ]
    if not keys:
        raise AirflowException(f"Aucun fichier Parquet trouve dans s3://{MINIO_BUCKET}/{partition_prefix}")

    s3_client = s3_hook.get_conn()
    frames = []
    for key in sorted(keys):
        obj = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
        frames.append(pd.read_parquet(BytesIO(obj["Body"].read())))

    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        raise AirflowException(f"Partition gold vide dans s3://{MINIO_BUCKET}/{partition_prefix}")
    return df


def _prepare_load_frame(df: pd.DataFrame, columns: list[str], run_ds: str) -> pd.DataFrame:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise AirflowException(f"Colonnes gold manquantes: {', '.join(missing)}")

    load_df = df.loc[:, columns].copy()
    load_df["dt"] = pd.to_datetime(load_df["dt"], errors="coerce").dt.date
    expected_dt = pd.to_datetime(run_ds).date()
    load_df = load_df[load_df["dt"] == expected_dt].reset_index(drop=True)
    if load_df.empty:
        raise AirflowException(f"Aucune ligne gold pour dt={run_ds}")

    return load_df


def _value_or_none(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def _rows_for_insert(df: pd.DataFrame) -> list[tuple[Any, ...]]:
    return [tuple(_value_or_none(value) for value in row) for row in df.itertuples(index=False, name=None)]


def _load_dataframe_idempotent(
    postgres_hook: PostgresHook,
    table: str,
    df: pd.DataFrame,
    columns: list[str],
    run_ds: str,
) -> int:
    if table not in LOAD_TARGETS:
        raise ValueError(f"Table de chargement non autorisee: {table}")

    load_df = _prepare_load_frame(df, columns, run_ds)
    rows = _rows_for_insert(load_df)
    column_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    delete_sql = f"DELETE FROM {table} WHERE dt = %s"
    insert_sql = f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})"

    conn = postgres_hook.get_conn()
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(delete_sql, (run_ds,))
                cursor.executemany(insert_sql, rows)
    except Exception:
        LOGGER.exception("Chargement idempotent echoue pour %s dt=%s", table, run_ds)
        raise

    return len(rows)


def _load_gold_to_postgres(run_ds: str) -> dict[str, int]:
    s3_hook = S3Hook(aws_conn_id="minio_default")
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    fact_df = _read_gold_partition(s3_hook, GOLD_FACT_PREFIX, run_ds)
    kpi_df = _read_gold_partition(s3_hook, GOLD_KPI_PREFIX, run_ds)

    fact_count = _load_dataframe_idempotent(
        postgres_hook=postgres_hook,
        table="dwh.fact_biens",
        df=fact_df,
        columns=FACT_COLUMNS,
        run_ds=run_ds,
    )
    kpi_count = _load_dataframe_idempotent(
        postgres_hook=postgres_hook,
        table="analytics.kpi_commune_mensuel",
        df=kpi_df,
        columns=KPI_COLUMNS,
        run_ds=run_ds,
    )
    return {"fact_biens": fact_count, "kpi_commune_mensuel": kpi_count}


@dag(
    dag_id="immolake_analytics_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "analytics", "serving"],
)
def immolake_analytics_daily():
    @task
    def load_gold_to_postgres(ds: str | None = None) -> dict[str, int]:
        run_ds = _ds(ds)
        counts = _load_gold_to_postgres(run_ds)
        LOGGER.info(
            "Chargement gold -> Postgres termine pour dt=%s: %s fact_biens, %s kpi_commune_mensuel",
            run_ds,
            counts["fact_biens"],
            counts["kpi_commune_mensuel"],
        )
        return counts

    @task
    def detect_and_alert(load_counts: dict[str, int]) -> None:
        LOGGER.info("Alerte Telegram non activee pour le MVP. Chargement: %s", load_counts)

    detect_and_alert(load_gold_to_postgres())


immolake_analytics_daily()
