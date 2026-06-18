"""Construction quotidienne des marts analytiques (Parquet dans MinIO), en DuckDB.

Remplace `immolake_analytics_daily` (qui chargeait PostgreSQL) : plus aucun `PostgresHook`.
DuckDB lit le gold + `ref/` et matérialise `mart_commune`, `mart_commune_type`,
`mart_opportunites`. `detect_and_alert` lit les opportunités (alerte WhatsApp non activée — bonus).
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, get_current_context, task

sys.path.insert(0, "/opt/airflow/include")
from duckdb_lake import connect, run_sql  # noqa: E402

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "immolake")
OPP_K = os.getenv("OPPORTUNITE_K", "1.0")
OPP_SEUIL = os.getenv("OPPORTUNITE_SEUIL_NB_DPE", "30")
LOGGER = logging.getLogger(__name__)


def _ds(ds: str | None) -> str:
    return ds or get_current_context()["ds"]


def _s3(path: str) -> str:
    return f"s3://{MINIO_BUCKET}/{path}"


def _purge(s3: S3Hook, prefix: str) -> None:
    keys = s3.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix) or []
    if keys:
        s3.delete_objects(bucket=MINIO_BUCKET, keys=keys)


@dag(
    dag_id="immolake_marts_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "marts", "duckdb"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
)
def immolake_marts_daily():
    @task
    def build_marts(ds: str | None = None) -> dict:
        run_ds = _ds(ds)
        s3 = S3Hook(aws_conn_id="minio_default")
        fact = _s3(f"gold/fact_biens/dt={run_ds}/data.parquet")
        dim_commune = _s3("ref/dim_commune/*.parquet")
        dim_dpe = _s3("ref/dim_dpe/*.parquet")
        mart_ct = _s3("gold/mart_commune_type/data.parquet")
        for name in ("mart_commune", "mart_commune_type", "mart_opportunites"):
            _purge(s3, f"gold/{name}/")

        con = connect()
        run_sql(con, "mart_commune.sql", fact=fact, dim_dpe=dim_dpe, dim_commune=dim_commune,
                out=_s3("gold/mart_commune/data.parquet"))
        run_sql(con, "mart_commune_type.sql", fact=fact, dim_dpe=dim_dpe, dim_commune=dim_commune,
                out=mart_ct)
        run_sql(con, "mart_opportunites.sql", mart_commune_type=mart_ct, k=OPP_K, seuil=OPP_SEUIL,
                out=_s3("gold/mart_opportunites/data.parquet"))
        con.close()
        return {"dt": run_ds}

    @task
    def detect_and_alert(marts: dict) -> None:
        con = connect()
        try:
            n = con.execute(
                f"SELECT count(*) FROM read_parquet('{_s3('gold/mart_opportunites/data.parquet')}') "
                "WHERE est_opportunite"
            ).fetchone()[0]
        finally:
            con.close()
        LOGGER.info("Opportunites detectees : %s (alerte WhatsApp non activee - bonus). marts=%s", n, marts)

    detect_and_alert(build_marts())


immolake_marts_daily()
