"""Transformation quotidienne en DuckDB : raw -> silver -> gold (Parquet dans MinIO).

Tout en SQL DuckDB streaming (mémoire bornée) : remplace les helpers pandas qui calaient
au-delà de ~200k logements/ville. Les requêtes vivent dans `include/sql/`. Plus aucun Postgres :
les dimensions sont jointes depuis `ref/*.parquet`.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, get_current_context, task

sys.path.insert(0, "/opt/airflow/include")
from duckdb_lake import connect, run_sql  # noqa: E402

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "immolake")
REF_DIM_COMMUNE = "ref/dim_commune/*.parquet"
REF_DIM_DPE = "ref/dim_dpe/*.parquet"
LOGGER = logging.getLogger(__name__)


def _ds(ds: str | None) -> str:
    return ds or get_current_context()["ds"]


def _s3(path: str) -> str:
    return f"s3://{MINIO_BUCKET}/{path}"


def _purge(s3: S3Hook, prefix: str) -> None:
    """Idempotence : vide la partition de sortie avant de la réécrire."""
    keys = s3.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix) or []
    if keys:
        s3.delete_objects(bucket=MINIO_BUCKET, keys=keys)


@dag(
    dag_id="immolake_transform_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "transform", "duckdb"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2), "execution_timeout": timedelta(hours=2)},
)
def immolake_transform_daily():
    @task
    def raw_to_silver_dpe(ds: str | None = None) -> str:
        run_ds = _ds(ds)
        s3 = S3Hook(aws_conn_id="minio_default")
        out = f"silver/dpe/dt={run_ds}/"
        _purge(s3, out)
        con = connect()
        run_sql(
            con, "silver_dpe.sql",
            ds=run_ds,
            raw_glob=_s3(f"raw/dpe/dt={run_ds}/dep=*/*.parquet"),
            out=_s3(f"{out}data.parquet"),
        )
        con.close()
        return out

    @task
    def dvf_to_raw(ds: str | None = None) -> str:
        run_ds = _ds(ds)
        url = os.getenv("DVF_CSV_URL")
        if not url:
            raise AirflowException("DVF_CSV_URL doit etre renseigne pour produire raw/dvf")
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        key = f"raw/dvf/dt={run_ds}/data.csv.gz"
        S3Hook(aws_conn_id="minio_default").load_bytes(
            bytes_data=response.content, key=key, bucket_name=MINIO_BUCKET, replace=True
        )
        return key

    @task
    def raw_to_silver_dvf(ds: str | None = None) -> str:
        run_ds = _ds(ds)
        s3 = S3Hook(aws_conn_id="minio_default")
        out = f"silver/dvf/dt={run_ds}/"
        _purge(s3, out)
        con = connect()
        run_sql(
            con, "silver_dvf.sql",
            dvf_csv=_s3(f"raw/dvf/dt={run_ds}/data.csv.gz"),
            out=_s3(f"{out}data.parquet"),
        )
        con.close()
        return out

    @task
    def build_fact_biens(ds: str | None = None) -> int:
        run_ds = _ds(ds)
        s3 = S3Hook(aws_conn_id="minio_default")
        out = f"gold/fact_biens/dt={run_ds}/"
        _purge(s3, out)
        con = connect()
        run_sql(
            con, "gold_fact_biens.sql",
            ds=run_ds,
            silver_dpe=_s3(f"silver/dpe/dt={run_ds}/data.parquet"),
            silver_dvf=_s3(f"silver/dvf/dt={run_ds}/data.parquet"),
            dim_commune=_s3(REF_DIM_COMMUNE),
            dim_dpe=_s3(REF_DIM_DPE),
            out=_s3(f"{out}data.parquet"),
        )
        n = con.execute(f"SELECT count(*) FROM read_parquet('{_s3(out + 'data.parquet')}')").fetchone()[0]
        con.close()
        LOGGER.info("gold/fact_biens dt=%s : %s lignes", run_ds, n)
        return n

    @task
    def build_kpi_commune(ds: str | None = None) -> str:
        run_ds = _ds(ds)
        s3 = S3Hook(aws_conn_id="minio_default")
        out = f"gold/kpi_commune/dt={run_ds}/"
        _purge(s3, out)
        con = connect()
        run_sql(
            con, "gold_kpi_commune.sql",
            ds=run_ds,
            fact=_s3(f"gold/fact_biens/dt={run_ds}/data.parquet"),
            dim_dpe=_s3(REF_DIM_DPE),
            out=_s3(f"{out}data.parquet"),
        )
        con.close()
        return out

    dpe_silver = raw_to_silver_dpe()
    dvf_silver = raw_to_silver_dvf()
    dvf_to_raw() >> dvf_silver
    fact = build_fact_biens()
    [dpe_silver, dvf_silver] >> fact >> build_kpi_commune()


immolake_transform_daily()
