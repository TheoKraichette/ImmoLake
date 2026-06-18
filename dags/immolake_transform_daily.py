"""Transformation en DuckDB : raw -> silver -> gold (Parquet dans MinIO).

Tout en SQL DuckDB streaming (mémoire bornée) : remplace les helpers pandas qui calaient
au-delà de ~200k logements/ville. Les requêtes vivent dans `include/sql/`. Plus aucun Postgres.

Chaînage event-driven : **planifié sur l'asset RAW_DPE** (déclenché par l'ingestion),
**produit l'asset GOLD_FACT** (déclenche les marts) — mais seulement si le gate qualité passe.
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
from assets import GOLD_FACT, RAW_DPE, RETRY_ARGS  # noqa: E402
from duckdb_lake import connect, run_sql  # noqa: E402

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "immolake")
REF_DIM_COMMUNE = "ref/dim_commune/*.parquet"
REF_DIM_DPE = "ref/dim_dpe/*.parquet"
VALID_DPE = "('A','B','C','D','E','F','G')"
LOGGER = logging.getLogger(__name__)


def _ds(ds: str | None = None) -> str:
    """Résout le `ds` métier : priorité au `ds` porté par l'asset amont (extra), sinon le ds du run."""
    if ds:
        return ds
    ctx = get_current_context()
    for events in (ctx.get("triggering_asset_events") or {}).values():
        for event in events:
            extra = getattr(event, "extra", None) or {}
            if extra.get("ds"):
                return extra["ds"]
    return ctx["ds"]


def _s3(path: str) -> str:
    return f"s3://{MINIO_BUCKET}/{path}"


def _purge(s3: S3Hook, prefix: str) -> None:
    """Idempotence : vide la partition de sortie avant de la réécrire."""
    keys = s3.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix) or []
    if keys:
        s3.delete_objects(bucket=MINIO_BUCKET, keys=keys)


@dag(
    dag_id="immolake_transform_daily",
    schedule=[RAW_DPE],
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "transform", "duckdb"],
    default_args={**RETRY_ARGS, "execution_timeout": timedelta(hours=2)},
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

    @task.short_circuit
    def data_quality_gate(ds: str | None = None) -> bool:
        """Gate qualité sur le gold (DuckDB) : un gold invalide court-circuite kpi + marts."""
        run_ds = _ds(ds)
        fact = _s3(f"gold/fact_biens/dt={run_ds}/data.parquet")
        con = connect()
        try:
            total, n_null, n_bad_dpe, n_future = con.execute(
                f"""
                SELECT count(*),
                       count(*) FILTER (WHERE code_insee IS NULL),
                       count(*) FILTER (WHERE etiquette NOT IN {VALID_DPE}),
                       count(*) FILTER (WHERE dt > current_date)
                FROM read_parquet('{fact}')
                """
            ).fetchone()
        finally:
            con.close()

        failures = []
        if total == 0:
            failures.append("not_empty")
        if n_null:
            failures.append(f"code_insee_null={n_null}")
        if n_bad_dpe:
            failures.append(f"bad_dpe={n_bad_dpe}")
        if n_future:
            failures.append(f"future_date={n_future}")

        if failures:
            LOGGER.warning("Data quality KO %s -> court-circuit (kpi + marts non declenches)", failures)
            return False
        LOGGER.info("Data quality OK : %s lignes valides", total)
        return True

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

    @task(outlets=[GOLD_FACT])
    def mark_gold_ready(ds: str | None = None) -> dict:
        """Produit l'asset GOLD_FACT (déclenche les marts) avec le `ds` métier."""
        run_ds = _ds(ds)
        get_current_context()["outlet_events"][GOLD_FACT].extra = {"ds": run_ds}
        LOGGER.info("gold pret pour dt=%s -> marts declenches", run_ds)
        return {"ds": run_ds}

    dpe_silver = raw_to_silver_dpe()
    dvf_silver = raw_to_silver_dvf()
    dvf_to_raw() >> dvf_silver
    fact = build_fact_biens()
    [dpe_silver, dvf_silver] >> fact >> data_quality_gate() >> build_kpi_commune() >> mark_gold_ready()


immolake_transform_daily()
