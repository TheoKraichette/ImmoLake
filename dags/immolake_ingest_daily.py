"""Ingestion quotidienne : API ADEME -> MinIO raw, en streaming par département.

Ne charge jamais l'ensemble en mémoire : pagination en générateur (`iter_dpe`) + flush
incrémental en Parquet partitionné (`dt=/dep=`). Permet de viser la France entière
(~15M DPE) sans OOM. Le périmètre est piloté par `ADEME_DEPARTEMENTS` (liste blanche).
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta
from io import BytesIO

import pandas as pd
import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, get_current_context, task

from hooks.ademe_api_hook import AdemeApiHook

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "immolake")
RAW_PREFIX = "raw/dpe"
LOGGER = logging.getLogger(__name__)


def _ds(ds: str | None) -> str:
    if ds:
        return ds
    return get_current_context()["ds"]


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    return int(value) if value else None


def _departements() -> list[str]:
    return [d.strip() for d in os.getenv("ADEME_DEPARTEMENTS", "").split(",") if d.strip()]


def _to_parquet_bytes(rows: list[dict]) -> bytes:
    buffer = BytesIO()
    pd.DataFrame(rows).to_parquet(buffer, engine="pyarrow", index=False, compression="zstd")
    return buffer.getvalue()


@dag(
    dag_id="immolake_ingest_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "ingestion"],
    max_active_tasks=int(os.getenv("ADEME_MAX_ACTIVE_TASKS", "4")),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=2),
    },
)
def immolake_ingest_daily():
    @task
    def list_departements() -> list[str]:
        deps = _departements()
        if not deps:
            LOGGER.warning("ADEME_DEPARTEMENTS vide : aucun departement a ingerer.")
        return deps

    @task
    def ingest_departement(dep: str, ds: str | None = None) -> dict:
        run_ds = _ds(ds)
        size = int(os.getenv("ADEME_PAGE_SIZE", "1000"))
        flush_rows = int(os.getenv("ADEME_FLUSH_ROWS", "50000"))
        max_pages = _optional_int_env("ADEME_MAX_PAGES")
        prefix = f"{RAW_PREFIX}/dt={run_ds}/dep={dep}/"

        s3 = S3Hook(aws_conn_id="minio_default")
        # Idempotence : on repart d'une partition propre (re-run = remplacement, pas de doublon).
        existing = s3.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix) or []
        if existing:
            s3.delete_objects(bucket=MINIO_BUCKET, keys=existing)

        buffer: list[dict] = []
        part = 0
        total = 0

        def flush() -> None:
            nonlocal buffer, part, total
            if not buffer:
                return
            s3.load_bytes(
                bytes_data=_to_parquet_bytes(buffer),
                key=f"{prefix}part-{part:04d}.parquet",
                bucket_name=MINIO_BUCKET,
                replace=True,
            )
            total += len(buffer)
            part += 1
            buffer = []

        for page_rows in AdemeApiHook().iter_dpe(departement=dep, size=size, max_pages=max_pages):
            buffer.extend(page_rows)
            if len(buffer) >= flush_rows:
                flush()
        flush()

        LOGGER.info("Ingestion dep=%s dt=%s : %s lignes, %s part(s)", dep, run_ds, total, part)
        return {"departement": dep, "rows": total, "parts": part, "dt": run_ds}

    ingest_departement.expand(dep=list_departements())


immolake_ingest_daily()
