"""Ingestion quotidienne : API ADEME -> MinIO raw."""
from __future__ import annotations

import json
import os

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, get_current_context, task

from hooks.ademe_api_hook import AdemeApiHook

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "immolake")
RAW_PREFIX = "raw/dpe"


def _ds(ds: str | None) -> str:
    if ds:
        return ds
    return get_current_context()["ds"]


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    return int(value) if value else None


@dag(
    dag_id="immolake_ingest_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "ingestion"],
)
def immolake_ingest_daily():
    @task
    def extract_to_raw(ds: str | None = None) -> str:
        run_ds = _ds(ds)
        rows = AdemeApiHook().get_dpe(
            code_postal=os.getenv("ADEME_CODE_POSTAL") or None,
            code_insee=os.getenv("ADEME_CODE_INSEE") or None,
            size=int(os.getenv("ADEME_PAGE_SIZE", "1000")),
            max_pages=_optional_int_env("ADEME_MAX_PAGES"),
        )
        raw_key = f"{RAW_PREFIX}/dt={run_ds}/data.json"
        payload = {
            "dt": run_ds,
            "source": "ademe:dpe03existant",
            "count": len(rows),
            "results": rows,
        }

        S3Hook(aws_conn_id="minio_default").load_string(
            string_data=json.dumps(payload, ensure_ascii=False),
            key=raw_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        return raw_key

    extract_to_raw()


immolake_ingest_daily()
