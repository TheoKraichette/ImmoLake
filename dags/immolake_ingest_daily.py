"""Ingestion quotidienne : API ADEME -> MinIO raw -> staging.dpe."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


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
        # TODO: AdemeApiHook -> upload JSON brut dans MinIO raw/dt=ds
        raise NotImplementedError

    @task
    def load_to_staging(raw_key: str, ds: str | None = None) -> None:
        # TODO: charger raw -> staging.dpe (idempotent par dt)
        raise NotImplementedError

    load_to_staging(extract_to_raw())


immolake_ingest_daily()
