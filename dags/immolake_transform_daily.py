"""Transformation staging.dpe -> dwh.fact_biens (idempotent par dt)."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="immolake_transform_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "transform"],
)
def immolake_transform_daily():
    @task
    def refresh_dimensions() -> None:
        # TODO: refresh_dim_commune.sql
        raise NotImplementedError

    @task
    def load_fact_biens(ds: str | None = None) -> None:
        # TODO: transform_fact_biens.sql (DELETE + INSERT WHERE dt = ds)
        raise NotImplementedError

    refresh_dimensions() >> load_fact_biens()


immolake_transform_daily()
