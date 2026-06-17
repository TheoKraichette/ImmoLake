"""Agrégats analytics + alerte Telegram (bonus)."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="immolake_analytics_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "analytics"],
)
def immolake_analytics_daily():
    @task
    def build_kpis(ds: str | None = None) -> None:
        # TODO: build_kpi_commune.sql (idempotent par dt)
        raise NotImplementedError

    @task
    def detect_and_alert(ds: str | None = None) -> None:
        # TODO: détection anomalies -> alerte Telegram
        raise NotImplementedError

    build_kpis() >> detect_and_alert()


immolake_analytics_daily()
