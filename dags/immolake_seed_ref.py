"""Seed des dimensions de référence (`ref/`) dans MinIO, en Parquet.

DAG manuel (`schedule=None`) : reloge les dimensions hors Postgres, idempotent (`replace`).
Les Parquet sont aussi committés dans `include/ref/` et chargés au boot par `minio-init` ;
ce DAG les régénère depuis `include/communes.json` + `arrondissements.json`.
"""
from __future__ import annotations

import os
import sys

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/include")
from seed_ref import build_dimensions, to_parquet_bytes  # noqa: E402

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "immolake")


@dag(
    dag_id="immolake_seed_ref",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "ref", "duckdb"],
)
def immolake_seed_ref():
    @task
    def seed() -> dict[str, int]:
        s3 = S3Hook(aws_conn_id="minio_default")
        counts: dict[str, int] = {}
        for name, df in build_dimensions().items():
            s3.load_bytes(
                bytes_data=to_parquet_bytes(df),
                key=f"ref/{name}/{name}.parquet",
                bucket_name=MINIO_BUCKET,
                replace=True,
            )
            counts[name] = len(df)
        return counts

    seed()


immolake_seed_ref()
