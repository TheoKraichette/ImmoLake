"""Idempotence tests for gold Parquet writers."""
from io import BytesIO

import pandas as pd

from immolake_transform_daily import _write_gold_kpi_partition, _write_gold_partition


class FakeS3Client:
    def __init__(self, objects):
        self.objects = objects

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        return {"Body": BytesIO(self.objects[Key])}


class FakeS3Hook:
    def __init__(self):
        self.objects = {}
        self.deleted_batches = []

    def list_keys(self, bucket_name, prefix):
        return sorted(key for key in self.objects if key.startswith(prefix))

    def delete_objects(self, bucket, keys):
        self.deleted_batches.append(list(keys))
        for key in keys:
            self.objects.pop(key, None)

    def load_bytes(self, bytes_data, key, bucket_name, replace=False):
        if not replace and key in self.objects:
            raise AssertionError(f"{key} already exists")
        self.objects[key] = bytes_data

    def get_conn(self):
        return FakeS3Client(self.objects)


def _read_parquet_object(s3_hook, key):
    return pd.read_parquet(BytesIO(s3_hook.objects[key]))


def test_write_gold_partition_is_replayable_and_replaces_partition():
    s3 = FakeS3Hook()
    run_ds = "2026-06-17"
    stale_key = f"gold/fact_biens/dt={run_ds}/old.parquet"
    s3.objects[stale_key] = b"stale"
    fact_df = pd.DataFrame(
        [
            {
                "dt": pd.to_datetime(run_ds).date(),
                "code_insee": "75101",
                "etiquette": "A",
                "type_bien_id": 1,
                "surface": 42.0,
                "prix": 420000.0,
                "prix_m2": 10000.0,
                "conso_energie": 60.0,
            }
        ]
    )

    key = _write_gold_partition(s3, fact_df, run_ds)
    first = _read_parquet_object(s3, key)
    replay_key = _write_gold_partition(s3, fact_df, run_ds)
    replay = _read_parquet_object(s3, replay_key)

    assert key == replay_key == f"gold/fact_biens/dt={run_ds}/fact_biens.parquet"
    assert stale_key not in s3.objects
    assert sorted(s3.objects) == [key]
    pd.testing.assert_frame_equal(first, replay)


def test_write_gold_kpi_partition_is_replayable_and_replaces_partition():
    s3 = FakeS3Hook()
    run_ds = "2026-06-17"
    stale_key = f"gold/kpi_commune/dt={run_ds}/old.parquet"
    s3.objects[stale_key] = b"stale"
    kpi_df = pd.DataFrame(
        [
            {
                "dt": pd.to_datetime(run_ds).date(),
                "code_insee": "75101",
                "prix_m2_median": 9000.0,
                "pct_passoires": 50.0,
                "decote_passoire_pct": -36.36,
                "nb_transactions": 4,
            }
        ]
    )

    key = _write_gold_kpi_partition(s3, kpi_df, run_ds)
    first = _read_parquet_object(s3, key)
    replay_key = _write_gold_kpi_partition(s3, kpi_df, run_ds)
    replay = _read_parquet_object(s3, replay_key)

    assert key == replay_key == f"gold/kpi_commune/dt={run_ds}/kpi_commune.parquet"
    assert stale_key not in s3.objects
    assert sorted(s3.objects) == [key]
    pd.testing.assert_frame_equal(first, replay)
