"""Unit tests for gold -> Postgres serving load."""
from __future__ import annotations

import pandas as pd
import pytest
from airflow.exceptions import AirflowException

from immolake_analytics_daily import (
    FACT_COLUMNS,
    KPI_COLUMNS,
    _load_dataframe_idempotent,
    _prepare_load_frame,
    _rows_for_insert,
)


class FakeCursor:
    def __init__(self) -> None:
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append(("execute", sql, params))

    def executemany(self, sql, rows):
        self.executed.append(("executemany", sql, list(rows)))


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_obj = FakeCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_obj


class FakePostgresHook:
    def __init__(self) -> None:
        self.conn = FakeConnection()

    def get_conn(self):
        return self.conn


def test_prepare_load_frame_filters_partition_and_columns():
    df = pd.DataFrame(
        [
            {
                "dt": "2026-06-17",
                "code_insee": "75056",
                "etiquette": "C",
                "type_bien_id": 1,
                "surface": 42.0,
                "prix": 399000.0,
                "prix_m2": 9500.0,
                "conso_energie": 150.0,
                "extra": "ignored",
            },
            {
                "dt": "2026-06-16",
                "code_insee": "75056",
                "etiquette": "D",
                "type_bien_id": 1,
                "surface": 50.0,
                "prix": 400000.0,
                "prix_m2": 8000.0,
                "conso_energie": 220.0,
            },
        ]
    )

    load_df = _prepare_load_frame(df, FACT_COLUMNS, "2026-06-17")

    assert list(load_df.columns) == FACT_COLUMNS
    assert len(load_df) == 1
    assert load_df.loc[0, "dt"].isoformat() == "2026-06-17"
    assert load_df.loc[0, "code_insee"] == "75056"


def test_prepare_load_frame_rejects_missing_columns():
    df = pd.DataFrame([{"dt": "2026-06-17", "code_insee": "75056"}])

    with pytest.raises(AirflowException, match="Colonnes gold manquantes"):
        _prepare_load_frame(df, FACT_COLUMNS, "2026-06-17")


def test_rows_for_insert_converts_pandas_nulls_to_none():
    df = pd.DataFrame(
        [
            {
                "dt": pd.to_datetime("2026-06-17").date(),
                "code_insee": "75056",
                "prix_m2_median": pd.NA,
                "pct_passoires": 25.0,
                "decote_passoire_pct": float("nan"),
                "nb_transactions": 4,
            }
        ],
        columns=KPI_COLUMNS,
    )

    rows = _rows_for_insert(df)

    assert rows == [(pd.to_datetime("2026-06-17").date(), "75056", None, 25.0, None, 4)]


def test_load_dataframe_idempotent_deletes_partition_then_inserts_rows():
    hook = FakePostgresHook()
    df = pd.DataFrame(
        [
            {
                "dt": "2026-06-17",
                "code_insee": "75056",
                "prix_m2_median": 9500.0,
                "pct_passoires": 50.0,
                "decote_passoire_pct": -10.5,
                "nb_transactions": 2,
            }
        ]
    )

    inserted = _load_dataframe_idempotent(
        postgres_hook=hook,
        table="analytics.kpi_commune_mensuel",
        df=df,
        columns=KPI_COLUMNS,
        run_ds="2026-06-17",
    )

    calls = hook.conn.cursor_obj.executed
    assert inserted == 1
    assert calls[0] == (
        "execute",
        "DELETE FROM analytics.kpi_commune_mensuel WHERE dt = %s",
        ("2026-06-17",),
    )
    assert calls[1][0] == "executemany"
    assert calls[1][1].startswith("INSERT INTO analytics.kpi_commune_mensuel")
    assert calls[1][2][0][1] == "75056"
