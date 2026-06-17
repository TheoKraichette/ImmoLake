"""Operator de data quality (bonus). Règles : not_null, not_empty, no_future_date."""
from __future__ import annotations

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    def __init__(
        self,
        *,
        postgres_conn_id: str = "dwh_postgres",
        table: str,
        rules: list[dict],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.rules = rules  # ex: [{"type": "not_null", "column": "code_insee"}]

    def execute(self, context) -> None:
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        errors: list[str] = []
        for rule in self.rules:
            # TODO: générer le COUNT par type de règle et empiler les violations
            self.log.info("Règle %s sur %s", rule, self.table)
        if errors:
            raise AirflowException("Data Quality KO : " + "; ".join(errors))
