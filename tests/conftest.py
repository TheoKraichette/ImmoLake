"""Fixtures pytest partagées."""
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder="dags", include_examples=False)
