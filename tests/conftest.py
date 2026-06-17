"""Shared pytest fixtures."""
import sys
from pathlib import Path

import pytest
from airflow.models import DagBag

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for folder in ("dags", "plugins"):
    path = str(PROJECT_ROOT / folder)
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder="dags", include_examples=False)
