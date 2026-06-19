"""Shared pytest fixtures."""
import sys
from pathlib import Path

import pytest
from airflow.models import DagBag

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Racine projet (pour `import streamlit_app.lib...`) + dossiers importés à plat (`from hooks...`,
# `from duckdb_lake...`, `from lib...`).
for path in (str(PROJECT_ROOT), *(str(PROJECT_ROOT / f) for f in ("dags", "plugins", "include", "streamlit_app"))):
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder="dags", include_examples=False)
