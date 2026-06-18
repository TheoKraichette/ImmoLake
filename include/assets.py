"""Orchestration event-driven : Assets Airflow 3 + politique de retries partagée.

Le pipeline est chaîné par des **assets** plutôt que par un ordre manuel :
    ingest --(RAW_DPE)--> transform --(GOLD_FACT)--> marts
Un DAG amont *produit* l'asset (`outlets=[...]`) ; le DAG aval est *planifié dessus*
(`schedule=[...]`) et se déclenche automatiquement quand l'asset est mis à jour.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.sdk import Asset

# Assets = points de synchronisation du pipeline (URI = zone MinIO produite).
RAW_DPE = Asset("s3://immolake/raw/dpe")
GOLD_FACT = Asset("s3://immolake/gold/fact_biens")

# Reprise sur échec : 3 tentatives, backoff exponentiel borné à 30 min.
RETRY_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}
