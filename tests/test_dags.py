"""Tests minimum (cf. Definition of Done).

À enrichir : test d'idempotence du transform (rejouer un run = même COUNT).
"""


def test_no_import_errors(dagbag):
    """Aucun DAG ne doit échouer à l'import."""
    assert not dagbag.import_errors, dagbag.import_errors


def test_expected_dags_present(dagbag):
    """Les DAGs du pipeline v2 sont chargés (ingestion, transfos DuckDB, marts, seed ref)."""
    for dag_id in (
        "immolake_ingest_daily",
        "immolake_transform_daily",
        "immolake_marts_daily",
        "immolake_seed_ref",
    ):
        assert dagbag.get_dag(dag_id) is not None


def test_all_dags_catchup_false(dagbag):
    """Pas de catchup (évite les backfills involontaires)."""
    for dag in dagbag.dags.values():
        assert dag.catchup is False, f"{dag.dag_id} a catchup=True"
