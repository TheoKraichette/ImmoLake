"""Tests minimum (cf. Definition of Done).

À enrichir : test d'idempotence du transform (rejouer un run = même COUNT).
"""


def test_no_import_errors(dagbag):
    """Aucun DAG ne doit échouer à l'import."""
    assert not dagbag.import_errors, dagbag.import_errors


def test_expected_dags_present(dagbag):
    """Les 3 DAGs du pipeline sont chargés."""
    for dag_id in (
        "immolake_ingest_daily",
        "immolake_transform_daily",
        "immolake_analytics_daily",
    ):
        assert dagbag.get_dag(dag_id) is not None


def test_all_dags_catchup_false(dagbag):
    """Pas de catchup (évite les backfills involontaires)."""
    for dag in dagbag.dags.values():
        assert dag.catchup is False, f"{dag.dag_id} a catchup=True"
