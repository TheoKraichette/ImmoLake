#!/usr/bin/env bash
# Régénère le snapshot de données committé (include/snapshot/gold/) depuis le gold MinIO courant.
# À lancer APRÈS un run frais du pipeline (ingest -> transform -> marts).
# Le snapshot est rechargé par `minio-init` au boot -> dashboards peuplés dès `docker compose up`.
set -euo pipefail

docker compose exec -T airflow-scheduler python /opt/airflow/include/export_snapshot.py

echo
echo "Snapshot écrit dans include/snapshot/gold/. Pour le versionner :"
echo "  git add include/snapshot && git commit -m \"chore(snapshot): rafraichit les donnees de demo\""
