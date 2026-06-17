#!/bin/bash
# Restaure le snapshot de serving au 1er démarrage (après schéma + dimensions).
# Sans snapshot committé, ne fait rien (les faits viennent alors du pipeline).
set -e
DIR=/docker-entrypoint-initdb.d/snapshot
if [ ! -f "$DIR/kpi_commune_mensuel.csv.gz" ]; then
  echo "[restore] pas de snapshot, skip (lancer le pipeline pour peupler les faits)"
  exit 0
fi

gunzip -c "$DIR/kpi_commune_mensuel.csv.gz" \
  | psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "\copy analytics.kpi_commune_mensuel FROM STDIN WITH CSV HEADER"

gunzip -c "$DIR/fact_biens.csv.gz" \
  | psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "\copy dwh.fact_biens(dt, code_insee, etiquette, type_bien_id, surface, prix, prix_m2, conso_energie) FROM STDIN WITH CSV HEADER"

echo "[restore] snapshot chargé (serving peuplé)"
