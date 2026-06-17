#!/usr/bin/env bash
# Génère un snapshot des tables de serving (pour partage via git).
# Le snapshot est restauré automatiquement au 1er `docker compose up`
# (voir init-db/zz9_restore_snapshot.sh).
#
# Usage : bash scripts/make_snapshot.sh [LIMIT_FACT]   (défaut 200000)
set -e
LIMIT="${1:-200000}"
OUT="init-db/snapshot"
mkdir -p "$OUT"

# KPI par commune : table petite -> snapshot complet
docker compose exec -T postgres-dwh psql -U dwh_user -d immolake \
  -c "\copy (SELECT dt, code_insee, prix_m2_median, pct_passoires, decote_passoire_pct, nb_transactions FROM analytics.kpi_commune_mensuel) TO STDOUT WITH CSV HEADER" \
  | gzip > "$OUT/kpi_commune_mensuel.csv.gz"

# Faits : échantillon aléatoire borné (dashboards identiques, repo léger)
docker compose exec -T postgres-dwh psql -U dwh_user -d immolake \
  -c "\copy (SELECT dt, code_insee, etiquette, type_bien_id, surface, prix, prix_m2, conso_energie FROM dwh.fact_biens ORDER BY random() LIMIT ${LIMIT}) TO STDOUT WITH CSV HEADER" \
  | gzip > "$OUT/fact_biens.csv.gz"

echo "Snapshot écrit dans $OUT/ (fact limité à ${LIMIT} lignes)"
ls -la "$OUT" 2>/dev/null || dir "$OUT"
