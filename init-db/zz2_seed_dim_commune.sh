#!/bin/bash
# Charge dim_commune depuis le snapshot INSEE (communes.json), via jsonb.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE TEMP TABLE _raw(doc jsonb);
\copy _raw(doc) FROM '/docker-entrypoint-initdb.d/communes.json' WITH (FORMAT csv, QUOTE E'\x01', DELIMITER E'\x02')
INSERT INTO dwh.dim_commune (code_insee, nom, departement, region, population)
SELECT c->>'code', c->>'nom', c->>'codeDepartement', c->>'codeRegion', (c->>'population')::int
FROM _raw, jsonb_array_elements(_raw.doc) AS c
ON CONFLICT (code_insee) DO NOTHING;
SQL
echo "[seed] dim_commune chargee"
