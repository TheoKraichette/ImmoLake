-- staging.dpe -> dwh.fact_biens, idempotent par partition (DELETE + INSERT).
BEGIN;

DELETE FROM dwh.fact_biens WHERE dt = '{{ ds }}';

INSERT INTO dwh.fact_biens (dt, code_insee, etiquette, type_bien_id, surface, prix, prix_m2, conso_energie)
SELECT
    s.dt,
    s.code_insee,
    s.etiquette_dpe,
    t.id,
    s.surface_habitable,
    NULL::numeric,        -- prix : enrichi via DVF
    NULL::numeric,        -- prix_m2 : idem
    s.conso_energie
FROM staging.dpe s
LEFT JOIN dwh.dim_type_bien t ON t.type = lower(s.type_batiment)
WHERE s.dt = '{{ ds }}'
  AND s.code_insee IN (SELECT code_insee FROM dwh.dim_commune);

COMMIT;
