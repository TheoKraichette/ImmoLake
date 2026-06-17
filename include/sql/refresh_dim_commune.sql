-- UPSERT des communes vues dans le staging.
INSERT INTO dwh.dim_commune (code_insee, nom, departement, region, population)
SELECT DISTINCT
    s.code_insee,
    NULL::text,                  -- nom : à enrichir (référentiel INSEE)
    left(s.code_insee, 2),       -- departement
    NULL::text,                  -- region
    NULL::integer                -- population
FROM staging.dpe s
WHERE s.code_insee IS NOT NULL
ON CONFLICT (code_insee) DO UPDATE
    SET departement = EXCLUDED.departement;
