-- silver/dpe + référentiel prix DVF (médiane par commune+type) + dims ref/ -> gold/fact_biens.
-- Garde le LIBELLÉ type_bien (plus de type_bien_id SERIAL : la dim n'a plus de Postgres).
COPY (
    WITH dpe AS (
        SELECT
            code_insee,
            etiquette_dpe AS etiquette,
            CASE
                WHEN type_batiment LIKE '%appart%' THEN 'appartement'
                WHEN type_batiment LIKE '%maison%'  THEN 'maison'
                ELSE 'autre'
            END AS type_bien,
            surface_habitable AS surface,
            conso_energie
        FROM read_parquet('${silver_dpe}')
        WHERE code_insee IS NOT NULL AND surface_habitable > 0
    ),
    dvf_ref AS (
        SELECT code_insee, type_bien, median(prix_m2) AS dvf_prix_m2
        FROM read_parquet('${silver_dvf}')
        WHERE prix_m2 > 0
        GROUP BY code_insee, type_bien
    )
    SELECT
        DATE '${ds}'                AS dt,
        d.code_insee,
        d.etiquette,
        d.type_bien,
        d.surface,
        r.dvf_prix_m2               AS prix_m2,
        d.surface * r.dvf_prix_m2   AS prix,
        d.conso_energie
    FROM dpe d
    JOIN read_parquet('${dim_commune}') c USING (code_insee)
    JOIN read_parquet('${dim_dpe}')     e ON e.etiquette = d.etiquette
    LEFT JOIN dvf_ref r ON r.code_insee = d.code_insee AND r.type_bien = d.type_bien
) TO '${out}' (FORMAT PARQUET);
