-- silver/dpe + référentiel prix DVF + dims ref/ -> gold/fact_biens.
-- Garde le LIBELLÉ type_bien (plus de type_bien_id SERIAL : la dim n'a plus de Postgres).
-- v2-11 : enrichissement à la MAILLE SURFACE (commune × type × tranche) -> signal intra-commune
--         (un studio et un grand appart n'ont plus le même prix/m²) ; fallback (commune × type) si
--         aucune transaction sur la tranche ; `date_etablissement` remontée (axe temporel des tendances).
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
            CASE
                WHEN surface_habitable < 40  THEN '<40'
                WHEN surface_habitable < 70  THEN '40-70'
                WHEN surface_habitable < 100 THEN '70-100'
                WHEN surface_habitable < 150 THEN '100-150'
                ELSE '150+'
            END AS tranche_surface,
            conso_energie,
            date_etablissement
        FROM read_parquet('${silver_dpe}')
        WHERE code_insee IS NOT NULL AND surface_habitable > 0
    ),
    dvf AS (
        SELECT
            code_insee, type_bien, prix_m2,
            CASE
                WHEN surface < 40  THEN '<40'
                WHEN surface < 70  THEN '40-70'
                WHEN surface < 100 THEN '70-100'
                WHEN surface < 150 THEN '100-150'
                ELSE '150+'
            END AS tranche_surface
        FROM read_parquet('${silver_dvf}')
        WHERE prix_m2 > 0 AND surface > 0
    ),
    ref_surface AS (
        SELECT code_insee, type_bien, tranche_surface, median(prix_m2) AS prix_m2
        FROM dvf GROUP BY code_insee, type_bien, tranche_surface
    ),
    ref_commune AS (
        SELECT code_insee, type_bien, median(prix_m2) AS prix_m2
        FROM dvf GROUP BY code_insee, type_bien
    )
    SELECT
        DATE '${ds}'                                    AS dt,
        d.code_insee,
        d.etiquette,
        d.type_bien,
        d.tranche_surface,
        d.surface,
        COALESCE(rs.prix_m2, rc.prix_m2)                AS prix_m2,
        d.surface * COALESCE(rs.prix_m2, rc.prix_m2)    AS prix,
        d.conso_energie,
        d.date_etablissement
    FROM dpe d
    JOIN read_parquet('${dim_commune}') c USING (code_insee)
    JOIN read_parquet('${dim_dpe}')     e ON e.etiquette = d.etiquette
    LEFT JOIN ref_surface rs
        ON rs.code_insee = d.code_insee AND rs.type_bien = d.type_bien AND rs.tranche_surface = d.tranche_surface
    LEFT JOIN ref_commune rc
        ON rc.code_insee = d.code_insee AND rc.type_bien = d.type_bien
) TO '${out}' (FORMAT PARQUET);
