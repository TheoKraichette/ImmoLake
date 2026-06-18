-- raw/dvf (CSV gz) -> silver/dvf : transactions nettoyées, prix/m² calculé (lat/long conservés).
COPY (
    SELECT
        lpad(trim(CAST(code_commune AS VARCHAR)), 5, '0')       AS code_insee,
        CASE
            WHEN lower(CAST(type_local AS VARCHAR)) LIKE '%appart%' THEN 'appartement'
            WHEN lower(CAST(type_local AS VARCHAR)) LIKE '%maison%'  THEN 'maison'
            ELSE 'autre'
        END                                                     AS type_bien,
        TRY_CAST(surface_reelle_bati AS DOUBLE)                 AS surface,
        TRY_CAST(valeur_fonciere AS DOUBLE)                     AS prix,
        TRY_CAST(valeur_fonciere AS DOUBLE)
            / NULLIF(TRY_CAST(surface_reelle_bati AS DOUBLE), 0) AS prix_m2,
        TRY_CAST(date_mutation AS DATE)                         AS date_mutation,
        TRY_CAST(longitude AS DOUBLE)                           AS longitude,
        TRY_CAST(latitude AS DOUBLE)                            AS latitude
    FROM read_csv_auto('${dvf_csv}', header = true)
    WHERE type_local IN ('Appartement', 'Maison')
      AND TRY_CAST(valeur_fonciere AS DOUBLE) > 0
      AND TRY_CAST(surface_reelle_bati AS DOUBLE) > 0
) TO '${out}' (FORMAT PARQUET);
