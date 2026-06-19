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
    -- quote='"' explicite : certaines adresses DVF contiennent une virgule entre guillemets et
    -- l'auto-détection peut manquer le quote sur l'échantillon. ignore_errors : tolère les rares
    -- lignes malformées (données réelles), on ne calcule que des médianes.
    FROM read_csv_auto('${dvf_csv}', header = true, quote = '"', escape = '"', ignore_errors = true)
    WHERE type_local IN ('Appartement', 'Maison')
      AND TRY_CAST(valeur_fonciere AS DOUBLE) > 0
      AND TRY_CAST(surface_reelle_bati AS DOUBLE) >= 9          -- exclut garages/dépendances à surface ~nulle
      -- Borne le prix/m² à une plage réaliste : élimine les ventes multi-lots / surfaces aberrantes
      -- (sinon la médiane des petites communes part en vrille : 0 €/m² ou 600 000 €/m²).
      AND TRY_CAST(valeur_fonciere AS DOUBLE) / NULLIF(TRY_CAST(surface_reelle_bati AS DOUBLE), 0)
          BETWEEN 200 AND 40000
) TO '${out}' (FORMAT PARQUET);
