-- raw/dpe (Parquet partitionné dt=/dep=) -> silver/dpe : nettoyage, typage, dédup numero_dpe.
COPY (
    SELECT
        CAST(numero_dpe AS VARCHAR)                              AS numero_dpe,
        DATE '${ds}'                                             AS dt,
        trim(CAST(code_insee_ban AS VARCHAR))                   AS code_insee,
        trim(CAST(code_postal_ban AS VARCHAR))                  AS code_postal,
        lower(trim(CAST(type_batiment AS VARCHAR)))             AS type_batiment,
        TRY_CAST(surface_habitable_logement AS DOUBLE)          AS surface_habitable,
        upper(trim(CAST(etiquette_dpe AS VARCHAR)))             AS etiquette_dpe,
        TRY_CAST(conso_5_usages_par_m2_ep AS DOUBLE)            AS conso_energie,
        TRY_CAST(date_etablissement_dpe AS DATE)                AS date_etablissement
    FROM read_parquet('${raw_glob}', union_by_name = true)
    WHERE numero_dpe IS NOT NULL
      AND code_insee_ban IS NOT NULL
      AND etiquette_dpe IS NOT NULL
      AND (surface_habitable_logement IS NULL OR TRY_CAST(surface_habitable_logement AS DOUBLE) > 0)
    QUALIFY row_number() OVER (
        PARTITION BY numero_dpe
        ORDER BY TRY_CAST(date_etablissement_dpe AS DATE) DESC NULLS LAST
    ) = 1
) TO '${out}' (FORMAT PARQUET);
