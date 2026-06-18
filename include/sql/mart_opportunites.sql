-- mart_commune_type -> mart_opportunites : détecteur de bonnes affaires (commune vs DÉPARTEMENT).
-- ⚠️ La variance n'existe qu'entre communes (prix DVF = médiane par commune+type) : on compare
-- une commune à son département, jamais un bien à sa commune. Garde-fou : nb_dpe >= seuil.
-- v2-7 (#32) : le scoring (score_opportunite, etiquette_opportunite) est matérialisé ICI (data layer),
-- pas recalculé dans le front.
COPY (
    WITH base AS (
        SELECT * FROM read_parquet('${mart_commune_type}')
        WHERE nb_dpe >= ${seuil}
    ),
    scored AS (
        SELECT
            *,
            median(prix_m2_median) OVER w                        AS prix_m2_median_dpt,
            round(
                (prix_m2_median - avg(prix_m2_median) OVER w)
                / NULLIF(stddev_pop(prix_m2_median) OVER w, 0), 3
            )                                                    AS z,
            round(
                100.0 * (prix_m2_median - median(prix_m2_median) OVER w)
                / NULLIF(median(prix_m2_median) OVER w, 0), 1
            )                                                    AS ecart_pct
        FROM base
        WINDOW w AS (PARTITION BY departement, type_bien)
    )
    SELECT
        *,
        (z < -${k})                                              AS est_opportunite,
        -- score composite : 60 % sous-cotation territoriale (z négatif) + 40 % part de passoires.
        round(0.6 * greatest(-COALESCE(z, 0), 0) * 50 + 0.4 * COALESCE(pct_passoires, 0), 1) AS score_opportunite,
        CASE
            WHEN z < -${k} AND pct_passoires >= 20 THEN 'sous-cotee + parc passoires (potentiel renovation)'
            WHEN z < -${k}                          THEN 'sous-cotee'
            ELSE 'survalorisee'
        END                                                      AS etiquette_opportunite
    FROM scored
) TO '${out}' (FORMAT PARQUET);
