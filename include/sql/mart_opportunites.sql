-- mart_commune_type -> mart_opportunites : sous-cotation commune vs distribution DEPARTEMENTALE.
-- ⚠️ La variance n'existe qu'entre communes (prix DVF = médiane par commune+type) : on compare
-- donc une commune a son departement, jamais un bien a sa commune. Base v2-4 (z + flag) ;
-- le scoring fin (score_opportunite, etiquette_opportunite) est ajoute en v2-7 (#32).
COPY (
    WITH m AS (
        SELECT * FROM read_parquet('${mart_commune_type}')
        WHERE nb_dpe >= ${seuil}
    )
    SELECT
        *,
        median(prix_m2_median) OVER w                             AS prix_m2_median_dpt,
        round(
            (prix_m2_median - avg(prix_m2_median) OVER w)
            / NULLIF(stddev_pop(prix_m2_median) OVER w, 0), 3
        )                                                         AS z,
        round(
            100.0 * (prix_m2_median - median(prix_m2_median) OVER w)
            / NULLIF(median(prix_m2_median) OVER w, 0), 1
        )                                                         AS ecart_pct,
        (
            (prix_m2_median - avg(prix_m2_median) OVER w)
            / NULLIF(stddev_pop(prix_m2_median) OVER w, 0)
        ) < -${k}                                                 AS est_opportunite
    FROM m
    WINDOW w AS (PARTITION BY departement, type_bien)
) TO '${out}' (FORMAT PARQUET);
