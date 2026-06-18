-- gold/fact_biens -> gold/kpi_commune : prix/m² médian, % passoires, décote passoire, nb.
COPY (
    WITH fact AS (
        SELECT f.*, COALESCE(e.label_passoire, false) AS is_passoire
        FROM read_parquet('${fact}') f
        LEFT JOIN read_parquet('${dim_dpe}') e ON e.etiquette = f.etiquette
    )
    SELECT
        DATE '${ds}'                                             AS dt,
        code_insee,
        median(prix_m2)                                         AS prix_m2_median,
        round(100.0 * avg(CASE WHEN is_passoire THEN 1 ELSE 0 END), 2) AS pct_passoires,
        round(
            100.0 * (avg(prix_m2) FILTER (WHERE is_passoire) - avg(prix_m2) FILTER (WHERE NOT is_passoire))
            / NULLIF(avg(prix_m2) FILTER (WHERE NOT is_passoire), 0), 2
        )                                                       AS decote_passoire_pct,
        count(*)                                                AS nb_transactions
    FROM fact
    GROUP BY code_insee
) TO '${out}' (FORMAT PARQUET);
