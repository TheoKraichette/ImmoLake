-- silver/dvf -> gold/dvf_stats_commune_type : DISTRIBUTION du prix/m² (percentiles) par commune × type.
-- ⚠️ Les percentiles n'ont de sens que sur les TRANSACTIONS DVF BRUTES : fact_biens porte un prix/m²
-- déjà agrégé (médiane) donc plat. C'est ici qu'on retrouve la vraie dispersion des prix (box-plots).
COPY (
    SELECT
        d.code_insee,
        d.type_bien,
        c.nom,
        c.departement,
        c.region,
        count(*)                                          AS nb_transactions,
        round(quantile_cont(d.prix_m2, 0.10))             AS prix_m2_p10,
        round(quantile_cont(d.prix_m2, 0.25))             AS prix_m2_p25,
        round(quantile_cont(d.prix_m2, 0.50))             AS prix_m2_median,
        round(quantile_cont(d.prix_m2, 0.75))             AS prix_m2_p75,
        round(quantile_cont(d.prix_m2, 0.90))             AS prix_m2_p90
    FROM read_parquet('${silver_dvf}') d
    JOIN read_parquet('${dim_commune}') c USING (code_insee)
    WHERE d.prix_m2 > 0 AND d.type_bien IN ('appartement', 'maison')
    GROUP BY d.code_insee, d.type_bien, c.nom, c.departement, c.region
    HAVING count(*) >= 5
) TO '${out}' (FORMAT PARQUET);
