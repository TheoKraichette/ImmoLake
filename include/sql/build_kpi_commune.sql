-- Agrégats par commune pour la partition (idempotent par dt).
BEGIN;

DELETE FROM analytics.kpi_commune_mensuel WHERE dt = '{{ ds }}';

INSERT INTO analytics.kpi_commune_mensuel
    (dt, code_insee, prix_m2_median, pct_passoires, decote_passoire_pct, nb_transactions)
SELECT
    f.dt,
    f.code_insee,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY f.prix_m2),
    100.0 * avg(CASE WHEN d.label_passoire THEN 1 ELSE 0 END),
    100.0 * (
        avg(f.prix_m2) FILTER (WHERE d.label_passoire)
        - avg(f.prix_m2) FILTER (WHERE NOT d.label_passoire)
    ) / NULLIF(avg(f.prix_m2) FILTER (WHERE NOT d.label_passoire), 0),
    count(*)
FROM dwh.fact_biens f
JOIN dwh.dim_dpe d ON d.etiquette = f.etiquette
WHERE f.dt = '{{ ds }}'
GROUP BY f.dt, f.code_insee;

COMMIT;
