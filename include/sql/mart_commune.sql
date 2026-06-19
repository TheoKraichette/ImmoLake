-- gold/fact_biens + dims ref/ -> mart_commune : KPIs commune + noms + indicateurs territoriaux.
COPY (
    WITH fact AS (
        SELECT f.*, COALESCE(e.label_passoire, false) AS is_passoire
        FROM read_parquet('${fact}') f
        LEFT JOIN read_parquet('${dim_dpe}') e ON e.etiquette = f.etiquette
    ),
    commune AS (
        SELECT
            code_insee,
            median(prix_m2)                                        AS prix_m2_median,
            count(*)                                               AS nb_dpe,
            round(100.0 * avg(CASE WHEN is_passoire THEN 1 ELSE 0 END), 2) AS pct_passoires,
            round(avg(conso_energie), 1)                           AS conso_energie_moy,
            round(avg(emission_ges), 1)                            AS emission_ges_moy,
            median(cout_energie_annuel)                            AS cout_energie_annuel_median,
            median(annee_construction)                            AS annee_construction_mediane,
            round(100.0 * avg(CASE WHEN etiquette_ges IN ('F', 'G') THEN 1 ELSE 0 END), 2) AS pct_ges_passoires
        FROM fact
        GROUP BY code_insee
    )
    SELECT
        c.code_insee, dc.nom, dc.departement, dc.region, dc.population,
        c.prix_m2_median, c.nb_dpe, c.pct_passoires, c.conso_energie_moy,
        c.emission_ges_moy, c.cout_energie_annuel_median, c.annee_construction_mediane, c.pct_ges_passoires,
        (c.nb_dpe >= 30)                                           AS fiable,
        round(
            100.0 * (c.prix_m2_median - median(c.prix_m2_median) OVER w)
            / NULLIF(median(c.prix_m2_median) OVER w, 0), 1
        )                                                          AS indice_sous_cotation,
        round(
            (c.prix_m2_median - avg(c.prix_m2_median) OVER w)
            / NULLIF(stddev_pop(c.prix_m2_median) OVER w, 0), 3
        )                                                          AS z_prix_dpt,
        rank() OVER (PARTITION BY dc.departement ORDER BY c.prix_m2_median DESC) AS rang_prix_dpt
    FROM commune c
    JOIN read_parquet('${dim_commune}') dc USING (code_insee)
    WINDOW w AS (PARTITION BY dc.departement)
) TO '${out}' (FORMAT PARQUET);
