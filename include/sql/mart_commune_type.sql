-- gold/fact_biens + dims ref/ -> mart_commune_type : prix/m² médian par commune × type de bien.
COPY (
    WITH ct AS (
        SELECT
            f.code_insee, f.type_bien,
            median(f.prix_m2)                                      AS prix_m2_median,
            count(*)                                               AS nb_dpe,
            round(100.0 * avg(CASE WHEN COALESCE(e.label_passoire, false) THEN 1 ELSE 0 END), 2) AS pct_passoires,
            median(f.cout_energie_annuel)                          AS cout_energie_annuel_median,
            median(f.annee_construction)                           AS annee_construction_mediane
        FROM read_parquet('${fact}') f
        LEFT JOIN read_parquet('${dim_dpe}') e ON e.etiquette = f.etiquette
        GROUP BY f.code_insee, f.type_bien
    )
    SELECT ct.*, dc.nom, dc.departement, dc.region
    FROM ct
    JOIN read_parquet('${dim_commune}') dc USING (code_insee)
) TO '${out}' (FORMAT PARQUET);
