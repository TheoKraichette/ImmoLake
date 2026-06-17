-- Seed des dimensions statiques (exécuté après schema.sql).

-- dim_type_bien : une ligne par type (jointure 1:1 dans le transform)
INSERT INTO dwh.dim_type_bien (type, tranche_surface)
SELECT * FROM (VALUES ('appartement', NULL::text), ('maison', NULL), ('autre', NULL)) v
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_type_bien);

-- dim_date : calendrier 2020-2027
INSERT INTO dwh.dim_date (dt, annee, mois, trimestre, jour_semaine)
SELECT d::date,
       extract(year   FROM d)::int,
       extract(month  FROM d)::int,
       extract(quarter FROM d)::int,
       extract(isodow FROM d)::int
FROM generate_series('2020-01-01'::date, '2027-12-31'::date, interval '1 day') AS g(d)
ON CONFLICT (dt) DO NOTHING;
