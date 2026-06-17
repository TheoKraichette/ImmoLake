-- Schéma du DWH ImmoLake (exécuté au 1er démarrage de postgres-dwh).
-- Postgres = couche de service, chargée depuis le gold (Parquet, MinIO).
-- dwh = étoile (Kimball), analytics = agrégats. Le silver vit en Parquet dans MinIO.

CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- dwh (étoile)
CREATE TABLE IF NOT EXISTS dwh.dim_commune (
    code_insee  TEXT PRIMARY KEY,
    nom         TEXT,
    departement TEXT,
    region      TEXT,
    population  INTEGER
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt           DATE PRIMARY KEY,
    annee        INTEGER,
    mois         INTEGER,
    trimestre    INTEGER,
    jour_semaine INTEGER
);

CREATE TABLE IF NOT EXISTS dwh.dim_dpe (
    etiquette      TEXT PRIMARY KEY,
    conso_min      NUMERIC,
    conso_max      NUMERIC,
    label_passoire BOOLEAN          -- TRUE pour F et G
);

CREATE TABLE IF NOT EXISTS dwh.dim_type_bien (
    id              SERIAL PRIMARY KEY,
    type            TEXT,
    tranche_surface TEXT
);

CREATE TABLE IF NOT EXISTS dwh.fact_biens (
    id            BIGSERIAL PRIMARY KEY,
    dt            DATE NOT NULL REFERENCES dwh.dim_date(dt),
    code_insee    TEXT NOT NULL REFERENCES dwh.dim_commune(code_insee),
    etiquette     TEXT REFERENCES dwh.dim_dpe(etiquette),
    type_bien_id  INTEGER REFERENCES dwh.dim_type_bien(id),
    surface       NUMERIC,
    prix          NUMERIC,
    prix_m2       NUMERIC,
    conso_energie NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_fact_biens_dt         ON dwh.fact_biens(dt);
CREATE INDEX IF NOT EXISTS idx_fact_biens_code_insee ON dwh.fact_biens(code_insee);

-- analytics
CREATE TABLE IF NOT EXISTS analytics.kpi_commune_mensuel (
    dt                  DATE,
    code_insee          TEXT,
    prix_m2_median      NUMERIC,
    pct_passoires       NUMERIC,
    decote_passoire_pct NUMERIC,
    nb_transactions     INTEGER,
    PRIMARY KEY (dt, code_insee)
);

INSERT INTO dwh.dim_dpe (etiquette, conso_min, conso_max, label_passoire) VALUES
    ('A',   0,  70,  FALSE),
    ('B',  71, 110,  FALSE),
    ('C', 111, 180,  FALSE),
    ('D', 181, 250,  FALSE),
    ('E', 251, 330,  FALSE),
    ('F', 331, 420,  TRUE),
    ('G', 421, 9999, TRUE)
ON CONFLICT (etiquette) DO NOTHING;
