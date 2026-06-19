# CLAUDE.md

Guide de contexte pour l'agent de code travaillant sur ce dépôt : conventions, commandes, pièges à connaître.

## Le projet

**ImmoLake** — plateforme Data Lakehouse qui croise les **prix immobiliers (DVF, data.gouv)** avec
la **performance énergétique (DPE / API ADEME)** pour répondre à une question métier : *impact de
l'étiquette DPE sur le prix/m² par commune, et détection des biens sous/sur-cotés*. Projet pédagogique,
3 devs. La **v2 est dans `main`** : **DuckDB** (transforms + serving sur le Parquet MinIO) + front
**Streamlit**. Postgres et Metabase ont été retirés.

Contexte de reprise : `docs/RECAP.md` (handoff). ADR : `docs/ARCHITECTURE.md`. Règles Git : `docs/CONTRIBUTING.md`.

## Commandes

```bash
# Stack complète (MinIO + Airflow + Streamlit ; minio-init charge ref/ + snapshot -> dashboards peuplés au boot)
cp .env.example .env
docker compose up -d
docker compose logs -f airflow-init          # suivre l'init au 1er démarrage

# Tests (pytest tourne DANS le conteneur Airflow)
docker compose exec airflow-scheduler pytest tests/ -v
docker compose exec airflow-scheduler pytest tests/test_transform_sql.py -v   # un fichier

# Rejouer le pipeline (l'ordre s'enchaîne via les assets)
docker compose exec -T airflow-scheduler airflow dags unpause immolake_ingest_daily
docker compose exec -T airflow-scheduler airflow dags trigger  immolake_ingest_daily

# Inspecter le lac (DuckDB lit le Parquet MinIO)
docker compose exec -T airflow-scheduler python -c "import sys;sys.path.insert(0,'/opt/airflow/include');from duckdb_lake import connect;print(connect().execute(\"select count(*) from read_parquet('s3://immolake/gold/mart_commune/*.parquet')\").fetchone())"

# Reset total (supprime les volumes ; le snapshot committé repeuplera au prochain up)
docker compose down -v

# Régénérer le snapshot committé (après un run frais)
bash scripts/make_snapshot.sh
```

Accès : Airflow http://localhost:8080 (`airflow`/`airflow`) · **Streamlit http://localhost:8501** ·
MinIO http://localhost:9001 (`minio_admin`/`minio_password_2026`).

### Git / push
Le credential helper Windows pointe un autre compte GitHub. Pour pousser sur (`TheoKraichette/ImmoLake`) :
```bash
git -c credential.helper= -c credential.helper='!gh auth git-credential' push
```

## Architecture

**Lakehouse médaillon, tout en Parquet sur MinIO ; DuckDB pour transformer ET servir.**

```
Sources              Airflow 3 (Assets)         MinIO (Parquet) + DuckDB                 Front
API ADEME (DPE) ─► ingest ──(RAW_DPE)──► raw/dpe → silver/dpe ┐
geo-dvf (par dep) ─► transform ─────────► raw/dvf → silver/dvf ┴► gold/fact_biens → marts ─► Streamlit
                     marts ──(GOLD_FACT)─────────────────────────► gold/mart_* (DuckDB/httpfs)
```

Quatre DAGs TaskFlow (`@dag`/`@task` depuis **`airflow.sdk`** — Airflow 3), chaînés **event-driven**
par **Assets** (`schedule=[ASSET]` / `outlets=[ASSET]`), `catchup=False` :

| DAG | Fait |
|---|---|
| `immolake_ingest_daily` | API ADEME → `raw/dpe/dt=/dep=*` (1 tâche mappée/département, `select` des colonnes) ; produit `RAW_DPE` |
| `immolake_transform_daily` | DVF par département → `raw/dvf` ; DuckDB `silver`/`gold/fact_biens`/`kpi_commune` ; data quality gate ; produit `GOLD_FACT` |
| `immolake_marts_daily` | DuckDB → `mart_commune`, `mart_commune_type`, `mart_opportunites`, `dvf_stats_commune_type` |
| `immolake_seed_ref` | (re)génère les dimensions `ref/` + `geo_commune` (Parquet) |

Les transformations sont du **SQL DuckDB** dans `include/sql/*.sql` (paramétré par jetons `${...}`,
rendu par `include/duckdb_lake.py`). Les dimensions `ref/` sont committées (`include/ref/*.parquet`,
chargées au boot par `minio-init`) — `dim_commune` = 35 014 communes **+ 45 arrondissements** (75/69/13).

## Points non-évidents (lire avant de toucher au pipeline)

- **Chaînage par assets** : pas de déclenchement manuel ordonné nécessaire — `ingest` → `transform`
  → `marts` s'enchaînent via `RAW_DPE`/`GOLD_FACT`. Le `ds` métier est porté par l'`extra` de l'asset.
- **Airflow 3 + run manuel** : `logical_date` peut être `None` → `_ds()` retombe sur la date du jour
  (sinon `KeyError: 'ds'`). Présent dans les 3 DAGs.
- **`select` des colonnes ADEME** : `DPE_SELECT_FIELDS` (hook) ne récupère que ~14 colonnes/230.
  Ajouter un champ = l'ajouter là **et** dans `include/sql/silver_dpe.sql` (+ propager si besoin).
- **DVF multi-département** : `dvf_to_raw` dérive les fichiers `geo-dvf` de `ADEME_DEPARTEMENTS`
  (+ `DVF_YEAR`) → `raw/dvf/dt=/dep=*`. `DVF_CSV_URL` reste un override mono-fichier. ⚠️ **Pas de DVF
  en Alsace-Moselle (67/68/57, livre foncier)** : 404 ignoré (DPE gardés, sans prix).
- **Idempotence** : partitions de sortie purgées+réécrites (`raw/*/dt=/dep=*`, `silver`, `gold`, marts).
  Rejouer un `dt` = même `COUNT(*)`.
- **Snapshot committé** (`include/snapshot/gold/*`, chargé par `minio-init`) : dashboards peuplés dès
  `docker compose up`. Régénérer : `bash scripts/make_snapshot.sh` (export DuckDB du gold).
- **Enrichissement prix DVF (choix MVP)** : pas de matching adresse/parcelle ; `gold/fact_biens` joint un
  **prix/m² médian DVF par `code_insee × type_bien × tranche de surface`** (fallback commune×type).
- **Colonnes enrichies** : `etiquette_ges`, `emission_ges`, `cout_energie_annuel`, `energie_chauffage`,
  `annee_construction` (silver → gold → `mart_commune`/`mart_commune_type` → front).
- **Carte** : `ref/geo_commune` (centroïdes + contours) régénéré par `include/seed_geo_commune.py`
  (`--departements`). Sans contour, la carte retombe en points.
- **Alerte = WhatsApp via Twilio**, **prévue mais non activée** (le DAG loggue seulement).
- **Ingestion = partie longue** (pagination ADEME) : capper `ADEME_MAX_PAGES` pour une démo rapide.

## Conventions de code

- DAGs en **TaskFlow** (`@task`), MinIO via `S3Hook(aws_conn_id="minio_default")`, DuckDB via
  `include/duckdb_lake.py` (`connect()`/`run_sql()`). Connexions pré-câblées par `AIRFLOW_CONN_*`
  dans `docker-compose.yml`, pas dans l'UI.
- Transformations en **SQL DuckDB** (`include/sql/`), helpers privés préfixés `_`.
- Commentaires **sobres** ; docstrings courtes en tête de module/fonction.
- Tests **dans le conteneur** ; `conftest.py` ajoute `dags/`, `plugins/`, `include/` au `sys.path`.

## Workflow Git (obligatoire)

- **Jamais de commit direct sur `main`.** 1 issue = 1 branche (`feat/<num>-<slug>` ou `fix/<num>-<slug>`)
  → PR vers `main` → relecture par un autre membre → merge → suppression de branche.
- **Aucune auto-attribution** dans les commits, PR, commentaires **ou documentation** (ni co-author,
  ni mention d'outil).
- Definition of Done : DAG concerné en *success*, transforms idempotentes, `pytest tests/` vert,
  aucun secret commité (`.env` reste local).
