# ImmoLake — Récap de contexte (handoff pour reprise)

> Document de reprise (non destiné au produit final). État : **v2 en place dans `main`**
> (DuckDB + Streamlit), enrichie côté ingestion (colonnes pertinentes + couverture multi-villes).

## 1. Le projet
Plateforme **Data Lakehouse** croisant **prix immobilier (DVF)** × **performance énergétique
(DPE / ADEME)**. Question métier : *impact de l'étiquette DPE sur le prix/m² par commune, détecter
les biens sous/sur-cotés*. Équipe : 3 devs.

## 2. Architecture (v2)
- **Ingestion** : API ADEME (DPE) via `AdemeApiHook`, **par département**, en streaming, ne demandant
  que les **~14 colonnes pertinentes** (`select`) sur 230. DVF **par département** (geo-dvf), aligné
  sur le périmètre DPE.
- **Lac MinIO** : `raw → silver → gold → marts` en **Parquet**, transfos **DuckDB SQL** (`include/sql/`).
- **Front** : **Streamlit** (port 8501) interroge le `gold/*`+`ref/*` Parquet via DuckDB. Plus de
  Postgres ni Metabase.
- **Orchestration** : **Airflow 3**, chaînage **event-driven** par Assets
  (`ingest --RAW_DPE→ transform --GOLD_FACT→ marts`), retries/backoff, data quality gate.
- ADR détaillés (dont retrait Postgres, select colonnes, DVF multi-dep) : `docs/ARCHITECTURE.md`.

## 3. Données
- **Départements par défaut** (`.env`/`.env.example`) : **28 départements** — 12 métropoles
  (75, 69, 13, 33, 31, 06, 44, 76, 34, 59, 35, 38) + villes moyennes (21, 51, 63, 49, 37, 14, 54,
  30, 64, 17) + petits ruraux (48, 23, 15, 05, 90, 09). Ingestion **additive** : élargir = plus de villes.
- **DVF** dérivé de `ADEME_DEPARTEMENTS` + `DVF_YEAR`. ⚠️ Pas de DVF en **Alsace-Moselle**
  (67/68/57, livre foncier) → ignoré (404), DPE conservés sans prix.
- **Colonnes enrichies** : `etiquette_ges`, `emission_ges`, `cout_energie_annuel`,
  `energie_chauffage`, `annee_construction` (cf. dictionnaire dans le README).
- **Référentiels** : `dim_commune` (35 014, dont 45 arrondissements 75/69/13), `dim_dpe`,
  `dim_type_bien`, `geo_commune` (10 398 communes/arrondissements des 28 dép, centroïdes ; carte en points).

## 4. Lancer / rejouer
```bash
cp .env.example .env && docker compose up -d        # snapshot restauré -> dashboards peuplés au boot
# Rejouer sur données fraîches :
docker compose exec -T airflow-scheduler airflow dags unpause immolake_ingest_daily
docker compose exec -T airflow-scheduler airflow dags trigger  immolake_ingest_daily
# transform + marts s'enchaînent via les assets
```
- Streamlit http://localhost:8501 · Airflow http://localhost:8080 (`airflow`/`airflow`) ·
  MinIO http://localhost:9001 (`minio_admin`/`minio_password_2026`).
- Tests : `docker compose exec airflow-scheduler pytest tests/ -v`.
- **Snapshot** (données dès le `up`) : `include/snapshot/` chargé par `minio-init` ;
  régénérer via `bash scripts/make_snapshot.sh` après un run.

## 5. Pièges à connaître
- **Airflow 3 + run manuel** : `logical_date` peut être `None` → `_ds` retombe sur la date du jour
  (sinon `KeyError: 'ds'`). Corrigé dans les 3 DAGs.
- **Ingestion = partie longue** (pagination ADEME) : capper `ADEME_MAX_PAGES` pour une démo rapide.
- **`select` des colonnes** : ajouter un champ = l'ajouter à `DPE_SELECT_FIELDS` (hook) **et** au
  `silver_dpe.sql`.
- **Workflow Git** : 1 issue = 1 branche + PR ; jamais de commit direct sur `main` ; **aucune
  auto-attribution** (ni outil, ni co-author) dans commits/PR/doc.
