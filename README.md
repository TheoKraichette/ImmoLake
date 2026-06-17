# 🏠 ImmoLake — Prix × Performance énergétique

Plateforme **Data Lakehouse** qui croise les prix de l'immobilier (DVF) avec les
diagnostics de performance énergétique (DPE / ADEME) pour répondre à une question métier :

> **Quel est l'impact de l'étiquette DPE sur le prix au m² par commune, et peut-on
> détecter les biens sous-cotés (bonnes affaires) ou sur-cotés (risques) ?**

Contexte : avec la loi Climat & Résilience, les **passoires thermiques (DPE F/G)** sont
progressivement interdites à la location. L'objectif est d'aider un investisseur à cibler
les communes / biens au meilleur rapport prix / rénovation.

## 📑 Sommaire

- [Architecture](#-architecture)
- [Stack & accès](#-stack--accès)
- [Arborescence](#-arborescence)
- [Démarrage rapide](#-démarrage-rapide)
- [Commandes (Makefile)](#-commandes-makefile)
- [Modèle de données](#-modèle-de-données-étoile)
- [Idempotence](#-idempotence-obligatoire)
- [Dashboards](#-dashboards-metabase--2)
- [Automatisation](#-automatisation-bonus-telegram)
- [Tests](#-tests)
- [Répartition](#-répartition-3-devs)
- [Dépannage](#-dépannage)
- [Sources](#-sources-de-données)

## 🏗️ Architecture

```
        ┌────────────┐
        │ API ADEME  │   (DPE — source live, paginée)
        │   + DVF    │   (prix de transaction — enrichissement)
        └─────┬──────┘
              │  AdemeApiHook (Custom Hook)
              ▼
   ┌──────────────────────┐        ┌──────────────────────────────┐
   │   MinIO (S3)         │        │   PostgreSQL (DWH)            │
   │  raw → staging →     │ ─────► │  staging → dwh → analytics    │
   │       curated        │        │  (modèle en étoile, Kimball)  │
   └──────────────────────┘        └───────────────┬──────────────┘
              ▲                                     │
       Airflow (orchestration,                      ▼
        idempotence par dt)              ┌────────────────────┐
              │                          │     Metabase       │
              └── alerte Telegram (bonus)│   (dashboards)     │
                                         └────────────────────┘
```

Détails et choix techniques (ADR « pourquoi PostgreSQL ? », LocalExecutor, Metabase) :
voir **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## 🧰 Stack & accès

| Service | Rôle | Accès |
|---|---|---|
| **Airflow 3.x** (LocalExecutor) | Orchestration | http://localhost:8080 — `airflow` / `airflow` |
| **MinIO** | Data Lake S3 (raw/staging/curated) | http://localhost:9001 — `minio_admin` / `minio_password_2026` |
| **PostgreSQL 18** | Data Warehouse métier | `localhost:5433` — `dwh_user` / `dwh_password`, db `immolake` |
| **Metabase** | BI / dashboards | http://localhost:3000 |

Conteneurs annexes : `postgres-airflow` (métadonnées Airflow, interne), `minio-init` et
`airflow-init` (one-shot, s'arrêtent après initialisation).

> Versions calées sur la *Documentation commune* du cours — ajustables dans `.env` / `docker-compose.yml`.

## 📁 Arborescence

```
immolake/
├── docker-compose.yml          # Stack complète (Airflow + MinIO + Postgres + Metabase)
├── Makefile                    # Raccourcis (make up / down / test / idempotence…)
├── .env.example                # Variables (copier en .env)
├── requirements.txt  .gitignore
├── docs/
│   └── ARCHITECTURE.md         # Schéma détaillé + décisions (ADR)
├── init-db/                    # Joué au 1er démarrage de postgres-dwh
│   ├── schema.sql              # Schémas staging/dwh/analytics + dim_dpe
│   ├── zz1_seed_static.sql     # Seed dim_type_bien + dim_date
│   ├── zz2_seed_dim_commune.sh # Seed dim_commune depuis communes.json
│   └── communes.json           # Référentiel INSEE des communes (snapshot)
├── dags/
│   ├── immolake_ingest_daily.py      # API → MinIO raw → staging
│   ├── immolake_transform_daily.py   # staging → dwh (idempotent)
│   └── immolake_analytics_daily.py   # agrégats + alerte Telegram (bonus)
├── plugins/
│   ├── hooks/
│   │   └── ademe_api_hook.py   # Custom Hook API ADEME
│   └── operators/
│       └── data_quality_operator.py  # DataQualityOperator (bonus)
├── include/
│   └── sql/                    # SQL versionné (référencé par les DAGs)
│       ├── refresh_dim_commune.sql
│       ├── transform_fact_biens.sql  # pattern idempotent DELETE+INSERT
│       └── build_kpi_commune.sql
├── config/                     # airflow.cfg (généré au démarrage)
└── tests/
    ├── conftest.py
    ├── test_dags.py            # import, présence, catchup=False
    └── test_hook.py            # test unitaire du Hook (mock requests)
```

## 🚀 Démarrage rapide

```bash
# 1. Configuration
cp .env.example .env
# (Linux/Mac) aligner l'UID Airflow : echo "AIRFLOW_UID=$(id -u)" >> .env

# 2. Lancer la stack
docker compose up -d

# 3. Suivre l'init (1er démarrage : pull des images + migrations, quelques minutes)
docker compose logs -f airflow-init
```

Puis ouvrir **Airflow** (http://localhost:8080), dépauser les DAGs et les déclencher.

### Connexions Airflow (pré-câblées)

| Connection ID | Type | Usage |
|---|---|---|
| `dwh_postgres` | Postgres | Écriture dans le DWH |
| `minio_default` | AWS/S3 | Data Lake (endpoint `http://minio:9000`) |
| `ademe_api` | HTTP | API DPE ADEME (`https://data.ademe.fr`) |

## ⌨️ Commandes (Makefile)

```bash
make up            # démarre la stack
make down          # arrête (conserve les données)
make reset         # arrête + supprime les volumes (reset total)
make logs          # suit les logs
make ps            # état des conteneurs
make test          # lance pytest dans le conteneur
make idempotence   # rejoue 2x le DAG transform et compare le COUNT
make psql          # shell psql sur le DWH
make airflow       # shell bash dans le scheduler
```

> Pas de `make` sous Windows ? Utilisez les commandes `docker compose ...` équivalentes
> (visibles dans le `Makefile`), ou `choco install make`.

## 🗃️ Modèle de données (étoile)

```
dim_commune (code_insee PK)   dim_date (dt PK)   dim_dpe (etiquette PK)   dim_type_bien (id PK)
                         \         |          /              /
                          ▼        ▼         ▼              ▼
                      fact_biens (dt, code_insee, etiquette, type_bien_id,
                                  surface, prix, prix_m2, conso_energie)

analytics.kpi_commune_mensuel (dt, code_insee, prix_m2_median,
                               pct_passoires, decote_passoire_pct, nb_transactions)
```

Défini dans `init-db/schema.sql` (exécuté automatiquement au 1er démarrage de `postgres-dwh`).

## ♻️ Idempotence (obligatoire)

Chaque run daté `{{ ds }}` rejoue le même résultat via `DELETE + INSERT` par partition
(voir `include/sql/transform_fact_biens.sql`) :

```sql
BEGIN;
  DELETE FROM dwh.fact_biens WHERE dt = '{{ ds }}';
  INSERT INTO dwh.fact_biens SELECT ... FROM staging.dpe WHERE dt = '{{ ds }}';
COMMIT;
```

**Vérification** (rejouer 2x doit donner le même `COUNT(*)`) :

```bash
make idempotence
```

## 📊 Dashboards Metabase (≥ 2)

1. **Marché par commune** — prix/m² médian, volume, carte.
2. **Impact énergétique** — décote prix/m² F/G vs A/B, % de passoires.
3. *(bonus)* **Détecteur d'opportunités** — biens sous la médiane communale (anomalies).

Connexion Metabase → PostgreSQL : host `postgres-dwh`, port **5432** (interne), db `immolake`.

## 🔔 Automatisation (bonus Telegram)

`immolake_analytics_daily` détecte les communes à forte proportion de passoires sous-cotées
et envoie une **alerte Telegram** (renseigner `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` dans
`.env`). Condition claire → action justifiée.

## ✅ Tests

```bash
make test
# ou : docker compose exec airflow-scheduler pytest tests/ -v
```

- `test_dags.py` : aucun import en erreur, 3 DAGs présents, `catchup=False`.
- `test_hook.py` : test unitaire du Custom Hook (mock de `requests`, sans réseau).

## 👥 Répartition (3 devs)

| Dev | Lot | Fichiers |
|---|---|---|
| **A** | Ingestion | `plugins/hooks/ademe_api_hook.py`, `dags/immolake_ingest_daily.py` |
| **B** | Entrepôt | `init-db/schema.sql`, `dags/immolake_transform_daily.py`, `include/sql/*.sql` |
| **C** | Serving | `dags/immolake_analytics_daily.py`, Metabase, Telegram, `tests/`, doc |

## 🛠️ Dépannage

| Symptôme | Solution |
|---|---|
| `airflow-init` boucle / permissions logs | Sous Linux/Mac, fixer `AIRFLOW_UID=$(id -u)` dans `.env` puis `docker compose up -d` |
| Port déjà utilisé (8080/3000/5433/9000/9001) | Modifier le mapping dans `docker-compose.yml` |
| Tag d'image introuvable | Ajuster `AIRFLOW_IMAGE_NAME` / `postgres:18` selon la Doc commune |
| Metabase « Cannot connect » au DWH | Host = `postgres-dwh`, port **5432** (interne, pas 5433) |
| Bucket MinIO absent | `docker compose restart minio-init` ou le créer dans la console (9001) |
| Dashboards perdus après `down -v` | Normal : `-v` supprime les volumes. Utiliser `down` sans `-v` |

## 🔗 Sources de données

- [API DPE logements (ADEME)](https://data.ademe.fr/datasets/dpe03existant) — `GET /data-fair/api/v1/datasets/dpe03existant/lines`
- [DVF — Demandes de Valeurs Foncières](https://www.data.gouv.fr/datasets/dvf)
- [DVF géolocalisées](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees)

## 🛑 Arrêt / reset

```bash
make down      # arrêt (conserve les données)
make reset     # arrêt + suppression des volumes (reset total)
```
