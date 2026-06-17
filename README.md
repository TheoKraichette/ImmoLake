# 🏠 ImmoLake — Prix × Performance énergétique

Plateforme **Data Lakehouse** qui croise les prix de l'immobilier (DVF) avec les
diagnostics de performance énergétique (DPE / ADEME) pour répondre à une question métier :

> **Quel est l'impact de l'étiquette DPE sur le prix au m² par commune, et peut-on
> détecter les biens sous-cotés (bonnes affaires) ou sur-cotés (risques) ?**

Contexte : avec la loi Climat & Résilience, les **passoires thermiques (DPE F/G)** sont
progressivement interdites à la location. L'objectif est d'aider un investisseur à cibler
les communes / biens au meilleur rapport prix / rénovation.

## Architecture

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
voir [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Stack & accès

| Service | Rôle | Accès |
|---|---|---|
| **Airflow 3.x** (LocalExecutor) | Orchestration | http://localhost:8080 — `airflow` / `airflow` |
| **MinIO** | Data Lake S3 (raw/staging/curated) | http://localhost:9001 — `minio_admin` / `minio_password_2026` |
| **PostgreSQL 18** | Data Warehouse métier | `localhost:5433` — `dwh_user` / `dwh_password`, db `immolake` |
| **Metabase** | BI / dashboards | http://localhost:3000 |

Conteneurs annexes : `postgres-airflow` (métadonnées Airflow, interne), `minio-init` et
`airflow-init` (one-shot, s'arrêtent après initialisation).

> PostgreSQL n'est pas une interface web : on l'inspecte via un client SQL (DBeaver, pgAdmin…)
> sur `localhost:5433`, ou en ligne de commande (voir [Commandes utiles](#commandes-utiles)).

## Arborescence

```
immolake/
├── docker-compose.yml          # Stack complète (Airflow + MinIO + Postgres + Metabase)
├── .env.example                # Variables (copier en .env)
├── requirements.txt
├── .gitattributes  .gitignore
├── docs/
│   ├── ARCHITECTURE.md         # Schéma détaillé + décisions (ADR)
│   └── CONTRIBUTING.md         # Règles Git (1 issue = 1 branche)
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

## Démarrage rapide

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

## Commandes utiles

```bash
docker compose up -d                 # démarre la stack
docker compose down                  # arrête (conserve les données)
docker compose down -v               # arrête + supprime les volumes (reset total)
docker compose ps                    # état des conteneurs
docker compose logs -f <service>     # logs d'un service

# Shell psql sur le DWH
docker compose exec postgres-dwh psql -U dwh_user -d immolake

# Lancer les tests
docker compose exec airflow-scheduler pytest tests/ -v
```

## Modèle de données (étoile)

```
dim_commune (code_insee PK)   dim_date (dt PK)   dim_dpe (etiquette PK)   dim_type_bien (id PK)
                         \         |          /              /
                          ▼        ▼         ▼              ▼
                      fact_biens (dt, code_insee, etiquette, type_bien_id,
                                  surface, prix, prix_m2, conso_energie)

analytics.kpi_commune_mensuel (dt, code_insee, prix_m2_median,
                               pct_passoires, decote_passoire_pct, nb_transactions)
```

Tables définies dans `init-db/schema.sql` ; dimensions de référence peuplées par les seeds
`init-db/zz1_seed_static.sql` (dim_type_bien, dim_date) et `init-db/zz2_seed_dim_commune.sh`
(dim_commune, depuis le référentiel INSEE).

## Idempotence (obligatoire)

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
docker compose exec airflow-scheduler airflow dags test immolake_transform_daily 2026-06-17
docker compose exec airflow-scheduler airflow dags test immolake_transform_daily 2026-06-17
docker compose exec postgres-dwh psql -U dwh_user -d immolake \
  -c "SELECT COUNT(*) FROM dwh.fact_biens WHERE dt='2026-06-17';"
```

## Dashboards Metabase (≥ 2)

1. **Marché par commune** — prix/m² médian, volume, carte.
2. **Impact énergétique** — décote prix/m² F/G vs A/B, % de passoires.
3. *(bonus)* **Détecteur d'opportunités** — biens sous la médiane communale (anomalies).

Connexion Metabase → PostgreSQL : host `postgres-dwh`, port **5432** (interne), db `immolake`.

## Automatisation (bonus Telegram)

`immolake_analytics_daily` détecte les communes à forte proportion de passoires sous-cotées
et envoie une **alerte Telegram** (renseigner `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` dans
`.env`). Condition claire → action justifiée.

## Tests

```bash
docker compose exec airflow-scheduler pytest tests/ -v
```

- `test_dags.py` : aucun import en erreur, 3 DAGs présents, `catchup=False`.
- `test_hook.py` : test unitaire du Custom Hook (mock de `requests`, sans réseau).

## Contribution

Workflow obligatoire : **1 issue = 1 branche**, PR vers `main`, relecture, puis merge.
**Jamais de commit direct sur `main`.** Détails dans [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md).

## Dépannage

| Symptôme | Solution |
|---|---|
| `airflow-init` boucle / permissions logs | Sous Linux/Mac, fixer `AIRFLOW_UID=$(id -u)` dans `.env` puis relancer |
| Port déjà utilisé (8080/3000/5433/9000/9001) | Modifier le mapping dans `docker-compose.yml` |
| Tag d'image introuvable | Ajuster `AIRFLOW_IMAGE_NAME` / `postgres:18` dans `.env` |
| Metabase « Cannot connect » au DWH | Host = `postgres-dwh`, port **5432** (interne, pas 5433) |
| Bucket MinIO absent | `docker compose restart minio-init` ou le créer dans la console (9001) |
| Données perdues après `down -v` | Normal : `-v` supprime les volumes. Utiliser `down` sans `-v` |

## Sources de données

- [API DPE logements (ADEME)](https://data.ademe.fr/datasets/dpe03existant) — `GET /data-fair/api/v1/datasets/dpe03existant/lines`
- [Référentiel communes INSEE](https://geo.api.gouv.fr/communes) — seed de `dim_commune`
- [DVF — Demandes de Valeurs Foncières](https://www.data.gouv.fr/datasets/dvf)
- [DVF géolocalisées](https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees) — lat/long des transactions (carte Metabase, rattachement commune)
