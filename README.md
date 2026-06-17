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
   │   MinIO (S3)         │        │   PostgreSQL (serving)        │
   │  raw → silver →      │ ─────► │  dwh + analytics              │
   │     gold (Parquet)   │        │  (modèle en étoile, Kimball)  │
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
| **MinIO** | Data Lake S3 — médaillon `raw`/`silver`/`gold` (Parquet) | http://localhost:9001 — `minio_admin` / `minio_password_2026` |
| **PostgreSQL 18** | Serving (dwh + analytics) pour Metabase | `localhost:5433` — `dwh_user` / `dwh_password`, db `immolake` |
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
│   ├── schema.sql              # Schémas dwh/analytics + dim_dpe
│   ├── zz1_seed_static.sql     # Seed dim_type_bien + dim_date
│   ├── zz2_seed_dim_commune.sh # Seed dim_commune (communes + arrondissements)
│   ├── communes.json           # Référentiel INSEE des communes (snapshot)
│   └── arrondissements.json    # Arrondissements municipaux 75/69/13 (snapshot)
├── dags/
│   ├── immolake_ingest_daily.py      # API → MinIO raw (bronze)
│   ├── immolake_transform_daily.py   # raw → silver → gold (Parquet)
│   └── immolake_analytics_daily.py   # gold → Postgres + alerte (bonus)
├── plugins/
│   ├── hooks/
│   │   └── ademe_api_hook.py   # Custom Hook API ADEME
│   └── operators/
│       └── data_quality_operator.py  # DataQualityOperator (ébauche bonus, non câblé)
├── include/
│   └── sql/                    # (réservé) chargement gold → Postgres fait en Python (immolake_analytics_daily.py)
├── config/                     # airflow.cfg (généré au démarrage)
└── tests/
    ├── conftest.py
    ├── test_dags.py            # import, présence, catchup=False
    ├── test_hook.py            # Hook ADEME (mock requests)
    ├── test_transform.py       # nettoyage silver + enrichissement DVF
    ├── test_kpi.py             # agrégation KPI par commune
    ├── test_serving_load.py    # chargement gold → Postgres
    └── test_idempotence.py     # rejouer = même résultat
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

> Les dashboards sont déjà peuplés par le snapshot (voir plus bas). Pour **rejouer le pipeline**
> sur de vraies données, renseigner d'abord une **URL CSV DVF valide** dans `DVF_CSV_URL` (`.env`) —
> sinon la tâche `dvf_to_raw` échoue. Ordre des DAGs : `ingest → transform → analytics` pour un même `ds`.

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

Chaque run rejoue le même résultat. Les transformations raw→silver→gold (Parquet) écrasent
la partition `dt=`, et au chargement **gold → Postgres** (en Python, `_load_dataframe_idempotent`
dans `immolake_analytics_daily.py`) on remplace la partition du jour (`DELETE + INSERT WHERE dt = {{ ds }}`) :

```sql
BEGIN;
  DELETE FROM dwh.fact_biens WHERE dt = '{{ ds }}';
  -- INSERT depuis le gold (Parquet) chargé en mémoire
COMMIT;
```

**Vérification** (rejouer 2x le pipeline doit donner le même `COUNT(*)`) :

```bash
docker compose exec postgres-dwh psql -U dwh_user -d immolake \
  -c "SELECT COUNT(*) FROM dwh.fact_biens WHERE dt='2026-06-17';"
```

## Dashboards Metabase (≥ 2)

Provisionnés **par code** (idempotent) via `scripts/setup_metabase.py` — connexion DWH +
questions SQL + 2 dashboards (avec encarts explicatifs), sans clics manuels.

1. **Marché par commune** — prix/m² médian, nombre de logements, détail par commune.
2. **Impact énergétique (DPE)** — % de passoires (F/G), répartition des étiquettes A→G.

Provisioning (une fois la stack démarrée) :

```bash
docker compose exec -T airflow-scheduler python - < scripts/setup_metabase.py
```

Dashboards sur http://localhost:3000 (`admin@immolake.local` / `MB_ADMIN_PASSWORD`).
Connexion Metabase → PostgreSQL : host `postgres-dwh`, port **5432** (interne), db `immolake`.

## Données de démonstration (snapshot)

Le serving (`dwh.fact_biens` + `analytics.kpi_commune_mensuel`) est **pré-rempli dès le 1er
`docker compose up`** via un snapshot committé (`init-db/snapshot/*.csv.gz`, restauré par
`init-db/zz9_restore_snapshot.sh`) → **tout le monde a des dashboards peuplés sans relancer le pipeline.**
Couverture : 21 zones (19 grandes villes + Paris 1er/2e), ~200 k logements (échantillon).

Régénérer le snapshot depuis des données fraîches : lancer le pipeline puis `bash scripts/make_snapshot.sh`.

## Automatisation

L'automatisation du MVP, c'est l'**orchestration Airflow quotidienne idempotente** (run daté `{{ ds }}`,
condition claire → chargement rejouable). Une **alerte Telegram** est **prévue mais non activée** dans
`immolake_analytics_daily` (tâche `detect_and_alert`, qui se contente de logguer) : pour l'activer,
brancher l'appel `sendMessage` avec `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`. *(Bonus — voir aussi la
roadmap jour 2 : alerte WhatsApp via Twilio.)*

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
