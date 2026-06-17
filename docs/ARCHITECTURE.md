# Architecture — ImmoLake

## Vue d'ensemble

ImmoLake suit le pattern **Data Lakehouse** : un lac (MinIO) pour le brut rejouable,
un entrepôt structuré (PostgreSQL) pour le modèle propre, orchestré par Airflow et
exposé par Metabase.

```
 Sources              Orchestration       Data Lake (MinIO)                     Serving                 BI
 ┌──────────┐         ┌──────────┐        ┌────────────────────────────┐        ┌──────────────────┐    ┌──────────┐
 │ API ADEME│────────►│ Airflow  │───────►│ raw/dpe                    │        │ dwh.fact_biens   │───►│ Metabase │
 │ DPE      │         │          │        │ silver/dpe                 │        │ analytics.kpi_*  │    │          │
 └──────────┘         │          │        │                            │        └──────────────────┘    └──────────┘
 ┌──────────┐         │          │        │ raw/dvf                    │                 ▲
 │ CSV DVF  │────────►│          │───────►│ silver/dvf                 │                 │
 └──────────┘         └──────────┘        │ gold/fact_biens            │─────────────────┘
                                          │ gold/kpi_commune           │
                                          └────────────────────────────┘
```

## Flux d'un run quotidien

1. **Bronze DPE** : le Hook appelle l'API ADEME → JSON brut déposé dans `raw/dpe/dt=`.
2. **Bronze DVF** : Airflow télécharge le CSV `DVF_CSV_URL` → `raw/dvf/dt=`.
3. **Silver** : nettoyage / typage avec pandas/pyarrow → `silver/dpe/dt=` et `silver/dvf/dt=`.
4. **Gold facts** : construction de `gold/fact_biens/dt=` avec rattachement dimensions et enrichissement prix DVF.
5. **Gold KPIs** : agrégation de `fact_biens` → `gold/kpi_commune/dt=`.
6. **Serving** : chargement idempotent du gold → `dwh.fact_biens` et `analytics.kpi_commune_mensuel`
   (`DELETE + INSERT` par `dt`). *(Bonus prévu, non activé : alerte WhatsApp via Twilio.)*

## Couches de stockage

**Lac (MinIO, Parquet) — médaillon :**

| Zone | Rôle |
|---|---|
| `raw/dpe` | JSON brut ADEME, rejouable |
| `raw/dvf` | CSV DVF brut téléchargé |
| `silver/dpe` | DPE nettoyés, typés, dédupliqués |
| `silver/dvf` | transactions/prix DVF nettoyés |
| `gold/fact_biens` | table de faits Parquet enrichie prix/DPE |
| `gold/kpi_commune` | indicateurs métier par commune |

**Serving (PostgreSQL) :**

| Schéma | Rôle |
|---|---|
| `dwh` | modèle en étoile (dimensions + faits), intégrité FK |
| `analytics` | agrégats pré-calculés pour Metabase |

## Choix de modélisation MVP

### Enrichissement prix DVF

Le matching exact DPE ↔ transaction DVF nécessite des clés fines (adresse normalisée,
parcelle, géolocalisation fiable). Pour rester robuste en MVP, ImmoLake enrichit les biens
avec un **prix/m² médian DVF par `code_insee + type_bien`** :

1. `silver/dvf` calcule `prix_m2 = prix / surface` quand la colonne n'existe pas.
2. `gold/fact_biens` joint ce référentiel agrégé sur `code_insee` et `type_bien`.
3. `prix = surface * prix_m2` est recalculé dans le gold.

Cette approche est explicable, rejouable et suffisante pour produire des KPI prix par commune.
Elle pourra évoluer vers un matching adresse/parcelle si les données sont disponibles.

### KPI communaux

`gold/kpi_commune` agrège `gold/fact_biens` par commune :

- `prix_m2_median` : médiane des prix/m² enrichis ;
- `pct_passoires` : part de DPE F/G ;
- `decote_passoire_pct` : écart moyen prix/m² F/G vs non-passoires ;
- `nb_transactions` : nombre de biens dans le fait.

---

## ADR-001 — Pourquoi PostgreSQL comme Data Warehouse ?

**Statut :** accepté · **Contexte :** projet de week-end, volumes modérés, sujet imposant Postgres.

PostgreSQL est une base **OLTP** détournée en rôle de DWH. Ce n'est pas un entrepôt
colonnaire (BigQuery, Snowflake, ClickHouse), et c'est volontaire :

- **Volume adapté** : quelques millions de lignes filtrées par commune → Postgres + index
  répond en millisecondes. Un DWH colonnaire serait du sur-engineering.
- **Conforme au sujet** : les TP imposent PostgreSQL pour le modèle `dwh`/`analytics`.
- **Kimball en relationnel** : PK/FK et jointures dim↔fait sont le terrain naturel de Postgres
  (un moteur colonnaire gère mal les FK).
- **Idempotence transactionnelle** : le pattern `BEGIN; DELETE; INSERT; COMMIT;` repose sur
  l'ACID de Postgres.
- **Gratuit, local, Dockerisable** : reproductible pour la soutenance, sans compte cloud.

> Le rôle « lakehouse » est porté par **MinIO + Postgres ensemble** : MinIO garde le brut
> rejouable, Postgres porte le modèle propre.

**Évolution possible (si gros volumes)** : interroger directement le gold Parquet via
**DuckDB** (écarté ici pour rester simple), ou migrer le serving vers **ClickHouse**.

---

## ADR-002 — Pourquoi Airflow en LocalExecutor ?

Le sujet de référence (Marketplace) utilise CeleryExecutor (Redis + workers). Pour ImmoLake
on choisit **LocalExecutor** : les tâches s'exécutent dans le scheduler, ce qui **supprime
Redis et les workers Celery**. Moins de conteneurs, démarrage plus rapide, suffisant pour
un pipeline `@daily` à faible parallélisme. Le passage à Celery reste trivial si besoin.

## ADR-003 — Pourquoi Metabase plutôt que Superset ?

Metabase : installation Docker triviale, auto-détection des relations du modèle en étoile,
suffisant pour 2–4 dashboards. Superset est plus puissant mais plus complexe à configurer
— hors budget pour un week-end.

## ADR-004 — Médaillon matérialisé dans MinIO (Parquet), sans DuckDB

Les couches **silver** et **gold** sont des fichiers **Parquet** dans MinIO (pas seulement des
schémas Postgres). Les transformations raw→silver→gold se font en **Python (pandas/pyarrow)**
dans Airflow. PostgreSQL ne porte que le **serving** (dwh + analytics), chargé depuis le gold,
pour que Metabase interroge du SQL.

DuckDB n'est **pas** utilisé : pour le volume du projet, charger le gold dans Postgres suffit
et évite une techno de plus. Conséquences : pas de schéma `staging` dans Postgres (le silver
vit dans le lac) et les transformations ne sont plus écrites en SQL.

## Exploitation Metabase

Metabase interroge uniquement PostgreSQL :

- `dwh.fact_biens` pour les analyses détaillées ;
- `analytics.kpi_commune_mensuel` pour les dashboards agrégés.

La connexion Metabase utilise le réseau Docker interne : host `postgres-dwh`, port `5432`,
base `immolake`, utilisateur `dwh_user`.
