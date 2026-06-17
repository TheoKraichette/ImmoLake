# Architecture — ImmoLake

## Vue d'ensemble

ImmoLake suit le pattern **Data Lakehouse** : un lac (MinIO) pour le brut rejouable,
un entrepôt structuré (PostgreSQL) pour le modèle propre, orchestré par Airflow et
exposé par Metabase.

```
 Sources           Ingestion        Data Lake (MinIO)         Data Warehouse (PostgreSQL)     BI
 ┌──────────┐      ┌──────────┐     ┌──────────────────┐      ┌──────────────────────────┐   ┌──────────┐
 │ API ADEME│─────►│AdemeHook │────►│ raw → staging →  │─────►│ staging → dwh → analytics│──►│ Metabase │
 │   DVF    │      │ (Airflow)│     │     curated      │      │   (étoile / Kimball)     │   │          │
 └──────────┘      └──────────┘     └──────────────────┘      └──────────────────────────┘   └──────────┘
                        ▲                                                  │
                   Orchestration Airflow (idempotence par dt)             ▼
                                                              Alerte Telegram (bonus)
```

## Flux d'un run quotidien

1. `immolake_ingest_daily` : le Hook appelle l'API ADEME → JSON brut déposé dans
   `raw/dt=YYYY-MM-DD/` (MinIO) → chargé dans `staging.dpe` (idempotent par `dt`).
2. `immolake_transform_daily` : `refresh_dim_commune.sql` puis `transform_fact_biens.sql`
   (DELETE + INSERT par partition) → `dwh.fact_biens`.
3. `immolake_analytics_daily` : `build_kpi_commune.sql` → `analytics.kpi_commune_mensuel`,
   puis détection d'anomalies → alerte Telegram (bonus).

## Couche de stockage : 3 schémas

| Schéma | Rôle | Granularité |
|---|---|---|
| `staging` | Données brutes typées, 1 ligne = 1 enregistrement source | par run `dt` |
| `dwh` | Modèle en étoile (dimensions + faits), intégrité FK | historisé |
| `analytics` | Agrégats pré-calculés pour Metabase | par `dt` / commune |

---

## ADR-001 — Pourquoi PostgreSQL comme Data Warehouse ?

**Statut :** accepté · **Contexte :** projet de week-end, volumes modérés, sujet imposant Postgres.

PostgreSQL est une base **OLTP** détournée en rôle de DWH. Ce n'est pas un entrepôt
colonnaire (BigQuery, Snowflake, ClickHouse), et c'est volontaire :

- **Volume adapté** : quelques millions de lignes filtrées par commune → Postgres + index
  répond en millisecondes. Un DWH colonnaire serait du sur-engineering.
- **Conforme au sujet** : les TP imposent PostgreSQL avec `staging`/`dwh`/`analytics`.
- **Kimball en relationnel** : PK/FK et jointures dim↔fait sont le terrain naturel de Postgres
  (un moteur colonnaire gère mal les FK).
- **Idempotence transactionnelle** : le pattern `BEGIN; DELETE; INSERT; COMMIT;` repose sur
  l'ACID de Postgres.
- **Gratuit, local, Dockerisable** : reproductible pour la soutenance, sans compte cloud.

> Le rôle « lakehouse » est porté par **MinIO + Postgres ensemble** : MinIO garde le brut
> rejouable, Postgres porte le modèle propre.

**Évolution possible (si gros volumes)** : passer les fichiers MinIO en **Parquet** et
interroger via **DuckDB**, ou migrer le DWH vers **ClickHouse**. À mentionner en soutenance.

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
