# Architecture — ImmoLake

## Vue d'ensemble

ImmoLake suit le pattern **Data Lakehouse** : un lac (MinIO) pour le brut rejouable,
un entrepôt structuré (PostgreSQL) pour le modèle propre, orchestré par Airflow et
exposé par Metabase.

```
 Sources           Ingestion        Data Lake (MinIO)         Data Warehouse (PostgreSQL)     BI
 ┌──────────┐      ┌──────────┐     ┌──────────────────┐      ┌──────────────────────────┐   ┌──────────┐
 │ API ADEME│─────►│AdemeHook │────►│ raw → silver →   │─────►│ dwh + analytics (serving)│──►│ Metabase │
 │   DVF    │      │ (Airflow)│     │  gold (Parquet)  │      │   (étoile / Kimball)     │   │          │
 └──────────┘      └──────────┘     └──────────────────┘      └──────────────────────────┘   └──────────┘
                        ▲                                                  │
                   Orchestration Airflow (idempotence par dt)             ▼
                                                              Alerte Telegram (bonus)
```

## Flux d'un run quotidien

1. **Bronze** : le Hook appelle l'API ADEME → JSON brut déposé dans `raw/dpe/dt=` (MinIO).
2. **Silver** : nettoyage / typage (pandas/pyarrow) → `silver/dpe/dt=` en Parquet.
3. **Gold** : modélisation (faits) + agrégats → `gold/fact_biens/dt=` et `gold/kpi_commune/dt=` (Parquet).
4. **Serving** : chargement idempotent du gold → `dwh.fact_biens` et `analytics.kpi_commune_mensuel`
   (DELETE + INSERT par `dt`). *(Bonus prévu, non activé : détection d'anomalies → alerte Telegram.)*

## Couches de stockage

**Lac (MinIO, Parquet) — médaillon :**

| Zone | Rôle |
|---|---|
| `raw` | JSON brut de l'API, rejouable (bronze) |
| `silver` | données nettoyées / typées / dédupliquées (Parquet) |
| `gold` | faits + KPIs modélisés (Parquet) |

**Serving (PostgreSQL) :**

| Schéma | Rôle |
|---|---|
| `dwh` | modèle en étoile (dimensions + faits), intégrité FK |
| `analytics` | agrégats pré-calculés pour Metabase |

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
