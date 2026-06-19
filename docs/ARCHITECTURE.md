# Architecture — ImmoLake

## Vue d'ensemble (v2)

ImmoLake suit le pattern **Data Lakehouse** : **MinIO** porte tout le médaillon en **Parquet**
(brut rejouable → modèle propre), **DuckDB** exécute les transformations et sert les requêtes
analytiques directement sur ce Parquet (via `httpfs`/S3), **Airflow 3** orchestre (chaînage
**event-driven** par Assets), et un front **Streamlit** expose les dashboards.

```
 Sources                 Orchestration        Data Lake (MinIO, Parquet) + DuckDB            Front
 ┌──────────┐            ┌──────────┐         ┌───────────────────────────────────┐         ┌───────────┐
 │ API ADEME│───────────►│ Airflow 3│────────►│ raw/dpe  raw/dvf                   │         │ Streamlit │
 │ DPE      │            │ (Assets) │         │ silver/dpe  silver/dvf            │◄────────│ (DuckDB)  │
 └──────────┘            │          │         │ gold/fact_biens  gold/kpi_commune │         │ 5 pages   │
 ┌──────────┐            │          │         │ gold/mart_* (commune, type,       │         └───────────┘
 │ geo-dvf  │───────────►│          │────────►│   opportunites, dvf_stats)        │
 │ par dep  │            └──────────┘         │ ref/* (dimensions + geo_commune)  │
 └──────────┘                                 └───────────────────────────────────┘
```

## Flux d'un run (event-driven)

1. **`immolake_ingest_daily`** — le Hook `AdemeApiHook` pagine l'API ADEME **par département**
   (générateur, mémoire bornée) en ne demandant que les **colonnes pertinentes** (`select`) →
   `raw/dpe/dt=/dep=*`. Produit l'asset **`RAW_DPE`**.
2. **`immolake_transform_daily`** (déclenché par `RAW_DPE`) — télécharge le **DVF par département**
   (`raw/dvf/dt=/dep=*`), puis DuckDB en SQL : `silver/dpe`, `silver/dvf`, `gold/fact_biens`
   (enrichissement prix), `gold/kpi_commune`. Un **data quality gate** court-circuite la suite si
   le gold est invalide. Produit l'asset **`GOLD_FACT`**.
3. **`immolake_marts_daily`** (déclenché par `GOLD_FACT`) — DuckDB matérialise `mart_commune`,
   `mart_commune_type`, `mart_opportunites` (détecteur médiane − k·σ) et `dvf_stats_commune_type`
   (percentiles). `detect_and_alert` logge les opportunités (alerte WhatsApp prévue, non activée).

`immolake_seed_ref` (manuel) régénère les dimensions `ref/` (committées en Parquet, chargées au
boot par `minio-init`) + pousse `geo_commune`.

## Couches de stockage (MinIO, Parquet)

| Zone | Rôle |
|---|---|
| `raw/dpe/dt=/dep=*` | DPE bruts ADEME (colonnes sélectionnées), par département |
| `raw/dvf/dt=/dep=*` | CSV DVF bruts (geo-dvf), par département |
| `silver/dpe`, `silver/dvf` | nettoyés, typés, dédupliqués (DuckDB) |
| `gold/fact_biens` | faits enrichis prix/DPE/GES/coût/émissions/année |
| `gold/kpi_commune` | indicateurs métier par commune |
| `gold/mart_*` | marts servis au front (commune, type, opportunités, percentiles DVF) |
| `ref/*` | dimensions (`dim_commune`, `dim_dpe`, `dim_type_bien`) + `geo_commune` (carte) |

## Choix de modélisation

### Enrichissement prix DVF
Pas de matching adresse/parcelle (clés fines indisponibles en MVP). `gold/fact_biens` joint un
**prix/m² médian DVF par `code_insee × type_bien × tranche de surface`** (fallback `code_insee ×
type_bien`), puis `prix = surface × prix_m2`. La vraie **dispersion** (percentiles p10..p90) vit
dans `gold/dvf_stats_commune_type`, calculée sur les transactions DVF brutes.

### Colonnes pertinentes
silver/gold ne portent que les champs utiles aux cas d'usage : étiquettes DPE **et GES**, conso,
**émissions**, **coût énergie annuel**, **énergie de chauffage**, **année de construction** — plus
les agrégats commune correspondants dans `mart_commune` (% passoires DPE & GES, coût médian, etc.).

---

## ADR-005 — DuckDB sur le Parquet du lac (retrait de PostgreSQL) · *v2, remplace ADR-001 & ADR-004*

**Statut :** accepté (v2).
La v1 calait : transformations **pandas tout en mémoire** (~200 k logements/ville max) + un serving
**PostgreSQL** à charger. La v2 exécute tout en **DuckDB SQL streaming** lisant/écrivant le Parquet
MinIO via `httpfs` (`memory_limit` + spill disque) → tient des volumes 10–100× supérieurs sans OOM,
et **supprime Postgres** : le gold/marts Parquet EST le serving, interrogé directement par DuckDB.
Conséquences : transformations en SQL versionné (`include/sql/*.sql`), plus de provider Postgres,
idempotence par purge+réécriture de partition (au lieu de `DELETE/INSERT` transactionnel).

## ADR-006 — Front Streamlit (remplace Metabase) · *v2, remplace ADR-003*

**Statut :** accepté (v2).
Metabase imposait Postgres + un provisioning par script. Le front **Streamlit** interroge le
Parquet via DuckDB (même moteur que les transforms), permet un produit sur-mesure (détecteur de
bonnes affaires, carte choroplèthe, comparateur) et supprime un service + une base.

## ADR-007 — `select` des colonnes à l'ingestion ADEME

**Statut :** accepté.
Le dataset `dpe03existant` compte ~230 colonnes ; les cas d'usage en exploitent ~14. Le Hook passe
désormais `select=…` à l'API data-fair : pages réseau et Parquet `raw` **fortement allégés**, ce qui
réduit le coût d'ingestion et permet de **couvrir plus de départements/villes** à budget égal. La
liste est canonique (les `silver_*` lisent exactement ces champs) et inclut le signal métier capté
en plus (GES, coût énergie, énergie de chauffage, émissions, année de construction).

## ADR-008 — Couverture DVF dérivée des départements DPE

**Statut :** accepté.
L'ingestion DPE est multi-département ; le DVF l'était resté en mono-fichier (`DVF_CSV_URL`) → prix
NULL hors de ce périmètre. La v2 **dérive les fichiers DVF** (`geo-dvf` par département) de la liste
`ADEME_DEPARTEMENTS` (+ `DVF_YEAR`), écrits sous `raw/dvf/dt=/dep=*` et lus en glob → **couverture
prix alignée sur les DPE**. `DVF_CSV_URL` subsiste en override mono-fichier. Les départements sans
DVF (**Alsace-Moselle 67/68/57**, régime du livre foncier) renvoient 404 et sont **ignorés** (DPE
conservés, sans prix) plutôt que de faire échouer le run.

---

## ADR (v1 — historique, révisés en v2)

- **ADR-001 — PostgreSQL comme DWH** : *révisé* — le serving Postgres est retiré (voir ADR-005).
- **ADR-002 — Airflow LocalExecutor** : *toujours valable* — tâches dans le scheduler, pas de
  Celery/Redis ; suffisant pour un pipeline `@daily`.
- **ADR-003 — Metabase plutôt que Superset** : *révisé* — front Streamlit (voir ADR-006).
- **ADR-004 — Médaillon Parquet sans DuckDB** : *révisé* — DuckDB est désormais central (ADR-005).
