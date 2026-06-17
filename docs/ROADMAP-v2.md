# Roadmap v2 — ImmoLake (lakehouse-native : DuckDB + front Streamlit)

> Objectif : passer d'un BI générique (Metabase) à une **app data sur-mesure** alimentée par
> **DuckDB** qui interroge directement le gold Parquet (vrai lakehouse), avec un **détecteur de
> bonnes affaires** comme produit d'appel. Budget : **3 devs, 1 journée**. On reste simple.

## Critique de l'archi « big data » du MVP (honnête et courte)

1. **Transfo en pandas, tout en mémoire** : a calé au-delà de ~200 k logements/ville (RAM).
   → **DuckDB** lit le Parquet en streaming, dépasse la RAM, et le SQL analytique est plus lisible.
2. **PostgreSQL détourné en « serving »** : ok pour le MVP, mais DuckDB peut interroger le gold
   Parquet **directement** dans MinIO (`httpfs`/S3) → Postgres devient optionnel, archi plus lakehouse.
3. **BI générique (Metabase)** : pratique mais impersonnel → un **front sur-mesure** raconte mieux
   l'histoire métier (carte, opportunités, filtres).
4. **Astuces MVP assumées** (hors scope 1 jour) : 1 fichier DVF par run, pas de dépendances
   inter-DAG (ordre manuel), 1 ville = 1 `dt`, snapshot committé. À industrialiser plus tard
   (DVF départemental, Assets Airflow 3) — on **ne** s'en occupe **pas** en v2 pour rester simple.

## Stack v2

- **DuckDB** (embarqué, pas de service) : moteur analytique sur `gold/*.parquet` via `httpfs` → MinIO.
- **Streamlit** (recommandé vs React pour 1 jour : Python natif, data-first, rapide à livrer) :
  front sur-mesure, service Docker `streamlit` (port 8501). React resterait possible mais coûte
  une API + un build → hors budget 1 jour.
- Postgres / Metabase **conservés** (rétro-compat) ; le front v2 lit **DuckDB**.

## Issues v2 (ordre)

| # | Issue | Dépend de | Effort |
|---|---|---|---|
| 1 | [Infra] DuckDB + service Streamlit dans la stack | — | ~1,5 h 🚧 |
| 2 | [DuckDB] Couche analytique sur le gold Parquet (httpfs MinIO) | 1 | ~2 h 🚧 |
| 3 | [DuckDB] Détecteur de bonnes affaires (médiane − k·σ) | 2 | ~2 h |
| 4 | [Front] App Streamlit — pages Marché & Énergie + filtres | 2 | ~3 h |
| 5 | [Front] Carte interactive prix/m² (DVF géolocalisées) | 4 | ~2 h |
| 6 | [Front] Page « Bonnes affaires » | 3, 4 | ~1,5 h |
| 7 | [Doc] README v2 + ADR DuckDB/Streamlit + critique big-data | 4, 6 | ~1,5 h |

## Répartition (3 devs / 1 jour)

| Dev | Focus |
|---|---|
| A | #1 + #2 (socle DuckDB + couche analytique) |
| B | #4 + #5 (pages Streamlit + carte) |
| C | #3 + #6 (détecteur + page opportunités) |
| Tous | #7 (doc) en fin de journée |

## Bonus (si le temps)

- **Alerte WhatsApp** (Twilio) sur opportunités détectées.
- Migrer les transfos `silver → gold` de pandas vers **DuckDB** (perf / scale).

## Plus tard (au-delà de la v2)

Pistes hors budget 1 jour, à reprendre ensuite :
- **Observabilité** : Prometheus + Grafana (métriques Airflow via statsd-exporter, postgres-exporter).
- **Data Quality** : câbler le `DataQualityOperator` (not_null / not_empty / no_future_date) + branching.
- **CI** : GitHub Actions (pytest + `docker compose config` + lint) à chaque PR.
- **Historisation par date métier** : partitionner sur `date_etablissement` / `date_mutation` (pas la date de run) pour de vraies tendances.
- **Industrialisation pipeline** : DVF départemental (au lieu d'1 fichier/run), dépendances inter-DAG (Assets Airflow 3).
