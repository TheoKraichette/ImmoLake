# Feuille de route — Jour 2 (post-MVP)

> À transformer en issues **une fois le MVP fini** (#15 serving, #9/#10 dashboards, #11 tests, #12 doc mergées).
> Chaque entrée est prête à devenir une issue (titre + contexte + checklist + dépendances).

---

## 🥇 Prioritaire

### A. Détecteur de « bonnes affaires » (en DuckDB)
**Pourquoi :** c'est le cœur du pitch (aider à cibler les biens sous-cotés) — pas encore construit.

**À faire**
- [ ] DAG/tâche analytics : lire `gold/fact_biens/**/*.parquet` via **DuckDB**
- [ ] par commune : `median(prix_m2)` et `stddev(prix_m2)` (window functions)
- [ ] flagger les biens : `prix_m2 < médiane − k·écart_type` (k ≈ 1 à 1,5)
- [ ] ignorer les communes avec trop peu de biens (< N) et les `prix_m2` nuls
- [ ] bonus : double signal si passoire F/G (sous-coté **et** rénovable)
- [ ] écrire `gold/opportunites/dt=…` (Parquet) → chargé dans `analytics.opportunites` (serving)
- [ ] MAJ `docs/ARCHITECTURE.md` ADR-004 (DuckDB devient moteur de transfo sur Parquet)

**Dépend de :** gold `fact_biens` (#6) ; serving (#15) pour l'exposer dans Metabase
**Effort :** ~3 h · **Labels :** lot-C, analyse

> Requête type :
> ```sql
> SELECT *, median(prix_m2) OVER (PARTITION BY code_insee) AS med,
>           stddev(prix_m2) OVER (PARTITION BY code_insee) AS sd
> FROM 'gold/fact_biens/**/*.parquet'
> QUALIFY prix_m2 < med - 1.0 * sd;
> ```

### B. Alerte WhatsApp (Twilio sandbox)
**Pourquoi :** exigence « condition claire → action déclenchée ». Condition = détection A.

**À faire**
- [ ] compte Twilio + sandbox WhatsApp (rejoindre le numéro sandbox depuis WhatsApp)
- [ ] secrets `TWILIO_ACCOUNT_SID` / `TWILIO_AUTH_TOKEN` / `TWILIO_WHATSAPP_FROM` / `WHATSAPP_TO` dans `.env`
- [ ] tâche Airflow : si opportunités détectées → `POST` Twilio REST « X biens sous-cotés à <commune> »
- [ ] **pas de ngrok** nécessaire (envoi sortant uniquement)

**Dépend de :** A
**Effort :** ~3 h · **Labels :** lot-C, automatisation

> Voie alternative officielle : WhatsApp Cloud API (Meta) — plus lourde (templates pré-approuvés, numéro vérifié).
> Bidirectionnel (consulter des KPI par message) = webhook entrant + ngrok + récepteur (n8n/Flask) → hors scope initial.

### C. Historisation par date métier ⚠️
**Piège à corriger d'abord :** aujourd'hui `dt` = **date du run**, pas la date de la donnée. Backfiller des dates de run ré-ingère la même photo → aucune vraie histoire.

**À faire**
- [ ] partitionner/agréger sur la **date métier** : `date_etablissement_dpe` (DPE), `date_mutation` (DVF)
- [ ] KPI agrégés **par mois** → courbes de tendance (prix/m², % passoires)
- [ ] backfill sur l'historique disponible

**Dépend de :** ajustement de la clé de partition (sinon cosmétique → à arbitrer)
**Effort :** ~2-3 h · **Labels :** lot-B

---

## 🥈 Secondaire (production-readiness / bonus TP)

### D. Observabilité Prometheus + Grafana
- `statsd-exporter` (métriques Airflow) + `postgres-exporter`, dashboards Grafana **11010** (Airflow) / **9628** (Postgres).
- **Effort :** ~½ journée · bonus « Could-have » des TP, très visuel en soutenance.

### E. Data Quality (câbler le `DataQualityOperator`)
- Règles `not_null(code_insee)`, `not_empty`, `no_future_date` + **branching** : DQ KO → alerte au lieu du load.
- **Effort :** ~2-3 h.

### F. CI GitHub Actions
- `pytest` + `docker compose config` + lint `ruff` à chaque PR → fait respecter la règle « 1 branche = 1 PR ».
- **Effort :** ~1-2 h.

### G. Soutenance / doc
- README avec schéma **Mermaid**, `docs/ARCHITECTURE.md` finalisé (#12), captures des dashboards, script de démo `up → run → dashboards`.
- **Effort :** ~2 h.

---

## ❌ Déconseillé (hors budget / faible ROI ici)
- **dbt** à la place de pandas/DuckDB (réécriture inutile vu la volumétrie)
- **Metabase directement sur DuckDB** (driver communautaire instable → garder Postgres pour le serving)
- migration cloud, dimensions SCD2

---

## Répartition indicative (3 devs, 1 journée)
| Dev | Focus |
|---|---|
| A | A (opportunités DuckDB) + B (WhatsApp) |
| B | D (observabilité Prometheus/Grafana) |
| C | C (historisation) + E (data quality) + F (CI) |
| Tous | G (soutenance) en fin de journée |
