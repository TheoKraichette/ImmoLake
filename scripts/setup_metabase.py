#!/usr/bin/env python3
"""Provisionne Metabase (idempotent) : connexion DWH + questions SQL + 2 dashboards.

Exécution (depuis la racine du repo, stack lancée) :
  docker run --rm --network immolake_default -v "$PWD/scripts:/s" \
    -e MB_URL=http://metabase:3000 python:3.12-slim \
    sh -c "pip install -q requests && python /s/setup_metabase.py"

Variables : MB_URL, MB_ADMIN_EMAIL, MB_ADMIN_PASSWORD, DWH_HOST/PORT/DB/USER/PASSWORD.
"""
import os
import sys
import time

import requests

MB = os.environ.get("MB_URL", "http://metabase:3000").rstrip("/")
EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@immolake.local")
PWD = os.environ.get("MB_ADMIN_PASSWORD", "immolake_admin_2026")
DB_NAME = "ImmoLake DWH"
DWH = {
    "host": os.environ.get("DWH_HOST", "postgres-dwh"),
    "port": int(os.environ.get("DWH_PORT", "5432")),
    "dbname": os.environ.get("DWH_DB", "immolake"),
    "user": os.environ.get("DWH_USER", "dwh_user"),
    "password": os.environ.get("DWH_PASSWORD", "dwh_password"),
}

s = requests.Session()

KPI = "analytics.kpi_commune_mensuel"
FACT = "dwh.fact_biens"
DIM = "dwh.dim_commune"


def wait_for_metabase():
    for _ in range(60):
        try:
            if s.get(f"{MB}/api/health", timeout=5).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    sys.exit("Metabase ne répond pas")


def authenticate():
    props = s.get(f"{MB}/api/session/properties").json()
    token = props.get("setup-token")
    if token and not props.get("has-user-setup"):
        r = s.post(f"{MB}/api/setup", json={
            "token": token,
            "user": {"first_name": "Admin", "last_name": "Immo",
                     "email": EMAIL, "password": PWD, "site_name": "ImmoLake"},
            "prefs": {"site_name": "ImmoLake", "allow_tracking": False},
        })
        r.raise_for_status()
        session_id = r.json()["id"]
        print("Metabase configuré (admin créé).")
    else:
        r = s.post(f"{MB}/api/session", json={"username": EMAIL, "password": PWD})
        r.raise_for_status()
        session_id = r.json()["id"]
        print("Connecté à Metabase existant.")
    s.headers.update({"X-Metabase-Session": session_id})


def ensure_database():
    data = s.get(f"{MB}/api/database").json()
    dbs = data.get("data", data) if isinstance(data, dict) else data
    for db in dbs:
        if db.get("name") == DB_NAME:
            return db["id"]
    r = s.post(f"{MB}/api/database", json={
        "name": DB_NAME, "engine": "postgres",
        "details": {**DWH, "ssl": False, "schema-filters-type": "all", "tunnel-enabled": False},
    })
    r.raise_for_status()
    db_id = r.json()["id"]
    s.post(f"{MB}/api/database/{db_id}/sync_schema")
    return db_id


def _bar_viz(dimension, metric, x_title, y_title):
    return {
        "graph.dimensions": [dimension],
        "graph.metrics": [metric],
        "graph.x_axis.title_text": x_title,
        "graph.y_axis.title_text": y_title,
        "graph.show_values": True,
    }


def ensure_card(db_id, name, sql, display, viz=None):
    """Crée ou met à jour (par nom) une question native."""
    existing = {c["name"]: c for c in s.get(f"{MB}/api/card").json()}
    payload = {
        "name": name,
        "dataset_query": {"type": "native", "native": {"query": sql}, "database": db_id},
        "display": display,
        "visualization_settings": viz or {},
    }
    if name in existing:
        cid = existing[name]["id"]
        s.put(f"{MB}/api/card/{cid}", json=payload).raise_for_status()
        return cid
    r = s.post(f"{MB}/api/card", json=payload)
    r.raise_for_status()
    return r.json()["id"]


def _card_dc(i, cid, row, col, sx, sy):
    return {"id": -(i + 1), "card_id": cid, "row": row, "col": col, "size_x": sx, "size_y": sy}


def _text_dc(i, md, row, col, sx, sy):
    return {
        "id": -(i + 1), "card_id": None, "row": row, "col": col, "size_x": sx, "size_y": sy,
        "visualization_settings": {
            "text": md,
            "virtual_card": {"name": None, "display": "text", "dataset_query": {}, "visualization_settings": {}},
        },
    }


def ensure_dashboard(name, items):
    """items = liste de ('card', card_id, row, col, sx, sy) ou ('text', markdown, row, col, sx, sy)."""
    existing = {d["name"]: d["id"] for d in s.get(f"{MB}/api/dashboard").json()}
    dash_id = existing.get(name)
    if dash_id is None:
        r = s.post(f"{MB}/api/dashboard", json={"name": name})
        r.raise_for_status()
        dash_id = r.json()["id"]
    dashcards = []
    for i, it in enumerate(items):
        if it[0] == "text":
            dashcards.append(_text_dc(i, it[1], it[2], it[3], it[4], it[5]))
        else:
            dashcards.append(_card_dc(i, it[1], it[2], it[3], it[4], it[5]))
    s.put(f"{MB}/api/dashboard/{dash_id}", json={"dashcards": dashcards}).raise_for_status()
    print(f"Dashboard '{name}' prêt (id={dash_id}, {len(items)} blocs).")
    return dash_id


def main():
    wait_for_metabase()
    authenticate()
    db_id = ensure_database()

    # --- Cartes : Marché ---
    nb_biens = ensure_card(db_id, "Logements analysés",
                           f"SELECT count(*) AS logements FROM {FACT}", "scalar")
    prix_global = ensure_card(db_id, "Prix médian au m² (toutes communes)",
                              f"SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY prix_m2)) AS prix_m2 "
                              f"FROM {FACT} WHERE prix_m2 IS NOT NULL", "scalar")
    prix_commune = ensure_card(db_id, "Prix médian au m² par commune (€)",
                               f"SELECT c.nom AS commune, round(k.prix_m2_median) AS prix_m2 "
                               f"FROM {KPI} k JOIN {DIM} c USING (code_insee) ORDER BY k.prix_m2_median DESC",
                               "bar", _bar_viz("commune", "prix_m2", "Commune", "Prix au m² (€)"))
    volume_commune = ensure_card(db_id, "Nombre de logements diagnostiqués par commune",
                                 f"SELECT c.nom AS commune, k.nb_transactions AS logements "
                                 f"FROM {KPI} k JOIN {DIM} c USING (code_insee) ORDER BY k.nb_transactions DESC",
                                 "bar", _bar_viz("commune", "logements", "Commune", "Nb de logements"))
    detail = ensure_card(db_id, "Détail par commune",
                         f'SELECT c.nom AS "Commune", c.departement AS "Dép.", round(k.prix_m2_median) AS "Prix médian €/m²", '
                         f'k.nb_transactions AS "Nb logements", round(k.pct_passoires, 1) AS "% passoires" '
                         f"FROM {KPI} k JOIN {DIM} c USING (code_insee) ORDER BY c.nom", "table")

    # --- Cartes : Énergie ---
    part_passoires = ensure_card(db_id, "Part de passoires (toutes communes, %)",
                                 f"SELECT round(100.0 * avg(CASE WHEN etiquette IN ('F','G') THEN 1 ELSE 0 END), 1) AS pct "
                                 f"FROM {FACT}", "scalar")
    passoires_commune = ensure_card(db_id, "Part de passoires thermiques F/G par commune (%)",
                                    f"SELECT c.nom AS commune, round(k.pct_passoires, 1) AS pct_passoires "
                                    f"FROM {KPI} k JOIN {DIM} c USING (code_insee) ORDER BY k.pct_passoires DESC",
                                    "bar", _bar_viz("commune", "pct_passoires", "Commune", "% de passoires"))
    dpe_repartition = ensure_card(db_id, "Répartition des étiquettes DPE (A → G)",
                                  f"SELECT etiquette, count(*) AS logements FROM {FACT} GROUP BY etiquette ORDER BY etiquette",
                                  "bar", _bar_viz("etiquette", "logements", "Étiquette DPE", "Nb de logements"))

    # --- Dashboards (grille 24 colonnes) ---
    intro_marche = (
        "## 🏠 Marché immobilier par commune\n"
        "Prix médian au **m²** (source DVF — transactions réelles) et nombre de logements diagnostiqués, "
        "**par ville**. Comparez les villes entre elles ; cliquez une barre pour explorer."
    )
    ensure_dashboard("Marché par commune", [
        ("text", intro_marche, 0, 0, 24, 2),
        ("card", nb_biens, 2, 0, 6, 3),
        ("card", prix_global, 2, 6, 6, 3),
        ("card", prix_commune, 5, 0, 12, 7),
        ("card", volume_commune, 5, 12, 12, 7),
        ("card", detail, 12, 0, 24, 6),
    ])

    intro_energie = (
        "## ⚡ Performance énergétique (DPE)\n"
        "Part de **passoires thermiques** (étiquettes F/G, à terme interdites à la location) par ville, "
        "et répartition des étiquettes A→G. Plus une ville compte de passoires, plus le potentiel de rénovation est élevé."
    )
    ensure_dashboard("Impact énergétique (DPE)", [
        ("text", intro_energie, 0, 0, 24, 2),
        ("card", part_passoires, 2, 0, 8, 3),
        ("card", passoires_commune, 5, 0, 12, 7),
        ("card", dpe_repartition, 5, 12, 12, 7),
    ])
    print("OK — dashboards disponibles sur " + MB)


if __name__ == "__main__":
    main()
