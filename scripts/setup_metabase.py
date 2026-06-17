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


def ensure_dashboard(name, layout):
    """layout = liste de (card_id, row, col, size_x, size_y)."""
    existing = {d["name"]: d["id"] for d in s.get(f"{MB}/api/dashboard").json()}
    dash_id = existing.get(name)
    if dash_id is None:
        r = s.post(f"{MB}/api/dashboard", json={"name": name})
        r.raise_for_status()
        dash_id = r.json()["id"]
    dashcards = [
        {"id": -(i + 1), "card_id": cid, "row": row, "col": col, "size_x": sx, "size_y": sy}
        for i, (cid, row, col, sx, sy) in enumerate(layout)
    ]
    s.put(f"{MB}/api/dashboard/{dash_id}", json={"dashcards": dashcards}).raise_for_status()
    print(f"Dashboard '{name}' prêt (id={dash_id}, {len(layout)} cartes).")
    return dash_id


def main():
    wait_for_metabase()
    authenticate()
    db_id = ensure_database()

    # --- Cartes : Marché ---
    nb_biens = ensure_card(db_id, "Biens analysés (total)",
                           f"SELECT count(*) AS biens FROM {FACT}", "scalar")
    prix_global = ensure_card(db_id, "Prix/m² médian (global)",
                              f"SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY prix_m2)) AS prix_m2 "
                              f"FROM {FACT} WHERE prix_m2 IS NOT NULL", "scalar")
    prix_commune = ensure_card(db_id, "Prix/m² médian par commune",
                               f"SELECT c.nom AS commune, round(k.prix_m2_median) AS prix_m2 "
                               f"FROM {KPI} k JOIN {DIM} c USING (code_insee) ORDER BY k.prix_m2_median DESC",
                               "bar", _bar_viz("commune", "prix_m2", "Commune", "Prix/m² (€)"))
    volume_commune = ensure_card(db_id, "Nombre de biens par commune",
                                 f"SELECT c.nom AS commune, k.nb_transactions AS nb_biens "
                                 f"FROM {KPI} k JOIN {DIM} c USING (code_insee) ORDER BY k.nb_transactions DESC",
                                 "bar", _bar_viz("commune", "nb_biens", "Commune", "Nb de biens"))
    detail = ensure_card(db_id, "Détail par commune",
                         f"SELECT c.nom AS commune, c.departement AS dep, round(k.prix_m2_median) AS prix_m2, "
                         f"k.nb_transactions AS nb_biens, round(k.pct_passoires, 1) AS pct_passoires "
                         f"FROM {KPI} k JOIN {DIM} c USING (code_insee) ORDER BY c.nom", "table")

    # --- Cartes : Énergie ---
    part_passoires = ensure_card(db_id, "Part de passoires (global, %)",
                                 f"SELECT round(100.0 * avg(CASE WHEN etiquette IN ('F','G') THEN 1 ELSE 0 END), 1) AS pct "
                                 f"FROM {FACT}", "scalar")
    passoires_commune = ensure_card(db_id, "% de passoires (F/G) par commune",
                                    f"SELECT c.nom AS commune, round(k.pct_passoires, 1) AS pct_passoires "
                                    f"FROM {KPI} k JOIN {DIM} c USING (code_insee) ORDER BY k.pct_passoires DESC",
                                    "bar", _bar_viz("commune", "pct_passoires", "Commune", "% passoires"))
    dpe_repartition = ensure_card(db_id, "Répartition des étiquettes DPE",
                                  f"SELECT etiquette, count(*) AS nb FROM {FACT} GROUP BY etiquette ORDER BY etiquette",
                                  "bar", _bar_viz("etiquette", "nb", "Étiquette DPE", "Nb de biens"))

    # --- Dashboards (grille 24 colonnes) ---
    ensure_dashboard("Marché par commune", [
        (nb_biens, 0, 0, 6, 3),
        (prix_global, 0, 6, 6, 3),
        (prix_commune, 3, 0, 12, 7),
        (volume_commune, 3, 12, 12, 7),
        (detail, 10, 0, 24, 6),
    ])
    ensure_dashboard("Impact énergétique (DPE)", [
        (part_passoires, 0, 0, 6, 3),
        (passoires_commune, 0, 6, 18, 7),
        (dpe_repartition, 7, 0, 12, 7),
    ])
    print("OK — dashboards disponibles sur " + MB)


if __name__ == "__main__":
    main()
