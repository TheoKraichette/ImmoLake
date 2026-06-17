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
    if token:
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
            print(f"Base '{DB_NAME}' déjà connectée (id={db['id']}).")
            return db["id"]
    r = s.post(f"{MB}/api/database", json={
        "name": DB_NAME, "engine": "postgres",
        "details": {**DWH, "ssl": False, "schema-filters-type": "all", "tunnel-enabled": False},
    })
    r.raise_for_status()
    db_id = r.json()["id"]
    s.post(f"{MB}/api/database/{db_id}/sync_schema")
    print(f"Base '{DB_NAME}' connectée (id={db_id}).")
    return db_id


def ensure_card(db_id, name, sql, display):
    existing = {c["name"]: c["id"] for c in s.get(f"{MB}/api/card").json()}
    if name in existing:
        return existing[name]
    r = s.post(f"{MB}/api/card", json={
        "name": name,
        "dataset_query": {"type": "native", "native": {"query": sql}, "database": db_id},
        "display": display,
        "visualization_settings": {},
    })
    r.raise_for_status()
    return r.json()["id"]


def ensure_dashboard(name, card_ids):
    existing = {d["name"]: d["id"] for d in s.get(f"{MB}/api/dashboard").json()}
    dash_id = existing.get(name)
    if dash_id is None:
        r = s.post(f"{MB}/api/dashboard", json={"name": name})
        r.raise_for_status()
        dash_id = r.json()["id"]
    dashcards = []
    for i, cid in enumerate(card_ids):
        dashcards.append({
            "id": -(i + 1), "card_id": cid,
            "row": (i // 2) * 6, "col": (i % 2) * 12,
            "size_x": 12, "size_y": 6,
        })
    s.put(f"{MB}/api/dashboard/{dash_id}", json={"dashcards": dashcards})
    print(f"Dashboard '{name}' prêt (id={dash_id}, {len(card_ids)} cartes).")
    return dash_id


def main():
    wait_for_metabase()
    authenticate()
    db_id = ensure_database()

    KPI = "analytics.kpi_commune_mensuel"
    FACT = "dwh.fact_biens"

    marche = [
        ensure_card(db_id, "Prix/m² médian par commune",
                    f"SELECT code_insee, prix_m2_median FROM {KPI} ORDER BY prix_m2_median DESC", "bar"),
        ensure_card(db_id, "Volume de biens par commune",
                    f"SELECT code_insee, nb_transactions FROM {KPI} ORDER BY nb_transactions DESC", "bar"),
        ensure_card(db_id, "Détail marché par commune",
                    f"SELECT code_insee, prix_m2_median, nb_transactions FROM {KPI} ORDER BY code_insee", "table"),
    ]
    energie = [
        ensure_card(db_id, "% de passoires (F/G) par commune",
                    f"SELECT code_insee, pct_passoires FROM {KPI} ORDER BY pct_passoires DESC", "bar"),
        ensure_card(db_id, "Décote des passoires (%) par commune",
                    f"SELECT code_insee, decote_passoire_pct FROM {KPI} ORDER BY decote_passoire_pct", "bar"),
        ensure_card(db_id, "Répartition des étiquettes DPE",
                    f"SELECT etiquette, count(*) AS nb FROM {FACT} GROUP BY etiquette ORDER BY etiquette", "bar"),
    ]

    ensure_dashboard("Marché par commune", marche)
    ensure_dashboard("Impact énergétique (DPE)", energie)
    print("OK — dashboards disponibles sur " + MB)


if __name__ == "__main__":
    main()
