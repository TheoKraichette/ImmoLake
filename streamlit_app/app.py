"""ImmoLake — front DuckDB / Streamlit (v2).

Page d'accueil : valide le socle (DuckDB lit le gold Parquet dans MinIO, sans Postgres).
Les pages métier (Marché, Énergie, Carte, Bonnes affaires) arrivent dans les issues v2-5 → v2-12.
"""
from __future__ import annotations

import streamlit as st

from lib.connection import get_con, gold

st.set_page_config(page_title="ImmoLake", page_icon="🏠", layout="wide")
st.title("🏠 ImmoLake — prix × performance énergétique")
st.caption("Lakehouse DuckDB : interrogation directe du gold Parquet dans MinIO (sans Postgres).")

con = get_con()

st.subheader("État du Data Lake")
try:
    count = con.execute(
        f"SELECT count(*) FROM read_parquet('{gold('fact_biens')}')"
    ).fetchone()[0]
    st.metric("Biens dans le gold (`fact_biens`)", f"{count:,}".replace(",", " "))
    st.success("DuckDB lit le gold Parquet dans MinIO ✅")
except Exception as exc:  # gold pas encore produit (avant v2-4) : affichage propre
    st.info(
        "Le gold n'est pas encore disponible dans MinIO. "
        "Il sera produit par le pipeline DuckDB (issue v2-4)."
    )
    st.caption(f"Détail technique : {exc}")

st.divider()
st.caption("Pages métier à venir : Marché, Énergie, Carte, Bonnes affaires (issues v2-5 → v2-12).")
