"""ImmoLake Streamlit front."""
from __future__ import annotations

import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters


st.set_page_config(page_title="ImmoLake", layout="wide")

st.title("ImmoLake")
st.caption("Lakehouse DuckDB : interrogation directe du gold Parquet dans MinIO.")

filters = render_sidebar_filters()
market = queries.get_market_data(filters)

left, middle, right, last = st.columns(4)
left.metric("Communes", f"{market['commune'].nunique():,}".replace(",", " "))
middle.metric("Prix/m2 median", f"{market['prix_m2'].median():,.0f} EUR".replace(",", " "))
right.metric("Passoires", f"{market['pct_passoires'].mean():.1f} %")
last.metric("Biens", f"{int(market['nb_dpe'].sum()):,}".replace(",", " "))

st.dataframe(
    market[["commune", "departement", "type_bien", "prix_m2", "pct_passoires", "nb_dpe"]].sort_values(
        "prix_m2",
        ascending=False,
    ),
    use_container_width=True,
    hide_index=True,
)
