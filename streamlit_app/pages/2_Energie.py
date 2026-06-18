from __future__ import annotations

import plotly.express as px
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters


st.set_page_config(page_title="Energie - ImmoLake", layout="wide")
st.title("Energie")

filters = render_sidebar_filters()
df = queries.get_market_data(filters)
dpe = queries.get_dpe_distribution(filters)

col1, col2, col3 = st.columns(3)
col1.metric("Passoires F-G", f"{df['pct_passoires'].mean():.1f} %")
col2.metric("Conso mediane", f"{df['conso_energie_med'].median():.0f} kWh/m2/an")
col3.metric("Communes", f"{df['commune'].nunique():,}".replace(",", " "))

left, right = st.columns([1, 1])

left.plotly_chart(
    px.bar(
        dpe,
        x="commune",
        y="nb",
        color="etiquette",
        category_orders={"etiquette": ["A", "B", "C", "D", "E", "F", "G"]},
        labels={"nb": "DPE", "commune": "Commune"},
    ),
    use_container_width=True,
)

right.plotly_chart(
    px.bar(
        df.sort_values("pct_passoires", ascending=False),
        x="commune",
        y="pct_passoires",
        color="departement",
        labels={"pct_passoires": "% passoires", "commune": "Commune"},
    ),
    use_container_width=True,
)

st.info("Loi Climat: G interdits en location en 2025, F en 2028, E en 2034.")
