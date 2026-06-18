from __future__ import annotations

import plotly.express as px
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters
from lib import ui


ui.configure_page("Energie")
ui.hero("Energie", "Suivre la distribution DPE et le poids des passoires thermiques.", ["DPE", "passoires", "conso"])

filters = render_sidebar_filters()
df = queries.get_market_data(filters)
dpe = queries.get_dpe_distribution(filters)

if df.empty:
    ui.empty_state("Aucune donnee energie avec les filtres courants.")
    st.stop()

ui.metric_row(
    [
        ("Passoires F-G", ui.format_pct(df["pct_passoires"].mean()), None),
        ("Conso mediane", f"{df['conso_energie_med'].median():.0f} kWh/m2/an", None),
        ("Communes", ui.format_int(df["commune"].nunique()), None),
    ]
)

left, right = st.columns([1, 1])

dpe_fig = px.bar(
    dpe,
    x="commune",
    y="nb",
    color="etiquette",
    category_orders={"etiquette": ["A", "B", "C", "D", "E", "F", "G"]},
    labels={"nb": "DPE", "commune": "Commune"},
    title="Repartition des etiquettes DPE",
)
dpe_fig.update_layout(height=460, margin=dict(l=10, r=10, t=50, b=10))
left.plotly_chart(dpe_fig, use_container_width=True)

passoire_fig = px.bar(
    df.sort_values("pct_passoires", ascending=False),
    x="commune",
    y="pct_passoires",
    color="departement",
    labels={"pct_passoires": "% passoires", "commune": "Commune"},
    title="Communes avec le plus de passoires",
)
passoire_fig.update_layout(height=460, margin=dict(l=10, r=10, t=50, b=10))
right.plotly_chart(passoire_fig, use_container_width=True)

ui.note("Loi Climat : logements G interdits en location en 2025, F en 2028, E en 2034.")
