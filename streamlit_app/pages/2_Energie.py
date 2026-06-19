from __future__ import annotations

import plotly.express as px
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters
from lib import ui


ui.configure_page("Energie")
filters = render_sidebar_filters()
df = queries.get_market_data(filters)
dpe = queries.get_dpe_distribution(filters)

if df.empty:
    ui.empty_state("Aucune donnee energie avec les filtres courants.")
    st.stop()

ui.hero(
    "Energie",
    "Identifier les zones ou la performance energetique devient un risque de marche.",
    [
        ("Passoires", ui.format_pct(df["pct_passoires"].mean())),
        ("Conso moyenne", f"{df['conso_energie_med'].median():.0f}"),
        ("Communes", ui.format_int(df["commune"].nunique())),
        ("DPE", ui.format_int(df["nb_dpe"].sum())),
    ],
)

ui.metric_row(
    [
        ("Passoires DPE F-G", ui.format_pct(df["pct_passoires"].mean()), None),
        ("Passoires GES F-G", ui.format_pct(df["pct_ges_passoires"].mean()), None),
        ("Conso moyenne", f"{df['conso_energie_med'].median():.0f} kWh/m2/an", None),
        ("Cout energie median", f"{ui.format_number(df['cout_energie_annuel_median'].median(), 0)} EUR/an", None),
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
left.plotly_chart(dpe_fig, width="stretch")

passoire_fig = px.bar(
    df.sort_values("pct_passoires", ascending=False),
    x="commune",
    y="pct_passoires",
    color="departement",
    labels={"pct_passoires": "% passoires", "commune": "Commune"},
    title="Communes avec le plus de passoires",
)
passoire_fig.update_layout(height=460, margin=dict(l=10, r=10, t=50, b=10))
right.plotly_chart(passoire_fig, width="stretch")

cout = df.dropna(subset=["cout_energie_annuel_median"]).sort_values(
    "cout_energie_annuel_median", ascending=False
).head(20)
if not cout.empty:
    cout_fig = px.bar(
        cout,
        x="commune",
        y="cout_energie_annuel_median",
        color="pct_ges_passoires",
        color_continuous_scale="OrRd",
        labels={
            "cout_energie_annuel_median": "Cout energie (EUR/an)",
            "commune": "Commune",
            "pct_ges_passoires": "% GES F-G",
        },
        title="Cout energetique annuel median (facture estimee)",
    )
    cout_fig.update_layout(height=420, margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(cout_fig, width="stretch")

ui.note("Loi Climat : logements G interdits en location en 2025, F en 2028, E en 2034.")
