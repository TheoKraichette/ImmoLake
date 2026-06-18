from __future__ import annotations

import plotly.express as px
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters
from lib import ui


ui.configure_page("Marche")
filters = render_sidebar_filters()
df = queries.get_market_data(filters)

if df.empty:
    ui.empty_state("Aucune commune avec les filtres courants.")
    st.stop()

ui.hero(
    "Marche",
    "Comparer les prix, le volume observe et la sous-cotation territoriale.",
    [
        ("Communes", ui.format_int(df["commune"].nunique())),
        ("Prix median", ui.format_eur_m2(df["prix_m2"].median())),
        ("DPE", ui.format_int(df["nb_dpe"].sum())),
        ("Sous-cotation", ui.format_pct(df["indice_sous_cotation"].mean())),
    ],
)

ui.metric_row(
    [
        ("Prix/m2 median", ui.format_eur_m2(df["prix_m2"].median()), None),
        ("DPE", ui.format_int(df["nb_dpe"].sum()), None),
        ("Sous-cotation moyenne", ui.format_pct(df["indice_sous_cotation"].mean()), None),
        ("Communes fiables", ui.format_int((df["nb_dpe"] >= filters.nb_dpe_min).sum()), None),
    ]
)

left, right = st.columns([1, 1])

ranking = df.sort_values("prix_m2", ascending=True).tail(20)
ranking_fig = px.bar(
    ranking,
    x="prix_m2",
    y="commune",
    color="departement",
    orientation="h",
    labels={"prix_m2": "Prix/m2", "commune": "Commune"},
    title="Communes les plus cheres",
)
ranking_fig.update_layout(height=460, margin=dict(l=10, r=10, t=50, b=10))
left.plotly_chart(ranking_fig, use_container_width=True)

scatter_fig = px.scatter(
    df,
    x="prix_m2",
    y="pct_passoires",
    size="nb_dpe",
    color="departement",
    hover_name="commune",
    labels={"prix_m2": "Prix/m2", "pct_passoires": "% passoires"},
    title="Prix et risque energetique",
)
scatter_fig.update_layout(height=460, margin=dict(l=10, r=10, t=50, b=10))
right.plotly_chart(scatter_fig, use_container_width=True)

st.dataframe(
    df[["commune", "departement", "type_bien", "prix_m2", "indice_sous_cotation", "pct_passoires", "nb_dpe"]],
    use_container_width=True,
    hide_index=True,
    column_config={
        "prix_m2": st.column_config.NumberColumn("Prix/m2", format="%.0f EUR"),
        "indice_sous_cotation": st.column_config.NumberColumn("Sous-cotation", format="%.1f %%"),
        "pct_passoires": st.column_config.NumberColumn("Passoires", format="%.1f %%"),
        "nb_dpe": st.column_config.NumberColumn("DPE", format="%d"),
    },
)
