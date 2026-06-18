from __future__ import annotations

import plotly.express as px
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters


st.set_page_config(page_title="Marche - ImmoLake", layout="wide")
st.title("Marche")

filters = render_sidebar_filters()
df = queries.get_market_data(filters)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Prix/m2 median", f"{df['prix_m2'].median():,.0f} EUR".replace(",", " "))
col2.metric("Nb biens", f"{int(df['nb_dpe'].sum()):,}".replace(",", " "))
col3.metric("Sous-cotation", f"{df['indice_sous_cotation'].mean():.1f} %")
col4.metric("Communes fiables", f"{int((df['nb_dpe'] >= filters.nb_dpe_min).sum())}")

left, right = st.columns([1, 1])

ranking = df.sort_values("prix_m2", ascending=True).tail(20)
left.plotly_chart(
    px.bar(
        ranking,
        x="prix_m2",
        y="commune",
        color="departement",
        orientation="h",
        labels={"prix_m2": "Prix/m2", "commune": "Commune"},
    ),
    use_container_width=True,
)

right.plotly_chart(
    px.scatter(
        df,
        x="prix_m2",
        y="pct_passoires",
        size="nb_dpe",
        color="departement",
        hover_name="commune",
        labels={"prix_m2": "Prix/m2", "pct_passoires": "% passoires"},
    ),
    use_container_width=True,
)

st.dataframe(
    df[["commune", "departement", "type_bien", "prix_m2", "indice_sous_cotation", "pct_passoires", "nb_dpe"]],
    use_container_width=True,
    hide_index=True,
)
