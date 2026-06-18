from __future__ import annotations

import plotly.express as px
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters


st.set_page_config(page_title="Comparateur - ImmoLake", layout="wide")
st.title("Comparateur")

filters = render_sidebar_filters()
available = queries.get_market_data(filters)
choices = sorted(available["commune"].dropna().unique().tolist())
default = choices[: min(2, len(choices))]
selected = st.multiselect("Communes", choices, default=default, max_selections=4)
df = queries.get_comparison_data(tuple(selected), filters)

if len(selected) < 2:
    st.warning("Selectionner 2 a 4 communes.")
else:
    st.dataframe(
        df[["commune", "prix_m2", "pct_passoires", "conso_energie_med", "indice_sous_cotation", "nb_dpe"]],
        use_container_width=True,
        hide_index=True,
    )

    metrics = ["prix_m2", "pct_passoires", "conso_energie_med", "indice_sous_cotation"]
    melted = df.melt(id_vars=["commune"], value_vars=metrics, var_name="indicateur", value_name="valeur")
    st.plotly_chart(
        px.bar(
            melted,
            x="indicateur",
            y="valeur",
            color="commune",
            barmode="group",
            labels={"indicateur": "Indicateur", "valeur": "Valeur"},
        ),
        use_container_width=True,
    )

    radar = df[["commune", "prix_m2", "pct_passoires", "conso_energie_med"]].copy()
    for column in ["prix_m2", "pct_passoires", "conso_energie_med"]:
        max_value = radar[column].max()
        radar[column] = 0 if max_value == 0 else radar[column] / max_value
    radar = radar.melt(id_vars=["commune"], var_name="indicateur", value_name="score_normalise")
    st.plotly_chart(
        px.line_polar(
            radar,
            r="score_normalise",
            theta="indicateur",
            color="commune",
            line_close=True,
        ),
        use_container_width=True,
    )
