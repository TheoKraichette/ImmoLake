from __future__ import annotations

import plotly.express as px
import pydeck as pdk
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters
from lib.map_layers import build_map_layers, initial_view_state


st.set_page_config(page_title="Bonnes affaires - ImmoLake", layout="wide")
st.title("Bonnes affaires")

filters = render_sidebar_filters()
df = queries.get_opportunities(filters)

st.info("Sous-cotation = commune vs departement, PAS bien vs marche local ; enrichissement prix = mediane DVF.")

if df.empty:
    st.warning("Aucune commune sous-cotee avec les filtres courants.")
else:
    featured = df[df["etiquette_opportunite"].str.contains("passoires", na=False)]
    if not featured.empty:
        st.success(f"{len(featured)} opportunite(s) avec double signal : sous-cotation + parc de passoires.")

    left, right = st.columns([1, 1])
    left.plotly_chart(
        px.scatter(
            df,
            x="indice_sous_cotation",
            y="pct_passoires",
            size="nb_dpe",
            color="score_opportunite",
            hover_name="commune",
            labels={
                "indice_sous_cotation": "Ecart prix vs departement (%)",
                "pct_passoires": "% passoires",
                "score_opportunite": "Score",
            },
        ),
        use_container_width=True,
    )

    mapped = df.dropna(subset=["latitude", "longitude"])
    if mapped.empty:
        right.warning("Aucune opportunite geolocalisee.")
    else:
        right.pydeck_chart(
            pdk.Deck(
                map_style=None,
                initial_view_state=initial_view_state(mapped),
                layers=build_map_layers(mapped, "score_opportunite"),
                tooltip={
                    "html": (
                        "<b>{commune}</b><br/>"
                        "Type: {type_bien}<br/>"
                        "Prix/m2: {prix_m2}<br/>"
                        "Ecart: {indice_sous_cotation}%<br/>"
                        "Score: {score_opportunite}"
                    ),
                    "style": {"backgroundColor": "white", "color": "black"},
                },
            ),
            use_container_width=True,
        )

    table = df[
        [
            "commune",
            "departement",
            "type_bien",
            "prix_m2",
            "indice_sous_cotation",
            "z",
            "pct_passoires",
            "score_opportunite",
            "etiquette_opportunite",
        ]
    ].copy()
    st.dataframe(table, use_container_width=True, hide_index=True)
