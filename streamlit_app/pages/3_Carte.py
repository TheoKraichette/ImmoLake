from __future__ import annotations

import pydeck as pdk
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters
from lib.map_layers import build_map_layers, initial_view_state
from lib import ui


ui.configure_page("Carte")
ui.hero("Carte", "Visualiser le prix et les passoires au niveau commune.", ["carte", "prix/m2", "passoires"])

filters = render_sidebar_filters()
metric = st.segmented_control("Couche", ["prix_m2", "pct_passoires"], default="prix_m2", label_visibility="collapsed")
df = queries.get_map_data(filters).dropna(subset=["latitude", "longitude"])
ui.map_legend(metric)

if df.empty:
    ui.empty_state("Aucune commune geolocalisee avec les filtres courants.")
else:
    ui.metric_row(
        [
            ("Communes carte", ui.format_int(len(df)), None),
            ("Prix/m2 median", ui.format_eur_m2(df["prix_m2"].median()), None),
            ("Passoires", ui.format_pct(df["pct_passoires"].mean()), None),
        ]
    )
    deck = pdk.Deck(
        map_style=None,
        initial_view_state=initial_view_state(df),
        layers=build_map_layers(df, metric),
        tooltip={
            "html": (
                "<b>{commune}</b><br/>"
                "Prix/m2 median: {prix_m2}<br/>"
                "Passoires: {pct_passoires}%<br/>"
                "DPE: {nb_dpe}<br/>"
                "Sous-cotation: {indice_sous_cotation}%"
            ),
            "style": {"backgroundColor": "white", "color": "black"},
        },
    )
    st.pydeck_chart(deck, use_container_width=True)

ui.note("Couleur au niveau commune : mediane DVF commune/type, pas geocodage au bien.")
