from __future__ import annotations

import plotly.express as px
import pydeck as pdk
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters
from lib.map_layers import build_map_layers, initial_view_state
from lib import ui


ui.configure_page("Bonnes affaires")
ui.hero(
    "Bonnes affaires",
    "Identifier les communes sous-cotees avec un potentiel de renovation energetique.",
    ["sous-cotation", "passoires", "score"],
)

filters = render_sidebar_filters()
df = queries.get_opportunities(filters)

ui.note("Sous-cotation = commune vs departement, PAS bien vs marche local ; enrichissement prix = mediane DVF.")

if df.empty:
    ui.empty_state("Aucune commune sous-cotee avec les filtres courants.")
else:
    featured = df[df["etiquette_opportunite"].str.contains("passoires", na=False)]
    best = df.iloc[0]
    ui.metric_row(
        [
            ("Opportunites", ui.format_int(len(df)), None),
            ("Double signal", ui.format_int(len(featured)), None),
            ("Meilleur score", f"{best['score_opportunite']:.1f}", best["commune"]),
            ("Ecart min", ui.format_pct(df["indice_sous_cotation"].min()), None),
        ]
    )

    st.markdown(ui.opportunity_badges(best), unsafe_allow_html=True)

    left, right = st.columns([1, 1])
    scatter_fig = px.scatter(
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
        title="Sous-cotation et parc de passoires",
    )
    scatter_fig.update_layout(height=460, margin=dict(l=10, r=10, t=50, b=10))
    left.plotly_chart(scatter_fig, use_container_width=True)

    mapped = df.dropna(subset=["latitude", "longitude"])
    if mapped.empty:
        right.warning("Aucune opportunite geolocalisee.")
    else:
        ui.map_legend("score_opportunite")
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
    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        column_config={
            "prix_m2": st.column_config.NumberColumn("Prix/m2", format="%.0f EUR"),
            "indice_sous_cotation": st.column_config.NumberColumn("Ecart", format="%.1f %%"),
            "z": st.column_config.NumberColumn("z", format="%.2f"),
            "pct_passoires": st.column_config.NumberColumn("Passoires", format="%.1f %%"),
            "score_opportunite": st.column_config.NumberColumn("Score", format="%.1f"),
        },
    )
