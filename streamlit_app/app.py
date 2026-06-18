"""ImmoLake Streamlit front."""
from __future__ import annotations

import plotly.express as px
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters
from lib import ui


ui.configure_page("ImmoLake")

filters = render_sidebar_filters()
market = queries.get_market_data(filters)
opportunities = queries.get_opportunities(filters)

top_market = market.sort_values("prix_m2", ascending=False).iloc[0] if not market.empty else None
top_opp = opportunities.iloc[0] if not opportunities.empty else None

ui.hero(
    "ImmoLake",
    "Un cockpit pour reperer les marches tendus, les passoires thermiques et les communes sous-cotees.",
    [
        ("Communes", ui.format_int(market["commune"].nunique())),
        ("Prix median", ui.format_eur_m2(market["prix_m2"].median())),
        ("Passoires", ui.format_pct(market["pct_passoires"].mean())),
        ("Opportunites", ui.format_int(len(opportunities))),
    ],
)

ui.signal_grid(
    [
        (
            "Marche le plus cher",
            str(top_market["commune"]) if top_market is not None else "-",
            ui.format_eur_m2(top_market["prix_m2"]) if top_market is not None else "Aucune donnee",
            "light",
        ),
        (
            "Signal renovation",
            str(market.sort_values("pct_passoires", ascending=False).iloc[0]["commune"]) if not market.empty else "-",
            f"{ui.format_pct(market['pct_passoires'].max())} de passoires",
            "light",
        ),
        (
            "Meilleure opportunite",
            str(top_opp["commune"]) if top_opp is not None else "-",
            f"Score {ui.format_number(top_opp['score_opportunite'], 1)}" if top_opp is not None else "Mart opportunites vide",
            "dark",
        ),
        (
            "Volume analyse",
            ui.format_int(market["nb_dpe"].sum()),
            "DPE consolides dans les marts",
            "light",
        ),
    ]
)

left, right = st.columns([0.42, 0.58])
with left:
    ui.page_header("Opportunites prioritaires", "Communes a regarder en premier")
    ui.leaderboard(opportunities)

with right:
    ui.page_header("Carte des tensions", "Prix et passoires sur un seul plan")
    if market.empty:
        ui.empty_state("Aucune donnee avec les filtres courants.")
    else:
        fig = px.scatter(
            market,
            x="prix_m2",
            y="pct_passoires",
            size="nb_dpe",
            color="indice_sous_cotation",
            hover_name="commune",
            color_continuous_scale=["#2dd4bf", "#f6b44b", "#ef625b"],
            labels={
                "prix_m2": "Prix/m2",
                "pct_passoires": "% passoires",
                "indice_sous_cotation": "Sous-cotation",
            },
        )
        fig.update_layout(height=430, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

ui.page_header("Table de marche", "Communes triees par prix median")

st.dataframe(
    market[["commune", "departement", "type_bien", "prix_m2", "pct_passoires", "nb_dpe"]].sort_values(
        "prix_m2",
        ascending=False,
    ),
    use_container_width=True,
    hide_index=True,
    column_config={
        "prix_m2": st.column_config.NumberColumn("Prix/m2", format="%.0f EUR"),
        "pct_passoires": st.column_config.NumberColumn("Passoires", format="%.1f %%"),
        "nb_dpe": st.column_config.NumberColumn("DPE", format="%d"),
    },
)
