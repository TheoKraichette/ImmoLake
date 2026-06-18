"""ImmoLake Streamlit front."""
from __future__ import annotations

import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters
from lib import ui


ui.configure_page("ImmoLake")

ui.hero(
    "ImmoLake",
    "Pilotage immobilier et energetique sur DuckDB, Parquet et MinIO.",
    ["prix/m2", "passoires", "opportunites", "communes"],
)

filters = render_sidebar_filters()
market = queries.get_market_data(filters)

ui.metric_row(
    [
        ("Communes", ui.format_int(market["commune"].nunique()), None),
        ("Prix/m2 median", ui.format_eur_m2(market["prix_m2"].median()), None),
        ("Passoires", ui.format_pct(market["pct_passoires"].mean()), None),
        ("DPE", ui.format_int(market["nb_dpe"].sum()), None),
    ]
)

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
