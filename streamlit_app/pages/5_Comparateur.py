from __future__ import annotations

import plotly.express as px
import streamlit as st

from lib import queries
from lib.filters import render_sidebar_filters
from lib import ui


ui.configure_page("Comparateur")
filters = render_sidebar_filters()
available = queries.get_market_data(filters)
choices = sorted(available["commune"].dropna().unique().tolist())
default = choices[: min(2, len(choices))]
selected = st.multiselect("Communes", choices, default=default, max_selections=4)
df = queries.get_comparison_data(tuple(selected), filters)

ui.hero(
    "Comparateur",
    "Mettre plusieurs communes face a face pour arbitrer prix, energie et profondeur de marche.",
    [
        ("Selection", ui.format_int(len(selected))),
        ("Prix median", ui.format_eur_m2(df["prix_m2"].median()) if not df.empty else "-"),
        ("Passoires", ui.format_pct(df["pct_passoires"].mean()) if not df.empty else "-"),
        ("DPE", ui.format_int(df["nb_dpe"].sum()) if not df.empty else "-"),
    ],
)

if len(selected) < 2:
    ui.empty_state("Selectionner 2 a 4 communes.")
else:
    ui.metric_row(
        [
            ("Communes", ui.format_int(len(df)), None),
            ("Prix/m2 median", ui.format_eur_m2(df["prix_m2"].median()), None),
            ("Passoires moyennes", ui.format_pct(df["pct_passoires"].mean()), None),
            ("DPE", ui.format_int(df["nb_dpe"].sum()), None),
        ]
    )

    st.dataframe(
        df[["commune", "prix_m2", "pct_passoires", "conso_energie_med", "indice_sous_cotation", "nb_dpe"]],
        width="stretch",
        hide_index=True,
        column_config={
            "prix_m2": st.column_config.NumberColumn("Prix/m2", format="%.0f EUR"),
            "pct_passoires": st.column_config.NumberColumn("Passoires", format="%.1f %%"),
            "conso_energie_med": st.column_config.NumberColumn("Conso", format="%.0f"),
            "indice_sous_cotation": st.column_config.NumberColumn("Sous-cotation", format="%.1f %%"),
            "nb_dpe": st.column_config.NumberColumn("DPE", format="%d"),
        },
    )

    metrics = ["prix_m2", "pct_passoires", "conso_energie_med", "indice_sous_cotation"]
    melted = df.melt(id_vars=["commune"], value_vars=metrics, var_name="indicateur", value_name="valeur")
    bar_fig = px.bar(
        melted,
        x="indicateur",
        y="valeur",
        color="commune",
        barmode="group",
        labels={"indicateur": "Indicateur", "valeur": "Valeur"},
        title="Comparaison directe",
    )
    bar_fig.update_layout(height=430, margin=dict(l=10, r=10, t=50, b=10))
    st.plotly_chart(bar_fig, width="stretch")

    radar = df[["commune", "prix_m2", "pct_passoires", "conso_energie_med"]].copy()
    for column in ["prix_m2", "pct_passoires", "conso_energie_med"]:
        max_value = radar[column].max()
        radar[column] = 0 if max_value == 0 else radar[column] / max_value
    radar = radar.melt(id_vars=["commune"], var_name="indicateur", value_name="score_normalise")
    radar_fig = px.line_polar(
        radar,
        r="score_normalise",
        theta="indicateur",
        color="commune",
        line_close=True,
        title="Profil normalise",
    )
    radar_fig.update_layout(height=480, margin=dict(l=10, r=10, t=60, b=10))
    st.plotly_chart(radar_fig, width="stretch")
