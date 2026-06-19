"""Reusable filters for Streamlit pages."""
from __future__ import annotations

import streamlit as st

from lib.filter_state import Filters


def _as_tuple(values: list[str]) -> tuple[str, ...]:
    return tuple(value for value in values if value)


def render_sidebar_filters() -> Filters:
    """Render all shared filters once and return an immutable filter object."""
    from lib import queries

    options = queries.get_filter_options()

    with st.expander("Filtres", expanded=False):
        location_col, asset_col, market_col = st.columns([1.05, 1.05, 0.9])

        with location_col:
            st.caption("Localisation")
            regions = st.multiselect("Region", options.regions)
            departements = st.multiselect("Departement", options.departements)
            communes = st.multiselect("Commune", options.communes)

        with asset_col:
            st.caption("Bien et energie")
            types_bien = st.multiselect("Type de bien", options.types_bien)
            etiquettes = st.multiselect("Etiquette DPE", options.etiquettes)
            passoires_only = st.checkbox("Passoires F-G", value=False)
            surface = st.slider(
                "Surface",
                min_value=options.surface_min,
                max_value=options.surface_max,
                value=(options.surface_min, options.surface_max),
                step=5,
            )

        with market_col:
            st.caption("Marche")
            prix_m2 = st.slider(
                "Prix/m2",
                min_value=options.prix_m2_min,
                max_value=options.prix_m2_max,
                value=(options.prix_m2_min, options.prix_m2_max),
                step=100,
            )
            nb_dpe_min = st.number_input("nb_dpe minimum", min_value=1, max_value=1000, value=30, step=5)
            opportunite_k = st.slider("Seuil k opportunites", 0.5, 3.0, 1.2, 0.1)

    return Filters(
        regions=_as_tuple(regions),
        departements=_as_tuple(departements),
        communes=_as_tuple(communes),
        types_bien=_as_tuple(types_bien),
        etiquettes=_as_tuple(etiquettes),
        passoires_only=passoires_only,
        prix_m2_min=int(prix_m2[0]),
        prix_m2_max=int(prix_m2[1]),
        surface_min=int(surface[0]),
        surface_max=int(surface[1]),
        nb_dpe_min=int(nb_dpe_min),
        opportunite_k=float(opportunite_k),
    )
