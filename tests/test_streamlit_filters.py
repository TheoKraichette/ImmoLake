from streamlit_app.lib.filter_state import Filters
from streamlit_app.lib.filters_sql import build_where


def test_build_where_is_parameterized():
    filters = Filters(
        regions=("75",),
        communes=("Bordeaux' OR 1=1 --",),
        types_bien=("appartement", "maison"),
        passoires_only=True,
        prix_m2_min=3000,
        prix_m2_max=6000,
        surface_min=20,
        surface_max=120,
        nb_dpe_min=30,
    )

    where = build_where(filters, alias="m")

    assert "Bordeaux" not in where.sql
    assert "m.commune IN (?)" in where.sql
    assert "m.type_bien IN (?, ?)" in where.sql
    assert "m.etiquette IN ('F', 'G')" in where.sql
    assert where.params == (
        "75",
        "Bordeaux' OR 1=1 --",
        "appartement",
        "maison",
        3000,
        6000,
        20,
        120,
        30,
    )
