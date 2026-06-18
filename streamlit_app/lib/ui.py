"""Shared presentation helpers for the Streamlit front."""
from __future__ import annotations

import pandas as pd

from lib.st_compat import st

PAGE_TITLE_SUFFIX = " - ImmoLake"


def configure_page(title: str) -> None:
    st.set_page_config(page_title=f"{title}{PAGE_TITLE_SUFFIX}", layout="wide")
    apply_theme()


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
            --immo-ink: #17202a;
            --immo-muted: #5f6b7a;
            --immo-line: #d7dde5;
            --immo-blue: #2f6f9f;
            --immo-teal: #1f8a70;
            --immo-red: #c84c4c;
            --immo-amber: #b8871f;
            --immo-soft: #f6f8fb;
        }
        .block-container {
            padding-top: 1.25rem;
            padding-bottom: 2rem;
            max-width: 1320px;
        }
        h1, h2, h3 {
            color: var(--immo-ink);
            letter-spacing: 0;
        }
        [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid var(--immo-line);
            border-radius: 8px;
            padding: 0.85rem 1rem;
            min-height: 104px;
        }
        [data-testid="stMetricLabel"] {
            color: var(--immo-muted);
        }
        [data-testid="stMetricValue"] {
            color: var(--immo-ink);
            font-size: 1.55rem;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid var(--immo-line);
            border-radius: 8px;
            overflow: hidden;
        }
        .immo-hero {
            background: linear-gradient(90deg, #eef5f8 0%, #f7f9f5 55%, #fff8ed 100%);
            border: 1px solid var(--immo-line);
            border-radius: 8px;
            padding: 1rem 1.15rem;
            margin-bottom: 1rem;
        }
        .immo-hero p {
            color: var(--immo-muted);
            margin: 0.25rem 0 0 0;
        }
        .immo-badge-row {
            display: flex;
            gap: 0.45rem;
            flex-wrap: wrap;
            margin: 0.15rem 0 0.8rem 0;
        }
        .immo-badge {
            border: 1px solid var(--immo-line);
            border-radius: 999px;
            padding: 0.18rem 0.55rem;
            font-size: 0.82rem;
            background: #ffffff;
            color: var(--immo-ink);
        }
        .immo-badge.passoire {
            border-color: #e4aaa6;
            color: #8f2d2d;
            background: #fff3f1;
        }
        .immo-badge.good {
            border-color: #9fcfbd;
            color: #1f6f5c;
            background: #effaf5;
        }
        .immo-note {
            border-left: 4px solid var(--immo-amber);
            background: #fff9ea;
            padding: 0.7rem 0.85rem;
            border-radius: 6px;
            color: #5c4a17;
            margin: 0.7rem 0 1rem 0;
        }
        .immo-legend {
            display: flex;
            align-items: center;
            gap: 0.6rem;
            margin: 0.25rem 0 0.8rem 0;
            color: var(--immo-muted);
            font-size: 0.86rem;
        }
        .immo-ramp {
            width: 180px;
            height: 10px;
            border-radius: 999px;
            border: 1px solid var(--immo-line);
        }
        .immo-ramp.prix {
            background: linear-gradient(90deg, #4575b4, #f46d43);
        }
        .immo-ramp.passoires {
            background: linear-gradient(90deg, #2b83ba, #d73027);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def hero(title: str, subtitle: str, badges: list[str] | None = None) -> None:
    badge_html = ""
    if badges:
        badge_html = "<div class='immo-badge-row'>" + "".join(
            f"<span class='immo-badge'>{badge}</span>" for badge in badges
        ) + "</div>"
    st.markdown(
        f"""
        <div class="immo-hero">
            <h1>{title}</h1>
            <p>{subtitle}</p>
            {badge_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def note(text: str) -> None:
    st.markdown(f"<div class='immo-note'>{text}</div>", unsafe_allow_html=True)


def format_int(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "-"
    return f"{int(round(float(numeric))):,}".replace(",", " ")


def format_eur_m2(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric):,.0f} EUR/m2".replace(",", " ")


def format_pct(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric):.1f} %"


def metric_row(items: list[tuple[str, str, str | None]]) -> None:
    columns = st.columns(len(items))
    for column, (label, value, delta) in zip(columns, items):
        column.metric(label, value, delta=delta)


def empty_state(message: str) -> None:
    st.warning(message)


def map_legend(metric: str) -> None:
    if metric == "pct_passoires":
        label = "Faible part de passoires -> forte part de passoires"
        ramp = "passoires"
    elif metric == "score_opportunite":
        label = "Score faible -> score eleve"
        ramp = "passoires"
    else:
        label = "Prix bas -> prix eleve"
        ramp = "prix"
    st.markdown(
        f"""
        <div class="immo-legend">
            <span>{label}</span>
            <span class="immo-ramp {ramp}"></span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def opportunity_badges(row: pd.Series) -> str:
    badges = []
    if pd.to_numeric(pd.Series([row.get("indice_sous_cotation")]), errors="coerce").iloc[0] < 0:
        badges.append("<span class='immo-badge good'>sous-cotee</span>")
    if pd.to_numeric(pd.Series([row.get("pct_passoires")]), errors="coerce").iloc[0] >= 20:
        badges.append("<span class='immo-badge passoire'>parc passoires</span>")
    return "<div class='immo-badge-row'>" + "".join(badges) + "</div>" if badges else ""
