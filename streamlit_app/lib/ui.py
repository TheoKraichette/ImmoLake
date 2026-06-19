"""Shared presentation helpers for the Streamlit front."""
from __future__ import annotations

import html
import pandas as pd

from lib.st_compat import st

PAGE_TITLE_SUFFIX = " - ImmoLake"


def configure_page(title: str) -> None:
    st.set_page_config(
        page_title=f"{title}{PAGE_TITLE_SUFFIX}",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    apply_theme()
    top_nav(title)


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
            --ink: #12140f;
            --panel: #171b15;
            --panel-2: #20261d;
            --paper: #f7f3e8;
            --muted: #9fa894;
            --line: rgba(247, 243, 232, 0.14);
            --teal: #2dd4bf;
            --green: #84cc16;
            --amber: #f6b44b;
            --red: #ef625b;
            --steel: #8fb1c6;
        }
        .stApp {
            background:
                linear-gradient(180deg, #f7f3e8 0%, #f7f3e8 58%, #edf3ea 100%);
            color: var(--ink);
        }
        .block-container {
            max-width: 1380px;
            padding-top: 0.7rem;
            padding-bottom: 2.5rem;
            color: var(--ink);
        }
        h1, h2, h3, h4 {
            letter-spacing: 0;
            color: var(--ink);
        }
        section[data-testid="stSidebar"],
        div[data-testid="stSidebarNav"],
        div[data-testid="collapsedControl"],
        [data-testid="stSidebarCollapsedControl"],
        [data-testid="stHeader"],
        [data-testid="stToolbar"],
        [data-testid="stDecoration"],
        [data-testid="stStatusWidget"],
        .stAppToolbar,
        #MainMenu,
        footer {
            display: none !important;
            visibility: hidden !important;
            height: 0 !important;
        }
        [data-testid="stAppViewBlockContainer"] {
            padding-top: 0.7rem;
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            background: rgba(255, 250, 240, 0.92);
            border-color: rgba(18, 20, 15, 0.12);
            box-shadow: 0 14px 34px rgba(18, 20, 15, 0.07);
        }
        div[data-testid="stButton"] > button {
            min-height: 40px;
            border-radius: 7px;
            border: 1px solid rgba(18, 20, 15, 0.12);
            background: #fffaf0;
            color: var(--ink);
            font-weight: 750;
        }
        div[data-testid="stButton"] > button[kind="primary"] {
            background: #173d35;
            border-color: #173d35;
            color: #fffaf0;
        }
        div[data-testid="stButton"] > button:hover {
            border-color: rgba(13, 113, 103, 0.45);
            color: var(--ink);
        }
        div[data-testid="stButton"] > button[kind="primary"]:hover {
            color: #fffaf0;
            border-color: #0f766e;
        }
        div[data-testid="stButton"] > button p,
        div[data-testid="stButton"] > button span {
            color: inherit;
        }
        div[data-testid="stExpander"] {
            border: 1px solid rgba(18, 20, 15, 0.12);
            border-radius: 8px;
            background: rgba(255, 250, 240, 0.94);
            box-shadow: 0 18px 42px rgba(18, 20, 15, 0.08);
            margin-bottom: 0.85rem;
        }
        div[data-testid="stExpander"] summary {
            color: var(--ink);
            font-weight: 760;
        }
        [data-testid="stMetric"] {
            background: rgba(247, 243, 232, 0.94);
            border: 1px solid rgba(18, 20, 15, 0.1);
            border-radius: 8px;
            padding: 0.95rem 1rem;
            min-height: 108px;
            box-shadow: 0 14px 32px rgba(18, 20, 15, 0.08);
        }
        [data-testid="stMetricLabel"] {
            color: #596052;
        }
        [data-testid="stMetricValue"] {
            color: #12140f;
            font-size: 1.55rem;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(18, 20, 15, 0.12);
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 16px 34px rgba(18, 20, 15, 0.07);
        }
        .wow-hero {
            position: relative;
            min-height: 365px;
            overflow: hidden;
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 2rem;
            margin-bottom: 1rem;
            background:
                radial-gradient(circle at 82% 32%, rgba(45, 212, 191, 0.16), transparent 28%),
                linear-gradient(120deg, #12140f 0%, #1c241a 42%, #263022 100%);
            box-shadow: 0 30px 80px rgba(0, 0, 0, 0.26);
        }
        .wow-hero::before {
            content: "";
            position: absolute;
            inset: 0;
            background-image:
                linear-gradient(rgba(247, 243, 232, 0.055) 1px, transparent 1px),
                linear-gradient(90deg, rgba(247, 243, 232, 0.055) 1px, transparent 1px);
            background-size: 34px 34px;
            mask-image: linear-gradient(90deg, transparent 0%, #000 42%, #000 100%);
        }
        .wow-copy {
            position: relative;
            z-index: 2;
            max-width: 610px;
        }
        .wow-kicker {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.28rem 0.65rem;
            border: 1px solid rgba(45, 212, 191, 0.38);
            border-radius: 999px;
            color: #a7f3d0;
            background: rgba(45, 212, 191, 0.08);
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        .wow-hero h1 {
            margin: 0.75rem 0 0.45rem 0;
            color: #fffaf0;
            font-size: clamp(2.4rem, 5vw, 5rem);
            line-height: 0.95;
            font-weight: 800;
        }
        .wow-hero p {
            color: rgba(247, 243, 232, 0.78);
            font-size: 1.08rem;
            max-width: 560px;
            margin-bottom: 1.15rem;
        }
        .wow-scene {
            position: absolute;
            right: 1.5rem;
            bottom: 1.2rem;
            width: min(520px, 44vw);
            height: 285px;
            z-index: 1;
        }
        .wow-map {
            position: absolute;
            inset: 10px 10px 58px 22px;
            transform: perspective(640px) rotateX(58deg) rotateZ(-12deg);
            border: 1px solid rgba(247, 243, 232, 0.16);
            background:
                linear-gradient(90deg, rgba(45, 212, 191, 0.35), transparent 1px),
                linear-gradient(rgba(246, 180, 75, 0.22), transparent 1px);
            background-size: 38px 38px;
            box-shadow: 0 0 48px rgba(45, 212, 191, 0.15);
            animation: mapGlow 4.8s ease-in-out infinite;
        }
        .wow-line {
            position: absolute;
            height: 2px;
            background: linear-gradient(90deg, transparent, var(--teal), var(--amber), transparent);
            transform-origin: left center;
            opacity: 0.85;
        }
        .wow-l1 { width: 230px; right: 165px; top: 86px; transform: rotate(-21deg); }
        .wow-l2 { width: 290px; right: 70px; top: 150px; transform: rotate(9deg); }
        .wow-l3 { width: 180px; right: 220px; top: 198px; transform: rotate(27deg); }
        .wow-dot {
            position: absolute;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: var(--amber);
            box-shadow: 0 0 20px rgba(246, 180, 75, 0.8);
            animation: pulseDot 2.6s ease-in-out infinite;
        }
        .wow-d1 { right: 390px; top: 70px; }
        .wow-d2 { right: 140px; top: 140px; background: var(--teal); box-shadow: 0 0 20px rgba(45, 212, 191, 0.8); }
        .wow-d3 { right: 260px; top: 210px; background: var(--red); box-shadow: 0 0 20px rgba(239, 98, 91, 0.75); animation-delay: 0.7s; }
        .wow-buildings {
            position: absolute;
            right: 38px;
            bottom: 0;
            display: flex;
            align-items: flex-end;
            gap: 8px;
        }
        .wow-building {
            width: 28px;
            background: linear-gradient(180deg, rgba(247, 243, 232, 0.92), rgba(143, 177, 198, 0.52));
            border: 1px solid rgba(247, 243, 232, 0.22);
            box-shadow: inset 0 0 18px rgba(18, 20, 15, 0.25);
        }
        @keyframes pulseDot {
            0%, 100% { transform: scale(1); opacity: 0.75; }
            50% { transform: scale(1.45); opacity: 1; }
        }
        @keyframes mapGlow {
            0%, 100% { box-shadow: 0 0 42px rgba(45, 212, 191, 0.12); }
            50% { box-shadow: 0 0 82px rgba(246, 180, 75, 0.22); }
        }
        .wow-metrics {
            position: relative;
            z-index: 2;
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.75rem;
            max-width: 900px;
        }
        .wow-mini {
            border: 1px solid rgba(247, 243, 232, 0.14);
            background: rgba(247, 243, 232, 0.08);
            border-radius: 8px;
            padding: 0.75rem;
            backdrop-filter: blur(10px);
        }
        .wow-mini span {
            display: block;
            color: rgba(247, 243, 232, 0.6);
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }
        .wow-mini strong {
            color: #fffaf0;
            font-size: 1.32rem;
        }
        .signal-grid {
            display: grid;
            grid-template-columns: repeat(12, 1fr);
            gap: 1rem;
            margin: 1rem 0;
        }
        .mission-rail {
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 0.7rem;
            margin: 1rem 0;
            color: #fffaf0;
        }
        .mission-step {
            position: relative;
            overflow: hidden;
            border: 1px solid rgba(247, 243, 232, 0.14);
            border-radius: 8px;
            background: linear-gradient(180deg, rgba(255,250,240,0.1), rgba(255,250,240,0.035));
            padding: 0.9rem 0.95rem;
            min-height: 112px;
        }
        .mission-step::after {
            content: "";
            position: absolute;
            inset: auto -40% 0 -40%;
            height: 2px;
            background: linear-gradient(90deg, transparent, #2dd4bf, #f6b44b, transparent);
            animation: railFlow 3.2s linear infinite;
        }
        .mission-step b {
            display: block;
            color: #fffaf0;
            font-size: 1.02rem;
            margin-bottom: 0.2rem;
        }
        .mission-step span {
            color: rgba(247, 243, 232, 0.62);
            font-size: 0.86rem;
        }
        .mission-index {
            display: inline-flex;
            width: 26px;
            height: 26px;
            align-items: center;
            justify-content: center;
            border-radius: 999px;
            background: rgba(45, 212, 191, 0.16);
            color: #99f6e4;
            margin-bottom: 0.55rem;
            font-weight: 800;
        }
        @keyframes railFlow {
            from { transform: translateX(-20%); }
            to { transform: translateX(20%); }
        }
        .decision-board {
            display: grid;
            grid-template-columns: 1.15fr 0.85fr;
            gap: 1rem;
            margin: 1rem 0 1.25rem;
        }
        .decision-panel {
            border-radius: 8px;
            padding: 1.1rem;
            color: #fffaf0;
            background:
                radial-gradient(circle at 10% 0%, rgba(45, 212, 191, 0.18), transparent 30%),
                linear-gradient(145deg, #151914, #232a20);
            border: 1px solid rgba(247, 243, 232, 0.14);
            box-shadow: 0 24px 55px rgba(18, 20, 15, 0.22);
        }
        .decision-panel h3 {
            margin: 0 0 0.25rem 0;
            color: #fffaf0;
            font-size: 1.28rem;
        }
        .decision-panel p {
            color: rgba(247, 243, 232, 0.68);
            margin: 0 0 0.9rem 0;
        }
        .decision-row {
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 0.8rem;
            padding: 0.72rem 0;
            border-top: 1px solid rgba(247, 243, 232, 0.1);
        }
        .decision-row strong { color: #fffaf0; display: block; }
        .decision-row small { color: rgba(247, 243, 232, 0.58); }
        .decision-pill {
            align-self: center;
            border-radius: 999px;
            background: rgba(246, 180, 75, 0.14);
            color: #ffd18a;
            padding: 0.28rem 0.58rem;
            font-weight: 780;
        }
        .radar-tile {
            min-height: 100%;
            border-radius: 8px;
            padding: 1.1rem;
            color: #12140f;
            background:
                linear-gradient(135deg, rgba(255,250,240,0.98), rgba(230,239,230,0.98));
            border: 1px solid rgba(18,20,15,0.1);
        }
        .radar-ring {
            width: 185px;
            height: 185px;
            border-radius: 50%;
            margin: 0.35rem auto 0.7rem;
            background:
                radial-gradient(circle, #fffaf0 0 24%, transparent 25%),
                conic-gradient(#2dd4bf 0 34%, #f6b44b 34% 72%, #ef625b 72% 100%);
            box-shadow: inset 0 0 0 15px rgba(255,250,240,0.72), 0 18px 38px rgba(18,20,15,0.14);
        }
        .radar-tile h3 {
            color: #12140f;
            margin: 0;
        }
        .radar-tile p {
            color: #626b5b;
            margin: 0.25rem 0 0 0;
        }
        .signal-card {
            grid-column: span 3;
            background: #fffaf0;
            color: #12140f;
            border: 1px solid rgba(18, 20, 15, 0.1);
            border-radius: 8px;
            padding: 1rem;
            box-shadow: 0 16px 35px rgba(18, 20, 15, 0.08);
        }
        .signal-card.wide { grid-column: span 6; }
        .signal-card.dark {
            background: #171b15;
            color: #fffaf0;
            border-color: rgba(247, 243, 232, 0.14);
        }
        .signal-label {
            color: #707766;
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 0.35rem;
        }
        .signal-card.dark .signal-label { color: rgba(247, 243, 232, 0.58); }
        .signal-value {
            font-size: 1.65rem;
            line-height: 1.1;
            font-weight: 780;
        }
        .signal-caption {
            margin-top: 0.45rem;
            color: #656d5f;
            font-size: 0.92rem;
        }
        .signal-card.dark .signal-caption { color: rgba(247, 243, 232, 0.68); }
        .section-title {
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            color: #12140f;
            margin: 1.35rem 0 0.6rem;
        }
        .section-title h2 {
            color: #12140f;
            margin: 0;
            font-size: 1.55rem;
        }
        .section-title span {
            color: #656d5f;
            font-size: 0.92rem;
        }
        .top-list {
            display: grid;
            gap: 0.6rem;
        }
        .top-row {
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 0.75rem;
            align-items: center;
            border: 1px solid rgba(18, 20, 15, 0.1);
            background: rgba(255, 250, 240, 0.88);
            color: #12140f;
            border-radius: 8px;
            padding: 0.78rem 0.85rem;
        }
        .top-row strong { display: block; }
        .top-row small { color: #626b5b; }
        .score-chip {
            min-width: 74px;
            text-align: center;
            padding: 0.35rem 0.55rem;
            border-radius: 999px;
            background: #171b15;
            color: #fffaf0;
            font-weight: 750;
        }
        .badge-row {
            display: flex;
            gap: 0.45rem;
            flex-wrap: wrap;
            margin: 0.15rem 0 0.8rem 0;
        }
        .badge {
            border: 1px solid rgba(18, 20, 15, 0.12);
            border-radius: 999px;
            padding: 0.2rem 0.58rem;
            font-size: 0.82rem;
            background: #fffaf0;
            color: #12140f;
        }
        .badge.passoire {
            border-color: rgba(239, 98, 91, 0.34);
            color: #9f2e29;
            background: #fff0ed;
        }
        .badge.good {
            border-color: rgba(45, 212, 191, 0.34);
            color: #0d7167;
            background: #eafcf8;
        }
        .note {
            border-left: 4px solid var(--amber);
            background: #fff5d8;
            padding: 0.75rem 0.9rem;
            border-radius: 6px;
            color: #4f3f18;
            margin: 0.7rem 0 1rem 0;
        }
        .legend {
            display: flex;
            align-items: center;
            gap: 0.65rem;
            margin: 0.25rem 0 0.8rem 0;
            color: #626b5b;
            font-size: 0.9rem;
        }
        .ramp {
            width: 210px;
            height: 11px;
            border-radius: 999px;
            border: 1px solid rgba(18, 20, 15, 0.12);
        }
        .ramp.prix { background: linear-gradient(90deg, #2dd4bf, #f6b44b, #ef625b); }
        .ramp.passoires { background: linear-gradient(90deg, #8fb1c6, #f6b44b, #ef625b); }
        @media (max-width: 900px) {
            .wow-scene { opacity: 0.35; width: 86vw; }
            .wow-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .signal-card, .signal-card.wide { grid-column: span 12; }
            .mission-rail { grid-template-columns: 1fr; }
            .decision-board { grid-template-columns: 1fr; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _escape(value: object) -> str:
    return html.escape(str(value))


def top_nav(current_title: str) -> None:
    links = [
        ("app.py", "Accueil", "ImmoLake"),
        ("pages/1_Marche.py", "Marche", "Marche"),
        ("pages/2_Energie.py", "Energie", "Energie"),
        ("pages/3_Carte.py", "Carte", "Carte"),
        ("pages/4_Bonnes_affaires.py", "Affaires", "Bonnes affaires"),
        ("pages/5_Comparateur.py", "Comparer", "Comparateur"),
    ]
    with st.container(border=True):
        brand_col, *nav_cols = st.columns([1.15, 0.86, 0.86, 0.86, 0.86, 1.02, 1.02])
        with brand_col:
            st.markdown("**ImmoLake**")
            st.caption("Gold marts · DPE · DVF")
        for column, (page, label, title) in zip(nav_cols, links):
            with column:
                button_type = "primary" if current_title == title else "secondary"
                if st.button(label, key=f"nav_{label}", type=button_type, width="stretch"):
                    st.switch_page(page)


def hero(title: str, subtitle: str, metrics: list[tuple[str, str]] | None = None) -> None:
    metric_html = ""
    if metrics:
        metric_html = "<div class='wow-metrics'>" + "".join(
            f"<div class='wow-mini'><span>{_escape(label)}</span><strong>{_escape(value)}</strong></div>"
            for label, value in metrics
        ) + "</div>"
    st.markdown(
        f"""
        <section class="wow-hero">
            <div class="wow-copy">
                <div class="wow-kicker">Lakehouse command center</div>
                <h1>{_escape(title)}</h1>
                <p>{_escape(subtitle)}</p>
                {metric_html}
            </div>
            <div class="wow-scene" aria-hidden="true">
                <div class="wow-map"></div>
                <div class="wow-line wow-l1"></div>
                <div class="wow-line wow-l2"></div>
                <div class="wow-line wow-l3"></div>
                <div class="wow-dot wow-d1"></div>
                <div class="wow-dot wow-d2"></div>
                <div class="wow-dot wow-d3"></div>
                <div class="wow-buildings">
                    <div class="wow-building" style="height:84px"></div>
                    <div class="wow-building" style="height:132px"></div>
                    <div class="wow-building" style="height:104px"></div>
                    <div class="wow-building" style="height:168px"></div>
                    <div class="wow-building" style="height:118px"></div>
                    <div class="wow-building" style="height:146px"></div>
                </div>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def page_header(title: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="section-title">
            <div>
                <h2>{_escape(title)}</h2>
                <span>{_escape(subtitle)}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def pipeline_rail() -> None:
    steps = [
        ("01", "Raw", "DPE et DVF captes"),
        ("02", "Silver", "Nettoye et type"),
        ("03", "Gold", "Biens enrichis prix"),
        ("04", "Marts", "Commune et opportunites"),
        ("05", "Front", "Decision investisseur"),
    ]
    columns = st.columns(len(steps))
    for column, (index, title, caption) in zip(columns, steps):
        with column.container(border=True):
            st.caption(index)
            st.markdown(f"**{title}**")
            st.caption(caption)
    # Stockage et calcul sont transverses aux couches (pas une etape) -> legende infra.
    st.caption(
        "Stockage : **MinIO** (Parquet, medaillon raw->marts)  ·  "
        "Calcul & requetes : **DuckDB**  ·  Orchestration : **Airflow 3** (Assets)"
    )


def decision_board(opportunities: pd.DataFrame, market: pd.DataFrame) -> None:
    risk = market["pct_passoires"].mean() if not market.empty else None
    price = market["prix_m2"].median() if not market.empty else None
    left, right = st.columns([1.15, 0.85])
    with left.container(border=True):
        st.subheader("Decision board")
        st.caption("Les signaux les plus forts remontent directement depuis les marts Parquet.")
        if opportunities.empty:
            empty_state("Aucune opportunite avec les filtres courants.")
        else:
            for row in opportunities.head(3).itertuples(index=False):
                data = row._asdict()
                row_left, row_right = st.columns([0.78, 0.22])
                with row_left:
                    st.markdown(f"**{data.get('commune', '-')}**")
                    st.caption(
                        f"{data.get('departement', '-')} · {format_eur_m2(data.get('prix_m2'))}"
                        f" · {format_pct(data.get('pct_passoires'))} passoires"
                    )
                row_right.metric("Score", format_number(data.get("score_opportunite"), 1))
    with right.container(border=True):
        st.subheader("Indice territoire")
        st.metric("Prix median", format_eur_m2(price))
        st.metric("Risque energie", format_pct(risk))
        st.progress(0 if risk is None or pd.isna(risk) else min(max(float(risk) / 100, 0), 1))
        st.caption("Teal = attractivite prix · amber = tension marche · red = risque energie.")


def signal_grid(items: list[tuple[str, str, str, str]]) -> None:
    columns = st.columns(len(items))
    for column, (label, value, caption, _tone) in zip(columns, items):
        with column.container(border=True):
            st.caption(label)
            st.markdown(f"### {value}")
            st.caption(caption)


def leaderboard(df: pd.DataFrame, *, score_col: str = "score_opportunite", limit: int = 5) -> None:
    if df.empty:
        empty_state("Aucune opportunite avec les filtres courants.")
        return
    for row in df.head(limit).itertuples(index=False):
        row_dict = row._asdict()
        with st.container(border=True):
            left, right = st.columns([0.78, 0.22])
            with left:
                st.markdown(f"**{row_dict.get('commune', '-')}**")
                st.caption(
                    f"{row_dict.get('departement', '-')} · {row_dict.get('type_bien', 'tous')}"
                    f" · {format_eur_m2(row_dict.get('prix_m2'))}"
                    f" · {format_pct(row_dict.get('pct_passoires'))} passoires"
                )
            right.metric("Score", format_number(row_dict.get(score_col), 1))


def note(text: str) -> None:
    st.info(text)


def format_number(value: object, digits: int = 0) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric):,.{digits}f}".replace(",", " ")


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
    elif metric == "score_opportunite":
        label = "Score faible -> score eleve"
    else:
        label = "Prix bas -> prix eleve"
    st.caption(label)


def opportunity_badges(row: pd.Series) -> str:
    badges = []
    if pd.to_numeric(pd.Series([row.get("indice_sous_cotation")]), errors="coerce").iloc[0] < 0:
        badges.append("sous-cotee")
    if pd.to_numeric(pd.Series([row.get("pct_passoires")]), errors="coerce").iloc[0] >= 20:
        badges.append("parc passoires")
    return " · ".join(badges)
