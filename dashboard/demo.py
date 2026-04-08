"""
Demo mode — synthetic data, no PostgreSQL required.
Run: streamlit run dashboard/demo.py
"""
from __future__ import annotations

import time
import random
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Polymarket Pipeline",
    page_icon="./docs/favicon.ico",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Design system — injected via st.markdown unsafe_allow_html
# ---------------------------------------------------------------------------

STYLE = """
<style>
@import url('https://api.fontshare.com/v2/css?f[]=satoshi@400,500,700&f[]=cabinet-grotesk@800,900&display=swap');

/* ── Reset Streamlit defaults ── */
html, body, [class*="css"] {
    font-family: 'Satoshi', 'Helvetica Neue', sans-serif;
    background-color: #0d0d0d !important;
    color: #e8e6e1 !important;
}

/* ── Grain overlay on body ── */
body::before {
    content: "";
    position: fixed;
    inset: 0;
    pointer-events: none;
    z-index: 9999;
    opacity: 0.035;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E");
    background-repeat: repeat;
    background-size: 128px 128px;
}

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 2rem 3rem 4rem 3rem !important; max-width: 1400px !important; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #111111 !important;
    border-right: 1px solid #1f1f1f !important;
}
section[data-testid="stSidebar"] * { color: #a09e99 !important; }

/* ── Page header ── */
.page-header {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    padding-bottom: 2rem;
    border-bottom: 1px solid #1e1e1e;
    margin-bottom: 2.5rem;
}
.page-header h1 {
    font-family: 'Cabinet Grotesk', sans-serif;
    font-weight: 900;
    font-size: clamp(2rem, 4vw, 3.2rem);
    letter-spacing: -0.04em;
    color: #f0ede8;
    margin: 0;
    line-height: 1;
}
.page-header .badge {
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #3ecf8e;
    border: 1px solid #1e4d38;
    background: #0d2b1f;
    padding: 0.3rem 0.75rem;
    border-radius: 2px;
    margin-left: 1rem;
    vertical-align: middle;
}
.page-header .subtitle {
    font-size: 0.85rem;
    color: #5c5a56;
    margin-top: 0.4rem;
    letter-spacing: 0.01em;
}

/* ── KPI cards ── */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 1px;
    background: #1a1a1a;
    border: 1px solid #1a1a1a;
    border-radius: 4px;
    overflow: hidden;
    margin-bottom: 2.5rem;
}
.kpi-card {
    background: #111111;
    padding: 1.5rem 1.75rem;
    transition: background 180ms ease;
}
.kpi-card:hover { background: #141414; }
.kpi-label {
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: #4a4845;
    margin-bottom: 0.6rem;
}
.kpi-value {
    font-family: 'Cabinet Grotesk', sans-serif;
    font-weight: 900;
    font-size: clamp(1.6rem, 2.5vw, 2.4rem);
    letter-spacing: -0.03em;
    color: #f0ede8;
    line-height: 1;
}
.kpi-delta {
    font-size: 0.72rem;
    color: #3ecf8e;
    margin-top: 0.4rem;
    font-weight: 500;
}

/* ── Section titles ── */
.section-label {
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #3a3835;
    margin-bottom: 1rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid #1a1a1a;
}

/* ── Anomaly table ── */
.anomaly-row {
    display: grid;
    grid-template-columns: 1fr auto;
    align-items: center;
    padding: 0.8rem 1rem;
    border-bottom: 1px solid #161616;
    transition: background 150ms ease;
}
.anomaly-row:hover { background: #131313; }
.anomaly-row:last-child { border-bottom: none; }
.anomaly-market { font-size: 0.82rem; color: #c8c6c1; line-height: 1.3; }
.anomaly-id { font-size: 0.68rem; color: #3a3835; font-family: 'Satoshi', monospace; margin-top: 0.2rem; }
.anomaly-zscore {
    font-family: 'Cabinet Grotesk', sans-serif;
    font-weight: 900;
    font-size: 1.1rem;
    letter-spacing: -0.02em;
    color: #e05c5c;
    text-align: right;
}
.anomaly-zscore.mild { color: #e08c3c; }
.anomaly-zscore.strong { color: #e05c5c; }

/* ── Market selector ── */
.stSelectbox > div > div {
    background: #111111 !important;
    border: 1px solid #252525 !important;
    border-radius: 3px !important;
    color: #e8e6e1 !important;
    font-size: 0.875rem !important;
}

/* ── Divider ── */
.section-divider {
    height: 1px;
    background: #1a1a1a;
    margin: 2.5rem 0;
}

/* ── Footer ── */
.page-footer {
    margin-top: 4rem;
    padding-top: 1.5rem;
    border-top: 1px solid #1a1a1a;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.72rem;
    color: #3a3835;
    letter-spacing: 0.04em;
}
.page-footer a { color: #3ecf8e; text-decoration: none; }

/* ── Plotly container ── */
.js-plotly-plot .plotly { border-radius: 2px; }

/* ── Staggered fade-in ── */
@keyframes fadeUp {
    from { opacity: 0; transform: translateY(12px); }
    to   { opacity: 1; transform: translateY(0); }
}
.kpi-grid     { animation: fadeUp 0.4s ease both; animation-delay: 0.05s; }
.section-label { animation: fadeUp 0.4s ease both; animation-delay: 0.1s; }
</style>
"""

# ── Plotly theme ──────────────────────────────────────────────────────────────
PLOT_LAYOUT = dict(
    paper_bgcolor="#0d0d0d",
    plot_bgcolor="#0d0d0d",
    font=dict(family="Satoshi, Helvetica Neue, sans-serif", color="#5c5a56", size=11),
    xaxis=dict(
        gridcolor="#161616", gridwidth=1,
        zeroline=False, showline=False,
        tickfont=dict(color="#3a3835", size=10),
    ),
    yaxis=dict(
        gridcolor="#161616", gridwidth=1,
        zeroline=False, showline=False,
        tickfont=dict(color="#3a3835", size=10),
    ),
    margin=dict(l=0, r=0, t=24, b=0),
    legend=dict(
        bgcolor="rgba(0,0,0,0)", borderwidth=0,
        font=dict(color="#5c5a56", size=10),
        orientation="h", y=1.12,
    ),
    hoverlabel=dict(
        bgcolor="#161616", bordercolor="#252525",
        font=dict(color="#e8e6e1", size=11, family="Satoshi"),
    ),
)

# ---------------------------------------------------------------------------
# Synthetic data
# ---------------------------------------------------------------------------

MARKETS = [
    ("mkt-BTC",  "Will BTC exceed $100k before Dec 2025?"),
    ("mkt-FED",  "Will the Fed cut rates in Q2 2025?"),
    ("mkt-MUSK", "Will Musk remain CEO of X through 2025?"),
    ("mkt-ES",   "Will Spain win the 2025 Nations League?"),
    ("mkt-GPT",  "Will GPT-5 launch before July 2025?"),
    ("mkt-EU",   "Will EU inflation drop below 2% in 2025?"),
]

def make_markets() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "market_id": mid,
            "question": q,
            "avg_volume_24h": round(random.uniform(8000, 95000), 2),
            "avg_price": round(random.uniform(0.15, 0.85), 4),
        }
        for mid, q in MARKETS
    ]).sort_values("avg_volume_24h", ascending=False)

def make_history(market_id: str) -> pd.DataFrame:
    random.seed(hash(market_id) % 9999)
    base = 0.45 + random.uniform(-0.2, 0.2)
    rows = []
    price = base
    for i in range(72):
        price = max(0.02, min(0.98, price + random.gauss(0, 0.012)))
        z = random.gauss(0, 0.6)
        rows.append({
            "fetched_at": datetime.utcnow() - timedelta(hours=72 - i),
            "mid_price": round(price, 4),
            "zscore": round(z, 3),
            "is_anomaly": abs(z) > 2.0,
        })
    # one clear anomaly spike
    rows[45]["mid_price"] = round(min(0.97, base + 0.32), 4)
    rows[45]["zscore"] = 3.7
    rows[45]["is_anomaly"] = True
    return pd.DataFrame(rows)

def make_anomalies() -> pd.DataFrame:
    return pd.DataFrame([
        {"market_id": "mkt-BTC",  "question": "Will BTC exceed $100k before Dec 2025?",
         "fetched_at": datetime.utcnow() - timedelta(minutes=8),  "mid_price": 0.84, "zscore": 3.82},
        {"market_id": "mkt-MUSK", "question": "Will Musk remain CEO of X through 2025?",
         "fetched_at": datetime.utcnow() - timedelta(minutes=34), "mid_price": 0.11, "zscore": -2.61},
        {"market_id": "mkt-GPT",  "question": "Will GPT-5 launch before July 2025?",
         "fetched_at": datetime.utcnow() - timedelta(minutes=71), "mid_price": 0.73, "zscore": 2.14},
    ])

# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

def kpi_html(markets: pd.DataFrame, anomaly_count: int) -> str:
    total_vol = markets["avg_volume_24h"].sum()
    avg_price = markets["avg_price"].mean()
    return f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-label">Active markets</div>
            <div class="kpi-value">{len(markets)}</div>
            <div class="kpi-delta">last 24 hours</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Total volume 24h</div>
            <div class="kpi-value">${total_vol:,.0f}</div>
            <div class="kpi-delta">across all markets</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Anomalies flagged</div>
            <div class="kpi-value" style="color: {'#e05c5c' if anomaly_count > 0 else '#3ecf8e'}">{anomaly_count}</div>
            <div class="kpi-delta" style="color: #5c5a56">z-score &gt; 2.0 threshold</div>
        </div>
    </div>
    """

def anomaly_html(df: pd.DataFrame) -> str:
    if df.empty:
        return '<div style="padding:1.5rem;color:#3a3835;font-size:0.82rem;">No anomalies in the last 24 hours.</div>'
    rows_html = ""
    for _, row in df.iterrows():
        z = row["zscore"]
        z_class = "strong" if abs(z) > 3 else "mild"
        sign = "+" if z > 0 else ""
        mins_ago = int((datetime.utcnow() - row["fetched_at"]).total_seconds() / 60)
        rows_html += f"""
        <div class="anomaly-row">
            <div>
                <div class="anomaly-market">{row['question'][:52]}{'...' if len(row['question']) > 52 else ''}</div>
                <div class="anomaly-id">{row['market_id']} · {mins_ago}m ago · p={row['mid_price']:.3f}</div>
            </div>
            <div class="anomaly-zscore {z_class}">{sign}{z:.2f}σ</div>
        </div>"""
    return rows_html

def volume_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["avg_volume_24h"],
        y=df["question"].str[:40],
        orientation="h",
        marker=dict(
            color=df["avg_volume_24h"],
            colorscale=[[0, "#1a2e28"], [0.5, "#1d4d3a"], [1, "#3ecf8e"]],
            line=dict(width=0),
        ),
        hovertemplate="<b>%{y}</b><br>Volume: $%{x:,.0f}<extra></extra>",
    ))
    layout = {**PLOT_LAYOUT, "height": 320}
    layout["yaxis"] = {**PLOT_LAYOUT["yaxis"], "categoryorder": "total ascending"}
    fig.update_layout(**layout)
    return fig

def price_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    # Area fill under the line
    fig.add_trace(go.Scatter(
        x=df["fetched_at"], y=df["mid_price"],
        fill="tozeroy",
        fillcolor="rgba(62,207,142,0.04)",
        line=dict(color="#3ecf8e", width=1.5),
        name="mid price",
        hovertemplate="%{x|%H:%M}<br>p = %{y:.4f}<extra></extra>",
    ))

    # Anomaly markers
    anom = df[df["is_anomaly"]]
    if not anom.empty:
        fig.add_trace(go.Scatter(
            x=anom["fetched_at"], y=anom["mid_price"],
            mode="markers",
            marker=dict(color="#e05c5c", size=8, symbol="circle",
                        line=dict(color="#0d0d0d", width=1.5)),
            name="anomaly",
            hovertemplate="Anomaly<br>p = %{y:.4f}<extra></extra>",
        ))

    layout = {**PLOT_LAYOUT, "height": 280}
    fig.update_layout(**layout)
    fig.update_xaxes(tickformat="%H:%M", nticks=8)
    fig.update_yaxes(tickformat=".3f", nticks=5)
    return fig

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

st.markdown(STYLE, unsafe_allow_html=True)

# Sidebar (collapsed by default)
with st.sidebar:
    st.markdown("**Controls**")
    refresh_interval = st.slider("Refresh interval (s)", 30, 300, 60, 30)
    st.markdown("---")
    st.markdown(
        '<a href="https://github.com/blackcat112/polymarket-data-pipeline" '
        'target="_blank" rel="noopener">View on GitHub</a>',
        unsafe_allow_html=True,
    )
    st.caption("Polymarket Data Pipeline — demo mode")

# Header
st.markdown(
    '<div class="page-header">'
    '  <div>'
    '    <h1>Polymarket Pipeline<span class="badge">Live</span></h1>'
    '    <div class="subtitle">Prediction market analytics · PySpark · Airflow · PostgreSQL</div>'
    '  </div>'
    '</div>',
    unsafe_allow_html=True,
)

placeholder = st.empty()

while True:
    with placeholder.container():
        df_markets  = make_markets()
        df_anomalies = make_anomalies()

        # KPIs
        st.markdown(kpi_html(df_markets, len(df_anomalies)), unsafe_allow_html=True)

        # Main grid — 3:2 split
        col_chart, col_anom = st.columns([3, 2], gap="large")

        with col_chart:
            st.markdown('<div class="section-label">Volume by market · 24h</div>', unsafe_allow_html=True)
            st.plotly_chart(volume_chart(df_markets), use_container_width=True, config={"displayModeBar": False})

        with col_anom:
            st.markdown('<div class="section-label">Anomaly alerts</div>', unsafe_allow_html=True)
            st.markdown(anomaly_html(df_anomalies), unsafe_allow_html=True)

        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

        # Price history
        st.markdown('<div class="section-label">Price history</div>', unsafe_allow_html=True)
        col_sel, col_meta = st.columns([2, 1])
        with col_sel:
            selected = st.selectbox(
                "market",
                options=df_markets["market_id"].tolist(),
                format_func=lambda mid: df_markets.loc[
                    df_markets["market_id"] == mid, "question"
                ].values[0],
                label_visibility="collapsed",
            )
        with col_meta:
            sel_price = df_markets.loc[df_markets["market_id"] == selected, "avg_price"].values[0]
            sel_vol = df_markets.loc[df_markets["market_id"] == selected, "avg_volume_24h"].values[0]
            st.markdown(
                f'<div style="text-align:right;padding-top:0.5rem;">'
                f'<span style="font-size:0.72rem;color:#3a3835;letter-spacing:0.08em;text-transform:uppercase;">avg price</span> '
                f'<span style="font-family:\'Cabinet Grotesk\',sans-serif;font-weight:900;font-size:1.1rem;color:#f0ede8;letter-spacing:-0.02em;">{sel_price:.3f}</span>'
                f'&nbsp;&nbsp;&nbsp;'
                f'<span style="font-size:0.72rem;color:#3a3835;letter-spacing:0.08em;text-transform:uppercase;">vol</span> '
                f'<span style="font-family:\'Cabinet Grotesk\',sans-serif;font-weight:900;font-size:1.1rem;color:#f0ede8;letter-spacing:-0.02em;">${sel_vol:,.0f}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        df_hist = make_history(selected)
        st.plotly_chart(price_chart(df_hist), use_container_width=True, config={"displayModeBar": False})

        # Footer
        st.markdown(
            f'<div class="page-footer">'
            f'  <span>POLYMARKET DATA PIPELINE &nbsp;·&nbsp; Data Engineering Project</span>'
            f'  <span>Last updated {datetime.utcnow().strftime("%H:%M:%S")} UTC &nbsp;·&nbsp; '
            f'  <a href="https://github.com/blackcat112/polymarket-data-pipeline">github.com/blackcat112/polymarket-data-pipeline</a></span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    time.sleep(refresh_interval)
    st.rerun()