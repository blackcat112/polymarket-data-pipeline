"""
Demo mode — synthetic data aligned with the rewards pipeline model.
No PostgreSQL required.
Run: streamlit run dashboard/demo.py
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Polymarket Rewards Pipeline",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Design system
# ---------------------------------------------------------------------------

STYLE = """
<style>
@import url('https://api.fontshare.com/v2/css?f[]=satoshi@400,500,700&f[]=cabinet-grotesk@800,900&display=swap');

html, body, [class*="css"] {
    font-family: 'Satoshi', 'Helvetica Neue', sans-serif;
    background-color: #0d0d0d !important;
    color: #e8e6e1 !important;
}
body::before {
    content: "";
    position: fixed;
    inset: 0;
    pointer-events: none;
    z-index: 9999;
    opacity: 0.035;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E");
    background-size: 128px 128px;
}
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 2rem 3rem 4rem 3rem !important; max-width: 1400px !important; }

section[data-testid="stSidebar"] {
    background: #111111 !important;
    border-right: 1px solid #1f1f1f !important;
}
section[data-testid="stSidebar"] * { color: #a09e99 !important; }

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
.badge {
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
.subtitle {
    font-size: 0.85rem;
    color: #5c5a56;
    margin-top: 0.4rem;
    letter-spacing: 0.01em;
}

/* KPI grid */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1px;
    background: #1a1a1a;
    border: 1px solid #1a1a1a;
    border-radius: 4px;
    overflow: hidden;
    margin-bottom: 2.5rem;
}
.kpi-card { background: #111111; padding: 1.5rem 1.75rem; transition: background 180ms ease; }
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
.kpi-delta { font-size: 0.72rem; color: #3ecf8e; margin-top: 0.4rem; font-weight: 500; }

/* Section label */
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

/* Opportunity table */
.opp-row {
    display: grid;
    grid-template-columns: 1fr auto auto auto;
    align-items: center;
    gap: 1.5rem;
    padding: 0.9rem 1rem;
    border-bottom: 1px solid #161616;
    transition: background 150ms ease;
}
.opp-row:hover { background: #131313; }
.opp-row:last-child { border-bottom: none; }
.opp-question { font-size: 0.82rem; color: #c8c6c1; line-height: 1.3; }
.opp-id { font-size: 0.68rem; color: #3a3835; margin-top: 0.2rem; }
.opp-stat {
    font-family: 'Cabinet Grotesk', sans-serif;
    font-weight: 900;
    font-size: 1rem;
    letter-spacing: -0.02em;
    text-align: right;
}
.opp-stat-label { font-size: 0.65rem; color: #3a3835; text-transform: uppercase; letter-spacing: 0.08em; text-align: right; }

.section-divider { height: 1px; background: #1a1a1a; margin: 2.5rem 0; }

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

@keyframes fadeUp {
    from { opacity: 0; transform: translateY(12px); }
    to   { opacity: 1; transform: translateY(0); }
}
.kpi-grid { animation: fadeUp 0.4s ease both; animation-delay: 0.05s; }
</style>
"""

PLOT_LAYOUT = dict(
    paper_bgcolor="#0d0d0d",
    plot_bgcolor="#0d0d0d",
    font=dict(family="Satoshi, Helvetica Neue, sans-serif", color="#5c5a56", size=11),
    xaxis=dict(gridcolor="#161616", gridwidth=1, zeroline=False, showline=False,
               tickfont=dict(color="#3a3835", size=10)),
    yaxis=dict(gridcolor="#161616", gridwidth=1, zeroline=False, showline=False,
               tickfont=dict(color="#3a3835", size=10)),
    margin=dict(l=0, r=0, t=24, b=0),
    legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0,
                font=dict(color="#5c5a56", size=10), orientation="h", y=1.12),
    hoverlabel=dict(bgcolor="#161616", bordercolor="#252525",
                    font=dict(color="#e8e6e1", size=11, family="Satoshi")),
)

# ---------------------------------------------------------------------------
# Synthetic data — rewards pipeline model
# ---------------------------------------------------------------------------

MARKETS = [
    ("0xabc1", "Will BTC exceed $120k before Dec 2025?"),
    ("0xabc2", "Will the Fed cut rates in Q3 2025?"),
    ("0xabc3", "Will Musk remain CEO of X through 2025?"),
    ("0xabc4", "Will Spain win the 2025 Nations League?"),
    ("0xabc5", "Will GPT-5 launch before July 2025?"),
    ("0xabc6", "Will EU inflation drop below 2% in 2025?"),
    ("0xabc7", "Will ETH reach $5k in 2025?"),
    ("0xabc8", "Will Trump sign crypto regulation in 2025?"),
]


def make_opportunities() -> pd.DataFrame:
    """Simulate silver/gold layer output: scored reward opportunities."""
    rows = []
    for cid, question in MARKETS:
        pool = round(random.uniform(50, 800), 2)
        makers = random.randint(1, 12)
        score = round(pool / max(makers, 1), 4)
        rows.append({
            "condition_id": cid,
            "question": question,
            "pool_diario": pool,
            "num_makers": makers,
            "score_per_maker": score,
            "roi_1h_usdc": round(score / 24, 4),
            "max_spread": round(random.uniform(0.01, 0.05), 4),
            "midpoint": round(random.uniform(0.15, 0.85), 3),
            "avg_score_7d": round(score * random.uniform(0.8, 1.2), 4),
            "simulated_rewards_7d": round(score * 7 * random.uniform(0.7, 1.1), 2),
        })
    return pd.DataFrame(rows).sort_values("score_per_maker", ascending=False)


def make_score_history(condition_id: str) -> pd.DataFrame:
    """Simulate 72h of score_per_maker evolution for a market."""
    random.seed(hash(condition_id) % 9999)
    base_score = random.uniform(10, 120)
    rows = []
    score = base_score
    for i in range(72):
        score = max(0.5, score + random.gauss(0, base_score * 0.05))
        rows.append({
            "fetched_at": datetime.utcnow() - timedelta(hours=72 - i),
            "score_per_maker": round(score, 4),
            "roi_1h_usdc": round(score / 24, 4),
            "num_makers": max(1, int(random.gauss(5, 2))),
        })
    # inject a competition spike (many makers enter → score drops)
    rows[40]["num_makers"] = 18
    rows[40]["score_per_maker"] = round(rows[40]["score_per_maker"] / 3, 4)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

def kpi_html(df: pd.DataFrame) -> str:
    total_pool = df["pool_diario"].sum()
    best_roi = df["roi_1h_usdc"].max()
    avg_makers = df["num_makers"].mean()
    total_sim_7d = df["simulated_rewards_7d"].sum()
    return f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-label">Active reward markets</div>
            <div class="kpi-value">{len(df)}</div>
            <div class="kpi-delta">eligible for liquidity rewards</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Total daily pool</div>
            <div class="kpi-value">${total_pool:,.0f}</div>
            <div class="kpi-delta">USDC distributed today</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Best ROI/hour</div>
            <div class="kpi-value">${best_roi:.2f}</div>
            <div class="kpi-delta">USDC · top market right now</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Simulated rewards 7d</div>
            <div class="kpi-value">${total_sim_7d:,.0f}</div>
            <div class="kpi-delta">avg makers: {avg_makers:.1f} per market</div>
        </div>
    </div>
    """


def opportunities_html(df: pd.DataFrame) -> str:
    rows_html = ""
    for _, row in df.head(6).iterrows():
        spread_color = "#e05c5c" if row["max_spread"] > 0.035 else "#3ecf8e"
        rows_html += f"""
        <div class="opp-row">
            <div>
                <div class="opp-question">{row['question'][:55]}{'...' if len(row['question']) > 55 else ''}</div>
                <div class="opp-id">{row['condition_id']} · p={row['midpoint']:.3f}</div>
            </div>
            <div>
                <div class="opp-stat" style="color:#3ecf8e">${row['score_per_maker']:.2f}</div>
                <div class="opp-stat-label">score/maker</div>
            </div>
            <div>
                <div class="opp-stat" style="color:#f0ede8">${row['roi_1h_usdc']:.3f}</div>
                <div class="opp-stat-label">roi/hour</div>
            </div>
            <div>
                <div class="opp-stat" style="color:{spread_color}">{row['max_spread']:.3f}</div>
                <div class="opp-stat-label">max spread</div>
            </div>
        </div>"""
    return rows_html


def score_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["fetched_at"], y=df["score_per_maker"],
        fill="tozeroy", fillcolor="rgba(62,207,142,0.04)",
        line=dict(color="#3ecf8e", width=1.5),
        name="score/maker",
        hovertemplate="%{x|%H:%M}<br>score = $%{y:.3f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["fetched_at"], y=df["roi_1h_usdc"],
        line=dict(color="#4f98a3", width=1, dash="dot"),
        name="roi/hour",
        hovertemplate="%{x|%H:%M}<br>roi = $%{y:.4f}<extra></extra>",
    ))
    layout = {**PLOT_LAYOUT, "height": 260}
    fig.update_layout(**layout)
    fig.update_xaxes(tickformat="%H:%M", nticks=8)
    return fig


def pool_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["pool_diario"],
        y=df["question"].str[:38],
        orientation="h",
        marker=dict(
            color=df["score_per_maker"],
            colorscale=[[0, "#1a2e28"], [0.5, "#1d4d3a"], [1, "#3ecf8e"]],
            line=dict(width=0),
            colorbar=dict(
                title=dict(text="score/maker", font=dict(color="#3a3835", size=10)),
                tickfont=dict(color="#3a3835", size=9),
                thickness=8,
            ),
        ),
        hovertemplate="<b>%{y}</b><br>Pool: $%{x:,.0f}<extra></extra>",
    ))
    layout = {**PLOT_LAYOUT, "height": 340}
    layout["yaxis"] = {**PLOT_LAYOUT["yaxis"], "categoryorder": "total ascending"}
    fig.update_layout(**layout)
    return fig


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

st.markdown(STYLE, unsafe_allow_html=True)

with st.sidebar:
    st.markdown("**Controls**")
    refresh_interval = st.slider("Refresh interval (s)", 30, 300, 60, 30)
    min_roi = st.slider("Min ROI/hour ($)", 0.0, 5.0, 0.0, 0.1)
    st.markdown("---")
    st.markdown(
        '<a href="https://github.com/blackcat112/polymarket-data-pipeline" '
        'target="_blank" rel="noopener">View on GitHub</a>',
        unsafe_allow_html=True,
    )
    st.caption("Polymarket Rewards Pipeline — demo mode")

st.markdown(
    '<div class="page-header">'
    '  <div>'
    '    <h1>Rewards Pipeline<span class="badge">Demo</span></h1>'
    '    <div class="subtitle">Liquidity reward opportunities · PySpark · Airflow · PostgreSQL</div>'
    '  </div>'
    '</div>',
    unsafe_allow_html=True,
)

placeholder = st.empty()

while True:
    with placeholder.container():
        df_opp = make_opportunities()
        if min_roi > 0:
            df_opp = df_opp[df_opp["roi_1h_usdc"] >= min_roi]

        # KPIs
        st.markdown(kpi_html(df_opp), unsafe_allow_html=True)

        # Top opportunities + pool chart
        col_opp, col_pool = st.columns([3, 2], gap="large")

        with col_opp:
            st.markdown('<div class="section-label">Top reward opportunities · ranked by score/maker</div>',
                        unsafe_allow_html=True)
            st.markdown(opportunities_html(df_opp), unsafe_allow_html=True)

        with col_pool:
            st.markdown('<div class="section-label">Daily pool by market · color = score/maker</div>',
                        unsafe_allow_html=True)
            st.plotly_chart(pool_chart(df_opp), use_container_width=True,
                            config={"displayModeBar": False})

        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

        # Score history for selected market
        st.markdown('<div class="section-label">Score/maker history · 72h</div>',
                    unsafe_allow_html=True)
        col_sel, col_meta = st.columns([2, 1])
        with col_sel:
            selected = st.selectbox(
                "market",
                options=df_opp["condition_id"].tolist(),
                format_func=lambda cid: df_opp.loc[
                    df_opp["condition_id"] == cid, "question"
                ].values[0],
                label_visibility="collapsed",
            )
        with col_meta:
            sel = df_opp[df_opp["condition_id"] == selected].iloc[0]
            st.markdown(
                f'<div style="text-align:right;padding-top:0.5rem;">'
                f'<span style="font-size:0.72rem;color:#3a3835;letter-spacing:0.08em;text-transform:uppercase;">pool/day</span> '
                f'<span style="font-family:\'Cabinet Grotesk\',sans-serif;font-weight:900;font-size:1.1rem;color:#f0ede8;">${sel["pool_diario"]:,.0f}</span>'
                f'&nbsp;&nbsp;&nbsp;'
                f'<span style="font-size:0.72rem;color:#3a3835;letter-spacing:0.08em;text-transform:uppercase;">makers</span> '
                f'<span style="font-family:\'Cabinet Grotesk\',sans-serif;font-weight:900;font-size:1.1rem;color:#f0ede8;">{sel["num_makers"]}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

        df_hist = make_score_history(selected)
        st.plotly_chart(score_chart(df_hist), use_container_width=True,
                        config={"displayModeBar": False})

        st.markdown(
            f'<div class="page-footer">'
            f'  <span>POLYMARKET REWARDS PIPELINE &nbsp;·&nbsp; Data Engineering Project</span>'
            f'  <span>Last updated {datetime.utcnow().strftime("%H:%M:%S")} UTC &nbsp;·&nbsp;'
            f'  <a href="https://github.com/blackcat112/polymarket-data-pipeline">github.com/blackcat112/polymarket-data-pipeline</a></span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    time.sleep(refresh_interval)
    st.rerun()