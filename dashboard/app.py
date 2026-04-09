"""Streamlit dashboard — Polymarket Rewards Pipeline.

Design language: trading terminal meets editorial magazine.
- Dark surface, near-black background
- Clash Display (numbers/headings) + Satoshi (body)
- Single teal accent: #00D4AA
- Left-aligned, asymmetric 2-col layout
- CSS grain overlay for texture
- Animated metric counters + staggered card reveals
"""
from __future__ import annotations

import os
import asyncio

import asyncpg
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Rewards Pipeline — Polymarket",
    page_icon="■",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# CSS — design system
# ---------------------------------------------------------------------------

st.markdown("""
<style>
/* ---- Fonts ---- */
@import url('https://api.fontshare.com/v2/css?f[]=clash-display@400,500,600,700&f[]=satoshi@300,400,500,700&display=swap');

/* ---- Reset & base ---- */
html, body, [class*="css"] {
    font-family: 'Satoshi', 'Helvetica Neue', sans-serif;
    background-color: #0d0d0d;
    color: #e8e6e1;
    -webkit-font-smoothing: antialiased;
}

/* ---- Grain overlay via SVG feTurbulence ---- */
body::before {
    content: '';
    position: fixed;
    inset: 0;
    z-index: 0;
    pointer-events: none;
    opacity: 0.04;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='300' height='300'%3E%3Cfilter id='grain'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='300' height='300' filter='url(%23grain)'/%3E%3C/svg%3E");
}

/* ---- Hide default Streamlit chrome ---- */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 2rem 2.5rem 4rem; max-width: 1400px; }

/* ---- Typography ---- */
h1, h2, h3, .clash {
    font-family: 'Clash Display', 'Helvetica Neue', sans-serif;
    letter-spacing: -0.03em;
    line-height: 1.1;
}

/* ---- Divider ---- */
.divider {
    border: none;
    border-top: 1px solid #1f1f1f;
    margin: 1.5rem 0;
}

/* ---- Metric card ---- */
.metric-card {
    background: #131313;
    border: 1px solid #1e1e1e;
    border-radius: 6px;
    padding: 1.25rem 1.5rem;
    animation: fadeUp 0.4s ease both;
}
.metric-card:nth-child(2) { animation-delay: 0.08s; }
.metric-card:nth-child(3) { animation-delay: 0.16s; }
.metric-card:nth-child(4) { animation-delay: 0.24s; }

.metric-label {
    font-size: 0.72rem;
    font-weight: 500;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #555;
    margin-bottom: 0.4rem;
}

.metric-value {
    font-family: 'Clash Display', monospace;
    font-size: clamp(1.6rem, 2.5vw, 2.2rem);
    font-weight: 600;
    color: #e8e6e1;
    line-height: 1;
}

.metric-sub {
    font-size: 0.75rem;
    color: #3a3a3a;
    margin-top: 0.3rem;
}

/* ---- Market row ---- */
.market-row {
    display: flex;
    align-items: flex-start;
    gap: 1rem;
    padding: 0.9rem 0;
    border-bottom: 1px solid #161616;
    animation: fadeUp 0.35s ease both;
    cursor: default;
    transition: background 180ms ease;
}
.market-row:hover { background: #111; }

.rank-num {
    font-family: 'Clash Display', monospace;
    font-size: 0.7rem;
    font-weight: 600;
    color: #2a2a2a;
    min-width: 1.8rem;
    padding-top: 0.15rem;
}

.market-q {
    font-size: 0.82rem;
    color: #9e9b96;
    line-height: 1.4;
    flex: 1;
}

.market-score {
    font-family: 'Clash Display', monospace;
    font-size: 1rem;
    font-weight: 600;
    color: #00D4AA;
    white-space: nowrap;
}

/* ---- Anomaly badge ---- */
.badge-anomaly {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.68rem;
    font-weight: 500;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #ff6b6b;
    background: rgba(255,107,107,0.08);
    border: 1px solid rgba(255,107,107,0.2);
    border-radius: 3px;
    padding: 0.2rem 0.5rem;
}
.badge-anomaly::before {
    content: '';
    width: 5px; height: 5px;
    border-radius: 50%;
    background: #ff6b6b;
    animation: pulse 1.4s ease infinite;
}
.badge-normal {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.68rem;
    font-weight: 500;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #00D4AA;
    background: rgba(0,212,170,0.06);
    border: 1px solid rgba(0,212,170,0.15);
    border-radius: 3px;
    padding: 0.2rem 0.5rem;
}
.badge-normal::before {
    content: '';
    width: 5px; height: 5px;
    border-radius: 50%;
    background: #00D4AA;
}

/* ---- Section label ---- */
.section-label {
    font-size: 0.68rem;
    font-weight: 500;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #333;
    margin-bottom: 1rem;
}

/* ---- Keyframes ---- */
@keyframes fadeUp {
    from { opacity: 0; transform: translateY(10px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.3; }
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

async def _fetch_data() -> tuple[list[dict], list[dict]]:
    """Fetch opportunities + last 48h silver history from PostgreSQL."""
    conn = await asyncpg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "polymarket"),
        user=os.environ.get("POSTGRES_USER", "polymarket"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )
    try:
        opportunities = await conn.fetch(
            """
            SELECT condition_id, question, score_per_maker, pool_diario,
                   num_makers, avg_score_7d, peak_score_7d,
                   simulated_rewards_24h, simulated_rewards_7d,
                   best_hour_utc, last_updated
            FROM   rewards_opportunities
            ORDER  BY score_per_maker DESC
            LIMIT  20
            """
        )
        history = await conn.fetch(
            """
            SELECT condition_id, question, score_per_maker, num_makers,
                   roi_1h_usdc, competencia_rank, fetched_at
            FROM   silver_rewards
            WHERE  fetched_at >= NOW() - INTERVAL '48 hours'
            ORDER  BY fetched_at ASC
            """
        )
        return [dict(r) for r in opportunities], [dict(r) for r in history]
    finally:
        await conn.close()


@st.cache_data(ttl=60)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Cached wrapper so the dashboard auto-refreshes every 60 seconds."""
    opps, hist = asyncio.run(_fetch_data())
    df_opps = pd.DataFrame(opps) if opps else pd.DataFrame()
    df_hist = pd.DataFrame(hist) if hist else pd.DataFrame()
    return df_opps, df_hist


def detect_anomaly(row: pd.Series) -> bool:
    """Simple z-score anomaly: score > 2 std from the 7-day mean.

    Returns True if the current score is anomalously high —
    meaning competition has dropped sharply (good for the maker bot).
    """
    if pd.isna(row.get("avg_score_7d")) or pd.isna(row.get("peak_score_7d")):
        return False
    std_estimate = (row["peak_score_7d"] - row["avg_score_7d"]) / 3.0
    if std_estimate <= 0:
        return False
    z = (row["score_per_maker"] - row["avg_score_7d"]) / std_estimate
    return float(z) > 2.0


# ---------------------------------------------------------------------------
# Chart builder
# ---------------------------------------------------------------------------

def score_history_chart(df_hist: pd.DataFrame, condition_id: str) -> go.Figure:
    """48h score_per_maker timeline for a single market."""
    df = df_hist[df_hist["condition_id"] == condition_id].copy()

    fig = go.Figure()

    if not df.empty:
        fig.add_trace(go.Scatter(
            x=df["fetched_at"],
            y=df["score_per_maker"],
            mode="lines",
            line=dict(color="#00D4AA", width=1.5, shape="spline"),
            fill="tozeroy",
            fillcolor="rgba(0,212,170,0.04)",
            hovertemplate="%{x|%H:%M}<br>score: %{y:.4f}<extra></extra>",
        ))

    fig.update_layout(
        paper_bgcolor="transparent",
        plot_bgcolor="transparent",
        margin=dict(l=0, r=0, t=8, b=0),
        height=180,
        xaxis=dict(
            showgrid=False, zeroline=False,
            tickfont=dict(family="Satoshi", size=10, color="#333"),
            tickformat="%H:%M",
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="#161616",
            zeroline=False,
            tickfont=dict(family="Satoshi", size=10, color="#333"),
        ),
        hoverlabel=dict(
            bgcolor="#131313",
            bordercolor="#1e1e1e",
            font=dict(family="Satoshi", size=11, color="#e8e6e1"),
        ),
    )
    return fig


def competition_heatmap(df_hist: pd.DataFrame) -> go.Figure:
    """Hour-of-day x market heatmap of avg num_makers (competition level)."""
    if df_hist.empty:
        return go.Figure()

    df = df_hist.copy()
    df["hour"] = pd.to_datetime(df["fetched_at"]).dt.hour
    df["short_q"] = df["question"].str[:35] + "…"

    pivot = (
        df.groupby(["short_q", "hour"])["num_makers"]
        .mean()
        .reset_index()
        .pivot(index="short_q", columns="hour", values="num_makers")
    )

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f"{h:02d}h" for h in pivot.columns],
        y=pivot.index.tolist(),
        colorscale=[[0, "#0d0d0d"], [0.5, "#1a3a33"], [1, "#00D4AA"]],
        showscale=False,
        hovertemplate="%{y}<br>%{x} UTC<br>avg makers: %{z:.1f}<extra></extra>",
    ))

    fig.update_layout(
        paper_bgcolor="transparent",
        plot_bgcolor="transparent",
        margin=dict(l=0, r=0, t=8, b=0),
        height=max(180, len(pivot) * 28),
        xaxis=dict(
            tickfont=dict(family="Satoshi", size=9, color="#333"),
            side="top",
        ),
        yaxis=dict(
            tickfont=dict(family="Satoshi", size=9, color="#555"),
            autorange="reversed",
        ),
        hoverlabel=dict(
            bgcolor="#131313",
            bordercolor="#1e1e1e",
            font=dict(family="Satoshi", size=11, color="#e8e6e1"),
        ),
    )
    return fig


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

def render_header(df_opps: pd.DataFrame) -> None:
    total_markets = len(df_opps)
    top_score = float(df_opps["score_per_maker"].max()) if not df_opps.empty else 0.0
    total_pool = float(df_opps["pool_diario"].sum()) if not df_opps.empty else 0.0
    anomalies = int(df_opps.apply(detect_anomaly, axis=1).sum()) if not df_opps.empty else 0

    st.markdown("""
    <div style='padding: 2.5rem 0 1rem; animation: fadeUp 0.3s ease both;'>
        <div style='font-family:"Clash Display",monospace; font-size:0.68rem;
                    letter-spacing:0.18em; text-transform:uppercase; color:#333;
                    margin-bottom:0.6rem;'>Polymarket — Rewards Pipeline</div>
        <h1 style='font-size: clamp(2rem, 4vw, 3.2rem); font-weight:600;
                   color:#e8e6e1; margin:0; letter-spacing:-0.04em;'>Market<br>
            <span style='color:#00D4AA;'>Opportunities</span></h1>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<hr class="divider">', unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    for col, label, value, sub in [
        (c1, "Active Markets",  f"{total_markets}",       "tracked this cycle"),
        (c2, "Top Score",       f"{top_score:.4f}",       "score / maker / day"),
        (c3, "Total Pool",      f"${total_pool:,.0f}",    "daily rewards USDC"),
        (c4, "Anomalies",       f"{anomalies}",           "z-score > 2σ above 7d mean"),
    ]:
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">{label}</div>
                <div class="metric-value">{value}</div>
                <div class="metric-sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)


def render_main(df_opps: pd.DataFrame, df_hist: pd.DataFrame) -> None:
    col_rank, col_charts = st.columns([5, 7], gap="large")

    with col_rank:
        st.markdown('<div class="section-label">Ranking by score\/maker</div>', unsafe_allow_html=True)

        if df_opps.empty:
            st.markdown("<p style='color:#333; font-size:0.85rem;'>No data yet — run the pipeline first.</p>", unsafe_allow_html=True)
        else:
            for i, row in df_opps.head(12).iterrows():
                is_anomaly = detect_anomaly(row)
                badge = (
                    '<span class="badge-anomaly">anomaly</span>'
                    if is_anomaly
                    else '<span class="badge-normal">normal</span>'
                )
                question_short = (
                    row["question"][:72] + "…"
                    if len(row["question"]) > 72
                    else row["question"]
                )
                rank_display = str(i + 1).zfill(2)  # type: ignore
                pool_str = f"${float(row['pool_diario']):,.0f}/d"
                makers_str = f"{int(row['num_makers'])} makers"

                st.markdown(f"""
                <div class="market-row" style="animation-delay:{i * 0.04:.2f}s">
                    <div class="rank-num">{rank_display}</div>
                    <div style="flex:1">
                        <div class="market-q">{question_short}</div>
                        <div style="display:flex; gap:0.5rem; margin-top:0.35rem; align-items:center;">
                            {badge}
                            <span style="font-size:0.68rem; color:#2a2a2a;">{pool_str} &middot; {makers_str}</span>
                        </div>
                    </div>
                    <div class="market-score">{float(row['score_per_maker']):.4f}</div>
                </div>
                """, unsafe_allow_html=True)

    with col_charts:
        if not df_opps.empty and not df_hist.empty:
            top_id = df_opps.iloc[0]["condition_id"]
            top_q = df_opps.iloc[0]["question"][:55] + "…"

            st.markdown(f'<div class="section-label">48h score timeline — {top_q}</div>', unsafe_allow_html=True)
            st.plotly_chart(
                score_history_chart(df_hist, top_id),
                use_container_width=True,
                config={"displayModeBar": False},
            )

            st.markdown('<hr class="divider">', unsafe_allow_html=True)

            st.markdown('<div class="section-label">Competition heatmap — avg makers by hour (UTC)</div>', unsafe_allow_html=True)
            st.plotly_chart(
                competition_heatmap(df_hist),
                use_container_width=True,
                config={"displayModeBar": False},
            )
        else:
            st.markdown(
                "<p style='color:#333; font-size:0.85rem; padding-top:2rem;'>"
                "Charts appear after the first pipeline run.</p>",
                unsafe_allow_html=True,
            )


def render_footer(df_opps: pd.DataFrame) -> None:
    if df_opps.empty:
        return
    last_updated = df_opps["last_updated"].max()
    last_str = pd.to_datetime(last_updated).strftime("%Y-%m-%d %H:%M UTC") if pd.notna(last_updated) else "unknown"
    st.markdown(
        f"<div style='font-size:0.68rem; color:#252525; margin-top:3rem; "
        f"letter-spacing:0.1em;'>LAST PIPELINE RUN &mdash; {last_str}</div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    df_opps, df_hist = load_data()
    render_header(df_opps)
    render_main(df_opps, df_hist)
    render_footer(df_opps)

    st.markdown(
        "<script>setTimeout(()=>window.location.reload(), 60000)</script>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
