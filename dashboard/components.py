from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def render_kpi_row(df_markets: pd.DataFrame) -> None:
    """Top-level KPI cards: total markets, total volume, anomaly count."""
    col1, col2, col3 = st.columns(3)
    col1.metric("Active markets (24h)", len(df_markets))
    col2.metric(
        "Total volume (24h)",
        f"${df_markets['avg_volume_24h'].sum():,.0f}",
    )
    col3.metric(
        "Avg market price",
        f"{df_markets['avg_price'].mean():.3f}",
    )


def render_top_markets_chart(df: pd.DataFrame) -> None:
    st.subheader("🏆 Most active markets (24h)")
    if df.empty:
        st.info("No data available yet.")
        return
    fig = px.bar(
        df,
        x="avg_volume_24h",
        y="question",
        orientation="h",
        color="avg_volume_24h",
        color_continuous_scale="teal",
        labels={"avg_volume_24h": "Avg Volume 24h", "question": "Market"},
        height=400,
    )
    fig.update_layout(
        coloraxis_showscale=False,
        yaxis={"categoryorder": "total ascending"},
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_price_history(df: pd.DataFrame, market_id: str) -> None:
    st.subheader(f"📈 Price evolution — `{market_id}`")
    if df.empty:
        st.info("No history for this market yet.")
        return

    # Base price line
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["fetched_at"],
        y=df["mid_price"],
        mode="lines",
        name="Mid price",
        line=dict(color="#01696f", width=2),
    ))

    # Highlight anomaly points in red
    anomalies = df[df["is_anomaly"] == True]  # noqa: E712
    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies["fetched_at"],
            y=anomalies["mid_price"],
            mode="markers",
            name="Anomaly",
            marker=dict(color="#a12c7b", size=10, symbol="x"),
        ))

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Mid price",
        legend=dict(orientation="h", y=1.1),
        margin=dict(l=10, r=10, t=30, b=10),
        height=350,
    )
    st.plotly_chart(fig, use_container_width=True)


def render_anomaly_table(df: pd.DataFrame) -> None:
    st.subheader("🚨 Anomaly alerts (24h)")
    if df.empty:
        st.success("No anomalies detected in the last 24 hours.")
        return
    st.dataframe(
        df.style.background_gradient(subset=["zscore"], cmap="RdYlGn_r"),
        use_container_width=True,
        hide_index=True,
    )