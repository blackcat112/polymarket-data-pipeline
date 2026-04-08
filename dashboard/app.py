from __future__ import annotations

import time

import streamlit as st

from dashboard.components import (
    render_anomaly_table,
    render_kpi_row,
    render_price_history,
    render_top_markets_chart,
)
from dashboard.db import (
    fetch_active_markets,
    fetch_anomalies,
    fetch_price_history,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Polymarket Data Pipeline",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("⚙️ Controls")
refresh_interval = st.sidebar.slider(
    "Auto-refresh every (seconds)", min_value=30, max_value=300, value=60, step=30
)
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**Stack:** Python · PySpark · Airflow · PostgreSQL · Streamlit"
)
st.sidebar.markdown(
    "[GitHub](https://github.com/blackcat112/polymarket-data-pipeline)",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

st.title("📊 Polymarket Data Pipeline — Dashboard")
st.caption("Real-time metrics from the prediction markets pipeline.")

placeholder = st.empty()

while True:
    with placeholder.container():
        # --- Load data ---
        try:
            df_markets = fetch_active_markets()
            df_anomalies = fetch_anomalies()
            data_ok = True
        except Exception as e:
            st.error(f"Database connection error: {e}")
            data_ok = False

        if data_ok:
            # --- KPIs ---
            render_kpi_row(df_markets)
            st.markdown("---")

            # --- Top markets ---
            col_left, col_right = st.columns([2, 1])
            with col_left:
                render_top_markets_chart(df_markets)
            with col_right:
                render_anomaly_table(df_anomalies)

            st.markdown("---")

            # --- Price history selector ---
            if not df_markets.empty:
                selected = st.selectbox(
                    "Select a market to inspect price history:",
                    options=df_markets["market_id"].tolist(),
                    format_func=lambda mid: df_markets.loc[
                        df_markets["market_id"] == mid, "question"
                    ].values[0],
                )
                df_history = fetch_price_history(selected)
                render_price_history(df_history, selected)

    time.sleep(refresh_interval)
    st.rerun()