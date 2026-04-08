from __future__ import annotations

import os
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "polymarket"),
        user=os.environ.get("POSTGRES_USER", "pipeline"),
        password=os.environ.get("POSTGRES_PASSWORD", "changeme"),
    )


def fetch_active_markets() -> pd.DataFrame:
    """Top 10 markets by average volume in the last 24h."""
    sql = """
        SELECT
            market_id,
            question,
            ROUND(avg_volume_24h::numeric, 2)  AS avg_volume_24h,
            ROUND(avg_price::numeric, 4)        AS avg_price,
            MAX(processed_at)                   AS last_updated
        FROM silver_markets
        WHERE processed_at >= NOW() - INTERVAL '24 hours'
        GROUP BY market_id, question, avg_volume_24h, avg_price
        ORDER BY avg_volume_24h DESC
        LIMIT 10;
    """
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def fetch_price_history(market_id: str) -> pd.DataFrame:
    """Mid-price time series for a given market."""
    sql = """
        SELECT
            fetched_at,
            mid_price,
            price_volatility,
            zscore,
            is_anomaly
        FROM silver_markets
        WHERE market_id = %(market_id)s
        ORDER BY fetched_at ASC;
    """
    with get_connection() as conn:
        return pd.read_sql(sql, conn, params={"market_id": market_id})


def fetch_anomalies() -> pd.DataFrame:
    """All flagged anomalies from the last 24h."""
    sql = """
        SELECT
            market_id,
            question,
            fetched_at,
            ROUND(mid_price::numeric, 4)  AS mid_price,
            ROUND(zscore::numeric, 2)     AS zscore
        FROM silver_markets
        WHERE is_anomaly = TRUE
          AND processed_at >= NOW() - INTERVAL '24 hours'
        ORDER BY ABS(zscore) DESC
        LIMIT 20;
    """
    with get_connection() as conn:
        return pd.read_sql(sql, conn)


def fetch_aggregated_metrics() -> pd.DataFrame:
    """Latest snapshot from the gold metrics table."""
    sql = """
        SELECT
            market_id,
            question,
            ROUND(total_volume::numeric, 2)    AS total_volume,
            ROUND(avg_mid_price::numeric, 4)   AS avg_mid_price,
            anomaly_count,
            computed_at
        FROM metrics_aggregated
        ORDER BY total_volume DESC
        LIMIT 20;
    """
    with get_connection() as conn:
        return pd.read_sql(sql, conn)