-- =============================================================================
-- Polymarket Data Pipeline -- Database Schema
-- Medallion architecture: raw (bronze) -> silver -> metrics_aggregated (gold)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- BRONZE LAYER: raw_markets
-- Append-only. Stores the original API response as-is.
-- Never modified after insert. Source of truth for all reprocessing.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_markets (
    id          BIGSERIAL     PRIMARY KEY,
    market_id   TEXT          NOT NULL,
    question    TEXT          NOT NULL,
    raw_json    TEXT          NOT NULL,  -- TEXT, not JSONB: Spark writes plain strings
    fetched_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_raw_market_snapshot UNIQUE (market_id, fetched_at)
);

CREATE INDEX IF NOT EXISTS idx_raw_markets_market_id
    ON raw_markets (market_id);

CREATE INDEX IF NOT EXISTS idx_raw_markets_fetched_at
    ON raw_markets (fetched_at DESC);


-- -----------------------------------------------------------------------------
-- SILVER LAYER: silver_markets
-- Cleaned and enriched. Written by the PySpark job (mode=overwrite).
-- Column names must match exactly what transform_markets.py writes.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_markets (
    id               BIGSERIAL     PRIMARY KEY,
    market_id        TEXT          NOT NULL,
    question         TEXT          NOT NULL,
    fetched_at       TIMESTAMPTZ   NOT NULL,
    mid_price        NUMERIC(10,6) NOT NULL,
    avg_price        NUMERIC(10,6),
    price_volatility NUMERIC(10,6),
    volume_24h       NUMERIC(20,2),
    avg_volume_24h   NUMERIC(20,2),
    zscore           NUMERIC(10,4),   -- matches Spark column name exactly
    is_anomaly       BOOLEAN       NOT NULL DEFAULT FALSE,
    processed_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_markets_market_id
    ON silver_markets (market_id);

CREATE INDEX IF NOT EXISTS idx_silver_markets_processed_at
    ON silver_markets (processed_at DESC);

-- Partial index: only indexes the rows that matter for anomaly queries
-- instead of the full table. Speeds up the dashboard anomaly alert query.
CREATE INDEX IF NOT EXISTS idx_silver_markets_anomaly
    ON silver_markets (market_id, processed_at DESC)
    WHERE is_anomaly = TRUE;


-- -----------------------------------------------------------------------------
-- GOLD LAYER: metrics_aggregated
-- Query-optimised for the Streamlit dashboard.
-- Written by Airflow task 3 (load_metrics) via upsert on market_id.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS metrics_aggregated (
    id               BIGSERIAL     PRIMARY KEY,
    market_id        TEXT          NOT NULL,
    question         TEXT          NOT NULL,
    avg_price        NUMERIC(10,6),
    price_volatility NUMERIC(10,6),
    volume_24h       NUMERIC(20,2),
    anomaly_count    INTEGER       NOT NULL DEFAULT 0,
    last_updated     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_metrics_market UNIQUE (market_id)  -- required for ON CONFLICT in DAG
);

CREATE INDEX IF NOT EXISTS idx_metrics_last_updated
    ON metrics_aggregated (last_updated DESC);