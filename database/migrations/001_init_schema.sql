-- Bronze layer: raw API responses
CREATE TABLE IF NOT EXISTS raw_markets (
    id          BIGSERIAL PRIMARY KEY,
    market_id   TEXT        NOT NULL,
    question    TEXT        NOT NULL,
    raw_json    JSONB       NOT NULL,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_raw_market_snapshot UNIQUE (market_id, fetched_at)
);

CREATE INDEX IF NOT EXISTS idx_raw_markets_market_id  ON raw_markets (market_id);
CREATE INDEX IF NOT EXISTS idx_raw_markets_fetched_at ON raw_markets (fetched_at DESC);

-- Silver layer: processed and enriched
CREATE TABLE IF NOT EXISTS silver_markets (
    id              BIGSERIAL PRIMARY KEY,
    market_id       TEXT        NOT NULL,
    question        TEXT        NOT NULL,
    avg_price       NUMERIC(10, 6),
    price_volatility NUMERIC(10, 6),
    volume_24h      NUMERIC(18, 2),
    volume_avg      NUMERIC(18, 2),
    z_score         NUMERIC(10, 4),
    is_anomaly      BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_markets_market_id    ON silver_markets (market_id);
CREATE INDEX IF NOT EXISTS idx_silver_markets_processed_at ON silver_markets (processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_silver_markets_anomaly      ON silver_markets (is_anomaly) WHERE is_anomaly = TRUE;

-- Gold layer: aggregated metrics for the dashboard
CREATE TABLE IF NOT EXISTS metrics_aggregated (
    id              BIGSERIAL PRIMARY KEY,
    market_id       TEXT        NOT NULL,
    question        TEXT        NOT NULL,
    avg_price       NUMERIC(10, 6),
    price_volatility NUMERIC(10, 6),
    volume_24h      NUMERIC(18, 2),
    anomaly_count   INTEGER     NOT NULL DEFAULT 0,
    last_updated    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_metrics_market UNIQUE (market_id)
);