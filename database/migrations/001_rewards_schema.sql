-- =============================================================================
-- Polymarket Rewards Pipeline — Database Schema
-- Migration: 001_rewards_schema.sql
--
-- Medallion architecture:
--   raw_rewards (bronze)  → append-only API snapshots
--   silver_rewards        → parsed + enriched by PySpark
--   rewards_opportunities → gold layer, query-optimised for dashboard
--
-- Column names mirror modules/scanner.py exactly:
--   pool_diario, min_size, max_spread, num_makers, score, midpoint
-- =============================================================================


-- -----------------------------------------------------------------------------
-- BRONZE: raw_rewards
-- Append-only. One row per market per fetch cycle.
-- Stores the full API response as TEXT — never modified after insert.
-- Source of truth for all reprocessing.
-- NOTE: raw_json is TEXT (not JSONB) because the Spark JDBC driver writes
--       plain strings. Cast to ::jsonb at query time when needed.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_rewards (
    id            BIGSERIAL    PRIMARY KEY,
    condition_id  TEXT         NOT NULL,
    token_id      TEXT         NOT NULL,
    question      TEXT         NOT NULL,
    raw_json      TEXT         NOT NULL,
    fetched_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_rewards_condition
    ON raw_rewards (condition_id);

CREATE INDEX IF NOT EXISTS idx_raw_rewards_fetched_at
    ON raw_rewards (fetched_at DESC);


-- -----------------------------------------------------------------------------
-- SILVER: silver_rewards
-- Written by PySpark (append mode). One row per market per fetch cycle.
-- Never overwritten — full history preserved for rolling aggregations.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_rewards (
    id               BIGSERIAL     PRIMARY KEY,
    condition_id     TEXT          NOT NULL,
    token_id         TEXT          NOT NULL,
    question         TEXT          NOT NULL,
    -- Raw fields from API (mirrors scanner.py field names exactly)
    pool_diario      NUMERIC(12,4) NOT NULL,  -- total_daily_rate
    min_size         NUMERIC(10,4) NOT NULL,  -- rewards_min_size
    max_spread       NUMERIC(6,4)  NOT NULL,  -- max spread to qualify for rewards
    num_makers       INTEGER       NOT NULL,  -- len(bids) + len(asks) in order book
    midpoint         NUMERIC(10,6) NOT NULL,  -- (best_bid + best_ask) / 2
    yes_price        NUMERIC(10,6),
    no_price         NUMERIC(10,6),
    -- Computed by Spark
    score_per_maker  NUMERIC(14,6) NOT NULL,  -- pool_diario / max(num_makers, 1)
    roi_1h_usdc      NUMERIC(10,4),           -- score_per_maker / 24
    fetched_at       TIMESTAMPTZ   NOT NULL,
    processed_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_rewards_condition
    ON silver_rewards (condition_id);

CREATE INDEX IF NOT EXISTS idx_silver_rewards_fetched_at
    ON silver_rewards (fetched_at DESC);

-- Partial index: only top-scoring rows — used by dashboard ranking query
CREATE INDEX IF NOT EXISTS idx_silver_rewards_score
    ON silver_rewards (score_per_maker DESC)
    WHERE score_per_maker > 0;


-- -----------------------------------------------------------------------------
-- GOLD: rewards_opportunities
-- One row per market, upserted each Airflow run.
-- Powers the Streamlit dashboard directly — no joins needed.
--
-- Tracks simulated P&L: how much the bot WOULD have earned.
-- No real money involved — purely observational.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rewards_opportunities (
    condition_id          TEXT          PRIMARY KEY,
    token_id              TEXT          NOT NULL,
    question              TEXT          NOT NULL,
    -- Latest snapshot values
    pool_diario           NUMERIC(12,4) NOT NULL,
    num_makers            INTEGER       NOT NULL,
    score_per_maker       NUMERIC(14,6) NOT NULL,
    midpoint              NUMERIC(10,6) NOT NULL,
    max_spread            NUMERIC(6,4)  NOT NULL,
    -- 7-day rolling aggregates (computed by Spark over silver_rewards)
    avg_score_7d          NUMERIC(14,6),  -- rolling avg score: stability indicator
    peak_score_7d         NUMERIC(14,6),  -- best score seen: ceiling estimate
    avg_num_makers_7d     NUMERIC(8,2),   -- avg competition over 7 days
    best_hour_utc         SMALLINT,       -- hour (0-23) with lowest avg num_makers
    -- Simulated P&L (observational — no capital required)
    simulated_rewards_24h NUMERIC(12,4),  -- sum of score_per_maker last 24 snapshots
    simulated_rewards_7d  NUMERIC(12,4),  -- sum of score_per_maker last 7 days
    -- Meta
    first_seen_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_updated          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_rewards_opportunity UNIQUE (condition_id)
);

CREATE INDEX IF NOT EXISTS idx_opportunities_score
    ON rewards_opportunities (score_per_maker DESC);

CREATE INDEX IF NOT EXISTS idx_opportunities_updated
    ON rewards_opportunities (last_updated DESC);
