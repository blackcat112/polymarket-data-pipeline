-- =============================================================================
-- Migration 002: Rewards-specific schema
-- Reflects the actual bot logic from modules/scanner.py:
--   score = pool_diario / num_makers  (higher = less competition for same pool)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- BRONZE: raw_rewards
-- Append-only snapshot of /rewards/markets/current each run.
-- Stores full API response as raw_json for full reprocessability.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_rewards (
    id            BIGSERIAL    PRIMARY KEY,
    condition_id  TEXT         NOT NULL,
    token_id      TEXT         NOT NULL,
    question      TEXT         NOT NULL,
    raw_json      TEXT         NOT NULL,   -- full API response, never modified
    fetched_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_rewards_condition  ON raw_rewards (condition_id);
CREATE INDEX IF NOT EXISTS idx_raw_rewards_fetched_at ON raw_rewards (fetched_at DESC);


-- -----------------------------------------------------------------------------
-- SILVER: silver_rewards
-- Written by PySpark. Parsed and enriched from raw_rewards.
-- One row per market per fetch cycle — historical, never overwritten.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_rewards (
    id                BIGSERIAL     PRIMARY KEY,
    condition_id      TEXT          NOT NULL,
    token_id          TEXT          NOT NULL,
    question          TEXT          NOT NULL,
    pool_diario       NUMERIC(12,4) NOT NULL,  -- total_daily_rate from API
    min_size          NUMERIC(10,4) NOT NULL,  -- rewards_min_size
    max_spread        NUMERIC(6,4)  NOT NULL,  -- max spread allowed to qualify
    num_makers        INTEGER       NOT NULL,  -- bids + asks in book
    midpoint          NUMERIC(10,6) NOT NULL,  -- (best_bid + best_ask) / 2
    yes_price         NUMERIC(10,6),
    no_price          NUMERIC(10,6),
    -- Computed by Spark:
    score_per_maker   NUMERIC(14,6) NOT NULL,  -- pool_diario / max(num_makers,1)
    roi_1h_usdc       NUMERIC(10,4),           -- score_per_maker / 24
    competencia_rank  INTEGER,                 -- rank by score DESC within this fetch
    fetched_at        TIMESTAMPTZ   NOT NULL,
    processed_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_rewards_condition   ON silver_rewards (condition_id);
CREATE INDEX IF NOT EXISTS idx_silver_rewards_fetched_at  ON silver_rewards (fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_silver_rewards_score       ON silver_rewards (score_per_maker DESC);


-- -----------------------------------------------------------------------------
-- GOLD: rewards_opportunities
-- One row per market, upserted each run. Powers the dashboard directly.
-- Tracks 7-day simulated P&L: how much the bot WOULD have earned.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rewards_opportunities (
    condition_id        TEXT          PRIMARY KEY,
    token_id            TEXT          NOT NULL,
    question            TEXT          NOT NULL,
    -- Latest snapshot
    pool_diario         NUMERIC(12,4) NOT NULL,
    num_makers          INTEGER       NOT NULL,
    score_per_maker     NUMERIC(14,6) NOT NULL,
    midpoint            NUMERIC(10,6) NOT NULL,
    max_spread          NUMERIC(6,4)  NOT NULL,
    -- Historical aggregates (computed by Spark over silver_rewards)
    avg_score_7d        NUMERIC(14,6),   -- rolling 7-day avg score
    peak_score_7d       NUMERIC(14,6),   -- best score seen in 7 days
    avg_num_makers_7d   NUMERIC(8,2),    -- avg competition over 7 days
    best_hour_utc       SMALLINT,        -- hour with lowest avg num_makers (0-23)
    -- Simulated P&L (no real money — observational only)
    simulated_rewards_7d NUMERIC(12,4), -- sum of score_per_maker over last 7d snapshots
    simulated_rewards_24h NUMERIC(12,4),
    -- Meta
    first_seen_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_updated        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_rewards_opportunity UNIQUE (condition_id)
);

CREATE INDEX IF NOT EXISTS idx_opportunities_score
    ON rewards_opportunities (score_per_maker DESC);
CREATE INDEX IF NOT EXISTS idx_opportunities_updated
    ON rewards_opportunities (last_updated DESC);