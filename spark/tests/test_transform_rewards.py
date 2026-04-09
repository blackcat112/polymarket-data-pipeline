"""Unit tests for spark/jobs/transform_rewards.py.

Uses a local SparkSession (no PostgreSQL, no cluster).
Only pure transformation logic is tested — read/write helpers are excluded
because they require a live JDBC connection.
"""
from __future__ import annotations

import json

import pytest
from pyspark.sql import SparkSession

from spark.jobs.transform_rewards import compute_metrics, parse_raw_json


# ---------------------------------------------------------------------------
# Session fixture — one SparkSession for the whole module
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark() -> SparkSession:  # type: ignore[return]
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-transform-rewards")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raw_row(condition_id: str = "0xabc", num_makers: int = 4, pool: float = 240.0) -> dict:
    """Minimal raw_rewards row with a valid raw_json payload."""
    return {
        "condition_id": condition_id,
        "token_id": "0xtoken",
        "question": "Will BTC hit $100k?",
        "raw_json": json.dumps({
            "condition_id": condition_id,
            "token_id": "0xtoken",
            "question": "Will BTC hit $100k?",
            "total_daily_rate": pool,
            "rewards_min_size": 10.0,
            "max_spread": 0.02,
            "num_makers": num_makers,
            "midpoint": 0.5,
            "yes_price": 0.51,
            "no_price": 0.49,
            "active": True,
        }),
        "fetched_at": "2025-01-01 00:00:00",
    }


# ---------------------------------------------------------------------------
# parse_raw_json
# ---------------------------------------------------------------------------

class TestParseRawJson:
    def test_extracts_pool_diario(self, spark: SparkSession) -> None:
        df_raw = spark.createDataFrame([_raw_row()])
        df = parse_raw_json(df_raw)
        row = df.first()
        assert row["pool_diario"] == 240.0

    def test_extracts_num_makers(self, spark: SparkSession) -> None:
        df_raw = spark.createDataFrame([_raw_row(num_makers=7)])
        df = parse_raw_json(df_raw)
        assert df.first()["num_makers"] == 7

    def test_drops_raw_json_column(self, spark: SparkSession) -> None:
        df_raw = spark.createDataFrame([_raw_row()])
        df = parse_raw_json(df_raw)
        assert "raw_json" not in df.columns


# ---------------------------------------------------------------------------
# compute_metrics
# ---------------------------------------------------------------------------

class TestComputeMetrics:
    def test_score_per_maker_correct(self, spark: SparkSession) -> None:
        df_raw = spark.createDataFrame([_raw_row(num_makers=4, pool=240.0)])
        df = compute_metrics(parse_raw_json(df_raw))
        # score_per_maker = pool_diario / num_makers = 240 / 4 = 60
        assert df.first()["score_per_maker"] == pytest.approx(60.0)

    def test_roi_1h_usdc_is_score_divided_by_24(self, spark: SparkSession) -> None:
        df_raw = spark.createDataFrame([_raw_row(num_makers=4, pool=240.0)])
        df = compute_metrics(parse_raw_json(df_raw))
        # roi_1h = 60 / 24 = 2.5
        assert df.first()["roi_1h_usdc"] == pytest.approx(2.5)

    def test_num_makers_zero_doesnt_divide_by_zero(self, spark: SparkSession) -> None:
        df_raw = spark.createDataFrame([_raw_row(num_makers=0, pool=120.0)])
        df = compute_metrics(parse_raw_json(df_raw))
        # greatest(0, 1.0) = 1.0 → score = 120
        assert df.first()["score_per_maker"] == pytest.approx(120.0)

    def test_competencia_rank_column_exists(self, spark: SparkSession) -> None:
        df_raw = spark.createDataFrame([_raw_row()])
        df = compute_metrics(parse_raw_json(df_raw))
        assert "competencia_rank" in df.columns
