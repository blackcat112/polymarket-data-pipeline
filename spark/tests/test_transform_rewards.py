"""Unit tests for spark/jobs/transform_rewards.py.

Uses a local SparkSession (no PostgreSQL, no cluster).
Only pure transformation logic is tested — read/write helpers are excluded
because they require a live JDBC connection.
"""
from __future__ import annotations

import json
from collections.abc import Generator

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from spark.jobs.transform_rewards import build_opportunities, compute_metrics, parse_raw_json


# ---------------------------------------------------------------------------
# Session fixture — one SparkSession for the whole module
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
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

def _raw_row(
    condition_id: str = "0xabc",
    num_makers: int = 4,
    pool: float = 240.0,
    fetched_at: str = "2025-01-01 00:00:00",
) -> dict:
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
        "fetched_at": fetched_at,
    }


def _pipeline(spark: SparkSession, rows: list[dict]):
    """Run the full parse → compute_metrics pipeline on a list of rows."""
    return (
        spark.createDataFrame(rows)
        .transform(parse_raw_json)
        .transform(compute_metrics)
    )


def _first(spark: SparkSession, data: dict) -> Row:
    """Return the first Row of a single-row DataFrame, guaranteed non-None."""
    row = spark.createDataFrame([data]).transform(parse_raw_json).first()
    assert row is not None
    return row


# ---------------------------------------------------------------------------
# parse_raw_json
# ---------------------------------------------------------------------------

class TestParseRawJson:
    def test_extracts_pool_diario(self, spark: SparkSession) -> None:
        row = _first(spark, _raw_row())
        assert row["pool_diario"] == 240.0

    def test_extracts_num_makers(self, spark: SparkSession) -> None:
        row = _first(spark, _raw_row(num_makers=7))
        assert row["num_makers"] == 7

    def test_drops_raw_json_column(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_raw_row()]).transform(parse_raw_json)
        assert "raw_json" not in df.columns

    def test_null_pool_parsed_as_none(self, spark: SparkSession) -> None:
        """If total_daily_rate is missing from JSON, pool_diario should be None."""
        row_data = _raw_row()
        payload = json.loads(row_data["raw_json"])
        del payload["total_daily_rate"]
        row_data["raw_json"] = json.dumps(payload)
        row = _first(spark, row_data)
        assert row["pool_diario"] is None

    def test_null_num_makers_parsed_as_none(self, spark: SparkSession) -> None:
        """If num_makers is missing from JSON, num_makers column should be None."""
        row_data = _raw_row()
        payload = json.loads(row_data["raw_json"])
        del payload["num_makers"]
        row_data["raw_json"] = json.dumps(payload)
        row = _first(spark, row_data)
        assert row["num_makers"] is None


# ---------------------------------------------------------------------------
# compute_metrics
# ---------------------------------------------------------------------------

class TestComputeMetrics:
    def _compute(self, spark: SparkSession, data: dict) -> Row:
        df = spark.createDataFrame([data]).transform(parse_raw_json).transform(compute_metrics)
        row = df.first()
        assert row is not None
        return row

    def test_score_per_maker_correct(self, spark: SparkSession) -> None:
        row = self._compute(spark, _raw_row(num_makers=4, pool=240.0))
        assert row["score_per_maker"] == pytest.approx(60.0)

    def test_roi_1h_usdc_is_score_divided_by_24(self, spark: SparkSession) -> None:
        row = self._compute(spark, _raw_row(num_makers=4, pool=240.0))
        assert row["roi_1h_usdc"] == pytest.approx(2.5)

    def test_num_makers_zero_doesnt_divide_by_zero(self, spark: SparkSession) -> None:
        row = self._compute(spark, _raw_row(num_makers=0, pool=120.0))
        assert row["score_per_maker"] == pytest.approx(120.0)

    def test_competencia_rank_column_exists(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([_raw_row()]).transform(parse_raw_json).transform(compute_metrics)
        assert "competencia_rank" in df.columns


# ---------------------------------------------------------------------------
# build_opportunities
# ---------------------------------------------------------------------------

class TestBuildOpportunities:
    """Tests for build_opportunities — the gold layer builder.

    build_opportunities expects a DataFrame already processed by
    parse_raw_json + compute_metrics, so we use _pipeline() as setup.
    """

    def test_returns_one_row_per_condition_id(self, spark: SparkSession) -> None:
        """Multiple snapshots of the same market collapse to a single gold row."""
        rows = [
            _raw_row(condition_id="0xabc", fetched_at="2025-01-01 00:00:00"),
            _raw_row(condition_id="0xabc", fetched_at="2025-01-01 01:00:00"),
            _raw_row(condition_id="0xabc", fetched_at="2025-01-01 02:00:00"),
        ]
        df = _pipeline(spark, rows)
        gold = build_opportunities(df)
        assert gold.count() == 1

    def test_latest_snapshot_wins_for_pool_diario(self, spark: SparkSession) -> None:
        """The gold row must reflect the most recent fetched_at snapshot."""
        rows = [
            _raw_row(condition_id="0xabc", pool=100.0, fetched_at="2025-01-01 00:00:00"),
            _raw_row(condition_id="0xabc", pool=999.0, fetched_at="2025-01-01 06:00:00"),
        ]
        df = _pipeline(spark, rows)
        gold = build_opportunities(df)
        row = gold.first()
        assert row is not None
        assert row["pool_diario"] == pytest.approx(999.0)

    def test_avg_score_7d_is_computed(self, spark: SparkSession) -> None:
        """avg_score_7d must equal the mean of score_per_maker across all rows.

        With identical rows: score_per_maker = 240/4 = 60 → avg_score_7d = 60.
        """
        rows = [
            _raw_row(condition_id="0xabc", pool=240.0, num_makers=4,
                     fetched_at="2025-01-01 00:00:00"),
            _raw_row(condition_id="0xabc", pool=240.0, num_makers=4,
                     fetched_at="2025-01-01 01:00:00"),
        ]
        df = _pipeline(spark, rows)
        gold = build_opportunities(df)
        row = gold.filter(F.col("condition_id") == "0xabc").first()
        assert row is not None
        assert row["avg_score_7d"] == pytest.approx(60.0)

    def test_last_updated_column_exists(self, spark: SparkSession) -> None:
        """Gold layer must include a last_updated timestamp column."""
        df = _pipeline(spark, [_raw_row()])
        gold = build_opportunities(df)
        assert "last_updated" in gold.columns

    def test_multiple_markets_produce_multiple_rows(self, spark: SparkSession) -> None:
        """Each distinct condition_id produces exactly one gold row."""
        rows = [
            _raw_row(condition_id="0xaaa", fetched_at="2025-01-01 00:00:00"),
            _raw_row(condition_id="0xbbb", fetched_at="2025-01-01 00:00:00"),
            _raw_row(condition_id="0xccc", fetched_at="2025-01-01 00:00:00"),
        ]
        df = _pipeline(spark, rows)
        gold = build_opportunities(df)
        assert gold.count() == 3
