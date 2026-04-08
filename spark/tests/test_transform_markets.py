from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.jobs.transform_markets import (
    compute_volatility,
    compute_volume_avg,
    flag_anomalies,
    parse_json_column,
    transform,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Shared SparkSession for all tests in this module.
    scope='session' means Spark starts once and is reused,
    avoiding the ~10s startup cost per test.
    """
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-transform-markets")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture()
def raw_df(spark: SparkSession):
    """Minimal raw_markets DataFrame that mirrors the PostgreSQL table schema."""
    data = [
        {
            "market_id": "market-A",
            "question": "Will X happen?",
            "fetched_at": "2025-01-01 00:00:00",
            "raw_json": '{"volume24hr": "5000.0", "tokens": [{"price": "0.60"}, {"price": "0.40"}]}',
        },
        {
            "market_id": "market-A",
            "question": "Will X happen?",
            "fetched_at": "2025-01-01 01:00:00",
            "raw_json": '{"volume24hr": "6000.0", "tokens": [{"price": "0.65"}, {"price": "0.35"}]}',
        },
        {
            "market_id": "market-B",
            "question": "Will Y happen?",
            "fetched_at": "2025-01-01 00:00:00",
            "raw_json": '{"volume24hr": "1000.0", "tokens": [{"price": "0.20"}, {"price": "0.80"}]}',
        },
    ]
    return spark.createDataFrame(data)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_parse_json_column_extracts_fields(raw_df):
    result = parse_json_column(raw_df)
    assert "mid_price" in result.columns
    assert "price_yes" in result.columns
    assert "volume_24h" in result.columns


def test_parse_json_column_mid_price_range(raw_df):
    result = parse_json_column(raw_df)
    prices = [row.mid_price for row in result.collect()]
    for price in prices:
        assert 0.0 <= price <= 1.0, f"mid_price {price} out of expected [0, 1] range"


def test_compute_volatility_adds_column(raw_df):
    parsed = parse_json_column(raw_df)
    result = compute_volatility(parsed)
    assert "price_volatility" in result.columns


def test_compute_volatility_partitioned_by_market(raw_df):
    """Each market must have its own volatility, not a global one."""
    parsed = parse_json_column(raw_df)
    result = compute_volatility(parsed)
    market_a = result.filter(F.col("market_id") == "market-A").collect()
    market_b = result.filter(F.col("market_id") == "market-B").collect()
    # Market B has only one record: volatility should be null or 0
    for row in market_b:
        assert row.price_volatility is None or row.price_volatility == 0.0


def test_flag_anomalies_adds_column(raw_df):
    parsed = parse_json_column(raw_df)
    vol = compute_volatility(parsed)
    avg = compute_volume_avg(vol)
    result = flag_anomalies(avg)
    assert "is_anomaly" in result.columns
    assert "zscore" in result.columns


def test_flag_anomalies_no_division_by_zero(spark):
    """A market with a single record must not produce NaN z-score."""
    data = [
        {
            "market_id": "single-record",
            "question": "Lone market?",
            "fetched_at": "2025-01-01 00:00:00",
            "raw_json": '{"volume24hr": "100.0", "tokens": [{"price": "0.50"}, {"price": "0.50"}]}',
        }
    ]
    df = spark.createDataFrame(data)
    parsed = parse_json_column(df)
    vol = compute_volatility(parsed)
    avg = compute_volume_avg(vol)
    result = flag_anomalies(avg)
    row = result.collect()[0]
    assert row.zscore is not None
    import math
    assert not math.isnan(row.zscore)


def test_transform_returns_processed_at(raw_df):
    """Full pipeline run must add processed_at timestamp column."""
    result = transform(raw_df)
    assert "processed_at" in result.columns


def test_transform_row_count_preserved(raw_df):
    """Transform must not drop rows."""
    original_count = raw_df.count()
    result = transform(raw_df)
    assert result.count() == original_count