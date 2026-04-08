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


@pytest.fixture
def sample_df(spark: SparkSession):
    """
    mkt1: 8 rows with tight volume (~1000) + 1 extreme outlier (9999).
    With 9 rows, std stays low enough that 9999 exceeds z-score threshold of 2.0.
    mkt2: 6 rows with stable volume (~500) — no anomaly expected.
    """
    schema = StructType([
        StructField("market_id",  StringType(),    False),
        StructField("question",   StringType(),    False),
        StructField("price_yes",  DoubleType(),    True),
        StructField("volume_24h", DoubleType(),    True),
        StructField("fetched_at", TimestampType(), False),
    ])
    rows = [
        # mkt1 — tight cluster + outlier
        ("mkt1", "Will X happen?", 0.60, 1000.0, datetime(2025, 1, 1,  0, 0)),
        ("mkt1", "Will X happen?", 0.61, 1050.0, datetime(2025, 1, 1,  1, 0)),
        ("mkt1", "Will X happen?", 0.59,  980.0, datetime(2025, 1, 1,  2, 0)),
        ("mkt1", "Will X happen?", 0.60, 1020.0, datetime(2025, 1, 1,  3, 0)),
        ("mkt1", "Will X happen?", 0.62, 1010.0, datetime(2025, 1, 1,  4, 0)),
        ("mkt1", "Will X happen?", 0.58,  990.0, datetime(2025, 1, 1,  5, 0)),
        ("mkt1", "Will X happen?", 0.61, 1030.0, datetime(2025, 1, 1,  6, 0)),
        ("mkt1", "Will X happen?", 0.60, 1005.0, datetime(2025, 1, 1,  7, 0)),
        ("mkt1", "Will X happen?", 0.90, 9999.0, datetime(2025, 1, 1,  8, 0)),  # outlier
        # mkt2 — stable, no anomaly
        ("mkt2", "Will Y happen?", 0.30,  500.0, datetime(2025, 1, 1,  0, 0)),
        ("mkt2", "Will Y happen?", 0.30,  505.0, datetime(2025, 1, 1,  1, 0)),
        ("mkt2", "Will Y happen?", 0.31,  498.0, datetime(2025, 1, 1,  2, 0)),
        ("mkt2", "Will Y happen?", 0.29,  510.0, datetime(2025, 1, 1,  3, 0)),
        ("mkt2", "Will Y happen?", 0.30,  502.0, datetime(2025, 1, 1,  4, 0)),
        ("mkt2", "Will Y happen?", 0.30,  497.0, datetime(2025, 1, 1,  5, 0)),
    ]
    return spark.createDataFrame(data)

def test_compute_zscore_flags_outlier(sample_df) -> None:
    """9999.0 volume should exceed z-score threshold given tight surrounding distribution."""
    result = compute_zscore(sample_df)
    anomalies = result.filter(result.is_anomaly).collect()
    assert any(row.volume_24h == 9999.0 for row in anomalies)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_compute_zscore_no_anomaly_for_stable_market(sample_df) -> None:
    """mkt2 has a stable volume distribution — no rows should be flagged."""
    result = compute_zscore(sample_df)
    mkt2_anomalies = (
        result.filter(
            (result.market_id == "mkt2") & result.is_anomaly
        )
        .collect()
    )
    assert len(mkt2_anomalies) == 0


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