import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)
from datetime import datetime

from spark.jobs.transform_markets import (
    compute_zscore,
    compute_volatility,
    compute_volume_avg,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("test-transform")
        .master("local[1]")
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
    return spark.createDataFrame(rows, schema)


def test_compute_zscore_flags_outlier(sample_df) -> None:
    """9999.0 volume should exceed z-score threshold given tight surrounding distribution."""
    result = compute_zscore(sample_df)
    anomalies = result.filter(result.is_anomaly).collect()
    assert any(row.volume_24h == 9999.0 for row in anomalies)


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


def test_compute_volume_avg_returns_column(sample_df) -> None:
    result = compute_volume_avg(sample_df)
    assert "volume_avg" in result.columns


def test_compute_volatility_returns_column(sample_df) -> None:
    result = compute_volatility(sample_df)
    assert "price_volatility" in result.columns
