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
    schema = StructType([
        StructField("market_id",  StringType(),  False),
        StructField("question",   StringType(),  False),
        StructField("price_yes",  DoubleType(),  True),
        StructField("volume_24h", DoubleType(),  True),
        StructField("fetched_at", TimestampType(), False),
    ])
    rows = [
        ("mkt1", "Will X happen?", 0.6, 1000.0, datetime(2025, 1, 1, 0,  0)),
        ("mkt1", "Will X happen?", 0.7, 1200.0, datetime(2025, 1, 1, 1,  0)),
        ("mkt1", "Will X happen?", 0.5, 950.0,  datetime(2025, 1, 1, 2,  0)),
        ("mkt1", "Will X happen?", 0.9, 9999.0, datetime(2025, 1, 1, 3,  0)),
        ("mkt2", "Will Y happen?", 0.3, 500.0,  datetime(2025, 1, 1, 0,  0)),
        ("mkt2", "Will Y happen?", 0.3, 510.0,  datetime(2025, 1, 1, 1,  0)),
    ]
    return spark.createDataFrame(rows, schema)


def test_compute_zscore_flags_outlier(sample_df) -> None:
    result = compute_zscore(sample_df)
    anomalies = result.filter(result.is_anomaly).collect()
    assert any(row.volume_24h == 9999.0 for row in anomalies)


def test_compute_zscore_no_anomaly_for_stable_market(sample_df) -> None:
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
