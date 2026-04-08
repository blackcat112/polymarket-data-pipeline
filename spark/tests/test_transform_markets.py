from __future__ import annotations

import math
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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
    Shared SparkSession for all tests.
    scope='session' arranca Spark una sola vez para todos los tests
    en lugar de una vez por test (~10s de overhead cada vez).
    """
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-transform-markets")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def raw_df(spark: SparkSession):
    """
    DataFrame que simula raw_markets: tiene columna raw_json como string.
    Usado para testear parse_json_column, compute_volatility, flag_anomalies.
    """
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


@pytest.fixture
def parsed_df(raw_df):
    """DataFrame ya parseado. Evita repetir parse_json_column en cada test."""
    return parse_json_column(raw_df)


@pytest.fixture
def anomaly_df(spark: SparkSession):
    """
    DataFrame con columnas ya tipadas para testear flag_anomalies directamente.
    mkt1: 8 registros con precio estable + 1 outlier claro (0.90 vs ~0.60).
    mkt2: 6 registros estables, sin anomalía esperada.
    """
    schema = StructType([
        StructField("market_id",   StringType(),    False),
        StructField("question",    StringType(),    False),
        StructField("mid_price",   DoubleType(),    True),
        StructField("volume_24h",  DoubleType(),    True),
        StructField("avg_price",   DoubleType(),    True),
        StructField("avg_volume_24h", DoubleType(), True),
        StructField("fetched_at",  TimestampType(), False),
        StructField("price_yes",   DoubleType(),    True),
        StructField("price_no",    DoubleType(),    True),
        StructField("price_volatility", DoubleType(), True),
    ])
    rows = [
        ("mkt1", "Will X happen?", 0.60, 1000.0, 0.61, 1010.0, datetime(2025, 1, 1, 0, 0),  0.60, 0.40, 0.01),
        ("mkt1", "Will X happen?", 0.61, 1050.0, 0.61, 1010.0, datetime(2025, 1, 1, 1, 0),  0.61, 0.39, 0.01),
        ("mkt1", "Will X happen?", 0.59,  980.0, 0.61, 1010.0, datetime(2025, 1, 1, 2, 0),  0.59, 0.41, 0.01),
        ("mkt1", "Will X happen?", 0.60, 1020.0, 0.61, 1010.0, datetime(2025, 1, 1, 3, 0),  0.60, 0.40, 0.01),
        ("mkt1", "Will X happen?", 0.62, 1010.0, 0.61, 1010.0, datetime(2025, 1, 1, 4, 0),  0.62, 0.38, 0.01),
        ("mkt1", "Will X happen?", 0.58,  990.0, 0.61, 1010.0, datetime(2025, 1, 1, 5, 0),  0.58, 0.42, 0.01),
        ("mkt1", "Will X happen?", 0.61, 1030.0, 0.61, 1010.0, datetime(2025, 1, 1, 6, 0),  0.61, 0.39, 0.01),
        ("mkt1", "Will X happen?", 0.60, 1005.0, 0.61, 1010.0, datetime(2025, 1, 1, 7, 0),  0.60, 0.40, 0.01),
        ("mkt1", "Will X happen?", 0.90, 9999.0, 0.61, 1010.0, datetime(2025, 1, 1, 8, 0),  0.90, 0.10, 0.01),  # outlier
        ("mkt2", "Will Y happen?", 0.30,  500.0, 0.30,  502.0, datetime(2025, 1, 1, 0, 0),  0.30, 0.70, 0.005),
        ("mkt2", "Will Y happen?", 0.30,  505.0, 0.30,  502.0, datetime(2025, 1, 1, 1, 0),  0.30, 0.70, 0.005),
        ("mkt2", "Will Y happen?", 0.31,  498.0, 0.30,  502.0, datetime(2025, 1, 1, 2, 0),  0.31, 0.69, 0.005),
        ("mkt2", "Will Y happen?", 0.29,  510.0, 0.30,  502.0, datetime(2025, 1, 1, 3, 0),  0.29, 0.71, 0.005),
        ("mkt2", "Will Y happen?", 0.30,  502.0, 0.30,  502.0, datetime(2025, 1, 1, 4, 0),  0.30, 0.70, 0.005),
        ("mkt2", "Will Y happen?", 0.30,  497.0, 0.30,  502.0, datetime(2025, 1, 1, 5, 0),  0.30, 0.70, 0.005),
    ]
    return spark.createDataFrame(rows, schema)


# ---------------------------------------------------------------------------
# Tests: parse_json_column
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


# ---------------------------------------------------------------------------
# Tests: compute_volatility
# ---------------------------------------------------------------------------

def test_compute_volatility_adds_column(parsed_df):
    result = compute_volatility(parsed_df)
    assert "price_volatility" in result.columns


def test_compute_volatility_partitioned_by_market(parsed_df):
    """market-B tiene un solo registro: volatilidad debe ser null o 0."""
    result = compute_volatility(parsed_df)
    market_b = result.filter(F.col("market_id") == "market-B").collect()
    for row in market_b:
        assert row.price_volatility is None or row.price_volatility == 0.0


# ---------------------------------------------------------------------------
# Tests: flag_anomalies
# ---------------------------------------------------------------------------

def test_flag_anomalies_adds_columns(anomaly_df):
    result = flag_anomalies(anomaly_df)
    assert "is_anomaly" in result.columns
    assert "zscore" in result.columns


def test_flag_anomalies_detects_outlier(anomaly_df):
    """El registro con mid_price=0.90 debe ser detectado como anomalía."""
    result = flag_anomalies(anomaly_df)
    anomalies = result.filter(F.col("is_anomaly")).collect()
    assert any(row.mid_price == 0.90 for row in anomalies)


def test_flag_anomalies_stable_market_has_no_anomaly(anomaly_df):
    """mkt2 tiene distribución estable: ningún registro debe ser flaggeado."""
    result = flag_anomalies(anomaly_df)
    mkt2_anomalies = result.filter(
        (F.col("market_id") == "mkt2") & F.col("is_anomaly")
    ).collect()
    assert len(mkt2_anomalies) == 0


def test_flag_anomalies_no_division_by_zero(spark):
    """Un mercado con un solo registro no debe producir NaN en zscore."""
    data = [
        {
            "market_id": "single",
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
    assert not math.isnan(row.zscore)


# ---------------------------------------------------------------------------
# Tests: transform (pipeline completo)
# ---------------------------------------------------------------------------

def test_transform_adds_processed_at(raw_df):
    result = transform(raw_df)
    assert "processed_at" in result.columns


def test_transform_preserves_row_count(raw_df):
    """Las transformaciones de ventana no deben eliminar filas."""
    assert transform(raw_df).count() == raw_df.count()