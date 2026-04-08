from __future__ import annotations

import os

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "polymarket")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
JDBC_DRIVER = "org.postgresql.Driver"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": JDBC_DRIVER,
}

VOLATILITY_WINDOW_ROWS = 20
ZSCORE_THRESHOLD = 2.0


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("polymarket-transform")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

def read_raw_markets(spark: SparkSession) -> DataFrame:
    """Read all raw records from the bronze layer."""
    return spark.read.jdbc(
        url=JDBC_URL,
        table="raw_markets",
        properties=JDBC_PROPERTIES,
    )


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

def parse_json_column(df: DataFrame) -> DataFrame:
    """
    Expand the raw_json string column into typed fields.
    The JSON structure mirrors the Polymarket CLOB /markets response.
    """
    return df.select(
        F.col("market_id"),
        F.col("question"),
        F.col("fetched_at"),
        F.get_json_object("raw_json", "$.volume24hr").cast("double").alias("volume_24h"),
        F.get_json_object("raw_json", "$.tokens[0].price").cast("double").alias("price_yes"),
        F.get_json_object("raw_json", "$.tokens[1].price").cast("double").alias("price_no"),
    ).withColumn(
        "mid_price",
        (F.col("price_yes") + (F.lit(1.0) - F.col("price_no"))) / F.lit(2.0),
    )


def compute_volatility(df: DataFrame) -> DataFrame:
    """
    Rolling standard deviation of mid_price per market.
    Uses the last VOLATILITY_WINDOW_ROWS records ordered by fetched_at.
    """
    window = (
        Window
        .partitionBy("market_id")
        .orderBy(F.col("fetched_at").cast("long"))
        .rowsBetween(-VOLATILITY_WINDOW_ROWS, 0)
    )
    return df.withColumn(
        "price_volatility",
        F.stddev("mid_price").over(window),
    )


def compute_volume_avg(df: DataFrame) -> DataFrame:
    """Volume-weighted average price per market across all fetched records."""
    window = Window.partitionBy("market_id")
    return df.withColumn(
        "avg_price",
        F.avg("mid_price").over(window),
    ).withColumn(
        "avg_volume_24h",
        F.avg("volume_24h").over(window),
    )


def flag_anomalies(df: DataFrame) -> DataFrame:
    """
    Z-score anomaly detection on mid_price per market.
    A record is flagged when |z| > ZSCORE_THRESHOLD (default: 2.0).
    Z = (value - mean) / stddev
    """
    window = Window.partitionBy("market_id")
    df = df.withColumn("_mean", F.mean("mid_price").over(window))
    df = df.withColumn("_stddev", F.stddev("mid_price").over(window))
    df = df.withColumn(
        "zscore",
        F.when(
            F.col("_stddev") > 0,
            (F.col("mid_price") - F.col("_mean")) / F.col("_stddev"),
        ).otherwise(F.lit(0.0)),
    )
    return df.withColumn(
        "is_anomaly",
        F.abs(F.col("zscore")) > ZSCORE_THRESHOLD,
    ).drop("_mean", "_stddev")


def transform(df: DataFrame) -> DataFrame:
    """Apply all transformations in order. Single entry point for the DAG."""
    df = parse_json_column(df)
    df = compute_volatility(df)
    df = compute_volume_avg(df)
    df = flag_anomalies(df)
    return df.withColumn("processed_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def write_silver(df: DataFrame) -> None:
    """
    Overwrite the silver_markets table with the fully transformed dataset.
    Mode 'overwrite' is safe here because silver is always recomputed
    from the append-only bronze layer.
    """
    silver_cols = [
        "market_id",
        "question",
        "fetched_at",
        "mid_price",
        "avg_price",
        "price_volatility",
        "volume_24h",
        "avg_volume_24h",
        "zscore",
        "is_anomaly",
        "processed_at",
    ]
    (
        df.select(silver_cols)
        .write
        .jdbc(
            url=JDBC_URL,
            table="silver_markets",
            mode="overwrite",
            properties=JDBC_PROPERTIES,
        )
    )
    logger.info("silver_layer_written", rows=df.count())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    spark = get_spark_session()
    logger.info("spark_job_started")

    raw = read_raw_markets(spark)
    silver = transform(raw)
    write_silver(silver)

    logger.info("spark_job_completed")
    spark.stop()


if __name__ == "__main__":
    main()