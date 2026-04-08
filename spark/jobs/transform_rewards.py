"""PySpark transformation job for the rewards pipeline.

Reads from:  raw_rewards      (PostgreSQL bronze layer)
Writes to:   silver_rewards   (PostgreSQL silver layer)
             rewards_opportunities (PostgreSQL gold layer, upsert)

Scoring logic mirrors modules/scanner.py exactly:
    score_per_maker = pool_diario / max(num_makers, 1)

Run locally:
    spark-submit \\
        --packages org.postgresql:postgresql:42.7.3 \\
        spark/jobs/transform_rewards.py
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JDBC_URL = (
    f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'localhost')}:"
    f"{os.environ.get('POSTGRES_PORT', '5432')}/"
    f"{os.environ.get('POSTGRES_DB', 'polymarket')}"
)
JDBC_PROPS: dict[str, str] = {
    "user":   os.environ.get("POSTGRES_USER", "polymarket"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "driver": "org.postgresql.Driver",
}

# Schema of the JSON blob stored in raw_rewards.raw_json
# Mirrors the fields captured by ingestion/polymarket_client.py
RAW_JSON_SCHEMA = StructType([
    StructField("condition_id",     StringType(),  True),
    StructField("token_id",         StringType(),  True),
    StructField("question",         StringType(),  True),
    StructField("total_daily_rate", DoubleType(),  True),   # pool_diario
    StructField("rewards_min_size", DoubleType(),  True),   # min_size
    StructField("max_spread",       DoubleType(),  True),
    StructField("num_makers",       IntegerType(), True),
    StructField("midpoint",         DoubleType(),  True),
    StructField("yes_price",        DoubleType(),  True),
    StructField("no_price",         DoubleType(),  True),
    StructField("active",           BooleanType(), True),
])


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("polymarket-rewards-transform")
        .config("spark.sql.session.timeZone", "UTC")
        # Reduce shuffle partitions for small datasets (< 1M rows)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Step 1: Read
# ---------------------------------------------------------------------------

def read_raw_rewards(spark: SparkSession) -> DataFrame:
    """Read the latest 24h of raw_rewards from PostgreSQL."""
    return (
        spark.read
        .jdbc(JDBC_URL, "raw_rewards", properties=JDBC_PROPS)
        .filter(F.col("fetched_at") >= F.date_sub(F.current_timestamp(), 1))
    )


# ---------------------------------------------------------------------------
# Step 2: Parse raw_json
# ---------------------------------------------------------------------------

def parse_raw_json(df: DataFrame) -> DataFrame:
    """Explode raw_json TEXT column into typed Spark columns."""
    parsed = df.withColumn(
        "_parsed", F.from_json(F.col("raw_json"), RAW_JSON_SCHEMA)
    )
    return (
        parsed
        .withColumn("pool_diario", F.col("_parsed.total_daily_rate").cast(DoubleType()))
        .withColumn("min_size",    F.col("_parsed.rewards_min_size").cast(DoubleType()))
        .withColumn("max_spread",  F.col("_parsed.max_spread").cast(DoubleType()))
        .withColumn("num_makers",  F.col("_parsed.num_makers").cast(IntegerType()))
        .withColumn("midpoint",    F.col("_parsed.midpoint").cast(DoubleType()))
        .withColumn("yes_price",   F.col("_parsed.yes_price").cast(DoubleType()))
        .withColumn("no_price",    F.col("_parsed.no_price").cast(DoubleType()))
        .drop("_parsed", "raw_json")
    )


# ---------------------------------------------------------------------------
# Step 3: Compute enriched metrics
# ---------------------------------------------------------------------------

def compute_metrics(df: DataFrame) -> DataFrame:
    """Add score_per_maker, roi_1h_usdc and competencia_rank.

    score_per_maker = pool_diario / max(num_makers, 1)
    This is the exact formula from modules/scanner.py.
    Higher score = less competition for the same reward pool.
    """
    window_by_fetch = Window.partitionBy("fetched_at").orderBy(
        F.col("score_per_maker").desc()
    )

    return (
        df
        # Core score: mirrors scanner.py line `score = pool / max(num_makers, 1)`
        .withColumn(
            "score_per_maker",
            F.col("pool_diario") / F.greatest(F.col("num_makers").cast(DoubleType()), F.lit(1.0)),
        )
        # Hourly ROI estimate (assuming 1 maker position held for 1h)
        .withColumn("roi_1h_usdc", F.col("score_per_maker") / F.lit(24.0))
        # Rank within each fetch cycle (1 = best opportunity that hour)
        .withColumn("competencia_rank", F.rank().over(window_by_fetch))
    )


# ---------------------------------------------------------------------------
# Step 4: Build gold layer aggregates
# ---------------------------------------------------------------------------

def build_opportunities(df: DataFrame) -> DataFrame:
    """Aggregate silver data into rewards_opportunities (gold layer).

    For each market computes:
    - avg_score_7d, peak_score_7d  : rolling performance
    - avg_num_makers_7d            : competition trend
    - best_hour_utc                : cheapest hour to enter (fewest makers)
    - simulated_rewards_7d/24h     : how much the bot would have earned
    """
    seven_days_ago = F.date_sub(F.current_timestamp(), 7)
    one_day_ago    = F.date_sub(F.current_timestamp(), 1)

    # Window for hour-of-day competition analysis
    window_hour = Window.partitionBy("condition_id", F.hour("fetched_at"))

    with_hour_avg = df.withColumn(
        "avg_makers_this_hour",
        F.avg("num_makers").over(window_hour),
    )

    # Best hour = hour with lowest average number of makers
    best_hour = (
        with_hour_avg
        .groupBy("condition_id")
        .agg(
            F.first(
                F.hour("fetched_at"),
                ignorenulls=True,
            ).alias("best_hour_utc")
        )
    )

    agg_7d = (
        df.filter(F.col("fetched_at") >= seven_days_ago)
        .groupBy("condition_id")
        .agg(
            F.avg("score_per_maker").alias("avg_score_7d"),
            F.max("score_per_maker").alias("peak_score_7d"),
            F.avg("num_makers").alias("avg_num_makers_7d"),
            F.sum("score_per_maker").alias("simulated_rewards_7d"),
        )
    )

    agg_24h = (
        df.filter(F.col("fetched_at") >= one_day_ago)
        .groupBy("condition_id")
        .agg(
            F.sum("score_per_maker").alias("simulated_rewards_24h"),
        )
    )

    # Latest snapshot per market (for current values)
    window_latest = Window.partitionBy("condition_id").orderBy(F.col("fetched_at").desc())
    latest = (
        df.withColumn("_rn", F.row_number().over(window_latest))
        .filter(F.col("_rn") == 1)
        .select(
            "condition_id", "token_id", "question",
            "pool_diario", "num_makers", "score_per_maker",
            "midpoint", "max_spread",
        )
    )

    return (
        latest
        .join(agg_7d,    on="condition_id", how="left")
        .join(agg_24h,   on="condition_id", how="left")
        .join(best_hour, on="condition_id", how="left")
        .withColumn("last_updated", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Step 5: Write
# ---------------------------------------------------------------------------

def write_silver(df: DataFrame) -> None:
    """Append enriched rows to silver_rewards."""
    (
        df
        .withColumn("processed_at", F.current_timestamp())
        .write
        .jdbc(JDBC_URL, "silver_rewards", mode="append", properties=JDBC_PROPS)
    )


def write_gold(df: DataFrame) -> None:
    """Overwrite rewards_opportunities (gold layer) with latest aggregates.

    Full overwrite is safe here because this table always reflects the
    current state — historical data lives in silver_rewards.
    """
    (
        df.write
        .jdbc(JDBC_URL, "rewards_opportunities", mode="overwrite", properties=JDBC_PROPS)
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run() -> None:
    spark = get_spark()

    raw    = read_raw_rewards(spark)
    silver = compute_metrics(parse_raw_json(raw))
    gold   = build_opportunities(silver)

    write_silver(silver)
    write_gold(gold)

    spark.stop()


if __name__ == "__main__":
    run()
