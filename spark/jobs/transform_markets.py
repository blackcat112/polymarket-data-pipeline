import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType
)


ANOMALY_THRESHOLD = 2.0


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("polymarket-transform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


def read_from_postgres(spark: SparkSession, table: str) -> DataFrame:
    jdbc_url = (
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:"
        f"{os.environ.get('POSTGRES_PORT', '5432')}/"
        f"{os.environ['POSTGRES_DB']}"
    )
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def write_to_postgres(df: DataFrame, table: str, mode: str = "append") -> None:
    jdbc_url = (
        f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:"
        f"{os.environ.get('POSTGRES_PORT', '5432')}/"
        f"{os.environ['POSTGRES_DB']}"
    )
    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )


def extract_fields(df: DataFrame) -> DataFrame:
    """
    Parse relevant fields from the raw JSONB column into typed columns.
    """
    return df.select(
        F.col("market_id"),
        F.col("question"),
        F.col("fetched_at"),
        F.get_json_object(F.col("raw_json"), "$.volume24hr")
         .cast(DoubleType()).alias("volume_24h"),
        F.get_json_object(F.col("raw_json"), "$.tokens[0].price")
         .cast(DoubleType()).alias("price_yes"),
    )


def compute_volatility(df: DataFrame) -> DataFrame:
    """
    Rolling standard deviation of yes-price per market over the last 24 records.
    This approximates price volatility without needing a time-series library.
    """
    window = (
        Window
        .partitionBy("market_id")
        .orderBy("fetched_at")
        .rowsBetween(-23, 0)
    )
    return df.withColumn(
        "price_volatility",
        F.stddev("price_yes").over(window)
    )


def compute_volume_avg(df: DataFrame) -> DataFrame:
    """
    Rolling mean of volume_24h per market over the last 24 records.
    """
    window = (
        Window
        .partitionBy("market_id")
        .orderBy("fetched_at")
        .rowsBetween(-23, 0)
    )
    return df.withColumn(
        "volume_avg",
        F.avg("volume_24h").over(window)
    )


def compute_zscore(df: DataFrame) -> DataFrame:
    """
    Z-score of current volume_24h relative to the market's own distribution.
    Flags values more than ANOMALY_THRESHOLD standard deviations from the mean.
    """
    window = Window.partitionBy("market_id")

    df = df.withColumn("_vol_mean", F.avg("volume_24h").over(window))
    df = df.withColumn("_vol_std",  F.stddev("volume_24h").over(window))

    df = df.withColumn(
        "z_score",
        F.when(
            F.col("_vol_std") > 0,
            (F.col("volume_24h") - F.col("_vol_mean")) / F.col("_vol_std")
        ).otherwise(F.lit(0.0))
    )

    df = df.withColumn(
        "is_anomaly",
        F.abs(F.col("z_score")) > ANOMALY_THRESHOLD
    )

    return df.drop("_vol_mean", "_vol_std")


def build_silver_layer(df: DataFrame) -> DataFrame:
    """
    Chain all transformations and select the final silver schema.
    """
    df = extract_fields(df)
    df = compute_volatility(df)
    df = compute_volume_avg(df)
    df = compute_zscore(df)

    return df.select(
        "market_id",
        "question",
        F.col("price_yes").alias("avg_price"),
        "price_volatility",
        "volume_24h",
        "volume_avg",
        "z_score",
        "is_anomaly",
        F.current_timestamp().alias("processed_at"),
    )


def run() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_from_postgres(spark, "raw_markets")
    silver_df = build_silver_layer(raw_df)
    write_to_postgres(silver_df, "silver_markets", mode="append")

    print(f"[{datetime.utcnow().isoformat()}] silver layer written: {silver_df.count()} rows")
    spark.stop()


if __name__ == "__main__":
    run()