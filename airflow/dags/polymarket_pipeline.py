from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}


@dag(
    dag_id="polymarket_pipeline",
    description="Hourly pipeline: ingest Polymarket data, transform with Spark, aggregate metrics",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["polymarket", "data-engineering"],
)
def polymarket_pipeline() -> None:

    @task()
    def extract() -> dict:
        """
        Task 1 — Call Polymarket API and persist raw records to PostgreSQL.
        Returns a summary dict that Airflow passes to the next task via XCom.
        """
        import asyncio
        from ingestion.polymarket_client import fetch_active_markets
        from ingestion.db import persist_raw_records

        markets, raw_records = asyncio.run(fetch_active_markets(limit=200))
        inserted = persist_raw_records(raw_records)

        return {
            "markets_fetched": len(markets),
            "records_inserted": inserted,
            "run_at": datetime.utcnow().isoformat(),
        }

    spark_transform = BashOperator(
        task_id="transform",
        bash_command=(
            "spark-submit "
            "--master ${SPARK_MASTER_URL} "
            "--packages org.postgresql:postgresql:42.7.3 "
            "/opt/airflow/spark/jobs/transform_markets.py"
        ),
        env={
            "SPARK_MASTER_URL":  os.environ.get("SPARK_MASTER_URL", "local[*]"),
            "POSTGRES_HOST":     os.environ["POSTGRES_HOST"],
            "POSTGRES_PORT":     os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_DB":       os.environ["POSTGRES_DB"],
            "POSTGRES_USER":     os.environ["POSTGRES_USER"],
            "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
        },
    )

    @task()
    def load_metrics(extract_result: dict) -> None:
        """
        Task 3 — Aggregate silver_markets into metrics_aggregated for the dashboard.
        Uses an upsert so the table always reflects the latest state per market.
        """
        import psycopg2
        from psycopg2.extras import execute_values

        conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ.get("POSTGRES_PORT", 5432)),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO metrics_aggregated (
                        market_id,
                        question,
                        avg_price,
                        price_volatility,
                        volume_24h,
                        anomaly_count,
                        last_updated
                    )
                    SELECT
                        market_id,
                        MAX(question)          AS question,
                        AVG(avg_price)         AS avg_price,
                        AVG(price_volatility)  AS price_volatility,
                        MAX(volume_24h)        AS volume_24h,
                        SUM(is_anomaly::int)   AS anomaly_count,
                        NOW()                  AS last_updated
                    FROM silver_markets
                    WHERE processed_at >= NOW() - INTERVAL '1 hour'
                    GROUP BY market_id
                    ON CONFLICT (market_id)
                    DO UPDATE SET
                        question         = EXCLUDED.question,
                        avg_price        = EXCLUDED.avg_price,
                        price_volatility = EXCLUDED.price_volatility,
                        volume_24h       = EXCLUDED.volume_24h,
                        anomaly_count    = EXCLUDED.anomaly_count,
                        last_updated     = EXCLUDED.last_updated;
                """)

        conn.close()
        print(
            f"Metrics updated. "
            f"Markets fetched in previous task: {extract_result['markets_fetched']}"
        )

    extract_result = extract()
    extract_result >> spark_transform >> load_metrics(extract_result)


polymarket_pipeline()