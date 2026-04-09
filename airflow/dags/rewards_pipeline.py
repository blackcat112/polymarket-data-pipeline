"""Airflow DAG: Polymarket Rewards Pipeline.

Schedule: every hour
Pipeline:
    [1] fetch_rewards     — call /rewards/markets/current, persist to raw_rewards
    [2] spark_transform   — spark-submit transform_rewards.py
    [3] load_opportunities — verify gold layer was written, log top-3 markets

All tasks retry twice with exponential backoff.
Full pipeline SLA: 10 minutes.
"""
from __future__ import annotations

import asyncio
import logging
import os
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default task args — applied to every task unless overridden
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "polymarket-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}

SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT_BIN", "spark-submit")
SPARK_JOB_PATH = os.environ.get(
    "SPARK_JOB_PATH",
    "/opt/airflow/spark/jobs/transform_rewards.py",
)
POSTGRES_JDBC_JAR = os.environ.get(
    "POSTGRES_JDBC_JAR",
    "/opt/spark/jars/postgresql-42.7.3.jar",
)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="rewards_pipeline",
    description="Hourly Polymarket rewards ingestion → Spark transform → gold layer",
    schedule_interval="0 * * * *",   # every hour at :00
    start_date=days_ago(1),
    catchup=False,                   # don’t backfill missed runs on deploy
    max_active_runs=1,               # prevent overlapping hourly runs
    default_args=DEFAULT_ARGS,
    tags=["polymarket", "rewards", "ingestion"],
    sla_miss_callback=lambda *args: log.warning("SLA missed on rewards_pipeline"),
)
def rewards_pipeline() -> None:

    # -----------------------------------------------------------------------
    # TASK 1 — Fetch rewards from Polymarket API and persist to bronze layer
    # -----------------------------------------------------------------------
    @task(
        task_id="fetch_rewards",
        sla=timedelta(minutes=3),
    )
    def fetch_rewards() -> dict:
        """Call Polymarket /rewards/markets/current and insert into raw_rewards.

        Returns a summary dict passed to downstream tasks via XCom.
        """
        from ingestion.db import insert_raw_rewards
        from ingestion.polymarket_client import fetch_rewarded_markets

        markets, raw_records = asyncio.run(fetch_rewarded_markets())
        inserted = asyncio.run(insert_raw_rewards(raw_records))

        log.info(
            "fetch_rewards completed",
            extra={"markets": len(markets), "inserted": inserted},
        )
        return {"markets_fetched": len(markets), "rows_inserted": inserted}

    # -----------------------------------------------------------------------
    # TASK 2 — Spark transformation: bronze → silver + gold
    # -----------------------------------------------------------------------
    @task(
        task_id="spark_transform",
        sla=timedelta(minutes=8),
    )
    def spark_transform(fetch_summary: dict) -> dict:
        """Run the PySpark transform job via spark-submit.

        Uses subprocess so Airflow doesn’t need a Spark cluster driver inside
        the worker — spark-submit handles the connection to the standalone cluster.
        """
        if fetch_summary.get("rows_inserted", 0) == 0:
            log.warning("spark_transform skipped — no new rows from fetch_rewards")
            return {"skipped": True}

        cmd = [
            SPARK_SUBMIT,
            "--master", os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
            "--jars", POSTGRES_JDBC_JAR,
            "--conf", "spark.sql.shuffle.partitions=8",
            SPARK_JOB_PATH,
        ]

        log.info("spark_transform running", extra={"cmd": " ".join(cmd)})

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=480,   # 8 min hard timeout — matches task SLA
        )

        if result.returncode != 0:
            log.error("spark-submit failed", extra={"stderr": result.stderr[-2000:]})
            raise RuntimeError(f"spark-submit exited {result.returncode}")

        log.info("spark_transform done", extra={"stdout": result.stdout[-500:]})
        return {"skipped": False, "returncode": result.returncode}

    # -----------------------------------------------------------------------
    # TASK 3 — Verify gold layer and log top-3 opportunities
    # -----------------------------------------------------------------------
    @task(
        task_id="load_opportunities",
        sla=timedelta(minutes=10),
    )
    def load_opportunities(spark_summary: dict) -> None:
        """Verify rewards_opportunities was updated and log the top-3 markets.

        This task acts as a lightweight quality gate:
        - Checks that last_updated is within the last 10 minutes.
        - Logs the top-3 markets by score_per_maker for observability.
        """
        import asyncpg

        if spark_summary.get("skipped"):
            log.info("load_opportunities skipped — Spark was skipped upstream")
            return

        async def _check() -> list[dict]:
            conn = await asyncpg.connect(
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ.get("POSTGRES_PORT", "5432")),
                database=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
            try:
                rows = await conn.fetch(
                    """
                    SELECT condition_id, question, score_per_maker,
                           pool_diario, num_makers, last_updated
                    FROM   rewards_opportunities
                    WHERE  last_updated >= NOW() - INTERVAL '10 minutes'
                    ORDER  BY score_per_maker DESC
                    LIMIT  3
                    """
                )
                return [dict(r) for r in rows]
            finally:
                await conn.close()

        top3 = asyncio.run(_check())

        if not top3:
            raise ValueError(
                "rewards_opportunities was not updated in the last 10 minutes — "
                "Spark job may have silently failed."
            )

        for i, mkt in enumerate(top3, 1):
            log.info(
                f"Top-{i} opportunity",
                extra={
                    "condition_id": mkt["condition_id"],
                    "question": mkt["question"][:60],
                    "score": float(mkt["score_per_maker"]),
                    "pool": float(mkt["pool_diario"]),
                    "makers": mkt["num_makers"],
                },
            )

    # -----------------------------------------------------------------------
    # Task dependencies — linear pipeline
    # -----------------------------------------------------------------------
    fetch_summary  = fetch_rewards()
    spark_summary  = spark_transform(fetch_summary)
    load_opportunities(spark_summary)


# Instantiate the DAG (required by Airflow’s DAG discovery)
rewards_pipeline()
