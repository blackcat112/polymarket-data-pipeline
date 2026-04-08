"""PostgreSQL persistence layer for the ingestion module.

Responsibilities:
- Insert raw API snapshots into raw_rewards (bronze layer).
- All operations are async (asyncpg).
- Structured logging via structlog.
"""
from __future__ import annotations

import os
from typing import Sequence

import asyncpg
import structlog

from ingestion.models import RawRewardRecord

logger = structlog.get_logger(__name__)


async def _get_connection() -> asyncpg.Connection:
    """Return a single asyncpg connection from env vars."""
    return await asyncpg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


async def insert_raw_rewards(records: Sequence[RawRewardRecord]) -> int:
    """Bulk-insert raw reward snapshots into the bronze layer.

    Uses ON CONFLICT DO NOTHING so re-running the ingestion for the same
    fetch cycle is safe (idempotent).

    Args:
        records: Parsed reward market snapshots to persist.

    Returns:
        Number of rows actually inserted (duplicates are skipped).
    """
    if not records:
        logger.info("insert_raw_rewards.skipped", reason="empty_records")
        return 0

    conn = await _get_connection()
    inserted = 0

    try:
        # executemany is fastest for bulk inserts with asyncpg
        result = await conn.executemany(
            """
            INSERT INTO raw_rewards (condition_id, token_id, question, raw_json, fetched_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING
            """,
            [
                (
                    r.condition_id,
                    r.token_id,
                    r.question,
                    r.raw_json,
                    r.fetched_at,
                )
                for r in records
            ],
        )
        # asyncpg returns 'INSERT 0 N' — parse N
        inserted = int(str(result).split()[-1]) if result else len(records)
        logger.info("insert_raw_rewards.done", inserted=inserted, total=len(records))
    except asyncpg.PostgresError as exc:
        logger.error("insert_raw_rewards.failed", error=str(exc))
        raise
    finally:
        await conn.close()

    return inserted


async def fetch_latest_raw_rewards(limit: int = 200) -> list[dict]:
    """Read the most recent raw_rewards rows for Spark to process.

    Used by the Airflow task to pass data to the Spark job without
    re-calling the external API.

    Args:
        limit: Maximum number of rows to return.

    Returns:
        List of dicts with all raw_rewards columns.
    """
    conn = await _get_connection()
    try:
        rows = await conn.fetch(
            """
            SELECT condition_id, token_id, question, raw_json, fetched_at
            FROM   raw_rewards
            ORDER  BY fetched_at DESC
            LIMIT  $1
            """,
            limit,
        )
        logger.info("fetch_latest_raw_rewards.done", rows=len(rows))
        return [dict(r) for r in rows]
    finally:
        await conn.close()
