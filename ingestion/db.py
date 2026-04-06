import os
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from ingestion.models import RawMarketRecord


def _get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def persist_raw_records(records: list[RawMarketRecord]) -> int:
    """
    Insert raw market records into the bronze layer table.
    Skips duplicates via ON CONFLICT DO NOTHING.
    Returns the number of rows inserted.
    """
    if not records:
        return 0

    rows = [
        (r.market_id, r.question, r.raw_json, r.fetched_at)
        for r in records
    ]

    with _get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO raw_markets (market_id, question, raw_json, fetched_at)
                VALUES %s
                ON CONFLICT (market_id, fetched_at) DO NOTHING
                """,
                rows,
            )
            inserted: int = cur.rowcount

    return inserted