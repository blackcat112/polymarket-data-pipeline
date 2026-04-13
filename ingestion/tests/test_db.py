"""Unit tests for ingestion/db.py.

All asyncpg calls are mocked — no real PostgreSQL connection required.
Only the business logic (early return on empty, error propagation,
row count parsing) is tested here.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from ingestion.db import fetch_latest_raw_rewards, insert_raw_rewards
from ingestion.models import RawRewardRecord, RewardMarket


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(condition_id: str = "0xabc") -> RawRewardRecord:
    market = RewardMarket(
        condition_id=condition_id,
        token_id="0xtoken",
        question="Will BTC hit $100k?",
        pool_diario=240.0,
        min_size=10.0,
        max_spread=0.02,
        num_makers=4,
        midpoint=0.5,
        yes_price=0.51,
        no_price=0.49,
        score=60.0,
    )
    return RawRewardRecord.from_market(market, {"total_daily_rate": 240.0})


def _mock_conn(executemany_result: str = "INSERT 0 2") -> AsyncMock:
    """Return a mock asyncpg Connection."""
    conn = AsyncMock()
    conn.executemany = AsyncMock(return_value=executemany_result)
    conn.fetch = AsyncMock(return_value=[])
    conn.close = AsyncMock()
    return conn


# ---------------------------------------------------------------------------
# insert_raw_rewards
# ---------------------------------------------------------------------------

class TestInsertRawRewards:
    @pytest.mark.asyncio
    async def test_empty_list_returns_zero_without_connecting(self) -> None:
        """No DB call should be made when records is empty."""
        with patch("ingestion.db._get_connection") as mock_connect:
            result = await insert_raw_rewards([])
        assert result == 0
        mock_connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_inserts_records_and_returns_count(self) -> None:
        conn = _mock_conn(executemany_result="INSERT 0 2")
        with patch("ingestion.db._get_connection", return_value=conn):
            result = await insert_raw_rewards([_make_record("0xaaa"), _make_record("0xbbb")])
        assert result == 2
        conn.executemany.assert_called_once()
        conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_is_closed_even_on_postgres_error(self) -> None:
        """finally block must close the connection even when executemany raises."""
        conn = _mock_conn()
        conn.executemany = AsyncMock(side_effect=asyncpg.PostgresError("boom"))
        with patch("ingestion.db._get_connection", return_value=conn):
            with pytest.raises(asyncpg.PostgresError):
                await insert_raw_rewards([_make_record()])
        conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_executemany_receives_correct_tuple_shape(self) -> None:
        """Each row passed to executemany must have exactly 5 elements."""
        conn = _mock_conn()
        records = [_make_record("0xabc"), _make_record("0xdef")]
        with patch("ingestion.db._get_connection", return_value=conn):
            await insert_raw_rewards(records)
        _, call_rows = conn.executemany.call_args.args
        assert all(len(row) == 5 for row in call_rows)


# ---------------------------------------------------------------------------
# fetch_latest_raw_rewards
# ---------------------------------------------------------------------------

class TestFetchLatestRawRewards:
    @pytest.mark.asyncio
    async def test_returns_list_of_dicts(self) -> None:
        fake_row = MagicMock()
        fake_row.__iter__ = MagicMock(
            return_value=iter([("condition_id", "0xabc"), ("question", "test?")])
        )
        # asyncpg Record converts to dict via dict(row)
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"condition_id": "0xabc", "question": "test?"}])
        conn.close = AsyncMock()
        with patch("ingestion.db._get_connection", return_value=conn):
            rows = await fetch_latest_raw_rewards(limit=10)
        assert isinstance(rows, list)
        conn.fetch.assert_called_once()
        conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_table_returns_empty_list(self) -> None:
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        conn.close = AsyncMock()
        with patch("ingestion.db._get_connection", return_value=conn):
            rows = await fetch_latest_raw_rewards()
        assert rows == []

    @pytest.mark.asyncio
    async def test_limit_is_passed_to_query(self) -> None:
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        conn.close = AsyncMock()
        with patch("ingestion.db._get_connection", return_value=conn):
            await fetch_latest_raw_rewards(limit=42)
        _, limit_arg = conn.fetch.call_args.args
        assert limit_arg == 42
