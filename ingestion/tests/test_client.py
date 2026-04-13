"""Unit tests for ingestion/polymarket_client.py.

Two layers of testing:
  1. Pure functions (_parse_midpoint, _build_reward_market) — no mocks needed.
  2. fetch_rewarded_markets — httpx calls mocked with respx.

No real network calls are made in any test.
"""
from __future__ import annotations

import pytest
import respx
from httpx import Response

from ingestion.polymarket_client import (
    CLOB_BASE_URL,
    _build_reward_market,
    _parse_midpoint,
    fetch_rewarded_markets,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reward_data(
    condition_id: str = "0xabc",
    pool: float = 200.0,
    min_size: float = 10.0,
) -> dict:
    return {
        "condition_id": condition_id,
        "total_daily_rate": pool,
        "rewards_min_size": min_size,
    }


def _market_data(
    condition_id: str = "0xabc",
    active: bool = True,
    accepting_orders: bool = True,
) -> dict:
    return {
        "condition_id": condition_id,
        "question": "Will BTC hit $100k?",
        "active": active,
        "closed": False,
        "accepting_orders": accepting_orders,
        "rewards": {"max_spread": 0.02},
        "tokens": [
            {"token_id": "0xtoken", "price": 0.51},
            {"token_id": "0xtoken2", "price": 0.49},
        ],
    }


def _book_data(num_bids: int = 3, num_asks: int = 3) -> dict:
    return {
        "bids": [{"price": str(0.50 - i * 0.01)} for i in range(num_bids)],
        "asks": [{"price": str(0.51 + i * 0.01)} for i in range(num_asks)],
    }


# ---------------------------------------------------------------------------
# _parse_midpoint
# ---------------------------------------------------------------------------

class TestParseMidpoint:
    def test_returns_midpoint_from_bid_ask(self) -> None:
        bids = [{"price": "0.49"}]
        asks = [{"price": "0.51"}]
        result = _parse_midpoint(bids, asks, yes_price=0.5)
        assert result == pytest.approx(0.5)

    def test_falls_back_to_yes_price_when_no_book(self) -> None:
        result = _parse_midpoint([], [], yes_price=0.62)
        assert result == pytest.approx(0.62)

    def test_returns_none_when_book_empty_and_yes_price_zero(self) -> None:
        result = _parse_midpoint([], [], yes_price=0.0)
        assert result is None

    def test_ignores_book_when_bid_too_low(self) -> None:
        """bid <= 0.05 should be ignored; fall back to yes_price."""
        bids = [{"price": "0.04"}]
        asks = [{"price": "0.51"}]
        result = _parse_midpoint(bids, asks, yes_price=0.55)
        assert result == pytest.approx(0.55)

    def test_ignores_book_when_ask_too_high(self) -> None:
        """ask >= 0.95 should be ignored; fall back to yes_price."""
        bids = [{"price": "0.49"}]
        asks = [{"price": "0.96"}]
        result = _parse_midpoint(bids, asks, yes_price=0.55)
        assert result == pytest.approx(0.55)


# ---------------------------------------------------------------------------
# _build_reward_market
# ---------------------------------------------------------------------------

class TestBuildRewardMarket:
    def test_returns_reward_market_with_correct_score(self) -> None:
        market = _build_reward_market(
            _reward_data(pool=200.0),
            _market_data(),
            _book_data(num_bids=2, num_asks=2),
        )
        assert market is not None
        # score = 200 / max(4, 1) = 50.0
        assert market.score == pytest.approx(50.0)

    def test_returns_none_when_no_tokens(self) -> None:
        market_data = _market_data()
        market_data["tokens"] = []
        result = _build_reward_market(_reward_data(), market_data, _book_data())
        assert result is None

    def test_returns_none_when_token_id_missing(self) -> None:
        market_data = _market_data()
        market_data["tokens"] = [{"price": 0.5}]  # no token_id key
        result = _build_reward_market(_reward_data(), market_data, _book_data())
        assert result is None

    def test_returns_none_when_midpoint_is_none(self) -> None:
        """Empty book + yes_price=0 → midpoint=None → market skipped."""
        market_data = _market_data()
        market_data["tokens"] = [{"token_id": "0xtoken", "price": 0.0}]
        result = _build_reward_market(_reward_data(), market_data, {"bids": [], "asks": []})
        assert result is None

    def test_num_makers_is_sum_of_bids_and_asks(self) -> None:
        market = _build_reward_market(
            _reward_data(pool=100.0),
            _market_data(),
            _book_data(num_bids=5, num_asks=3),
        )
        assert market is not None
        assert market.num_makers == 8


# ---------------------------------------------------------------------------
# fetch_rewarded_markets
# ---------------------------------------------------------------------------

class TestFetchRewardedMarkets:
    @pytest.mark.asyncio
    @respx.mock
    async def test_returns_qualified_market(self) -> None:
        """Happy path: one market passes all filters and is returned."""
        respx.get(f"{CLOB_BASE_URL}/rewards/markets/current").mock(
            return_value=Response(200, json={"data": [_reward_data()]})
        )
        respx.get(f"{CLOB_BASE_URL}/markets/0xabc").mock(
            return_value=Response(200, json=_market_data())
        )
        respx.get(f"{CLOB_BASE_URL}/book").mock(
            return_value=Response(200, json=_book_data(num_bids=2, num_asks=2))
        )

        markets, records = await fetch_rewarded_markets()

        assert len(markets) == 1
        assert len(records) == 1
        assert markets[0].condition_id == "0xabc"

    @pytest.mark.asyncio
    @respx.mock
    async def test_skips_market_with_low_pool(self) -> None:
        """Markets below MIN_DAILY_REWARD are filtered out before any API call."""
        low_pool = _reward_data(pool=1.0)  # below default 50 threshold
        respx.get(f"{CLOB_BASE_URL}/rewards/markets/current").mock(
            return_value=Response(200, json={"data": [low_pool]})
        )
        markets, records = await fetch_rewarded_markets()
        assert markets == []
        assert records == []

    @pytest.mark.asyncio
    @respx.mock
    async def test_skips_inactive_market(self) -> None:
        respx.get(f"{CLOB_BASE_URL}/rewards/markets/current").mock(
            return_value=Response(200, json={"data": [_reward_data()]})
        )
        respx.get(f"{CLOB_BASE_URL}/markets/0xabc").mock(
            return_value=Response(200, json=_market_data(active=False))
        )
        markets, _ = await fetch_rewarded_markets()
        assert markets == []

    @pytest.mark.asyncio
    @respx.mock
    async def test_continues_on_http_error(self) -> None:
        """A 404 on /markets/{id} should not crash the whole ingestion."""
        respx.get(f"{CLOB_BASE_URL}/rewards/markets/current").mock(
            return_value=Response(200, json={"data": [_reward_data()]})
        )
        respx.get(f"{CLOB_BASE_URL}/markets/0xabc").mock(
            return_value=Response(404)
        )
        markets, records = await fetch_rewarded_markets()
        assert markets == []
        assert records == []

    @pytest.mark.asyncio
    @respx.mock
    async def test_empty_rewards_list_returns_empty(self) -> None:
        respx.get(f"{CLOB_BASE_URL}/rewards/markets/current").mock(
            return_value=Response(200, json={"data": []})
        )
        markets, records = await fetch_rewarded_markets()
        assert markets == []
        assert records == []
