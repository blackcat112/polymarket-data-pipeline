import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingestion.polymarket_client import _parse_market, fetch_active_markets


MOCK_MARKET = {
    "condition_id": "abc123",
    "question": "Will BTC exceed 100k by end of 2025?",
    "active": True,
    "volume24hr": "52341.50",
    "tokens": [
        {"outcome": "Yes", "price": "0.62", "volume": "30000"},
        {"outcome": "No",  "price": "0.38", "volume": "22341"},
    ],
}


def test_parse_market_returns_correct_fields() -> None:
    market = _parse_market(MOCK_MARKET)
    assert market.market_id == "abc123"
    assert market.question == "Will BTC exceed 100k by end of 2025?"
    assert market.active is True
    assert market.volume_24h == 52341.50
    assert len(market.outcomes) == 2


def test_parse_market_outcome_prices() -> None:
    market = _parse_market(MOCK_MARKET)
    yes = market.outcomes[0]
    no = market.outcomes[1]
    assert yes.price == 0.62
    assert no.price == 0.38
    assert yes.title == "Yes"


def test_parse_market_missing_tokens_returns_empty_outcomes() -> None:
    data = {**MOCK_MARKET, "tokens": []}
    market = _parse_market(data)
    assert market.outcomes == []


@pytest.mark.asyncio
async def test_fetch_active_markets_returns_markets_and_raw() -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": [MOCK_MARKET]}
    mock_response.raise_for_status = MagicMock()

    with patch("ingestion.polymarket_client.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        markets, raw_records = await fetch_active_markets(limit=10)

    assert len(markets) == 1
    assert markets[0].market_id == "abc123"
    assert len(raw_records) == 1
    assert raw_records[0].market_id == "abc123"