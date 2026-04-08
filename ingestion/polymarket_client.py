import json
from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.models import Market, MarketOutcome, RawMarketRecord

logger = structlog.get_logger(__name__)

BASE_URL = "https://clob.polymarket.com"
DEFAULT_TIMEOUT = 10.0
MAX_RETRIES = 5


def _configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.BoundLogger,
        logger_factory=structlog.PrintLoggerFactory(),
    )


@retry(
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(MAX_RETRIES),
    reraise=True,
)
async def _fetch_json(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    logger.info("fetching_url", url=url)
    response = await client.get(url, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _parse_market(raw: dict[str, Any]) -> Market:
    outcomes = [
        MarketOutcome(
            outcome_id=str(i),
            title=token.get("outcome", ""),
            price=float(token.get("price", 0.0)),
            volume=float(token.get("volume", 0.0)),
        )
        for i, token in enumerate(raw.get("tokens", []))
    ]
    return Market(
        market_id=raw["condition_id"],
        question=raw.get("question", ""),
        outcomes=outcomes,
        volume_24h=float(raw.get("volume24hr", 0.0)),
        active=raw.get("active", False),
        end_date=None,
    )


async def fetch_active_markets(
    limit: int = 100,
) -> tuple[list[Market], list[RawMarketRecord]]:
    """
    Fetch active markets from Polymarket CLOB API.
    Returns parsed Market objects and RawMarketRecord for persistence.
    """
    _configure_logging()
    url = f"{BASE_URL}/markets?active=true&limit={limit}"

    async with httpx.AsyncClient() as client:
        data = await _fetch_json(client, url)

    markets: list[Market] = []
    raw_records: list[RawMarketRecord] = []

    for item in data.get("data", []):
        try:
            market = _parse_market(item)
            markets.append(market)
            raw_records.append(
                RawMarketRecord(
                    market_id=market.market_id,
                    question=market.question,
                    raw_json=json.dumps(item),
                )
            )
        except (KeyError, ValueError) as exc:
            logger.warning("parse_error", error=str(exc), item_id=item.get("condition_id"))
            continue

    logger.info("markets_fetched", count=len(markets))
    return markets, raw_records


async def fetch_market_by_id(market_id: str) -> tuple[Market, RawMarketRecord] | None:
    """Fetch a single market by condition_id."""
    _configure_logging()
    url = f"{BASE_URL}/markets/{market_id}"

    async with httpx.AsyncClient() as client:
        try:
            data = await _fetch_json(client, url)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                logger.warning("market_not_found", market_id=market_id)
                return None
            raise

    market = _parse_market(data)
    raw = RawMarketRecord(
        market_id=market.market_id,
        question=market.question,
        raw_json=json.dumps(data),
    )
    return market, raw
