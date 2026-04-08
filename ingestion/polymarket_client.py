"""Async Polymarket CLOB client — rewards ingestion layer.

This module is the async, production-grade rewrite of modules/scanner.py.
Same three API endpoints, same filtering logic, same score formula:
    score = pool_diario / max(num_makers, 1)

Differences from the original bot:
  - async/await with httpx (non-blocking I/O)
  - Automatic retries with exponential backoff via tenacity
  - Structured JSON logs via structlog
  - Returns data models instead of raw dicts
  - No order placement — observe and persist only
"""
from __future__ import annotations

import json
import os
from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.models import RawRewardRecord, RewardMarket

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants — mirrors config.py values from the original bot
# ---------------------------------------------------------------------------
CLOB_BASE_URL    = "https://clob.polymarket.com"
DEFAULT_TIMEOUT  = 10.0
MAX_RETRIES      = 5

# Filtering thresholds (same as original scanner.py)
MIN_DAILY_REWARD = float(os.getenv("MIN_DAILY_REWARD", "50"))   # skip tiny pools
MAX_MIN_SIZE     = float(os.getenv("MAX_MIN_SIZE",     "50"))   # skip high-barrier markets
MAX_MAKERS       = int(os.getenv("MAX_MAKERS",         "20"))   # skip overcrowded books


# ---------------------------------------------------------------------------
# Low-level HTTP helper
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(MAX_RETRIES),
    reraise=True,
)
async def _get(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """GET request with automatic retry on network/HTTP errors."""
    logger.debug("http_get", url=url, params=params)
    response = await client.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_midpoint(bids: list, asks: list, yes_price: float) -> float | None:
    """Compute midpoint from order book. Falls back to yes_price.

    Replicates the exact midpoint logic from scanner.py:
      - Use (best_bid + best_ask) / 2 when book is healthy
      - Fall back to yes_price from the market endpoint
      - Return None if no valid price exists (market is skipped)
    """
    if bids and asks:
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        if best_bid > 0.05 and best_ask < 0.95:
            return round((best_bid + best_ask) / 2, 4)
    if yes_price > 0:
        return round(yes_price, 4)
    return None


def _build_reward_market(
    reward_data: dict,
    market_data: dict,
    book_data:   dict,
) -> RewardMarket | None:
    """Build a RewardMarket from the three API responses.

    Returns None if the market fails any filter (same filters as scanner.py).
    """
    try:
        pool_diario = float(reward_data.get("total_daily_rate", 0))
        min_size    = float(reward_data.get("rewards_min_size", 999))
        max_spread  = float(market_data.get("rewards", {}).get("max_spread", 4.5))

        tokens  = market_data.get("tokens", [])
        if not tokens:
            return None

        yes_price = float(tokens[0].get("price", 0.5))
        no_price  = float(tokens[1].get("price", 0.5)) if len(tokens) > 1 else 0.5
        token_id  = tokens[0].get("token_id", "")
        if not token_id:
            return None

        bids       = book_data.get("bids", [])
        asks       = book_data.get("asks", [])
        num_makers = len(bids) + len(asks)
        midpoint   = _parse_midpoint(bids, asks, yes_price)

        if midpoint is None:
            logger.info("skip_no_price", condition_id=reward_data.get("condition_id"))
            return None

        score = round(pool_diario / max(num_makers, 1), 6)

        return RewardMarket(
            condition_id=reward_data["condition_id"],
            token_id=token_id,
            question=market_data.get("question", ""),
            pool_diario=pool_diario,
            min_size=min_size,
            max_spread=max_spread,
            num_makers=num_makers,
            midpoint=midpoint,
            yes_price=yes_price,
            no_price=no_price,
            score=score,
        )

    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("parse_error", error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_rewarded_markets() -> tuple[list[RewardMarket], list[RawRewardRecord]]:
    """Fetch all markets currently eligible for liquidity rewards.

    Replicates scanner.get_rewarded_markets() with async I/O.
    Applies the same three filters:
      1. pool_diario >= MIN_DAILY_REWARD
      2. min_size    <= MAX_MIN_SIZE
      3. num_makers  <= MAX_MAKERS

    Returns:
        markets  — parsed RewardMarket objects sorted by score DESC
        records  — RawRewardRecord DTOs ready for bronze-layer persistence
    """
    markets: list[RewardMarket]     = []
    records: list[RawRewardRecord]  = []

    async with httpx.AsyncClient() as client:
        # Step 1: get all currently rewarded markets
        reward_list = await _get(client, f"{CLOB_BASE_URL}/rewards/markets/current")
        candidates  = reward_list.get("data", [])
        logger.info("rewards_fetched", total=len(candidates))

        for reward_data in candidates:
            condition_id = reward_data.get("condition_id")
            if not condition_id:
                continue

            # Pre-filter by pool size and min_size before extra API calls
            pool     = float(reward_data.get("total_daily_rate", 0))
            min_size = float(reward_data.get("rewards_min_size", 999))
            if pool < MIN_DAILY_REWARD:
                logger.debug("skip_low_pool", condition_id=condition_id, pool=pool)
                continue
            if min_size > MAX_MIN_SIZE:
                logger.debug("skip_high_min_size", condition_id=condition_id, min_size=min_size)
                continue

            try:
                # Step 2: get full market details
                market_data = await _get(client, f"{CLOB_BASE_URL}/markets/{condition_id}")

                if not market_data.get("active") or market_data.get("closed"):
                    continue
                if not market_data.get("accepting_orders"):
                    continue

                tokens   = market_data.get("tokens", [])
                token_id = tokens[0].get("token_id") if tokens else None
                if not token_id:
                    continue

                # Step 3: get order book to count makers and compute midpoint
                book_data  = await _get(
                    client,
                    f"{CLOB_BASE_URL}/book",
                    params={"token_id": token_id},
                )
                num_makers = len(book_data.get("bids", [])) + len(book_data.get("asks", []))
                if num_makers > MAX_MAKERS:
                    logger.debug("skip_too_many_makers",
                                 condition_id=condition_id, num_makers=num_makers)
                    continue

                # Build domain model
                market = _build_reward_market(reward_data, market_data, book_data)
                if market is None:
                    continue

                # Raw record for bronze layer — merge all three API responses
                raw_payload = {
                    "reward":  reward_data,
                    "market":  market_data,
                    "book":    book_data,
                }
                record = RawRewardRecord.from_market(market, raw_payload)

                markets.append(market)
                records.append(record)
                logger.info(
                    "market_qualified",
                    condition_id=condition_id,
                    question=market.question[:55],
                    pool=pool,
                    num_makers=num_makers,
                    score=market.score,
                )

            except httpx.HTTPStatusError as exc:
                logger.warning("http_error",
                               condition_id=condition_id,
                               status=exc.response.status_code)
                continue
            except Exception as exc:
                logger.warning("unexpected_error",
                               condition_id=condition_id, error=str(exc))
                continue

    # Sort by score DESC — same as original scanner.py
    markets.sort(key=lambda m: m.score, reverse=True)

    logger.info("ingestion_complete", qualified=len(markets))
    return markets, records
