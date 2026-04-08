"""Domain models for the Polymarket rewards pipeline.

Each dataclass maps directly to a concept in scanner.py:
  - RewardMarket   → a market eligible for liquidity rewards
  - RawRewardRecord → the bronze-layer DTO persisted to raw_rewards

Using dataclasses (not Pydantic) to keep the dependency footprint minimal
and because these models are pure data containers — no validation logic needed.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class RewardMarket:
    """A Polymarket market that is currently eligible for liquidity rewards.

    Fields mirror the output of scanner.get_rewarded_markets() exactly so that
    the ingestion layer and the original bot speak the same language.
    """
    condition_id: str
    token_id:     str
    question:     str
    pool_diario:  float   # total_daily_rate — USDC paid per day to all makers
    min_size:     float   # minimum order size to qualify for rewards
    max_spread:   float   # maximum spread allowed to qualify
    num_makers:   int     # current number of active makers in the order book
    midpoint:     float   # (best_bid + best_ask) / 2
    yes_price:    float
    no_price:     float
    score:        float   # pool_diario / max(num_makers, 1) — higher = less competition

    @property
    def roi_1h_usdc(self) -> float:
        """Estimated USDC rewards per hour if the bot entered this market."""
        return round(self.score / 24, 6)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RawRewardRecord:
    """Bronze-layer DTO — persisted as-is to raw_rewards.

    raw_json stores the complete API response so the bronze layer is always
    fully reprocessable without hitting the API again.
    """
    condition_id: str
    token_id:     str
    question:     str
    raw_json:     str        # JSON-serialised full API response
    fetched_at:   datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def from_market(cls, market: RewardMarket, raw_api_response: dict) -> RawRewardRecord:
        """Build a RawRewardRecord from a parsed RewardMarket and its raw API dict."""
        return cls(
            condition_id=market.condition_id,
            token_id=market.token_id,
            question=market.question,
            raw_json=json.dumps(raw_api_response),
        )
