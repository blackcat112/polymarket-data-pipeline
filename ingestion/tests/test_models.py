"""Unit tests for ingestion/models.py — no DB or network required."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

from ingestion.models import RawRewardRecord, RewardMarket


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_market() -> RewardMarket:
    return RewardMarket(
        condition_id="0xabc",
        token_id="0xtoken",
        question="Will BTC hit $100k?",
        pool_diario=240.0,
        min_size=10.0,
        max_spread=0.02,
        num_makers=4,
        midpoint=0.5,
        yes_price=0.51,
        no_price=0.49,
        score=48.0,
    )


# ---------------------------------------------------------------------------
# RewardMarket
# ---------------------------------------------------------------------------

class TestRewardMarket:
    def test_roi_1h_usdc_is_score_divided_by_24(self, sample_market: RewardMarket) -> None:
        assert sample_market.roi_1h_usdc == round(48.0 / 24, 6)

    def test_to_dict_contains_all_fields(self, sample_market: RewardMarket) -> None:
        d = sample_market.to_dict()
        assert d["condition_id"] == "0xabc"
        assert d["score"] == 48.0
        assert len(d) == 11  # one key per dataclass field

    def test_roi_zero_when_score_is_zero(self, sample_market: RewardMarket) -> None:
        sample_market.score = 0.0
        assert sample_market.roi_1h_usdc == 0.0


# ---------------------------------------------------------------------------
# RawRewardRecord
# ---------------------------------------------------------------------------

class TestRawRewardRecord:
    def test_from_market_serialises_raw_json(self, sample_market: RewardMarket) -> None:
        raw_api = {"total_daily_rate": 240.0, "extra": "data"}
        record = RawRewardRecord.from_market(sample_market, raw_api)

        assert record.condition_id == sample_market.condition_id
        assert record.token_id == sample_market.token_id
        assert record.question == sample_market.question
        assert json.loads(record.raw_json) == raw_api

    def test_fetched_at_defaults_to_now(self, sample_market: RewardMarket) -> None:
        record = RawRewardRecord.from_market(sample_market, {})
        assert isinstance(record.fetched_at, datetime)

    def test_raw_json_is_valid_json_string(self, sample_market: RewardMarket) -> None:
        payload = {"key": "value", "num": 42}
        record = RawRewardRecord.from_market(sample_market, payload)
        # Must not raise
        parsed = json.loads(record.raw_json)
        assert parsed["num"] == 42
