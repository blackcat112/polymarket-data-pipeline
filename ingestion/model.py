from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class MarketOutcome:
    outcome_id: str
    title: str
    price: float
    volume: float


@dataclass
class Market:
    market_id: str
    question: str
    outcomes: list[MarketOutcome]
    volume_24h: float
    active: bool
    end_date: Optional[datetime]
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class RawMarketRecord:
    market_id: str
    question: str
    raw_json: str
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
