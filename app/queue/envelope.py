from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


StreamType = Literal["index", "option", "orderbook", "greeks", "ohlcv"]


class EventEnvelope(BaseModel):
    stream: StreamType
    key: str
    payload: dict[str, Any]
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
