from __future__ import annotations

import asyncio
from typing import Any


def _f(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


class NiftyOhlcAggregator:
    """Aggregates NIFTY spot ticks for the active 3-minute candle bucket."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._open: float | None = None
        self._high: float | None = None
        self._low: float | None = None
        self._close: float | None = None
        self._bucket_start_volume: float | None = None
        self._latest_volume: float | None = None
        self._change_pct: Any = None
        self._tick_count = 0
        self._first_timestamp: Any = None
        self._last_timestamp: Any = None

    async def update(
        self,
        *,
        ltp: Any,
        volume: Any,
        change_pct: Any,
        timestamp: Any,
    ) -> None:
        price = _f(ltp)
        if price <= 0:
            return

        current_volume = _f(volume) if volume is not None else None
        async with self._lock:
            if self._open is None:
                self._open = price
                self._high = price
                self._low = price
                self._bucket_start_volume = current_volume
                self._first_timestamp = timestamp
            else:
                self._high = max(self._high or price, price)
                self._low = min(self._low or price, price)

            self._close = price
            if current_volume is not None:
                self._latest_volume = current_volume
            if change_pct is not None:
                self._change_pct = change_pct
            self._last_timestamp = timestamp
            self._tick_count += 1

    async def snapshot_and_reset(self) -> dict[str, Any]:
        async with self._lock:
            snapshot = self._snapshot_locked()
            self._reset_locked()
            return snapshot

    def _snapshot_locked(self) -> dict[str, Any]:
        volume = 0.0
        if self._latest_volume is not None and self._bucket_start_volume is not None:
            volume = max(self._latest_volume - self._bucket_start_volume, 0.0)

        return {
            "open": self._open,
            "high": self._high,
            "low": self._low,
            "close": self._close,
            "volume": volume,
            "change_pct": self._change_pct,
            "tick_count": self._tick_count,
            "first_timestamp": self._first_timestamp,
            "last_timestamp": self._last_timestamp,
        }

    def _reset_locked(self) -> None:
        self._open = None
        self._high = None
        self._low = None
        self._close = None
        self._bucket_start_volume = None
        self._latest_volume = None
        self._change_pct = None
        self._tick_count = 0
        self._first_timestamp = None
        self._last_timestamp = None
