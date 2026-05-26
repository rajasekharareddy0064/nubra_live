from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class OHLCV:
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float = 0.0
    cum_volume: float = 0.0
    oi: float | None = None
    tick_count: int = 0

    def update(
        self,
        price: float,
        volume: float = 0.0,
        oi: float | None = None,
        cum_volume: float | None = None,
    ) -> None:
        if self.open is None:
            self.open = price
            self.high = price
            self.low = price
        else:
            self.high = max(self.high or price, price)
            self.low = min(self.low or price, price)
        self.close = price
        self.volume += volume
        if cum_volume is not None:
            self.cum_volume = float(cum_volume)
        if oi is not None:
            # OI is point-in-time (non-cumulative), keep latest value.
            self.oi = oi
        self.tick_count += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "cum_volume": self.cum_volume,
            "oi": self.oi,
            "tick_count": self.tick_count,
            "is_empty": self.tick_count == 0,
        }

    def reset(self) -> None:
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.volume = 0.0
        self.cum_volume = 0.0
        self.oi = None
        self.tick_count = 0


@dataclass
class CandleBoard:
    """In-memory OHLCV for index, NIFTY fut, and per-symbol stock futures."""

    nifty: OHLCV = field(default_factory=OHLCV)
    nifty_futures: OHLCV = field(default_factory=OHLCV)
    futures: dict[str, OHLCV] = field(default_factory=dict)
    stock_futures: dict[str, OHLCV] = field(default_factory=dict)

    def reset_all(self) -> None:
        self.nifty.reset()
        self.nifty_futures.reset()
        for c in self.futures.values():
            c.reset()
        for c in self.stock_futures.values():
            c.reset()

    def ensure_futures(self, symbol: str) -> OHLCV:
        if symbol not in self.futures:
            self.futures[symbol] = OHLCV()
        return self.futures[symbol]

    def ensure_stock(self, symbol: str) -> OHLCV:
        if symbol not in self.stock_futures:
            self.stock_futures[symbol] = OHLCV()
        return self.stock_futures[symbol]
