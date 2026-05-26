from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MarketStateStore:
    """Latest normalized ticks for API and 3m summaries (no DB)."""

    nifty_index: dict[str, Any] = field(default_factory=dict)
    nifty_futures: dict[str, Any] = field(default_factory=dict)
    futures: dict[str, dict[str, Any]] = field(default_factory=dict)
    stock_futures: dict[str, dict[str, Any]] = field(default_factory=dict)
    stock_futures_by_underlying: dict[str, dict[str, Any]] = field(default_factory=dict)
    options_by_strike: dict[int, dict[str, Any]] = field(default_factory=dict)
    option_chain_row: dict[str, Any] = field(default_factory=dict)
    last_option_totals: dict[str, float] = field(default_factory=dict)
    # ATM-centered option-chain view reconstructed from
    # ``options_by_strike`` by :class:`app.realtime.options_chain.OptionsChainBuilder`
    # on every NIFTY tick. Read by the 3-min scheduler and the
    # ``/realtime/candles/current`` REST endpoint.
    option_chain_view: list[dict[str, Any]] = field(default_factory=list)
    option_metrics: dict[str, Any] = field(default_factory=dict)

    def snapshot(self) -> dict[str, Any]:
        return {
            "nifty_index": dict(self.nifty_index),
            "nifty_futures": dict(self.nifty_futures),
            "futures": {k: dict(v) for k, v in self.futures.items()},
            "stock_futures": {k: dict(v) for k, v in self.stock_futures.items()},
            "stock_futures_by_underlying": {k: dict(v) for k, v in self.stock_futures_by_underlying.items()},
            "options_by_strike": {str(k): v for k, v in sorted(self.options_by_strike.items())},
            "option_chain": dict(self.option_chain_row),
            "option_chain_view": list(self.option_chain_view),
            "option_metrics": dict(self.option_metrics),
        }
