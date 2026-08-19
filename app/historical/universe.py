"""Live-universe request groups for Nubra historical_data()."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

OHLC_FIELDS = ["open", "high", "low", "close", "cumulative_volume", "tick_volume"]
FUT_FIELDS = OHLC_FIELDS + ["cumulative_oi", "l1bid", "l1ask"]
OPT_FIELDS = FUT_FIELDS + ["theta", "delta", "gamma", "vega", "iv_mid"]


@dataclass
class UniverseGroup:
    """One historical_data() type + symbol list (batched later by the client)."""

    kind: str  # INDEX | STOCK | FUT | OPT
    exchange: str
    symbols: list[str]
    fields: list[str]
    meta: dict[str, Any] = field(default_factory=dict)


def build_universe(
    manager: Any,
    *,
    nifty_price: float | None = None,
    exchange: str = "NSE",
) -> list[UniverseGroup]:
    """Mirror the live WebSocket universe using master trading symbols."""
    exchange = str(exchange or "NSE").upper()
    groups: list[UniverseGroup] = []

    groups.append(
        UniverseGroup(
            kind="INDEX",
            exchange=exchange,
            symbols=["NIFTY"],
            fields=["open", "high", "low", "close", "cumulative_volume"],
        )
    )

    spots: list[str] = []
    getter = getattr(manager, "get_stock_spot_symbols", None)
    if callable(getter):
        spots = [str(s).strip().upper() for s in getter() if str(s).strip()]
    if spots:
        groups.append(
            UniverseGroup(
                kind="STOCK",
                exchange=exchange,
                symbols=sorted(set(spots)),
                fields=list(OHLC_FIELDS),
            )
        )

    fut_symbols: list[str] = []
    nifty_futs = getattr(manager, "get_nifty_fut_symbols", None)
    if callable(nifty_futs):
        fut_symbols.extend(str(s).strip().upper() for s in nifty_futs().values() if str(s).strip())
    stock_futs = getattr(manager, "get_stock_fut_trading_symbols", None)
    if callable(stock_futs):
        fut_symbols.extend(str(s).strip().upper() for s in stock_futs() if str(s).strip())
    fut_symbols = sorted({s for s in fut_symbols if s})
    if fut_symbols:
        groups.append(
            UniverseGroup(
                kind="FUT",
                exchange=exchange,
                symbols=fut_symbols,
                fields=list(FUT_FIELDS),
            )
        )

    legs: list[dict[str, Any]] = []
    opt_getter = getattr(manager, "get_atm_option_legs", None)
    if callable(opt_getter):
        legs = list(opt_getter(nifty_price) or [])
    opt_symbols: list[str] = []
    opt_meta: dict[str, dict[str, Any]] = {}
    for leg in legs:
        sym = str(leg.get("symbol") or "").strip().upper()
        if not sym:
            continue
        opt_symbols.append(sym)
        opt_meta[sym] = {
            "strike": int(leg.get("strike") or 0),
            "side": str(leg.get("side") or "").upper(),
            "ref_id": leg.get("ref_id"),
        }
    opt_symbols = sorted(set(opt_symbols))
    if opt_symbols:
        groups.append(
            UniverseGroup(
                kind="OPT",
                exchange=exchange,
                symbols=opt_symbols,
                fields=list(OPT_FIELDS),
                meta={"legs": opt_meta},
            )
        )
    return groups


def option_leg_map(groups: list[UniverseGroup]) -> dict[str, dict[str, Any]]:
    for group in groups:
        if group.kind == "OPT":
            legs = group.meta.get("legs") or {}
            return dict(legs)
    return {}
