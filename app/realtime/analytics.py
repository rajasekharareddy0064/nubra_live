from __future__ import annotations

import math
from collections import deque
from collections.abc import MutableSequence
from typing import Any

import numpy as np
import pandas as pd


NIFTY50_WEIGHTS: dict[str, float] = {
    "HDFCBANK": 13.3,
    "RELIANCE": 8.8,
    "ICICIBANK": 8.4,
    "INFY": 5.1,
}

NIFTY50_SECTORS: dict[str, str] = {
    "HDFCBANK": "Financial Services",
    "ICICIBANK": "Financial Services",
    "RELIANCE": "Energy",
    "INFY": "Information Technology",
}

TRADING_3M_CANDLES_PER_DAY = 75
TRADING_DAYS_PER_YEAR = 252
ANNUALIZATION_FACTOR = float(np.sqrt(TRADING_3M_CANDLES_PER_DAY * TRADING_DAYS_PER_YEAR))


def _f(value: Any) -> float:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _valid_price(value: Any) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return _f(value)


def _round(value: Any, digits: int = 4) -> float | None:
    if value is None:
        return None
    number = _f(value)
    return round(number, digits)


def _leg_oi(leg: dict[str, Any] | None) -> float:
    if not isinstance(leg, dict):
        return 0.0
    return _f(leg.get("open_interest") if leg.get("open_interest") is not None else leg.get("oi"))


def _leg_volume(leg: dict[str, Any] | None) -> float:
    if not isinstance(leg, dict):
        return 0.0
    return _f(leg.get("volume") if leg.get("volume") is not None else leg.get("traded_volume"))


def _leg_gamma(leg: dict[str, Any] | None) -> float:
    if not isinstance(leg, dict):
        return 0.0
    return _f(leg.get("gamma"))


def _leg_iv(leg: dict[str, Any] | None) -> float | None:
    if not isinstance(leg, dict):
        return None
    if leg.get("iv") is None:
        return None
    return _f(leg.get("iv"))


def _stock_move(candle: dict[str, Any]) -> int:
    open_price = _f(candle.get("open"))
    close_price = _f(candle.get("close"))
    if open_price <= 0 or close_price <= 0:
        return 0
    if close_price > open_price:
        return 1
    if close_price < open_price:
        return -1
    return 0


def _underlying(symbol: str, candle: dict[str, Any]) -> str:
    value = candle.get("underlying_symbol")
    if value:
        return str(value).strip().upper()
    return str(symbol or "").strip().upper()


def compute_breadth(stocks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    advancing = 0
    declining = 0
    unchanged = 0

    for candle in stocks.values():
        if not isinstance(candle, dict):
            continue
        move = _stock_move(candle)
        if move > 0:
            advancing += 1
        elif move < 0:
            declining += 1
        else:
            unchanged += 1

    total = advancing + declining + unchanged
    adr = (advancing / declining) if declining > 0 else None
    breadth_percent = (advancing / total * 100.0) if total > 0 else 0.0

    return {
        "advancing_stocks": advancing,
        "declining_stocks": declining,
        "unchanged_stocks": unchanged,
        "advance_decline_ratio": _round(adr),
        "breadth_percent": round(breadth_percent, 4),
    }


def compute_participation(
    stocks: dict[str, dict[str, Any]],
    breadth_percent: float,
) -> dict[str, Any]:
    sector_totals: dict[str, float] = {}
    sector_advancing: dict[str, float] = {}
    covered_weight = 0.0
    advancing_weight = 0.0

    for symbol, candle in stocks.items():
        if not isinstance(candle, dict):
            continue
        underlying = _underlying(symbol, candle)
        weight = NIFTY50_WEIGHTS.get(underlying)
        if weight is None:
            continue

        covered_weight += weight
        sector = NIFTY50_SECTORS.get(underlying, "Other")
        sector_totals[sector] = sector_totals.get(sector, 0.0) + weight

        if _stock_move(candle) > 0:
            advancing_weight += weight
            sector_advancing[sector] = sector_advancing.get(sector, 0.0) + weight

    weighted_breadth = (advancing_weight / covered_weight * 100.0) if covered_weight > 0 else 0.0
    sector_strength = {
        sector: round((sector_advancing.get(sector, 0.0) / total * 100.0), 4)
        for sector, total in sorted(sector_totals.items())
        if total > 0
    }
    participation_score = (weighted_breadth + breadth_percent) / 2.0

    return {
        "participation_score": round(participation_score, 4),
        "weighted_breadth": round(weighted_breadth, 4),
        "weighted_participation": round(weighted_breadth, 4),
        "sector_participation": sector_strength,
        "sector_strength": sector_strength,
        "weights_used": dict(NIFTY50_WEIGHTS),
    }


def compute_options_flow(chain: list[dict[str, Any]]) -> dict[str, Any]:
    total_call_oi = 0.0
    total_put_oi = 0.0
    call_volume = 0.0
    put_volume = 0.0
    max_call_oi = -1.0
    max_put_oi = -1.0
    call_wall: int | None = None
    put_wall: int | None = None

    for row in chain:
        if not isinstance(row, dict):
            continue
        strike = int(_f(row.get("strike")))
        ce = row.get("CE") if isinstance(row.get("CE"), dict) else None
        pe = row.get("PE") if isinstance(row.get("PE"), dict) else None
        ce_oi = _leg_oi(ce)
        pe_oi = _leg_oi(pe)

        total_call_oi += ce_oi
        total_put_oi += pe_oi
        call_volume += _leg_volume(ce)
        put_volume += _leg_volume(pe)

        if ce_oi > max_call_oi:
            max_call_oi = ce_oi
            call_wall = strike if strike > 0 else None
        if pe_oi > max_put_oi:
            max_put_oi = pe_oi
            put_wall = strike if strike > 0 else None

    return {
        "total_call_oi": round(total_call_oi, 4),
        "total_put_oi": round(total_put_oi, 4),
        "call_volume": round(call_volume, 4),
        "put_volume": round(put_volume, 4),
        "call_put_volume_ratio": _round(call_volume / put_volume) if put_volume > 0 else None,
        "call_wall": call_wall if max_call_oi > 0 else None,
        "put_wall": put_wall if max_put_oi > 0 else None,
        "support_level": put_wall if max_put_oi > 0 else None,
        "resistance_level": call_wall if max_call_oi > 0 else None,
    }


def compute_gex(chain: list[dict[str, Any]]) -> dict[str, Any]:
    strike_gex: list[tuple[int, float]] = []
    total_ce_gex = 0.0
    total_pe_gex = 0.0

    for row in chain:
        if not isinstance(row, dict):
            continue
        strike = int(_f(row.get("strike")))
        ce = row.get("CE") if isinstance(row.get("CE"), dict) else None
        pe = row.get("PE") if isinstance(row.get("PE"), dict) else None
        ce_gex = _leg_gamma(ce) * _leg_oi(ce)
        pe_gex = _leg_gamma(pe) * _leg_oi(pe)
        total_ce_gex += ce_gex
        total_pe_gex += pe_gex
        if strike > 0:
            strike_gex.append((strike, ce_gex - pe_gex))

    net_gex = total_ce_gex - total_pe_gex
    if net_gex > 0:
        gex_state = "Positive Gamma"
    elif net_gex < 0:
        gex_state = "Negative Gamma"
    else:
        gex_state = "Neutral Gamma"

    cumulative = 0.0
    previous_cumulative: float | None = None
    gamma_flip: int | None = None
    for strike, gex in sorted(strike_gex):
        cumulative += gex
        if cumulative == 0:
            gamma_flip = strike
            break
        if previous_cumulative is not None and previous_cumulative * cumulative < 0:
            gamma_flip = strike
            break
        previous_cumulative = cumulative

    return {
        "net_gex": round(net_gex, 4),
        "gex_state": gex_state,
        "gamma_flip": gamma_flip,
    }


def compute_iv(chain: list[dict[str, Any]]) -> dict[str, Any]:
    all_iv: list[float] = []
    call_iv: list[float] = []
    put_iv: list[float] = []
    atm_values: list[float] = []

    for row in chain:
        if not isinstance(row, dict):
            continue
        ce_iv = _leg_iv(row.get("CE") if isinstance(row.get("CE"), dict) else None)
        pe_iv = _leg_iv(row.get("PE") if isinstance(row.get("PE"), dict) else None)
        if ce_iv is not None:
            all_iv.append(ce_iv)
            call_iv.append(ce_iv)
            if row.get("is_atm"):
                atm_values.append(ce_iv)
        if pe_iv is not None:
            all_iv.append(pe_iv)
            put_iv.append(pe_iv)
            if row.get("is_atm"):
                atm_values.append(pe_iv)

    atm_iv = sum(atm_values) / len(atm_values) if atm_values else None
    average_iv = sum(all_iv) / len(all_iv) if all_iv else None
    avg_call_iv = sum(call_iv) / len(call_iv) if call_iv else None
    avg_put_iv = sum(put_iv) / len(put_iv) if put_iv else None
    iv_skew = (avg_put_iv - avg_call_iv) if avg_call_iv is not None and avg_put_iv is not None else None

    return {
        "atm_iv": _round(atm_iv),
        "average_iv": _round(average_iv),
        "iv_skew": _round(iv_skew),
    }


class _RollingStdWindow:
    def __init__(self, maxlen: int) -> None:
        self._values: deque[float] = deque(maxlen=maxlen)
        self._sum = 0.0
        self._sum_sq = 0.0

    def append(self, value: float) -> None:
        if len(self._values) == self._values.maxlen:
            old = self._values.popleft()
            self._sum -= old
            self._sum_sq -= old * old
        self._values.append(value)
        self._sum += value
        self._sum_sq += value * value

    def std(self) -> float | None:
        count = len(self._values)
        if count < 2:
            return None
        mean = self._sum / count
        variance = max((self._sum_sq / count) - (mean * mean), 0.0)
        return math.sqrt(variance)


class VolatilityService:
    """Rolling RV/VRP calculator updated once per completed 3-minute NIFTY candle."""

    def __init__(self) -> None:
        self.close_windows: dict[int, deque[float]] = {
            20: deque(maxlen=20),
            50: deque(maxlen=50),
            100: deque(maxlen=100),
        }
        self._return_windows: dict[int, _RollingStdWindow] = {
            20: _RollingStdWindow(20),
            50: _RollingStdWindow(50),
            100: _RollingStdWindow(100),
        }
        self._last_close: float | None = None
        self._last_snapshot: dict[str, Any] = self._empty_snapshot()

    def update(self, nifty_close: Any, atm_iv: float | None) -> dict[str, Any]:
        close = _valid_price(nifty_close)
        if close > 0:
            for window in self.close_windows.values():
                window.append(close)
            if self._last_close and self._last_close > 0:
                log_return = math.log(close / self._last_close)
                for window in self._return_windows.values():
                    window.append(log_return)
            self._last_close = close

        rv20 = self._annualized_rv(20)
        rv50 = self._annualized_rv(50)
        rv100 = self._annualized_rv(100)
        vrp20 = self._vrp(atm_iv, rv20)
        vrp50 = self._vrp(atm_iv, rv50)
        vrp100 = self._vrp(atm_iv, rv100)

        self._last_snapshot = {
            "rv20": _round(rv20),
            "rv50": _round(rv50),
            "rv100": _round(rv100),
            "atm_iv": _round(atm_iv),
            "vrp20": _round(vrp20),
            "vrp50": _round(vrp50),
            "vrp100": _round(vrp100),
            "volatility_regime": self._volatility_regime(rv20),
            "vrp_state": self._vrp_state(vrp20),
        }
        return dict(self._last_snapshot)

    def snapshot(self) -> dict[str, Any]:
        return dict(self._last_snapshot)

    def _annualized_rv(self, window: int) -> float | None:
        stdev = self._return_windows[window].std()
        if stdev is None:
            return None
        # Convert decimal log-return volatility to percentage points to match IV units.
        return stdev * ANNUALIZATION_FACTOR * 100.0

    @staticmethod
    def _vrp(atm_iv: float | None, rv: float | None) -> float | None:
        if atm_iv is None or rv is None:
            return None
        return atm_iv - rv

    @staticmethod
    def _vrp_state(vrp: float | None) -> str:
        if vrp is None:
            return "Insufficient Data"
        if vrp > 3:
            return "Options Expensive"
        if vrp < -3:
            return "Options Cheap"
        return "Fair Value"

    @staticmethod
    def _volatility_regime(rv20: float | None) -> str:
        if rv20 is None:
            return "Insufficient Data"
        if rv20 < 10:
            return "Low Volatility"
        if rv20 <= 18:
            return "Normal Volatility"
        return "High Volatility"

    @staticmethod
    def _empty_snapshot() -> dict[str, Any]:
        return {
            "rv20": None,
            "rv50": None,
            "rv100": None,
            "atm_iv": None,
            "vrp20": None,
            "vrp50": None,
            "vrp100": None,
            "volatility_regime": "Insufficient Data",
            "vrp_state": "Insufficient Data",
        }


def compute_market_regime(
    breadth_percent: float,
    weighted_participation: float,
    net_gex: float,
    volatility: dict[str, Any],
) -> dict[str, Any]:
    rv20 = volatility.get("rv20")
    vrp20 = volatility.get("vrp20")
    market_regime = "Neutral"
    confidence_score = 50.0

    if rv20 is not None and rv20 < 10 and net_gex > 0:
        market_regime = "Range Market"
        confidence_score = 75.0
    elif rv20 is not None and rv20 > 18 and net_gex < 0:
        market_regime = "Expansion Market"
        confidence_score = 75.0

    if breadth_percent > 70 and weighted_participation > 65 and net_gex < 0:
        market_regime = "Bullish Expansion"
        confidence_score = min(100.0, (breadth_percent + weighted_participation) / 2.0)
    elif breadth_percent < 30 and weighted_participation < 35 and net_gex < 0:
        market_regime = "Bearish Expansion"
        confidence_score = min(100.0, (100.0 - breadth_percent + 100.0 - weighted_participation) / 2.0)
    elif 40 <= breadth_percent <= 60 and net_gex > 0:
        market_regime = "Range Market"
        confidence_score = max(0.0, 100.0 - abs(50.0 - breadth_percent) * 2.0)

    volatility_bonus = 0.0
    if rv20 is not None:
        if "Range" in market_regime and rv20 < 10 and net_gex > 0:
            volatility_bonus += 12.5
        elif "Expansion" in market_regime and rv20 > 18 and net_gex < 0:
            volatility_bonus += 12.5
    if vrp20 is not None:
        if vrp20 > 3 and "Range" in market_regime:
            volatility_bonus += 5.0
        elif vrp20 < -3 and "Expansion" in market_regime:
            volatility_bonus += 5.0

    return {
        "market_regime": market_regime,
        "confidence_score": round(min(100.0, confidence_score + volatility_bonus), 4),
    }


def build_analytics_snapshot(
    *,
    timestamp: str,
    options_chain: list[dict[str, Any]],
    stocks: dict[str, dict[str, Any]],
    nifty_close: Any,
    volatility_service: VolatilityService,
    timeline_matrix: MutableSequence[dict[str, Any]],
) -> dict[str, Any]:
    breadth = compute_breadth(stocks)
    participation = compute_participation(stocks, _f(breadth.get("breadth_percent")))
    options_flow = compute_options_flow(options_chain)
    gex = compute_gex(options_chain)
    iv = compute_iv(options_chain)
    volatility = volatility_service.update(nifty_close, iv.get("atm_iv"))
    market_regime = compute_market_regime(
        _f(breadth.get("breadth_percent")),
        _f(participation.get("weighted_participation")),
        _f(gex.get("net_gex")),
        volatility,
    )

    timeline_row = {
        "timestamp": timestamp,
        "breadth_percent": breadth.get("breadth_percent"),
        "weighted_participation": participation.get("weighted_participation"),
        "net_gex": gex.get("net_gex"),
        "gamma_flip": gex.get("gamma_flip"),
        "atm_iv": iv.get("atm_iv"),
        "rv20": volatility.get("rv20"),
        "vrp": volatility.get("vrp20"),
        "market_regime": market_regime.get("market_regime"),
    }
    timeline_matrix.append(timeline_row)

    return {
        "breadth": breadth,
        "participation": participation,
        "options_flow": options_flow,
        "gex": {
            "net_gex": gex.get("net_gex"),
            "gex_state": gex.get("gex_state"),
            "gamma_flip": gex.get("gamma_flip"),
        },
        "iv": iv,
        "volatility": volatility,
        "market_regime": market_regime,
        "timeline_matrix": list(timeline_matrix),
    }
