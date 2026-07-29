from __future__ import annotations

import asyncio
import logging
import math
import re
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from app.realtime.options_chain import STRIKE_RADIUS, STRIKE_STEP, get_atm_strike

logger = logging.getLogger(__name__)

# ATM ±10 at 50-pt step → 21 strike rows in websocket + DB.
MIN_EMIT_STRIKES = 21
EMIT_RADIUS = 10


def _f(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _repr_field(value: str, field: str) -> float:
    match = re.search(rf"{field}\s*=\s*([0-9.]+)", value)
    return _f(match.group(1)) if match else 0.0


def _level_value(level: Any, *keys: str, tuple_index: int) -> float:
    if isinstance(level, str):
        if "quantity" in keys or "qty" in keys:
            return _repr_field(level, "quantity")
        return _repr_field(level, "price")
    if isinstance(level, dict):
        lower = {str(k).lower(): v for k, v in level.items()}
        for key in keys:
            value = lower.get(key.lower())
            if value is not None:
                return _f(value)
    if isinstance(level, (list, tuple)):
        # Common SDK shape is [price, quantity, orders].
        if len(level) > tuple_index:
            return _f(level[tuple_index])
    return 0.0


def _levels_quantity(levels: list[Any], *keys: str) -> float:
    if not levels:
        return 0.0
    return sum(_level_value(level, *keys, tuple_index=1) for level in levels[:5])


def _best_level_price(levels: list[Any], *keys: str) -> float:
    if not levels:
        return 0.0
    return _level_value(levels[0], *keys, tuple_index=0)


def _pick(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if payload.get(key) is not None:
            return payload.get(key)
    return None


def _scale_price(value: float, price_scale: float) -> float:
    if value <= 0:
        return 0.0
    return value / price_scale if price_scale > 0 else value


def _normalize_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"CE", "CALL", "C"}:
        return "CE"
    if side in {"PE", "PUT", "P"}:
        return "PE"
    return side


def _normalize_trade_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"BUY", "B", "BID"}:
        return "BUY"
    if side in {"SELL", "S", "ASK", "OFFER"}:
        return "SELL"
    return side


@dataclass
class _SideAggregate:
    bid_qty_avg_sum: float = 0.0
    ask_qty_avg_sum: float = 0.0
    buy_qty_sum: float = 0.0
    sell_qty_sum: float = 0.0
    volume_sum: float = 0.0
    ask_removed_sum: float = 0.0
    bid_removed_sum: float = 0.0
    oi_change_sum: float = 0.0
    oi: float = 0.0
    spread_sum: float = 0.0
    spread_change_sum: float = 0.0
    spread_count: int = 0
    samples: int = 0
    last_bid_qty: float | None = None
    last_ask_qty: float | None = None
    last_spread: float | None = None

    def update(
        self,
        *,
        bid_qty: float,
        ask_qty: float,
        bid_price: float,
        ask_price: float,
        buy_qty: float,
        sell_qty: float,
        volume: float,
        oi: float,
        oi_change: float,
    ) -> None:
        if self.last_ask_qty is not None:
            self.ask_removed_sum += max(self.last_ask_qty - ask_qty, 0.0)
        if self.last_bid_qty is not None:
            self.bid_removed_sum += max(self.last_bid_qty - bid_qty, 0.0)
        self.bid_qty_avg_sum += bid_qty
        self.ask_qty_avg_sum += ask_qty
        self.buy_qty_sum += buy_qty
        self.sell_qty_sum += sell_qty
        self.volume_sum += volume
        if oi > 0:
            self.oi = oi
        self.oi_change_sum += oi_change
        if ask_price > 0 and bid_price > 0 and ask_price >= bid_price:
            spread = ask_price - bid_price
            if self.last_spread is not None:
                self.spread_change_sum += spread - self.last_spread
            self.spread_sum += spread
            self.spread_count += 1
            self.last_spread = spread
        self.last_bid_qty = bid_qty
        self.last_ask_qty = ask_qty
        self.samples += 1

    def to_dict(self) -> dict[str, Any]:
        exec_delta = self.buy_qty_sum - self.sell_qty_sum
        last_bid = self.last_bid_qty or 0.0
        last_ask = self.last_ask_qty or 0.0
        book_delta = last_bid - last_ask
        imbalance_denominator = last_bid + last_ask
        imbalance = (
            (last_bid - last_ask) / imbalance_denominator
            if imbalance_denominator > 0
            else 0.0
        )
        avg_spread = (self.spread_sum / self.spread_count) if self.spread_count else 0.0
        avg_bid = (self.bid_qty_avg_sum / self.samples) if self.samples else 0.0
        avg_ask = (self.ask_qty_avg_sum / self.samples) if self.samples else 0.0
        return {
            "bid_qty_sum": _round(last_bid) or 0.0,
            "ask_qty_sum": _round(last_ask) or 0.0,
            "last_bid_qty": _round(last_bid) or 0.0,
            "last_ask_qty": _round(last_ask) or 0.0,
            "buy_qty_sum": _round(self.buy_qty_sum) or 0.0,
            "sell_qty_sum": _round(self.sell_qty_sum) or 0.0,
            "volume_sum": _round(self.volume_sum) or 0.0,
            "avg_bid_qty": _round(avg_bid) or 0.0,
            "avg_ask_qty": _round(avg_ask) or 0.0,
            "total_buy_qty": _round(self.buy_qty_sum) or 0.0,
            "total_sell_qty": _round(self.sell_qty_sum) or 0.0,
            "exec_delta": _round(exec_delta) or 0.0,
            "book_delta": _round(book_delta) or 0.0,
            "delta": _round(exec_delta) or 0.0,
            "imbalance": _round(imbalance) or 0.0,
            "ask_removed": _round(self.ask_removed_sum) or 0.0,
            "bid_removed": _round(self.bid_removed_sum) or 0.0,
            "avg_spread": _round(avg_spread) or 0.0,
            "spread_change": _round(self.spread_change_sum) or 0.0,
            "has_data": self.samples > 0,
            "tick_count": self.samples,
        }


@dataclass
class _StrikeAggregate:
    ce: _SideAggregate = field(default_factory=_SideAggregate)
    pe: _SideAggregate = field(default_factory=_SideAggregate)


class OrderBookAggregator:
    """Aggregates option orderbook updates for breakout detection.

    All internal strike keys are stored and compared in RUPEES regardless
    of the wire domain (paise on PROD, rupees on UAT). The price_scale
    from the pipeline converts wire strikes to rupees on every tick.
    """

    def __init__(
        self,
        *,
        strike_radius: int = 10,
        emit_radius: int = EMIT_RADIUS,
        strike_step: int = STRIKE_STEP,
    ) -> None:
        self._strike_radius = strike_radius
        self._emit_radius = emit_radius
        self._strike_step = strike_step  # always in RUPEES (50)
        self._atm: int | None = None           # stored in RUPEES
        self._strikes: dict[int, _StrikeAggregate] = {}  # keys in RUPEES
        self._spread_history: deque[float] = deque(maxlen=30)
        self._exec_delta_history: deque[float] = deque(maxlen=30)
        self._ask_removed_history: deque[float] = deque(maxlen=30)
        self._liquidity_delta_history: deque[float] = deque(maxlen=30)
        self._book_delta_history: deque[float] = deque(maxlen=30)
        self._cum_exec_delta_30_history: deque[float] = deque(maxlen=30)
        self._cum_book_delta_30_history: deque[float] = deque(maxlen=30)
        self._strike_shift_history: deque[float] = deque(maxlen=30)
        self._rolling_exec_delta_30: deque[float] = deque(maxlen=10)
        self._rolling_book_delta_30: deque[float] = deque(maxlen=10)
        self._rolling_ask_removed_30: deque[float] = deque(maxlen=10)
        self._last_activity_strike: int | None = None
        self._lock = asyncio.Lock()

    def _effective_emit_radius(self) -> int:
        # ATM ±r produces 2r+1 strikes; keep at least MIN_EMIT_STRIKES rows.
        min_radius = math.ceil((MIN_EMIT_STRIKES - 1) / 2)
        return max(self._emit_radius, min_radius)

    async def update_option(
        self,
        *,
        atm_source: Any,
        strike: int,
        option_type: str,
        payload: dict[str, Any],
        bids: list[Any],
        asks: list[Any],
        price_scale: float = 1.0,
        bucket_id: str | None = None,
    ) -> None:
        # atm_source is already in RUPEES (pipeline._to_rupees converts it).
        # strike comes from InstrumentManager.get_ref_maps() which already divides
        # by _strike_scale — so it is ALSO in RUPEES. No further conversion needed.
        strike_rupees = int(round(float(strike)))
        candidate_atm = get_atm_strike(float(atm_source or 0), step=self._strike_step)
        side = _normalize_side(option_type)
        if side not in {"CE", "PE"}:
            return

        bid_qty = _f(_pick(payload, "bid_qty", "bidQty", "best_bid_qty", "bestBidQty"))
        ask_qty = _f(_pick(payload, "ask_qty", "askQty", "best_ask_qty", "bestAskQty"))
        bid_price = _f(_pick(payload, "bid_price", "bidPrice", "best_bid_price", "bestBidPrice"))
        ask_price = _f(_pick(payload, "ask_price", "askPrice", "best_ask_price", "bestAskPrice"))

        if bid_qty <= 0:
            bid_qty = _levels_quantity(bids, "quantity", "qty", "bid_qty", "bidQty", "size")
        if ask_qty <= 0:
            ask_qty = _levels_quantity(asks, "quantity", "qty", "ask_qty", "askQty", "size")
        if bid_price <= 0:
            bid_price = _best_level_price(bids, "price", "bid_price", "bidPrice")
        if ask_price <= 0:
            ask_price = _best_level_price(asks, "price", "ask_price", "askPrice")
        bid_price = _scale_price(bid_price, price_scale)
        ask_price = _scale_price(ask_price, price_scale)

        traded_qty = _f(_pick(payload, "traded_qty", "tradedQty", "last_traded_quantity", "lastTradedQuantity", "ltq"))
        volume = _f(_pick(payload, "volume", "traded_volume", "tradedVolume", "total_volume", "totalVolume"))
        if volume <= 0:
            volume = traded_qty
        buy_qty = _f(_pick(payload, "buy_qty", "buyQty", "total_buy_qty", "totalBuyQty"))
        sell_qty = _f(_pick(payload, "sell_qty", "sellQty", "total_sell_qty", "totalSellQty"))
        oi = _f(_pick(payload, "oi", "open_interest", "openInterest"))
        oi_change = _f(_pick(payload, "oi_change", "oiChange", "open_interest_change", "openInterestChange"))
        trade_side = _normalize_trade_side(_pick(payload, "trade_side", "tradeSide", "side"))
        inferred_trade_side = False
        if buy_qty <= 0 and sell_qty <= 0:
            if trade_side == "BUY":
                buy_qty = traded_qty
            elif trade_side == "SELL":
                sell_qty = traded_qty
            else:
                inferred_trade_side = traded_qty > 0
                buy_qty = traded_qty if bid_qty >= ask_qty else 0.0
                sell_qty = traded_qty if ask_qty > bid_qty else 0.0

        async with self._lock:
            if candidate_atm is not None:
                self._atm = candidate_atm
            atm = self._atm or candidate_atm
            selected = bool(
                atm is not None
                and strike_rupees > 0
                and abs(strike_rupees - atm) <= self._strike_radius * self._strike_step
            )
            if not selected:
                self._log_update(
                    bucket_id=bucket_id,
                    atm=atm,
                    strike=strike_rupees,
                    option_type=side,
                    selected=False,
                    bid_qty=bid_qty,
                    ask_qty=ask_qty,
                    traded_qty=traded_qty,
                    buy_qty=buy_qty,
                    sell_qty=sell_qty,
                    current_delta=0.0,
                )
                return

            # Store aggregate keyed by rupee-strike
            strike_bucket = self._strikes.setdefault(strike_rupees, _StrikeAggregate())
            aggregate = strike_bucket.ce if side == "CE" else strike_bucket.pe
            aggregate.update(
                bid_qty=bid_qty,
                ask_qty=ask_qty,
                bid_price=bid_price,
                ask_price=ask_price,
                buy_qty=buy_qty,
                sell_qty=sell_qty,
                volume=volume,
                oi=oi,
                oi_change=oi_change,
            )
            if inferred_trade_side and traded_qty > 0:
                logger.debug(
                    "orderbook inferred trade side strike=%s side=%s traded=%s bid=%s ask=%s",
                    strike_rupees,
                    side,
                    traded_qty,
                    bid_qty,
                    ask_qty,
                )
            self._log_update(
                bucket_id=bucket_id,
                atm=atm,
                strike=strike_rupees,
                option_type=side,
                selected=True,
                bid_qty=bid_qty,
                ask_qty=ask_qty,
                traded_qty=traded_qty,
                buy_qty=buy_qty,
                sell_qty=sell_qty,
                current_delta=aggregate.buy_qty_sum - aggregate.sell_qty_sum,
            )

    async def snapshot_and_reset(
        self,
        *,
        atm_source: Any,
        options_by_strike: dict[Any, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        # atm_source is in RUPEES (from pipeline._last_nifty).
        atm = get_atm_strike(float(atm_source or 0), step=self._strike_step) or self._atm
        async with self._lock:
            snapshot = self._snapshot_locked(atm, options_by_strike or {})
            self._atm = None
            self._strikes = {}
            rows_with_data = sum(1 for r in snapshot.get("strikes", []) if r.get("has_data", True))
            total_rows = len(snapshot.get("strikes", []))
            logger.info(
                "SNAPSHOT_RESET | atm=%s strike_count=%d rows_with_data=%d is_empty=%s",
                atm,
                total_rows,
                rows_with_data,
                snapshot.get("is_empty", False),
            )
            return snapshot

    def _log_update(
        self,
        *,
        bucket_id: str | None,
        atm: int | None,
        strike: int,
        option_type: str,
        selected: bool,
        bid_qty: float,
        ask_qty: float,
        traded_qty: float,
        buy_qty: float,
        sell_qty: float,
        current_delta: float,
    ) -> None:
        logger.info(
            "order_book aggregate %s",
            {
                "bucket_id": bucket_id,
                "atm": atm,
                "strike": strike,
                "option_type": option_type,
                "selected": selected,
                "bid_qty": bid_qty,
                "ask_qty": ask_qty,
                "traded_qty": traded_qty,
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "current_delta": current_delta,
            },
        )

    def _snapshot_locked(
        self,
        atm: int | None,
        options_by_strike: dict[Any, dict[str, Any]],
    ) -> dict[str, Any]:
        selected_strikes = self._selected_strikes(atm, radius=self._effective_emit_radius())
        internal_strikes = self._selected_strikes(atm, radius=self._strike_radius)
        total_bid_qty = 0.0
        total_ask_qty = 0.0
        total_buy_qty = 0.0
        total_sell_qty = 0.0
        total_ask_removed = 0.0
        total_bid_removed = 0.0
        spread_sum = 0.0
        spread_count = 0
        # Per-side (CE/PE) accumulators. CE and PE are kept fully separate
        # here so they can be combined with their correct directional sign
        # (CE bullish, PE bearish) instead of being summed blindly.
        ce_buy = ce_sell = ce_bid = ce_ask = 0.0
        pe_buy = pe_sell = pe_bid = pe_ask = 0.0
        ce_ask_removed = ce_bid_removed = 0.0
        pe_ask_removed = pe_bid_removed = 0.0
        rows: list[dict[str, Any]] = []
        activity_by_strike: dict[int, float] = {}
        breakout_by_strike: dict[int, float] = {}

        for strike, aggregate in self._strikes.items():
            if strike in internal_strikes:
                activity_by_strike[strike] = self._activity_score(strike, atm, aggregate)
                breakout_by_strike[strike] = self._breakout_loading_score(strike, atm, aggregate)

        for strike in internal_strikes:
            aggregate = self._strikes.get(strike)
            if aggregate is None:
                continue
            ce = aggregate.ce
            pe = aggregate.pe
            ce_buy += ce.buy_qty_sum
            ce_sell += ce.sell_qty_sum
            ce_bid += ce.last_bid_qty or 0.0
            ce_ask += ce.last_ask_qty or 0.0
            ce_ask_removed += ce.ask_removed_sum
            ce_bid_removed += ce.bid_removed_sum
            pe_buy += pe.buy_qty_sum
            pe_sell += pe.sell_qty_sum
            pe_bid += pe.last_bid_qty or 0.0
            pe_ask += pe.last_ask_qty or 0.0
            pe_ask_removed += pe.ask_removed_sum
            pe_bid_removed += pe.bid_removed_sum
            for side in (ce, pe):
                spread_sum += side.spread_sum
                spread_count += side.spread_count

        # Gross totals (CE+PE) for summary / emptiness check.
        total_bid_qty = ce_bid + pe_bid
        total_ask_qty = ce_ask + pe_ask
        total_buy_qty = ce_buy + pe_buy
        total_sell_qty = ce_sell + pe_sell
        total_ask_removed = ce_ask_removed + pe_ask_removed
        total_bid_removed = ce_bid_removed + pe_bid_removed

        for strike in selected_strikes:
            aggregate = self._strikes.get(strike)
            if aggregate is None:
                rows.append(
                    {
                        "strike": strike,
                        "has_data": False,
                        "ce_has_data": False,
                        "pe_has_data": False,
                        "tick_count": 0,
                        "ce": {},
                        "pe": {},
                    }
                )
                continue
            ce_dict = aggregate.ce.to_dict()
            pe_dict = aggregate.pe.to_dict()
            ce_has_data = aggregate.ce.samples > 0
            pe_has_data = aggregate.pe.samples > 0
            rows.append(
                {
                    "strike": strike,
                    "has_data": ce_has_data or pe_has_data,
                    "ce_has_data": ce_has_data,
                    "pe_has_data": pe_has_data,
                    "tick_count": aggregate.ce.samples + aggregate.pe.samples,
                    "ce": ce_dict,
                    "pe": pe_dict,
                }
            )

        # ── Directional interpretation (CE bullish, PE bearish) ─────────
        # Raw per-side signed metrics. Retained as debug fields so future
        # classifiers (call/put buying vs writing, hedging, delta/gamma
        # hedging) need no further redesign.
        ce_exec_delta = ce_buy - ce_sell
        pe_exec_delta = pe_buy - pe_sell
        ce_book_delta = ce_bid - ce_ask
        pe_book_delta = pe_bid - pe_ask
        ce_imb_denom = ce_bid + ce_ask
        pe_imb_denom = pe_bid + pe_ask
        ce_imbalance = (ce_bid - ce_ask) / ce_imb_denom if ce_imb_denom > 0 else 0.0
        pe_imbalance = (pe_bid - pe_ask) / pe_imb_denom if pe_imb_denom > 0 else 0.0

        # Net market direction: CE contributes bullish, PE contributes bearish.
        #   net = CE side - PE side
        net_exec = ce_exec_delta - pe_exec_delta
        net_book_delta = ce_book_delta - pe_book_delta
        # Keep net_imbalance in [-1, 1] so the unchanged score formula
        # (which clips imbalance to [-1, 1]) behaves identically.
        net_imbalance = (ce_imbalance - pe_imbalance) / 2.0

        # Directional liquidity buckets:
        #   bullish = calls lifted (CE ask removed) + put bids pulled (PE bid removed)
        #   bearish = puts lifted  (PE ask removed) + call bids pulled (CE bid removed)
        bullish_liquidity = ce_ask_removed + pe_bid_removed
        bearish_liquidity = pe_ask_removed + ce_bid_removed

        # Informational directional pressures (debug only — NOT fed to score).
        bullish_exec = max(ce_exec_delta, 0.0) + max(-pe_exec_delta, 0.0)
        bearish_exec = max(pe_exec_delta, 0.0) + max(-ce_exec_delta, 0.0)
        bullish_pressure = bullish_exec + max(net_book_delta, 0.0) + bullish_liquidity
        bearish_pressure = bearish_exec + max(-net_book_delta, 0.0) + bearish_liquidity
        net_pressure = bullish_pressure - bearish_pressure

        # These NET values are what feed the (unchanged) breakout score,
        # rolling history and the exposed exec_delta / book_delta / imbalance.
        exec_delta = net_exec
        book_delta = net_book_delta
        imbalance = net_imbalance

        candle_has_data = (
            total_bid_qty > 0
            or total_buy_qty > 0
            or total_ask_removed > 0
            or total_bid_removed > 0
        )
        avg_spread = spread_sum / spread_count if spread_count else 0.0
        spread_zscore = self._zscore(avg_spread, self._spread_history)
        support_strike = self._oi_strike(options_by_strike, internal_strikes, "PE")
        resistance_strike = self._oi_strike(options_by_strike, internal_strikes, "CE")
        activity_strike = max(activity_by_strike, key=activity_by_strike.get) if activity_by_strike else None
        breakout_strike = max(breakout_by_strike, key=breakout_by_strike.get) if breakout_by_strike else None
        strike_shift = (
            float(activity_strike - self._last_activity_strike)
            if activity_strike is not None and self._last_activity_strike is not None
            else 0.0
        )

        self._rolling_exec_delta_30.append(exec_delta)
        self._rolling_book_delta_30.append(book_delta)
        self._rolling_ask_removed_30.append(bullish_liquidity)
        cum_exec_delta_30 = sum(self._rolling_exec_delta_30)
        cum_book_delta_30 = sum(self._rolling_book_delta_30)
        cum_ask_removed_30 = sum(self._rolling_ask_removed_30)

        breakout_score = self._breakout_score(
            exec_delta=exec_delta,
            book_delta=book_delta,
            ask_removed=bullish_liquidity,
            bid_removed=bearish_liquidity,
            imbalance=imbalance,
            cum_exec_delta_30=cum_exec_delta_30,
            cum_book_delta_30=cum_book_delta_30,
            strike_shift=strike_shift,
            spread_zscore=spread_zscore,
            activity_strike=activity_strike,
            breakout_strike=breakout_strike,
            resistance_strike=resistance_strike,
        )
        regime = self._regime(
            breakout_score=breakout_score,
        )

        self._append_metric_history(
            avg_spread=avg_spread,
            exec_delta=exec_delta,
            ask_removed=bullish_liquidity,
            bid_removed=bearish_liquidity,
            book_delta=book_delta,
            cum_exec_delta_30=cum_exec_delta_30,
            cum_book_delta_30=cum_book_delta_30,
            strike_shift=strike_shift,
        )
        if activity_strike is not None:
            self._last_activity_strike = activity_strike

        return {
            "atm": atm,
            "is_empty": not candle_has_data,
            "support_strike": support_strike,
            "resistance_strike": resistance_strike,
            "activity_strike": activity_strike,
            "breakout_strike": breakout_strike,
            "strike_shift": _round(strike_shift) or 0.0,
            # exec_delta / book_delta / imbalance / ask_removed / bid_removed
            # now carry NET MARKET DIRECTION (CE bullish − PE bearish),
            # not a blind CE+PE sum. Keys and types are unchanged.
            "exec_delta": _round(exec_delta) or 0.0,
            "book_delta": _round(book_delta) or 0.0,
            "imbalance": _round(imbalance) or 0.0,
            "ask_removed": _round(bullish_liquidity) or 0.0,
            "bid_removed": _round(bearish_liquidity) or 0.0,
            "spread_zscore": _round(spread_zscore) or 0.0,
            "cum_exec_delta_30": _round(cum_exec_delta_30) or 0.0,
            "cum_book_delta_30": _round(cum_book_delta_30) or 0.0,
            "cum_ask_removed_30": _round(cum_ask_removed_30) or 0.0,
            "active_strike": activity_strike,
            "active_strike_shift": _round(strike_shift) or 0.0,
            "breakout_score": _round(breakout_score, 2) or 0.0,
            "regime": regime,
            # ── Directional debug metrics (additive) ────────────────────
            # Raw per-side CE/PE metrics + net breakdown retained so future
            # classifiers (call/put buying vs writing, hedging, delta/gamma
            # hedging) need no further redesign. Existing DB writers read
            # only the fixed keys above, so these are safely ignored there.
            "directional": {
                "ce_exec_delta": _round(ce_exec_delta) or 0.0,
                "pe_exec_delta": _round(pe_exec_delta) or 0.0,
                "ce_book_delta": _round(ce_book_delta) or 0.0,
                "pe_book_delta": _round(pe_book_delta) or 0.0,
                "ce_imbalance": _round(ce_imbalance) or 0.0,
                "pe_imbalance": _round(pe_imbalance) or 0.0,
                "ce_ask_removed": _round(ce_ask_removed) or 0.0,
                "ce_bid_removed": _round(ce_bid_removed) or 0.0,
                "pe_ask_removed": _round(pe_ask_removed) or 0.0,
                "pe_bid_removed": _round(pe_bid_removed) or 0.0,
                "net_exec": _round(net_exec) or 0.0,
                "net_book_delta": _round(net_book_delta) or 0.0,
                "net_imbalance": _round(net_imbalance) or 0.0,
                "bullish_liquidity": _round(bullish_liquidity) or 0.0,
                "bearish_liquidity": _round(bearish_liquidity) or 0.0,
                "bullish_pressure": _round(bullish_pressure) or 0.0,
                "bearish_pressure": _round(bearish_pressure) or 0.0,
                "net_pressure": _round(net_pressure) or 0.0,
            },
            "summary": {
                "total_bid_qty": _round(total_bid_qty) or 0.0,
                "total_ask_qty": _round(total_ask_qty) or 0.0,
                "total_buy_qty": _round(total_buy_qty) or 0.0,
                "total_sell_qty": _round(total_sell_qty) or 0.0,
                "exec_delta": _round(exec_delta) or 0.0,
                "book_delta": _round(book_delta) or 0.0,
                "delta": _round(exec_delta) or 0.0,
                "imbalance": _round(imbalance) or 0.0,
                "ask_removed": _round(bullish_liquidity) or 0.0,
                "bid_removed": _round(bearish_liquidity) or 0.0,
                "gross_ask_removed": _round(total_ask_removed) or 0.0,
                "gross_bid_removed": _round(total_bid_removed) or 0.0,
                "avg_spread": _round(avg_spread) or 0.0,
                "spread_zscore": _round(spread_zscore) or 0.0,
            },
            "strikes": rows,
        }

    def _selected_strikes(self, atm: int | None, *, radius: int) -> list[int]:
        if atm is None:
            strikes = sorted(self._strikes)
            if len(strikes) >= MIN_EMIT_STRIKES:
                return strikes
            if strikes:
                center = strikes[len(strikes) // 2]
                return self._selected_strikes(center, radius=radius)
            return []
        return [
            int(atm + offset * self._strike_step)
            for offset in range(-radius, radius + 1)
        ]

    def _distance_weight(self, strike: int, atm: int | None) -> float:
        if atm is None:
            return 1.0
        distance_steps = abs(strike - atm) / max(self._strike_step, 1)
        return max(0.35, 1.0 - (0.06 * distance_steps))

    def _activity_score(self, strike: int, atm: int | None, aggregate: _StrikeAggregate) -> float:
        executed_qty = 0.0
        book_delta = 0.0
        volume = 0.0
        exec_delta = 0.0
        for side in (aggregate.ce, aggregate.pe):
            executed_qty += side.buy_qty_sum + side.sell_qty_sum
            last_bid = side.last_bid_qty or 0.0
            last_ask = side.last_ask_qty or 0.0
            book_delta += abs(last_bid - last_ask)
            volume += side.volume_sum
            exec_delta += abs(side.buy_qty_sum - side.sell_qty_sum)
        raw_score = executed_qty + book_delta + volume + exec_delta
        return raw_score * self._distance_weight(strike, atm)

    def _breakout_loading_score(self, strike: int, atm: int | None, aggregate: _StrikeAggregate) -> float:
        ask_removed = 0.0
        exec_delta = 0.0
        spread_change = 0.0
        for side in (aggregate.ce, aggregate.pe):
            ask_removed += side.ask_removed_sum
            exec_delta += max(side.buy_qty_sum - side.sell_qty_sum, 0.0)
            spread_change += max(side.spread_change_sum, 0.0)
        return (ask_removed + exec_delta + spread_change) * self._distance_weight(strike, atm)

    def _oi_strike(
        self,
        options_by_strike: dict[Any, dict[str, Any]],
        internal_strikes: list[int],
        side: str,
    ) -> int | None:
        max_oi = -1.0
        selected: int | None = None
        for strike in internal_strikes:
            legs = options_by_strike.get(strike) or options_by_strike.get(str(strike)) or {}
            leg = legs.get(side) if isinstance(legs, dict) else None
            oi = self._leg_oi(leg if isinstance(leg, dict) else None)
            if oi > max_oi:
                max_oi = oi
                selected = strike
        return selected if max_oi > 0 else None

    @staticmethod
    def _leg_oi(leg: dict[str, Any] | None) -> float:
        if not leg:
            return 0.0
        return _f(leg.get("open_interest") if leg.get("open_interest") is not None else leg.get("oi"))

    @staticmethod
    def _zscore(value: float, history: deque[float]) -> float:
        if len(history) < 2:
            return 0.0
        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std = math.sqrt(variance)
        if std <= 0:
            return 0.0
        return (value - mean) / std

    @staticmethod
    def _clip(value: float, low: float = -3.0, high: float = 3.0) -> float:
        return max(low, min(high, value))

    @staticmethod
    def _sigmoid(value: float) -> float:
        if value >= 0:
            z = math.exp(-value)
            return 1.0 / (1.0 + z)
        z = math.exp(value)
        return z / (1.0 + z)

    def _adaptive_component(self, value: float, history: deque[float]) -> float:
        scale = self._adaptive_scale(value, history)
        if scale <= 0:
            return 0.0
        return math.tanh(value / scale)

    @staticmethod
    def _adaptive_scale(value: float, history: deque[float]) -> float:
        abs_value = abs(value)
        if not history:
            return max(abs_value * 0.75, 1.0)
        mean_abs = sum(abs(x) for x in history) / len(history)
        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std = math.sqrt(variance)
        return max(std * 1.5, mean_abs * 0.50, abs_value * 0.35, 1.0)

    def _breakout_score(
        self,
        *,
        exec_delta: float,
        book_delta: float,
        ask_removed: float,
        bid_removed: float,
        imbalance: float,
        cum_exec_delta_30: float,
        cum_book_delta_30: float,
        strike_shift: float,
        spread_zscore: float,
        activity_strike: int | None,
        breakout_strike: int | None,
        resistance_strike: int | None,
    ) -> float:
        liquidity_total = ask_removed + bid_removed
        relative_liquidity = (ask_removed - bid_removed) / liquidity_total if liquidity_total > 0 else 0.0
        exec_threshold = self._execution_threshold(liquidity_total)
        exec_confirmed = exec_delta >= exec_threshold
        book_confirmed = book_delta > 0
        cum_exec_confirmed = cum_exec_delta_30 > 0

        pressure = (
            0.28 * self._adaptive_component(exec_delta, self._exec_delta_history)
            + 0.20 * self._adaptive_component(book_delta, self._book_delta_history)
            + 0.18 * self._adaptive_component(cum_exec_delta_30, self._cum_exec_delta_30_history)
            + 0.12 * self._adaptive_component(cum_book_delta_30, self._cum_book_delta_30_history)
            + 0.10 * self._clip(imbalance, -1.0, 1.0)
            + 0.07 * self._clip(relative_liquidity, -1.0, 1.0)
            + 0.03 * self._clip(spread_zscore / 3.0, -1.0, 1.0)
            + 0.02 * self._clip(abs(strike_shift) / 100.0, 0.0, 2.0)
        )
        alignment_bonus = 0.0
        if exec_confirmed and book_confirmed and activity_strike is not None and activity_strike == breakout_strike:
            alignment_bonus += 4.0
        if exec_confirmed and book_confirmed and breakout_strike is not None and breakout_strike == resistance_strike:
            alignment_bonus += 3.0
        if exec_confirmed and strike_shift > 0:
            alignment_bonus += min(abs(strike_shift) / 50.0, 3.0)
        score = 50.0 + (30.0 * pressure) + alignment_bonus

        if exec_delta <= 0:
            score = min(score - 15.0, 40.0)
        elif not exec_confirmed:
            weakness = 1.0 - max(exec_delta / exec_threshold, 0.0)
            score = min(score - (8.0 * weakness), 55.0)

        if not book_confirmed:
            score -= 4.0
        if not cum_exec_confirmed:
            score -= 6.0

        # A breakout regime needs execution, positive book pressure, and positive cumulative execution.
        if not (exec_confirmed and book_confirmed and cum_exec_confirmed):
            score = min(score, 69.99)
        return max(0.0, min(100.0, score))

    def _execution_threshold(self, liquidity_total: float) -> float:
        history = self._exec_delta_history
        positive_history = [abs(x) for x in history if x > 0]
        if positive_history:
            mean_abs = sum(positive_history) / len(positive_history)
            mean = sum(positive_history) / len(positive_history)
            variance = sum((x - mean) ** 2 for x in positive_history) / len(positive_history)
            std = math.sqrt(variance)
            adaptive = max(mean_abs * 0.35, std * 0.75)
        else:
            adaptive = 0.0
        liquidity_floor = liquidity_total * 0.005
        return max(5000.0, adaptive, liquidity_floor)

    @staticmethod
    def _regime(
        *,
        breakout_score: float,
    ) -> str:
        if breakout_score >= 80:
            return "CONFIRMED_BREAKOUT"
        if breakout_score >= 70:
            return "BREAK_ATTEMPT"
        if breakout_score >= 55:
            return "PRE_BREAKOUT"
        if breakout_score >= 40:
            return "LOADING"
        return "RANGE"

    def _append_metric_history(
        self,
        *,
        avg_spread: float,
        exec_delta: float,
        ask_removed: float,
        bid_removed: float,
        book_delta: float,
        cum_exec_delta_30: float,
        cum_book_delta_30: float,
        strike_shift: float,
    ) -> None:
        self._spread_history.append(avg_spread)
        self._exec_delta_history.append(exec_delta)
        self._ask_removed_history.append(ask_removed)
        liquidity_total = ask_removed + bid_removed
        self._liquidity_delta_history.append((ask_removed - bid_removed) / liquidity_total if liquidity_total > 0 else 0.0)
        self._book_delta_history.append(book_delta)
        self._cum_exec_delta_30_history.append(cum_exec_delta_30)
        self._cum_book_delta_30_history.append(cum_book_delta_30)
        self._strike_shift_history.append(strike_shift)
