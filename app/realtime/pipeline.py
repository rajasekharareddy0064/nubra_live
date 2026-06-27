from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import deque
from datetime import datetime
from typing import TYPE_CHECKING, Any

from app.core.config import settings
from app.queue.broker import QueueBroker
from app.queue.envelope import EventEnvelope
from app.realtime.analytics import VolatilityService, build_analytics_snapshot
from app.realtime.candles import CandleBoard
from app.realtime.hub import LiveHub
from app.realtime.interval_clock import (
    closed_bucket_start,
    floor_to_interval,
    market_tz,
    seconds_until_next_boundary,
)
from app.realtime.market_state import MarketStateStore
from app.realtime.nifty_ohlc import NiftyOhlcAggregator
from app.realtime.option_summary import stock_futures_strength, summarize_options_for_interval
from app.realtime.order_book import OrderBookAggregator
from app.realtime.options_chain import STRIKE_RADIUS as OPTION_CHAIN_RADIUS
from app.realtime.options_chain import OptionsChainBuilder

if TYPE_CHECKING:
    from app.ingestion.nubra_socket import NubraIngestionService
    from app.storage.db_writer import DBWriter


def print(*args: Any, **kwargs: Any) -> None:
    """Drop legacy per-tick debug prints.

    The live feed can deliver a lot of ticks per second. Writing full payloads
    to stdout from the event loop can block uvicorn on Windows when the console
    pipe is not drained, which stops health checks and interval DB flushes.
    """
    return None


def _f(x: Any) -> float:
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def _pick(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return None


def _pick_num(payload: dict[str, Any], *keys: str) -> float:
    return _f(_pick(payload, *keys))


def _int_or_zero(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def _coerce_option_value(raw: str) -> Any:
    value = raw.strip()
    if value == "None":
        return None
    if value in {"True", "False"}:
        return value == "True"
    try:
        if any(ch in value for ch in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value.strip("'\"")


def _option_row_from_repr(raw: Any) -> dict[str, Any] | None:
    """Parse Nubra SDK rows rendered as ``OptionData(k=v, ...)`` strings."""
    if isinstance(raw, dict):
        return raw
    if hasattr(raw, "__dict__"):
        return {
            key: value
            for key, value in vars(raw).items()
            if not key.startswith("_")
        }
    if not isinstance(raw, str):
        return None

    text = raw.strip()
    if not text.startswith("OptionData(") or not text.endswith(")"):
        return None

    row: dict[str, Any] = {}
    for key, value in re.findall(r"(\w+)=([^,)]*)", text):
        row[key] = _coerce_option_value(value)
    return row or None


def _normalize_option_rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        rows: list[dict[str, Any]] = []
        for item in raw:
            row = _option_row_from_repr(item)
            if row is not None:
                rows.append(row)
        return rows
    if isinstance(raw, dict):
        rows: list[dict[str, Any]] = []
        for strike, data in raw.items():
            row = _option_row_from_repr(data)
            if row is None:
                continue
            row = dict(row)
            row.setdefault("strike", strike)
            rows.append(row)
        return rows
    return []


def _norm_symbol(x: Any) -> str:
    return str(x or "").strip().upper()


NIFTY50_UNDERLYINGS = {
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL",
    "BPCL", "BRITANNIA", "CIPLA", "COALINDIA", "DRREDDY",
    "EICHERMOT", "ETERNAL", "GRASIM", "HCLTECH", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK",
    "INDUSINDBK", "INFY", "ITC", "JIOFIN", "JSWSTEEL",
    "KOTAKBANK", "LT", "MARUTI", "M&M", "NESTLEIND",
    "NTPC", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE",
    "SBIN", "SHRIRAMFIN", "SUNPHARMA", "TATACONSUM", "TATASTEEL",
    "TCS", "TECHM", "TITAN", "ULTRACEMCO",
}

HIGH_LIQUID = {"RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "SBIN"}


def _get_underlying(symbol: str) -> str:
    # Example: ADANIENT26APRFUT -> ADANIENT
    m = re.match(r"^([A-Z0-9&\-]+)\d{2}[A-Z]{3}FUT$", symbol)
    if m:
        return m.group(1)
    return symbol


def _option_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"CE", "CALL", "C"}:
        return "CE"
    if side in {"PE", "PUT", "P"}:
        return "PE"
    return side


def _option_from_state_by_ref(
    options_by_strike: dict[Any, dict[str, Any]],
    ref_id: int,
) -> tuple[int, str] | None:
    if ref_id <= 0:
        return None
    for strike_key, legs in options_by_strike.items():
        if not isinstance(legs, dict):
            continue
        strike = int(_f(strike_key))
        for side in ("CE", "PE"):
            leg = legs.get(side)
            if isinstance(leg, dict) and _int_or_zero(leg.get("ref_id")) == ref_id:
                return strike, side
    return None


def normalize_tick(msg: dict[str, Any]) -> dict[str, Any]:
    """
    Standardize incoming websocket payload shape across channels.
    """
    data = msg.get("data", {})
    if not isinstance(data, dict):
        data = msg if isinstance(msg, dict) else {}
    oi_raw = _pick(data, "oi", "open_interest", "openInterest", "oi_value", "volume_oi", "cumulative_oi")
    volume_raw = _pick(
        data,
        "volume",
        "total_volume",
        "totalVolume",
        "vol",
        "traded_volume",
        "tradedVolume",
        "volume_traded",
        "tick_volume",
        "cumulative_volume",
    )
    normalized = {
        "symbol": _norm_symbol(msg.get("key") or _pick(data, "symbol", "indexname", "asset", "name") or "UNKNOWN"),
        "channel": _norm_symbol(msg.get("channel") or msg.get("stream") or "unknown").lower(),
        "ltp": _f(
            _pick(
                data,
                "ltp",
                "last_price",
                "lastPrice",
                "last_traded_price",
                "lastTradedPrice",
                "index_value",
                "close",
            ),
        ),
        "oi": _f(oi_raw) if oi_raw is not None else None,
        "volume": _f(volume_raw) if volume_raw is not None else None,
        "timestamp": _pick(data, "timestamp", "exchange_timestamp", "exchangeTimestamp", "time"),
        "raw": data,
    }
    print("DATA KEYS:", list(data.keys()))
    print("RAW:", msg)
    print("NORMALIZED:", normalized)
    return normalized


class RealtimePipeline:
    """
    Fast path: consume ingestion queue, update in-memory state, push tick JSON to hub.
    Slow path: candle accumulation is reset by interval scheduler (separate task).
    """

    def __init__(
        self,
        broker: QueueBroker,
        hub: LiveHub,
        candles: CandleBoard,
        state: MarketStateStore,
        *,
        initial_nifty_price: float,
        ingestion: NubraIngestionService | None,
        nifty_ohlc_aggregator: NiftyOhlcAggregator | None = None,
        order_book_aggregator: OrderBookAggregator | None = None,
    ) -> None:
        self.broker = broker
        self.hub = hub
        self.candles = candles
        self.state = state
        self._initial_price = initial_nifty_price
        self._last_nifty = initial_nifty_price
        self._ingestion = ingestion
        self._nifty_ohlc_aggregator = nifty_ohlc_aggregator
        self._order_book_aggregator = order_book_aggregator
        self._ref_maps: dict[str, Any] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._unknown_orderbook_logged = 0
        self._last_atm_update_ts = 0.0
        self._prev_volume_by_symbol: dict[str, float] = {}
        self._candle_start_volume: dict[str, float] = {}
        self._missing_logged: set[str] = set()
        self._last_option_chain_broadcast_builds = 0
        # Throttled rebuilder for the ATM-centered chain view + ML metrics.
        # Reads from ``state.options_by_strike`` (mutated by per-ref
        # orderbook + greeks ticks) and writes to
        # ``state.option_chain_view`` / ``state.option_metrics`` so the
        # 3-min scheduler and the /realtime/candles/current endpoint
        # can read the same in-memory snapshot without re-aggregating.
        self._options_chain_builder = OptionsChainBuilder()

    def refresh_ref_maps(self) -> None:
        if not self._ingestion or not self._ingestion.instrument_manager:
            return
        px = self._last_nifty or self._initial_price
        self._ref_maps = self._ingestion.instrument_manager.get_ref_maps(px)

    def _price_scale(self) -> float:
        """Cached read of ``InstrumentManager.price_scale``.

        Used to convert wire-LTPs (paise on Nubra UAT) into rupees so
        candles, state, and the ATM/option-chain logic all operate on
        the same scale as ``state.options_by_strike`` keys.
        """
        if self._ingestion is None or self._ingestion.instrument_manager is None:
            return 1.0
        scale = getattr(self._ingestion.instrument_manager, "price_scale", 1)
        try:
            scale_f = float(scale)
        except (TypeError, ValueError):
            return 1.0
        return scale_f if scale_f > 0 else 1.0

    def _to_rupees(self, price: Any) -> float:
        if price is None:
            return 0.0
        try:
            return float(price) / self._price_scale()
        except (TypeError, ValueError):
            return 0.0

    def _primary_nifty_fut_symbol(self) -> str | None:
        refs = self._ref_maps.get("nifty_fut_refs") or []
        sym_map: dict[int, str] = self._ref_maps.get("nifty_fut_symbol_by_ref") or {}
        for rid in refs:
            sym = _norm_symbol(sym_map.get(int(rid)))
            if sym:
                return sym
        return None

    def _active_bucket_id(self) -> str:
        tz = market_tz(settings.market_timezone)
        now = datetime.now(tz)
        interval = int(settings.candle_interval_minutes)
        bucket_start = floor_to_interval(now, interval, tz)
        return f"{bucket_start.isoformat()}:{interval}m"

    async def run_forever(self) -> None:
        self.refresh_ref_maps()
        while True:
            event = await self.broker.consume()
            try:
                await self._handle(event)
            except Exception:
                self.logger.exception("realtime dispatch failed stream=%s", event.stream)
            finally:
                self.broker.task_done()

    async def _handle(self, event: EventEnvelope) -> None:
        stream = event.stream
        payload = event.payload
        if isinstance(payload, dict):
            print("DATA KEYS:", list(payload.keys()))
        normalized = normalize_tick({"channel": stream, "key": event.key, "data": payload})

        if stream == "index":
            await self._on_index(payload, normalized)
        elif stream == "option":
            await self._on_option_chain(payload, normalized)
        elif stream == "orderbook":
            await self._on_orderbook(payload, normalized)
        elif stream == "greeks":
            await self._on_greeks(payload, normalized)
        elif stream == "ohlcv":
            await self._emit_tick("ohlcv", event.key, payload)

    def _volume_delta(self, symbol: str, current_volume: float | None) -> float:
        if current_volume is None:
            print(symbol, "RAW:", current_volume, "DELTA:", 0.0)
            return 0.0
        prev = self._prev_volume_by_symbol.get(symbol)
        if prev is None:
            delta = 0.0
        else:
            delta = current_volume - prev
            delta = delta if delta >= 0 else 0.0
        self._prev_volume_by_symbol[symbol] = current_volume
        print(symbol, "RAW:", current_volume, "DELTA:", delta)
        return delta

    def _volume_from_candle_start(self, symbol: str, current_volume: float | None, candle: Any) -> float:
        """
        Compute per-candle volume from cumulative feed:
        volume = max(current_volume - candle_start_volume, 0)
        Return only the incremental add needed for the current tick.
        """
        if current_volume is None:
            print(symbol, "RAW:", current_volume, "DELTA:", 0.0)
            return 0.0
        if candle.open is None:
            self._candle_start_volume[symbol] = current_volume
        start = self._candle_start_volume.get(symbol)
        if start is None:
            self._candle_start_volume[symbol] = current_volume
            target_volume = 0.0
        else:
            target_volume = max(current_volume - start, 0.0)
        current_candle_volume = float(candle.volume or 0.0)
        delta = max(target_volume - current_candle_volume, 0.0)
        print(symbol, "RAW:", current_volume, "DELTA:", delta)
        return delta

    def _merge_stock_state(self, symbol: str, tick: dict[str, Any]) -> None:
        existing = dict(self.state.stock_futures.get(symbol) or {})
        ltp = _f(tick.get("ltp"))
        oi = tick.get("oi")
        vol = tick.get("volume")
        cum_vol = tick.get("cum_volume")
        if ltp:
            existing["ltp"] = ltp
        if oi is not None:
            existing["oi"] = _f(oi)
        if vol is not None:
            existing["volume"] = _f(vol)
        if cum_vol is not None:
            existing["cum_volume"] = _f(cum_vol)
        if tick.get("timestamp") is not None:
            existing["timestamp"] = tick.get("timestamp")
        existing["raw"] = tick.get("raw")
        existing["symbol"] = symbol
        self.state.stock_futures[symbol] = existing

        underlying = _get_underlying(symbol)
        under_state = dict(self.state.stock_futures_by_underlying.get(underlying) or {})
        for k in ("ltp", "oi", "volume", "cum_volume", "timestamp", "raw", "symbol"):
            if k in existing:
                under_state[k] = existing[k]
        self.state.stock_futures_by_underlying[underlying] = under_state

    async def _emit_tick(self, channel: str, key: str, data: dict[str, Any]) -> None:
        # Re-enable tick broadcasts on the websocket live stream.
        await self.hub.broadcast_json({"type": "tick", "channel": channel, "key": key, "data": data})

    async def _refresh_option_chain(
        self,
        *,
        force: bool = False,
        emit: bool = False,
        spot: float | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any], int | None]:
        spot = spot or self._last_nifty or self._initial_price
        if not force and self.state.options_by_strike and not self.state.option_chain_view:
            force = True
        chain, metrics, atm = self._options_chain_builder.maybe_rebuild(
            self.state.options_by_strike,
            spot,
            force=force,
        )
        self.state.option_chain_view = chain
        self.state.option_metrics = metrics

        builds = self._options_chain_builder.builds
        if emit and builds > self._last_option_chain_broadcast_builds:
            self._last_option_chain_broadcast_builds = builds
            await self.hub.broadcast_json(
                {
                    "type": "option_chain",
                    "spot": spot,
                    "atm": atm,
                    "strike_radius": OPTION_CHAIN_RADIUS,
                    "chain": list(chain),
                    "metrics": dict(metrics),
                    "updated_at": time.time(),
                }
            )
        return chain, metrics, atm

    async def _on_index(self, payload: dict[str, Any], normalized: dict[str, Any]) -> None:
        # Scale LTP to rupees once at the ingest edge so every
        # downstream consumer (candles, state, ATM, chain builder,
        # broadcasts) sees prices on the same scale as
        # state.options_by_strike keys. Volume / OI are NOT scaled —
        # they are contract counts, not prices.
        ltp = self._to_rupees(normalized["ltp"])
        vol = normalized["volume"]
        oi = normalized["oi"]
        symbol = _norm_symbol(payload.get("indexname") or payload.get("symbol") or normalized["symbol"] or "NIFTY")
        change_pct = _pick(payload, "change_pct", "changepercent", "changePercent", "change_percentage")
        if vol is None:
            print("No volume field for:", symbol)
        vol_delta = self._volume_delta(symbol, vol)
        is_futures = symbol.endswith("FUT")
        if not is_futures:
            self.state.nifty_index = {
                "symbol": symbol,
                "ltp": ltp,
                "change_pct": change_pct,
                "volume": _f(vol),
                "oi": _f(oi) if oi is not None else None,
                "timestamp": payload.get("timestamp"),
                "raw": normalized["raw"],
            }
        if ltp > 0 and symbol == "NIFTY":
            if self._nifty_ohlc_aggregator is not None:
                await self._nifty_ohlc_aggregator.update(
                    ltp=ltp,
                    volume=vol,
                    change_pct=change_pct,
                    timestamp=normalized.get("timestamp"),
                )
            self.candles.nifty.update(ltp, vol_delta, oi=oi)
            self._last_nifty = ltp
            if self._ingestion and self._ingestion.instrument_manager:
                now_ts = time.monotonic()
                # Prevent rapid subscribe/unsubscribe churn around ATM boundaries.
                if now_ts - self._last_atm_update_ts >= 1.0:
                    self._ingestion.instrument_manager.update_atm(ltp)
                    self.refresh_ref_maps()
                    self._last_atm_update_ts = now_ts
            # Refresh the ATM-centered chain view + ML metrics on every
            # NIFTY tick. The builder internally throttles to 500ms
            # (or fires immediately on ATM rolls), so this is cheap.
            await self._refresh_option_chain(emit=True)
        # Nubra index stream also emits futures symbols; use it as primary for futures OI/volume.
        if is_futures:
            print("RAW STOCK MSG:", payload)
            if isinstance(payload, dict):
                print("DATA KEYS:", list(payload.keys()))
            fut = {
                "symbol": symbol,
                "ltp": ltp,
                "cum_volume": _f(vol),
                "volume": 0.0,
                "oi": oi,
                "timestamp": payload.get("timestamp"),
                "raw": normalized["raw"],
            }
            print(f"SYMBOL: {symbol}, OI: {oi}")
            if oi is None:
                print("OI not available for", symbol)
            if symbol.startswith("NIFTY") and symbol.endswith("FUT"):
                self.state.futures[symbol] = fut
                if symbol == self._primary_nifty_fut_symbol():
                    self.state.nifty_futures = fut
                if ltp > 0:
                    fut_candle = self.candles.ensure_futures(symbol)
                    fut_add = self._volume_from_candle_start(symbol, vol, fut_candle)
                    fut_candle.update(ltp, fut_add, oi=oi, cum_volume=_f(vol) if vol is not None else None)
                    fut["volume"] = fut_candle.volume
                    if symbol == self._primary_nifty_fut_symbol():
                        self.candles.nifty_futures.update(
                            ltp,
                            fut_add,
                            oi=oi,
                            cum_volume=_f(vol) if vol is not None else None,
                        )
                await self._emit_tick("index", f"NIFTY_FUT:{symbol}", fut)
            else:
                self._merge_stock_state(symbol, fut)
                if ltp > 0:
                    stock_candle = self.candles.ensure_stock(symbol)
                    stock_add = self._volume_from_candle_start(symbol, vol, stock_candle)
                    stock_candle.update(ltp, stock_add, oi=oi, cum_volume=_f(vol) if vol is not None else None)
                    fut["volume"] = stock_candle.volume
                    if self.candles.ensure_stock(symbol).to_dict().get("volume") == 0:
                        print("WARNING: Volume not available for", symbol)
                await self._emit_tick("index", f"STOCK_FUT:{symbol}", fut)
            print("FUTURES OI:", self.state.nifty_futures.get("oi"))
            return

        await self._emit_tick("index", symbol or "NIFTY", self.state.nifty_index)

    async def _on_option_chain(self, payload: dict[str, Any], normalized: dict[str, Any]) -> None:
        core = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        self.state.option_chain_row = dict(core)

        ce = _normalize_option_rows(_pick(core, "ce", "CE", "call", "calls", "call_data", "callData"))
        pe = _normalize_option_rows(_pick(core, "pe", "PE", "put", "puts", "put_data", "putData"))
        atm_hint = self._to_rupees(_pick(core, "at_the_money_strike", "atm", "atm_strike", "atmStrike"))
        # Strike values from the chain stream are typically already in
        # the master's domain (paise); convert them to rupees so they
        # match keys written by _on_orderbook. LTPs are scaled too.
        scale = self._price_scale()

        def _strike_to_rupees(raw_strike: float) -> int:
            if scale > 1 and raw_strike >= 100_000:
                return int(round(raw_strike / scale))
            return int(raw_strike)

        def _chain_leg(row: dict[str, Any], existing: dict[str, Any] | None = None) -> dict[str, Any]:
            leg = dict(existing or {})
            leg.update(
                {
                    "ltp": self._to_rupees(_pick(row, "ltp", "last_traded_price", "lastTradedPrice", "last_price")),
                    "oi": _pick(row, "open_interest", "openInterest", "oi"),
                    "volume": _pick(row, "volume", "traded_volume", "tradedVolume"),
                    "delta": _pick(row, "delta"),
                    "gamma": _pick(row, "gamma"),
                    "theta": _pick(row, "theta"),
                    "vega": _pick(row, "vega"),
                    "iv": _pick(row, "iv"),
                    "ref_id": _pick(row, "ref_id", "refId", "refid"),
                    "timestamp": _pick(row, "timestamp", "exchange_timestamp", "exchangeTimestamp", "time"),
                }
            )
            return leg

        for row in ce:
            r = row or {}
            raw_strike = _f(_pick(r, "strike_price", "strikePrice", "strike", "strike_price_value"))
            strike = _strike_to_rupees(raw_strike)
            if strike <= 0:
                continue
            bucket = self.state.options_by_strike.setdefault(strike, {})
            bucket["CE"] = _chain_leg(r, bucket.get("CE") if isinstance(bucket.get("CE"), dict) else None)
        for row in pe:
            r = row or {}
            raw_strike = _f(_pick(r, "strike_price", "strikePrice", "strike", "strike_price_value"))
            strike = _strike_to_rupees(raw_strike)
            if strike <= 0:
                continue
            bucket = self.state.options_by_strike.setdefault(strike, {})
            bucket["PE"] = _chain_leg(r, bucket.get("PE") if isinstance(bucket.get("PE"), dict) else None)
        ce_oi = sum(_f(_pick((x or {}), "open_interest", "openInterest", "oi")) for x in ce)
        pe_oi = sum(_f(_pick((x or {}), "open_interest", "openInterest", "oi")) for x in pe)
        self.state.last_option_totals = {"ce_oi": ce_oi, "pe_oi": pe_oi}
        print("OPTIONS COUNT:", len(self.state.options_by_strike))
        await self._refresh_option_chain(force=True, emit=True, spot=atm_hint if atm_hint > 0 else None)
        await self._emit_tick(
            "option",
            "chain",
            {
                "chain": _pick(core, "asset", "symbol", "indexname", "name"),
                "rows_ce": len(ce),
                "rows_pe": len(pe),
                "ce_oi": ce_oi,
                "pe_oi": pe_oi,
            },
        )

    async def _on_orderbook(self, payload: dict[str, Any], normalized: dict[str, Any]) -> None:
        core = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        rid = _int_or_zero(
            _pick(
                core,
                "ref_id",
                "refId",
                "refid",
                "instrument_token",
                "instrumentToken",
                "token",
            )
            or normalized.get("symbol")
        )
        # Scale LTP to rupees at ingest. Volume / OI / quantities stay
        # raw because they are not prices.
        ltp = self._to_rupees(
            _pick_num(
                core,
                "last_traded_price",
                "lastTradedPrice",
                "ltp",
                "last_price",
                "lastPrice",
            )
        )
        vol = _pick_num(
            core,
            "volume",
            "total_volume",
            "traded_volume",
            "volume_traded",
            "tradedVolume",
            "cumulative_volume",
            "vol",
            "total_traded_volume",
            "totalTradedVolume",
        )
        raw_vol = _pick(
            core,
            "volume",
            "total_volume",
            "traded_volume",
            "volume_traded",
            "cumulative_volume",
            "vol",
            "tradedVolume",
            "total_traded_volume",
            "totalTradedVolume",
        )
        if raw_vol is None:
            print("No volume field for:", normalized.get("symbol") or str(rid))
        ltq = _pick_num(
            core,
            "last_traded_quantity",
            "lastTradedQuantity",
            "ltq",
            "last_qty",
            "lastQty",
        )
        bids = _pick(core, "bids", "buy", "buy_levels", "buyLevels")
        asks = _pick(core, "asks", "sell", "sell_levels", "sellLevels")
        if not isinstance(bids, list):
            bids = []
        if not isinstance(asks, list):
            asks = []
        ob = {
            "ref_id": rid,
            "ltp": ltp,
            "last_traded_price": ltp,
            "last_traded_quantity": ltq,
            "volume": vol,
            "oi": _f(_pick(core, "open_interest", "openInterest", "oi", "oi_value", "cumulative_oi", "volume_oi"))
            if _pick(core, "open_interest", "openInterest", "oi", "oi_value", "cumulative_oi", "volume_oi") is not None
            else None,
            "timestamp": _pick(core, "timestamp", "exchange_timestamp", "exchangeTimestamp", "time"),
            "bids": bids,
            "asks": asks,
            "raw": normalized["raw"],
        }
        fut_symbol_map: dict[int, str] = self._ref_maps.get("nifty_fut_symbol_by_ref") or {}
        stock_map: dict[int, str] = self._ref_maps.get("stock_fut_symbols") or {}
        opt_map: dict[int, tuple[int, str]] = self._ref_maps.get("option_by_ref") or {}
        opt_symbol_map: dict[int, str] = self._ref_maps.get("option_symbol_by_ref") or {}
        direct_strike_raw = _pick_num(
            core,
            "strike",
            "strike_price",
            "strikePrice",
            "strike_price_value",
        )
        direct_strike = int(
            round(direct_strike_raw / self._price_scale())
            if self._price_scale() > 1 and direct_strike_raw >= 100_000
            else round(direct_strike_raw)
        )
        direct_opt_type = _option_side(
            _pick(core, "option_type", "optionType", "opt_type", "optType", "right", "side")
        )
        state_option = _option_from_state_by_ref(self.state.options_by_strike, rid)

        if rid in fut_symbol_map:
            symbol = _norm_symbol(fut_symbol_map[rid] or normalized.get("symbol"))
            if ltp > 0:
                fut = dict(self.state.futures.get(symbol) or {})
                fut.update(
                    {
                        "symbol": symbol,
                        "ltp": ltp,
                        "last_traded_price": ltp,
                        "cum_volume": vol,
                        "oi": ob.get("oi"),
                        "timestamp": ob.get("timestamp"),
                        "raw": normalized["raw"],
                    }
                )
                fut_candle = self.candles.ensure_futures(symbol)
                fut_add = self._volume_from_candle_start(symbol, vol, fut_candle)
                fut_candle.update(ltp, fut_add, oi=ob.get("oi"), cum_volume=vol)
                fut["volume"] = fut_candle.volume
                self.state.futures[symbol] = fut
                if symbol == self._primary_nifty_fut_symbol():
                    self.state.nifty_futures = fut
                    self.candles.nifty_futures.update(ltp, fut_add, oi=ob.get("oi"), cum_volume=vol)
                self.logger.debug(
                    "candle update orderbook kind=nifty_future rid=%s symbol=%s ltp=%s candle=%s",
                    rid,
                    symbol,
                    ltp,
                    fut_candle.to_dict(),
                )
            await self._emit_tick("orderbook", f"NIFTY_FUT:{symbol}", ob)
        elif rid in stock_map:
            sym = _norm_symbol(stock_map[rid])
            if ltp > 0:
                stock_candle = self.candles.ensure_stock(sym)
                stock_add = self._volume_from_candle_start(sym, vol, stock_candle)
                stock_candle.update(ltp, stock_add, oi=ob.get("oi"), cum_volume=vol)
                self._merge_stock_state(
                    sym,
                    {
                        "symbol": sym,
                        "ltp": ltp,
                        "volume": stock_candle.volume,
                        "cum_volume": vol,
                        "oi": ob.get("oi"),
                        "timestamp": ob.get("timestamp"),
                        "raw": normalized["raw"],
                    },
                )
                self.logger.debug(
                    "candle update orderbook kind=stock_future rid=%s symbol=%s ltp=%s candle=%s",
                    rid,
                    sym,
                    ltp,
                    stock_candle.to_dict(),
                )
            await self._emit_tick("orderbook", f"STOCK_FUT:{sym}", ob)
        elif rid in opt_map or state_option is not None:
            strike, side = opt_map.get(rid) or state_option or (0, "")
            opt_key = opt_symbol_map.get(rid, f"NIFTY_{strike}_{side}")
            opt_type = opt_key.split("_")[-1]
            if opt_type not in {"CE", "PE"}:
                opt_type = _option_side(side)
            if self._order_book_aggregator is not None:
                await self._order_book_aggregator.update_option(
                    atm_source=self._last_nifty or self._initial_price,
                    strike=int(strike),
                    option_type=opt_type,
                    payload=core,
                    bids=bids,
                    asks=asks,
                    price_scale=self._price_scale(),
                    bucket_id=self._active_bucket_id(),
                )
            leg = self.state.options_by_strike.setdefault(strike, {})
            leg[opt_type] = {
                "ltp": ltp,
                "volume": vol,
                "open_interest": _pick(core, "open_interest", "openInterest", "oi"),
                "ref_id": rid,
            }
            print("OPTIONS COUNT:", len(self.state.options_by_strike))
            await self._refresh_option_chain(emit=True)
            await self._emit_tick("option", opt_key, leg[opt_type])
        elif direct_strike > 0 and direct_opt_type in {"CE", "PE"}:
            if self._order_book_aggregator is not None:
                await self._order_book_aggregator.update_option(
                    atm_source=self._last_nifty or self._initial_price,
                    strike=direct_strike,
                    option_type=direct_opt_type,
                    payload=core,
                    bids=bids,
                    asks=asks,
                    price_scale=self._price_scale(),
                    bucket_id=self._active_bucket_id(),
                )
            await self._emit_tick("orderbook", f"NIFTY_{direct_strike}_{direct_opt_type}", ob)
        else:
            # rid == 0 means the payload had no ref_id field at all
            # (a malformed message). rid != 0 here means a real ref_id
            # arrived but doesn't match any of our resolved maps — that
            # is the silent symptom of subscription / ATM skew (wire
            # subscribed to a different strike window than ref_maps
            # was built for). Surface BOTH but keep the log volume
            # bounded.
            if self._unknown_orderbook_logged < 10:
                self._unknown_orderbook_logged += 1
                self.logger.warning(
                    "orderbook unresolved ref_id=%s opt_map_size=%d "
                    "fut_map_size=%d stock_map_size=%d payload_keys=%s",
                    rid,
                    len(opt_map),
                    len(fut_symbol_map),
                    len(stock_map),
                    sorted(core.keys()),
                )
            await self._emit_tick("orderbook", str(rid), ob)

    async def _on_greeks(self, payload: dict[str, Any], normalized: dict[str, Any]) -> None:
        core = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        rid = int(
            _pick_num(
                core,
                "ref_id",
                "refId",
                "refid",
                "instrument_token",
                "instrumentToken",
                "token",
            ),
        )
        opt_map: dict[int, tuple[int, str]] = self._ref_maps.get("option_by_ref") or {}
        opt_symbol_map: dict[int, str] = self._ref_maps.get("option_symbol_by_ref") or {}
        stock_map: dict[int, str] = self._ref_maps.get("stock_fut_symbols") or {}
        fut_symbol_map: dict[int, str] = self._ref_maps.get("nifty_fut_symbol_by_ref") or {}
        # Greeks stream can carry OI-rich updates; merge for futures/stocks too.
        if rid in fut_symbol_map:
            symbol = _norm_symbol(fut_symbol_map[rid] or normalized.get("symbol"))
            oi_raw = _pick(core, "oi", "open_interest", "openInterest", "oi_value", "cumulative_oi", "volume_oi")
            oi = _f(oi_raw) if oi_raw is not None else None
            cur = dict(self.state.futures.get(symbol) or {})
            if oi is not None:
                cur["oi"] = oi
            cur["raw"] = normalized["raw"]
            self.state.futures[symbol] = cur
            if symbol == self._primary_nifty_fut_symbol():
                self.state.nifty_futures = cur
            # OI is point-in-time; only overwrite when present.
            if oi is not None:
                fut_candle = self.candles.ensure_futures(symbol)
                fut_candle.oi = oi
                if symbol == self._primary_nifty_fut_symbol():
                    self.candles.nifty_futures.oi = oi
            print(f"SYMBOL: NIFTY_FUT:{symbol}, OI: {oi}")
            if oi is None:
                print("OI not available for", symbol)
        elif rid in stock_map:
            sym = stock_map[rid]
            self._merge_stock_state(sym, {"symbol": sym, "oi": normalized["oi"], "raw": normalized["raw"]})
            print(f"SYMBOL: {sym}, OI: {normalized['oi']}")
            if normalized["oi"] == 0:
                print("WARNING: Missing OI for", sym)
        if rid in opt_map:
            strike, side = opt_map[rid]
            opt_key = opt_symbol_map.get(rid, f"NIFTY_{strike}_{side}")
            opt_type = opt_key.split("_")[-1]
            if opt_type not in {"CE", "PE"}:
                opt_type = side
            leg = self.state.options_by_strike.setdefault(strike, {})
            cur = dict(leg.get(opt_type) or {})
            cur.update(
                {
                    "delta": _pick(core, "delta"),
                    "gamma": _pick(core, "gamma"),
                    "theta": _pick(core, "theta"),
                    "vega": _pick(core, "vega"),
                    "iv": _pick(core, "iv"),
                    "open_interest": _pick(core, "open_interest", "openInterest", "oi"),
                }
            )
            leg[opt_type] = cur
            print("OPTIONS COUNT:", len(self.state.options_by_strike))
            await self._refresh_option_chain(emit=True)
            await self._emit_tick("option", opt_key, cur)
        await self._emit_tick("greeks", str(rid), core)


async def run_interval_scheduler(
    hub: LiveHub,
    candle_board: CandleBoard,
    state: MarketStateStore,
    *,
    interval_minutes: int,
    tz_name: str,
    debug_state: dict[str, Any] | None = None,
    nifty_ohlc_aggregator: NiftyOhlcAggregator | None = None,
    order_book_aggregator: OrderBookAggregator | None = None,
    db_writer: DBWriter | None = None,
) -> None:
    tz = market_tz(tz_name)
    last_emitted_start: datetime | None = None
    emitted_bucket_ids: set[str] = set()
    prev_totals: dict[str, float] = {}
    volatility_service = VolatilityService()
    timeline_matrix: deque[dict[str, Any]] = deque(maxlen=240)
    missing_logged: set[str] = set()
    log = logging.getLogger("realtime.scheduler")
    if debug_state is not None:
        debug_state.update(
            {
                "status": "starting",
                "interval_minutes": interval_minutes,
                "timezone": tz_name,
                "emits_total": 0,
                "last_error": None,
            }
        )

    def validate(symbol: str, candle: dict[str, Any]) -> None:
        if _f(candle.get("volume")) == 0:
            print("Low activity:", symbol)
        if candle.get("oi") is None:
            print("Missing OI:", symbol)

    from datetime import timedelta

    while True:
        delay = seconds_until_next_boundary(datetime.now(tz), interval_minutes, tz)
        if debug_state is not None:
            now_for_debug = datetime.now(tz)
            debug_state.update(
                {
                    "status": "sleeping",
                    "now": now_for_debug.isoformat(),
                    "seconds_until_next_emit": round(delay, 3),
                    "next_emit_at": (now_for_debug + timedelta(seconds=delay)).isoformat(),
                    "client_count": hub.client_count,
                    "open_index": candle_board.nifty.to_dict(),
                    "open_futures_count": len(candle_board.futures),
                    "open_stocks_count": len(candle_board.stock_futures),
                }
            )
        await asyncio.sleep(delay)
        try:
            if debug_state is not None:
                debug_state.update({"status": "building", "last_error": None})
            now = datetime.now(tz)
            # Bar that just closed at `now` (e.g. now=12:06:00 → [12:03, 12:06)).
            bucket_start = closed_bucket_start(now, interval_minutes, tz)
            bucket_end = bucket_start + timedelta(minutes=interval_minutes)
            bucket_id = f"{bucket_start.isoformat()}:{interval_minutes}m"
            if bucket_id in emitted_bucket_ids or (
                last_emitted_start is not None and bucket_start == last_emitted_start
            ):
                log.warning("duplicate candle bucket skipped bucket_id=%s", bucket_id)
                if debug_state is not None:
                    debug_state.update(
                        {
                            "status": "duplicate_skipped",
                            "last_duplicate_bucket_id": bucket_id,
                        }
                    )
                continue
            last_emitted_start = bucket_start
            emitted_bucket_ids.add(bucket_id)
            if len(emitted_bucket_ids) > 128:
                emitted_bucket_ids = set(list(emitted_bucket_ids)[-64:])

            opt_sum = summarize_options_for_interval(state.option_chain_row, state.options_by_strike, prev_totals)
            try:
                opt_sum = summarize_options_for_interval(state.option_chain_row, state.options_by_strike, prev_totals)
            except Exception as exc:
                log.exception("failed summarizing options for interval: %s", exc)
                opt_sum = {}
            prev_totals["ce_oi"] = float(opt_sum.get("total_ce_oi") or 0)
            prev_totals["pe_oi"] = float(opt_sum.get("total_pe_oi") or 0)

            futures_candle_dicts = {k: v.to_dict() for k, v in candle_board.futures.items()}
            stock_candle_dicts = {k: v.to_dict() for k, v in candle_board.stock_futures.items()}

            # Annotate stock futures with their underlying ticker so
            # downstream consumers can filter / join without parsing
            # "ADANIENT26MAYFUT" themselves.
            for sym, candle in stock_candle_dicts.items():
                candle["underlying_symbol"] = _get_underlying(sym)
            # Annotate NIFTY futures with their contract tag (current /
            # next / far) so the UI can label them by tenor instead of
            # parsing expiry months out of the symbol.
            nifty_fut_contracts: dict[str, str] = {}
            try:
                from app.main import APP_STATE  # late import: avoid cycle

                ingestion = APP_STATE.get("ingestion") if isinstance(APP_STATE, dict) else None
                if ingestion is not None and getattr(ingestion, "instrument_manager", None) is not None:
                    manager = ingestion.instrument_manager
                    contract_by_ref = manager.get_nifty_fut_contracts()
                    symbol_by_ref = manager.get_nifty_fut_symbols()
                    for ref_id, label in contract_by_ref.items():
                        sym = symbol_by_ref.get(ref_id)
                        if sym:
                            nifty_fut_contracts[sym] = label
                            if sym in futures_candle_dicts:
                                futures_candle_dicts[sym]["contract"] = label
                                futures_candle_dicts[sym]["underlying_symbol"] = "NIFTY"
            except Exception as exc:  # pragma: no cover — annotation failure must not break the bar
                log.debug("could not annotate NIFTY future contracts: %s", exc)

            strength = stock_futures_strength(state.stock_futures, stock_candle_dicts)
            print("TOTAL STOCKS:", len(stock_candle_dicts))
            for sym, candle in futures_candle_dicts.items():
                validate(sym, candle)
            for sym, candle in stock_candle_dicts.items():
                validate(sym, candle)
            # Coverage / OI diagnostics for required NIFTY list.
            for s in sorted(NIFTY50_UNDERLYINGS):
                if s not in state.stock_futures_by_underlying and s not in missing_logged:
                    print("Missing stock:", s)
                    missing_logged.add(s)
            for s, d in sorted(state.stock_futures_by_underlying.items()):
                if s in NIFTY50_UNDERLYINGS and not _f(d.get("oi")):
                    print("Missing OI:", s)
                if s in NIFTY50_UNDERLYINGS and not _f(d.get("volume")):
                    print("WARNING: Volume not available for", s)

            index_candle = candle_board.nifty.to_dict()
            analytics = build_analytics_snapshot(
                timestamp=bucket_end.isoformat(),
                options_chain=list(state.option_chain_view),
                stocks=stock_candle_dicts,
                nifty_close=index_candle.get("close"),
                volatility_service=volatility_service,
                timeline_matrix=timeline_matrix,
            )
            if order_book_aggregator is not None:
                order_book = await order_book_aggregator.snapshot_and_reset(
                    atm_source=(state.option_metrics.get("atm_strike") or index_candle.get("close")),
                    options_by_strike=state.options_by_strike,
                )
            else:
                order_book = None
            if nifty_ohlc_aggregator is not None:
                nifty = await nifty_ohlc_aggregator.snapshot_and_reset()
            else:
                nifty = {
                    "open": index_candle.get("open"),
                    "high": index_candle.get("high"),
                    "low": index_candle.get("low"),
                    "close": index_candle.get("close"),
                    "volume": index_candle.get("volume"),
                    "change_pct": state.nifty_index.get("change_pct"),
                    "tick_count": index_candle.get("tick_count"),
                    "first_timestamp": None,
                    "last_timestamp": state.nifty_index.get("timestamp"),
                }

            log.info(
                "emit candle_3m bucket_id=%s index_ticks=%s futures=%d stocks=%d clients=%d",
                bucket_id,
                candle_board.nifty.tick_count,
                len(futures_candle_dicts),
                len(stock_candle_dicts),
                hub.client_count,
            )

            msg: dict[str, Any] = {
                "type": "candle_3m",
                "bucket_id": bucket_id,
                "bucket_start": bucket_start.isoformat(),
                # bucket_end is the wall-clock at which this bar closed; many
                # charting clients label 3m bars by close-time, so we expose
                # both edges and let the consumer pick.
                "bucket_end": bucket_end.isoformat(),
                "interval_minutes": interval_minutes,
                "nifty": nifty,
                "futures": futures_candle_dicts,
                "stocks": stock_candle_dicts,
                "options": {
                    # ATM-centered chain reconstructed
                    # from options_by_strike on every NIFTY tick by
                    # OptionsChainBuilder. Empty list until the first
                    # NIFTY index tick lands.
                    "chain": list(state.option_chain_view),
                    "metrics": dict(state.option_metrics),
                    "summary": opt_sum,
                },
                "stock_futures_summary": strength,
                "analytics": analytics,
                "order_book": order_book,
                "is_empty": candle_board.nifty.tick_count == 0
                and not futures_candle_dicts
                and not stock_candle_dicts,
                "meta": {
                    "index": index_candle,
                    "high_liquid_symbols": sorted(HIGH_LIQUID),
                    # symbol -> "current" | "next" | "far" for whichever
                    # NIFTY monthly future contracts the master could
                    # resolve. Will be {} if the master cache is stale and
                    # only one expiry was found (see warning logged at
                    # boot in InstrumentManager._prepare_indices).
                    "nifty_fut_contracts": nifty_fut_contracts,
                },
            }
            if db_writer is not None and order_book is not None:
                try:
                    await db_writer.enqueue(
                        "order_book_3m",
                        {
                            "bucket_id": bucket_id,
                            "bucket_start": bucket_start.isoformat(),
                            "bucket_end": bucket_end.isoformat(),
                            "interval_minutes": interval_minutes,
                            "order_book": order_book,
                        },
                    )
                except Exception as exc:
                    log.exception("failed enqueueing order_book_3m bucket_id=%s: %s", bucket_id, exc)
            if db_writer is not None:
                try:
                    fut_meta: dict[str, dict[str, Any]] = {}
                    price_scale = 1.0
                    try:
                        from app.main import APP_STATE  # late import: avoid cycle

                        ingestion = APP_STATE.get("ingestion") if isinstance(APP_STATE, dict) else None
                    except Exception:
                        ingestion = None
                    manager = getattr(ingestion, "instrument_manager", None)
                    if manager is not None:
                        try:
                            price_scale = float(getattr(manager, "price_scale", 1) or 1)
                        except (TypeError, ValueError):
                            price_scale = 1.0
                        for sym in list(futures_candle_dicts) + list(stock_candle_dicts):
                            meta = manager.get_fut_meta(sym)
                            if meta:
                                fut_meta[sym] = meta
                    await db_writer.enqueue(
                        "futures_3m",
                        {
                            "bucket_id": bucket_id,
                            "bucket_end": bucket_end.isoformat(),
                            "price_scale": price_scale,
                            "futures": futures_candle_dicts,
                            "stocks": stock_candle_dicts,
                            "fut_meta": fut_meta,
                            "nifty50_underlyings": sorted(NIFTY50_UNDERLYINGS),
                        },
                    )
                except Exception as exc:
                    log.exception("failed enqueueing futures_3m bucket_id=%s: %s", bucket_id, exc)
            try:
                await hub.broadcast_json(msg)
            except Exception as exc:
                log.exception("candle emit failed: %s", exc)
                if debug_state is not None:
                    debug_state.update(
                        {
                            "status": "broadcast_error",
                            "last_error": repr(exc),
                            "last_error_at": datetime.now(tz).isoformat(),
                        }
                    )
                continue
            if debug_state is not None:
                debug_state.update(
                    {
                        "status": "emitted",
                        "emits_total": int(debug_state.get("emits_total") or 0) + 1,
                        "last_emit_at": datetime.now(tz).isoformat(),
                        "last_bucket_start": bucket_start.isoformat(),
                        "last_bucket_end": bucket_end.isoformat(),
                        "last_bucket_id": bucket_id,
                        "last_client_count": hub.client_count,
                        "last_index": candle_board.nifty.to_dict(),
                        "last_futures_count": len(futures_candle_dicts),
                        "last_stocks_count": len(stock_candle_dicts),
                    }
                )
            candle_board.reset_all()

            # Immediately announce the new bar that has just opened so
            # streaming consumers don't have to wait an entire interval to
            # know e.g. "the 12:06 candle is now live". The OHLC fields are
            # null at this moment — they will be filled in by ticks during
            # the bucket and the closed snapshot will arrive at bucket_end.
            new_bucket_start = bucket_end
            new_bucket_end = new_bucket_start + timedelta(minutes=interval_minutes)
            await hub.broadcast_json(
                {
                    "type": "candle_3m_open",
                    "bucket_id": f"{new_bucket_start.isoformat()}:{interval_minutes}m",
                    "bucket_start": new_bucket_start.isoformat(),
                    "bucket_end": new_bucket_end.isoformat(),
                    "interval_minutes": interval_minutes,
                }
            )
        except asyncio.CancelledError:
            if debug_state is not None:
                debug_state.update({"status": "cancelled"})
            raise
        except Exception as exc:
            if debug_state is not None:
                debug_state.update(
                    {
                        "status": "error",
                        "last_error": repr(exc),
                        "last_error_at": datetime.now(tz).isoformat(),
                    }
                )
            log.exception("candle interval scheduler iteration failed (will retry next boundary)")
