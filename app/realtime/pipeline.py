from __future__ import annotations

import asyncio
import logging
import re
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from app.queue.broker import QueueBroker
from app.queue.envelope import EventEnvelope
from app.realtime.candles import CandleBoard
from app.realtime.hub import LiveHub
from app.realtime.interval_clock import (
    closed_bucket_start,
    floor_to_interval,
    market_tz,
    seconds_until_next_boundary,
)
from app.realtime.market_state import MarketStateStore
from app.realtime.option_summary import stock_futures_strength, summarize_options_for_interval
from app.realtime.options_chain import OptionsChainBuilder

if TYPE_CHECKING:
    from app.ingestion.nubra_socket import NubraIngestionService


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


def _normalize_option_rows(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, dict):
        rows: list[dict[str, Any]] = []
        for strike, data in raw.items():
            if not isinstance(data, dict):
                continue
            row = dict(data)
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
    ) -> None:
        self.broker = broker
        self.hub = hub
        self.candles = candles
        self.state = state
        self._initial_price = initial_nifty_price
        self._last_nifty = initial_nifty_price
        self._ingestion = ingestion
        self._ref_maps: dict[str, Any] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._unknown_orderbook_logged = 0
        self._last_atm_update_ts = 0.0
        self._prev_volume_by_symbol: dict[str, float] = {}
        self._candle_start_volume: dict[str, float] = {}
        self._missing_logged: set[str] = set()
        # Throttled rebuilder for the 15-strike chain view + ML metrics.
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
        await self.hub.broadcast_json({"type": "tick", "channel": channel, "key": key, "data": data})

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
        if vol is None:
            print("No volume field for:", symbol)
        vol_delta = self._volume_delta(symbol, vol)
        is_futures = symbol.endswith("FUT")
        if not is_futures:
            self.state.nifty_index = {
                "symbol": symbol,
                "ltp": ltp,
                "change_pct": payload.get("changepercent"),
                "volume": _f(vol),
                "oi": _f(oi) if oi is not None else None,
                "timestamp": payload.get("timestamp"),
                "raw": normalized["raw"],
            }
        if ltp > 0 and symbol == "NIFTY":
            self.candles.nifty.update(ltp, vol_delta, oi=oi)
            self._last_nifty = ltp
            if self._ingestion and self._ingestion.instrument_manager:
                now_ts = time.monotonic()
                # Prevent rapid subscribe/unsubscribe churn around ATM boundaries.
                if now_ts - self._last_atm_update_ts >= 1.0:
                    self._ingestion.instrument_manager.update_atm(ltp)
                    self.refresh_ref_maps()
                    self._last_atm_update_ts = now_ts
            # Refresh the 15-strike chain view + ML metrics on every
            # NIFTY tick. The builder internally throttles to 500ms
            # (or fires immediately on ATM rolls), so this is cheap.
            chain, metrics, _ = self._options_chain_builder.maybe_rebuild(
                self.state.options_by_strike, ltp
            )
            self.state.option_chain_view = chain
            self.state.option_metrics = metrics
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
        # Strike values from the chain stream are typically already in
        # the master's domain (paise); convert them to rupees so they
        # match keys written by _on_orderbook. LTPs are scaled too.
        scale = self._price_scale()

        def _strike_to_rupees(raw_strike: float) -> int:
            if scale > 1 and raw_strike >= 100_000:
                return int(round(raw_strike / scale))
            return int(raw_strike)

        for row in ce:
            r = row or {}
            raw_strike = _f(_pick(r, "strike_price", "strikePrice", "strike", "strike_price_value"))
            strike = _strike_to_rupees(raw_strike)
            if strike <= 0:
                continue
            bucket = self.state.options_by_strike.setdefault(strike, {})
            bucket["CE"] = {
                "ltp": self._to_rupees(_pick(r, "ltp", "last_traded_price", "lastTradedPrice", "last_price")),
                "oi": _pick(r, "open_interest", "openInterest", "oi"),
                "volume": _pick(r, "volume", "traded_volume", "tradedVolume"),
            }
        for row in pe:
            r = row or {}
            raw_strike = _f(_pick(r, "strike_price", "strikePrice", "strike", "strike_price_value"))
            strike = _strike_to_rupees(raw_strike)
            if strike <= 0:
                continue
            bucket = self.state.options_by_strike.setdefault(strike, {})
            bucket["PE"] = {
                "ltp": self._to_rupees(_pick(r, "ltp", "last_traded_price", "lastTradedPrice", "last_price")),
                "oi": _pick(r, "open_interest", "openInterest", "oi"),
                "volume": _pick(r, "volume", "traded_volume", "tradedVolume"),
            }
        ce_oi = sum(_f(_pick((x or {}), "open_interest", "openInterest", "oi")) for x in ce)
        pe_oi = sum(_f(_pick((x or {}), "open_interest", "openInterest", "oi")) for x in pe)
        self.state.last_option_totals = {"ce_oi": ce_oi, "pe_oi": pe_oi}
        print("OPTIONS COUNT:", len(self.state.options_by_strike))
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

        if rid in fut_symbol_map:
            symbol = _norm_symbol(fut_symbol_map[rid] or normalized.get("symbol"))
            # Futures aggregation uses index channel only; orderbook is passthrough.
            await self._emit_tick("orderbook", f"NIFTY_FUT:{symbol}", ob)
        elif rid in stock_map:
            sym = stock_map[rid]
            # Stock futures aggregation uses index channel only; orderbook is passthrough.
            await self._emit_tick("orderbook", f"STOCK_FUT:{sym}", ob)
        elif rid in opt_map:
            strike, side = opt_map[rid]
            opt_key = opt_symbol_map.get(rid, f"NIFTY_{strike}_{side}")
            opt_type = opt_key.split("_")[-1]
            if opt_type not in {"CE", "PE"}:
                opt_type = side
            leg = self.state.options_by_strike.setdefault(strike, {})
            leg[opt_type] = {
                "ltp": ltp,
                "volume": vol,
                "open_interest": _pick(core, "open_interest", "openInterest", "oi"),
                "ref_id": rid,
            }
            print("OPTIONS COUNT:", len(self.state.options_by_strike))
            await self._emit_tick("option", opt_key, leg[opt_type])
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
            await self._emit_tick("option", opt_key, cur)
        await self._emit_tick("greeks", str(rid), core)


async def run_interval_scheduler(
    hub: LiveHub,
    candle_board: CandleBoard,
    state: MarketStateStore,
    *,
    interval_minutes: int,
    tz_name: str,
) -> None:
    tz = market_tz(tz_name)
    last_emitted_start: datetime | None = None
    prev_totals: dict[str, float] = {}
    missing_logged: set[str] = set()
    log = logging.getLogger("realtime.scheduler")

    def validate(symbol: str, candle: dict[str, Any]) -> None:
        if _f(candle.get("volume")) == 0:
            print("Low activity:", symbol)
        if candle.get("oi") is None:
            print("Missing OI:", symbol)

    from datetime import timedelta

    while True:
        delay = seconds_until_next_boundary(datetime.now(tz), interval_minutes, tz)
        await asyncio.sleep(delay)
        now = datetime.now(tz)
        # Bar that just closed at `now` (e.g. now=12:06:00 → [12:03, 12:06)).
        bucket_start = closed_bucket_start(now, interval_minutes, tz)
        bucket_end = bucket_start + timedelta(minutes=interval_minutes)
        if last_emitted_start is not None and bucket_start == last_emitted_start:
            log.warning("duplicate bucket skipped %s", bucket_start.isoformat())
            continue
        last_emitted_start = bucket_start

        opt_sum = summarize_options_for_interval(state.option_chain_row, state.options_by_strike, prev_totals)
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

        log.info(
            "emitting candle_3m | bucket_start=%s | /ws/live clients=%d",
            bucket_start.isoformat(),
            hub.client_count,
        )

        msg: dict[str, Any] = {
            "type": "candle_3m",
            "bucket_start": bucket_start.isoformat(),
            # bucket_end is the wall-clock at which this bar closed; many
            # charting clients label 3m bars by close-time, so we expose
            # both edges and let the consumer pick.
            "bucket_end": bucket_end.isoformat(),
            "interval_minutes": interval_minutes,
            "futures": futures_candle_dicts,
            "stocks": stock_candle_dicts,
            "options": {
                # 15-row chain (7 ITM + 1 ATM + 7 OTM) reconstructed
                # from options_by_strike on every NIFTY tick by
                # OptionsChainBuilder. Empty list until the first
                # NIFTY index tick lands.
                "chain": list(state.option_chain_view),
                "metrics": dict(state.option_metrics),
                # Legacy fields kept for backward compatibility with
                # any consumers that already read them.
                "summary": opt_sum,
                "by_strike": {str(k): v for k, v in sorted(state.options_by_strike.items())},
            },
            "stock_futures_summary": strength,
            "meta": {
                "index": candle_board.nifty.to_dict(),
                "high_liquid_symbols": sorted(HIGH_LIQUID),
                # symbol -> "current" | "next" | "far" for whichever
                # NIFTY monthly future contracts the master could
                # resolve. Will be {} if the master cache is stale and
                # only one expiry was found (see warning logged at
                # boot in InstrumentManager._prepare_indices).
                "nifty_fut_contracts": nifty_fut_contracts,
            },
        }
        await hub.broadcast_json(msg)
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
                "bucket_start": new_bucket_start.isoformat(),
                "bucket_end": new_bucket_end.isoformat(),
                "interval_minutes": interval_minutes,
            }
        )
