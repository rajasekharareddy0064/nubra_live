import asyncio
import json
import logging
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg

from app.realtime.options_chain import MIN_CHAIN_STRIKES, STRIKE_RADIUS, filter_chain_to_radius

IST = ZoneInfo("Asia/Kolkata")


class DBWriter:
    def __init__(
        self,
        dsn: str,
        batch_size: int = 500,
        flush_interval_ms: int = 1000,
        *,
        schema: str = "public",
    ) -> None:
        self.dsn = dsn
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_ms / 1000.0
        self.queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=200000)
        self.pool: asyncpg.Pool | None = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.enqueued_total = 0
        self.flushed_total = 0
        self.last_flush_topics: dict[str, int] = {}
        self.last_flush_error: str | None = None
        self.last_flush_at: datetime | None = None
        self.schema = self._validate_identifier(schema or "public")
        self.market_events_table = f"{self._q(self.schema)}.{self._q('market_events')}"
        self.order_book_table = f"{self._q(self.schema)}.{self._q('order_book_3m_strikes')}"
        self.order_book_candle_table = f"{self._q(self.schema)}.{self._q('order_book_3m_candles')}"
        self.futures_data_table = f"{self._q(self.schema)}.{self._q('futures_data')}"
        self.nifty50_stock_futures_table = f"{self._q(self.schema)}.{self._q('nifty50_stock_futures')}"
        self.options_data_table = f"{self._q(self.schema)}.{self._q('options_data')}"
        self.market_ohlc_table = f"{self._q(self.schema)}.{self._q('market_ohlc')}"
        self.market_ohlc_3m_table = f"{self._q(self.schema)}.{self._q('market_ohlc_3m')}"
        self.nifty50_stock_ohlc_table = f"{self._q(self.schema)}.{self._q('nifty50_stock_ohlc')}"

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)
        await self._ensure_table()

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def enqueue(self, topic: str, payload: dict[str, Any]) -> None:
        await self.queue.put((topic, payload))
        self.enqueued_total += 1

    async def run_forever(self) -> None:
        self.logger.info("DB_WRITER_STARTED")
        while True:
            try:
                await self.flush_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_flush_error = repr(exc)
                self.logger.exception("db flush failed: %s", exc)
            await asyncio.sleep(self.flush_interval_s)

    async def flush_once(self) -> None:
        if not self.pool:
            return

        batches: dict[str, list[dict[str, Any]]] = defaultdict(list)
        total_rows = 0
        while total_rows < self.batch_size and not self.queue.empty():
            topic, payload = await self.queue.get()
            batches[topic].append(payload)
            total_rows += 1
            self.queue.task_done()

        if not batches:
            return

        self.logger.info(
            "DB_FLUSH_START | topics=%s total_rows=%d queue_remaining=%d",
            {t: len(p) for t, p in batches.items()},
            total_rows,
            self.queue.qsize(),
        )

        import time as _time
        flush_start = _time.time()

        async with self.pool.acquire() as conn:
            self.last_flush_topics = {topic: len(payloads) for topic, payloads in batches.items()}
            order_book_rows = batches.pop("order_book_3m", [])
            if order_book_rows:
                await self._insert_order_book_3m(conn, order_book_rows)
                await self._insert_order_book_3m_candles(conn, order_book_rows)

            futures_rows = batches.pop("futures_3m", [])
            if futures_rows:
                await self._insert_futures_data(conn, futures_rows)
                await self._insert_nifty50_stock_futures(conn, futures_rows)

            options_rows = batches.pop("options_3m", [])
            if options_rows:
                await self._insert_options_data(conn, options_rows)

            market_ohlc_rows = batches.pop("market_ohlc_bundle", [])
            if market_ohlc_rows:
                await self._insert_market_ohlc_bundle(conn, market_ohlc_rows)

            rows: list[tuple[str, str, datetime]] = []
            now = datetime.now(timezone.utc)
            for topic, payloads in batches.items():
                rows.extend((topic, json.dumps(payload), now) for payload in payloads)
            if rows:
                await conn.executemany(
                    f"INSERT INTO {self.market_events_table}(topic, payload, created_at) VALUES($1, $2::jsonb, $3)",
                    rows,
                )

        elapsed_ms = (_time.time() - flush_start) * 1000
        self.flushed_total += total_rows
        self.last_flush_error = None
        self.last_flush_at = datetime.now(timezone.utc)
        self.logger.info(
            "DB_FLUSH_SUCCESS | rows=%d elapsed_ms=%.0f topics=%s",
            total_rows, elapsed_ms, dict(self.last_flush_topics),
        )

    def stats(self) -> dict[str, Any]:
        return {
            "connected": self.pool is not None,
            "queue_size": self.queue.qsize(),
            "enqueued_total": self.enqueued_total,
            "flushed_total": self.flushed_total,
            "last_flush_topics": dict(self.last_flush_topics),
            "last_flush_error": self.last_flush_error,
            "last_flush_at": self.last_flush_at.isoformat() if self.last_flush_at else None,
        }

    async def _insert_order_book_3m(
        self,
        conn: asyncpg.Connection,
        payloads: list[dict[str, Any]],
    ) -> None:
        rows: list[tuple[Any, ...]] = []
        now = datetime.now(timezone.utc)
        for payload in payloads:
            order_book = payload.get("order_book") if isinstance(payload.get("order_book"), dict) else {}
            strikes = self._normalize_strike_rows(order_book)
            timestamp = self._timestamp(payload.get("bucket_end") or payload.get("timestamp"))
            atm = self._num(order_book.get("atm"))
            for row in strikes:
                if not isinstance(row, dict):
                    continue
                # Only insert rows that have actual tick data
                if not row.get("has_data", True):
                    continue
                ce = row.get("ce") if isinstance(row.get("ce"), dict) else {}
                pe = row.get("pe") if isinstance(row.get("pe"), dict) else {}
                rows.append(
                    (
                        timestamp,
                        self._num(atm),
                        self._num(row.get("strike")),
                        self._num(ce.get("avg_bid_qty")),
                        self._num(ce.get("avg_ask_qty")),
                        self._num(ce.get("total_buy_qty")),
                        self._num(ce.get("total_sell_qty")),
                        self._num(ce.get("delta")),
                        self._num(ce.get("imbalance"), digits=4),
                        self._num(pe.get("avg_bid_qty")),
                        self._num(pe.get("avg_ask_qty")),
                        self._num(pe.get("total_buy_qty")),
                        self._num(pe.get("total_sell_qty")),
                        self._num(pe.get("delta")),
                        self._num(pe.get("imbalance"), digits=4),
                        now,
                    )
                )

        if not rows:
            return

        rows_total_checked = sum(
            len(self._normalize_strike_rows(payload.get("order_book") if isinstance(payload.get("order_book"), dict) else {}))
            for payload in payloads
        )
        rows_skipped = rows_total_checked - len(rows)
        self.logger.info(
            "ORDER_BOOK_3M_INSERT | rows_written=%d rows_skipped=%d",
            len(rows),
            rows_skipped,
        )

        strike_count = len({row[2] for row in rows})
        if strike_count < 21:
            self.logger.warning(
                "order_book_3m_strikes insert has only %d strikes for timestamp=%s (expected ATM±10 = 21)",
                strike_count,
                rows[0][0],
            )

        await conn.executemany(
            f"""
            INSERT INTO {self.order_book_table} (
                "timestamp",
                atm,
                strike,
                ce_avg_bid_qty,
                ce_avg_ask_qty,
                ce_total_buy_qty,
                ce_total_sell_qty,
                ce_delta,
                ce_imbalance,
                pe_avg_bid_qty,
                pe_avg_ask_qty,
                pe_total_buy_qty,
                pe_total_sell_qty,
                pe_delta,
                pe_imbalance,
                created_at
            )
            VALUES (
                $1::timestamp, $2, $3,
                $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15,
                $16
            )
            ON CONFLICT ("timestamp", strike) DO UPDATE SET
                atm = EXCLUDED.atm,
                ce_avg_bid_qty = EXCLUDED.ce_avg_bid_qty,
                ce_avg_ask_qty = EXCLUDED.ce_avg_ask_qty,
                ce_total_buy_qty = EXCLUDED.ce_total_buy_qty,
                ce_total_sell_qty = EXCLUDED.ce_total_sell_qty,
                ce_delta = EXCLUDED.ce_delta,
                ce_imbalance = EXCLUDED.ce_imbalance,
                pe_avg_bid_qty = EXCLUDED.pe_avg_bid_qty,
                pe_avg_ask_qty = EXCLUDED.pe_avg_ask_qty,
                pe_total_buy_qty = EXCLUDED.pe_total_buy_qty,
                pe_total_sell_qty = EXCLUDED.pe_total_sell_qty,
                pe_delta = EXCLUDED.pe_delta,
                pe_imbalance = EXCLUDED.pe_imbalance,
                created_at = EXCLUDED.created_at
            """,
            rows,
        )

    async def _insert_order_book_3m_candles(
        self,
        conn: asyncpg.Connection,
        payloads: list[dict[str, Any]],
    ) -> None:
        rows: list[tuple[Any, ...]] = []
        now = datetime.now(timezone.utc)
        for payload in payloads:
            order_book = payload.get("order_book") if isinstance(payload.get("order_book"), dict) else {}
            directional = order_book.get("directional") if isinstance(order_book.get("directional"), dict) else {}
            timestamp = self._timestamp(payload.get("bucket_end") or payload.get("timestamp"))
            rows.append(
                (
                    timestamp,
                    self._num(order_book.get("atm")),
                    self._num(order_book.get("exec_delta")),
                    self._num(order_book.get("book_delta")),
                    self._num(order_book.get("ask_removed")),
                    self._num(order_book.get("bid_removed")),
                    self._num(order_book.get("cum_exec_delta_30")),
                    self._num(order_book.get("cum_book_delta_30")),
                    self._num(order_book.get("imbalance"), digits=4),
                    self._num(order_book.get("strike_shift")),
                    self._num(order_book.get("breakout_score"), digits=2),
                    str(order_book.get("regime") or ""),
                    now,
                    # Additive directional breakdown (from snapshot["directional"]).
                    self._num(directional.get("ce_exec_delta")),
                    self._num(directional.get("pe_exec_delta")),
                    self._num(directional.get("ce_book_delta")),
                    self._num(directional.get("pe_book_delta")),
                    self._num(directional.get("ce_imbalance"), digits=4),
                    self._num(directional.get("pe_imbalance"), digits=4),
                    self._num(directional.get("ce_ask_removed")),
                    self._num(directional.get("ce_bid_removed")),
                    self._num(directional.get("pe_ask_removed")),
                    self._num(directional.get("pe_bid_removed")),
                    self._num(directional.get("bullish_pressure")),
                    self._num(directional.get("bearish_pressure")),
                    self._num(directional.get("net_pressure")),
                )
            )

        if not rows:
            return

        await conn.executemany(
            f"""
            INSERT INTO {self.order_book_candle_table} (
                "timestamp",
                atm,
                exec_delta,
                book_delta,
                ask_removed,
                bid_removed,
                cum_exec_delta_30,
                cum_book_delta_30,
                imbalance,
                strike_shift,
                score,
                regime,
                created_at,
                ce_exec_delta,
                pe_exec_delta,
                ce_book_delta,
                pe_book_delta,
                ce_imbalance,
                pe_imbalance,
                ce_ask_removed,
                ce_bid_removed,
                pe_ask_removed,
                pe_bid_removed,
                bullish_pressure,
                bearish_pressure,
                net_pressure
            )
            VALUES (
                $1::timestamp, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
            )
            ON CONFLICT ("timestamp") DO UPDATE SET
                atm = EXCLUDED.atm,
                exec_delta = EXCLUDED.exec_delta,
                book_delta = EXCLUDED.book_delta,
                ask_removed = EXCLUDED.ask_removed,
                bid_removed = EXCLUDED.bid_removed,
                cum_exec_delta_30 = EXCLUDED.cum_exec_delta_30,
                cum_book_delta_30 = EXCLUDED.cum_book_delta_30,
                imbalance = EXCLUDED.imbalance,
                strike_shift = EXCLUDED.strike_shift,
                score = EXCLUDED.score,
                regime = EXCLUDED.regime,
                created_at = EXCLUDED.created_at,
                ce_exec_delta = EXCLUDED.ce_exec_delta,
                pe_exec_delta = EXCLUDED.pe_exec_delta,
                ce_book_delta = EXCLUDED.ce_book_delta,
                pe_book_delta = EXCLUDED.pe_book_delta,
                ce_imbalance = EXCLUDED.ce_imbalance,
                pe_imbalance = EXCLUDED.pe_imbalance,
                ce_ask_removed = EXCLUDED.ce_ask_removed,
                ce_bid_removed = EXCLUDED.ce_bid_removed,
                pe_ask_removed = EXCLUDED.pe_ask_removed,
                pe_bid_removed = EXCLUDED.pe_bid_removed,
                bullish_pressure = EXCLUDED.bullish_pressure,
                bearish_pressure = EXCLUDED.bearish_pressure,
                net_pressure = EXCLUDED.net_pressure
            """,
            rows,
        )

    async def _insert_futures_data(
        self,
        conn: asyncpg.Connection,
        payloads: list[dict[str, Any]],
    ) -> None:
        rows: list[tuple[Any, ...]] = []
        for payload in payloads:
            futures = payload.get("futures") if isinstance(payload.get("futures"), dict) else {}
            fut_meta = payload.get("fut_meta") if isinstance(payload.get("fut_meta"), dict) else {}
            timestamp = self._timestamp(payload.get("bucket_end") or payload.get("timestamp"))
            scale = self._scale(payload.get("price_scale"))
            for symbol, candle in futures.items():
                if not isinstance(candle, dict) or candle.get("is_empty"):
                    continue
                sym = str(symbol).strip().upper()
                if not sym.startswith("NIFTY") or not sym.endswith("FUT"):
                    continue
                meta = fut_meta.get(sym) if isinstance(fut_meta.get(sym), dict) else {}
                expiry = str(meta.get("expiry") or "").strip()
                if not expiry:
                    self.logger.warning("skipping futures_data insert — missing expiry for %s", sym)
                    continue
                underlying = str(
                    meta.get("underlying_symbol") or candle.get("underlying_symbol") or "NIFTY"
                ).strip().upper()
                rows.append(
                    (
                        timestamp,
                        self._wire_float(candle.get("open"), scale),
                        self._wire_float(candle.get("high"), scale),
                        self._wire_float(candle.get("low"), scale),
                        self._wire_float(candle.get("close"), scale),
                        self._wire_float(candle.get("volume"), scale=1.0),
                        self._wire_float(candle.get("oi"), scale=1.0),
                        sym,
                        None,
                        None,
                        None,
                        None,
                        str(meta.get("instrument_type") or "FUT"),
                        None,
                        None,
                        expiry,
                        underlying,
                    )
                )

        if not rows:
            return

        await conn.executemany(
            f"""
            INSERT INTO {self.futures_data_table} (
                "timestamp",
                open,
                high,
                low,
                close,
                volume,
                oi,
                symbol,
                theta,
                delta,
                gamma,
                vega,
                instrument_type,
                strike_price,
                option_type,
                expiry,
                underlying_symbol
            )
            VALUES (
                $1::timestamp, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16, $17
            )
            ON CONFLICT (symbol, expiry, "timestamp") DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                oi = EXCLUDED.oi,
                instrument_type = EXCLUDED.instrument_type,
                underlying_symbol = EXCLUDED.underlying_symbol
            """,
            rows,
        )

    async def _insert_nifty50_stock_futures(
        self,
        conn: asyncpg.Connection,
        payloads: list[dict[str, Any]],
    ) -> None:
        rows: list[tuple[Any, ...]] = []
        for payload in payloads:
            stocks = payload.get("stocks") if isinstance(payload.get("stocks"), dict) else {}
            fut_meta = payload.get("fut_meta") if isinstance(payload.get("fut_meta"), dict) else {}
            nifty50 = payload.get("nifty50_underlyings")
            nifty50_set = (
                {str(x).strip().upper() for x in nifty50 if str(x).strip()}
                if isinstance(nifty50, (list, set, tuple))
                else set()
            )
            timestamp = self._timestamp(payload.get("bucket_end") or payload.get("timestamp"))
            scale = self._scale(payload.get("price_scale"))
            for symbol, candle in stocks.items():
                if not isinstance(candle, dict) or candle.get("is_empty"):
                    continue
                sym = str(symbol).strip().upper()
                meta = fut_meta.get(sym) if isinstance(fut_meta.get(sym), dict) else {}
                underlying = str(
                    meta.get("underlying_symbol") or candle.get("underlying_symbol") or ""
                ).strip().upper()
                if nifty50_set and underlying not in nifty50_set:
                    continue
                expiry_dt = meta.get("expiry_dt")
                if isinstance(expiry_dt, datetime) and expiry_dt.tzinfo is not None:
                    expiry_dt = expiry_dt.astimezone(IST).replace(tzinfo=None)
                if not isinstance(expiry_dt, datetime):
                    expiry_text = str(meta.get("expiry") or "").strip()
                    if expiry_text:
                        try:
                            expiry_dt = datetime.fromisoformat(expiry_text)
                        except ValueError:
                            expiry_dt = None
                if expiry_dt is None:
                    self.logger.warning(
                        "skipping nifty50_stock_futures insert — missing expiry for %s",
                        sym,
                    )
                    continue
                rows.append(
                    (
                        timestamp,
                        sym,
                        underlying or None,
                        expiry_dt,
                        self._wire_int(candle.get("open"), scale),
                        self._wire_int(candle.get("high"), scale),
                        self._wire_int(candle.get("low"), scale),
                        self._wire_int(candle.get("close"), scale),
                        self._wire_int(candle.get("volume"), scale=1.0),
                        self._wire_int(candle.get("oi"), scale=1.0),
                        str(meta.get("instrument_type") or "FUT"),
                    )
                )

        if not rows:
            return

        await conn.executemany(
            f"""
            INSERT INTO {self.nifty50_stock_futures_table} (
                "timestamp",
                symbol,
                underlying_symbol,
                expiry,
                open,
                high,
                low,
                close,
                volume,
                oi,
                instrument_type
            )
            VALUES ($1::timestamp, $2, $3, $4::timestamp, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (symbol, expiry, "timestamp") DO UPDATE SET
                underlying_symbol = EXCLUDED.underlying_symbol,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                oi = EXCLUDED.oi,
                instrument_type = EXCLUDED.instrument_type
            """,
            rows,
        )

    # ------------------------------------------------------------------
    # options_data (additive — snapshot of the ATM option chain per bar)
    # ------------------------------------------------------------------
    @staticmethod
    def _opt_leg_val(leg: dict[str, Any], *keys: str) -> float | None:
        """First non-null numeric among ``keys`` in an option leg dict."""
        for k in keys:
            v = leg.get(k)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    continue
        return None

    @staticmethod
    def _opt_moneyness(side: str, strike: float, spot: float | None) -> str | None:
        if not spot or spot <= 0:
            return None
        if abs(strike - spot) < 25:  # inside half a 50-pt step → at-the-money
            return "ATM"
        if side == "CE":
            return "ITM" if spot > strike else "OTM"
        return "ITM" if spot < strike else "OTM"

    @staticmethod
    def _grow_option_symbol(expiry_date: Any, strike: int, side: str) -> str:
        """Match Grow historical symbol format, e.g. NIFTY2672124550CE."""
        yy = int(expiry_date.year) % 100
        return f"NIFTY{yy}{int(expiry_date.month)}{int(expiry_date.day)}{int(strike)}{str(side).upper()}"

    @staticmethod
    def _opt_price_rupees(value: Any) -> float | None:
        """Normalize option prices to rupees for Grow-compatible storage."""
        if value is None:
            return None
        try:
            price = float(value)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None
        # Feed premiums often arrive in paise; Grow rows are rupees (e.g. 18.25).
        if price >= 500:
            return price / 100.0
        return price

    async def _insert_options_data(
        self,
        conn: asyncpg.Connection,
        payloads: list[dict[str, Any]],
    ) -> None:
        """Persist per-bar NIFTY option candles into options_data (Grow format).

        Uses tick-aggregated OHLCV when available; falls back to chain snapshot.
        Prices are stored in RUPEES to match Grow historical rows.
        """
        rows: list[tuple[Any, ...]] = []
        rows_received = 0
        skipped_no_expiry = 0
        skipped_no_price = 0
        for payload in payloads:
            chain = payload.get("chain") if isinstance(payload.get("chain"), list) else []
            option_candles = payload.get("option_candles") if isinstance(payload.get("option_candles"), dict) else {}
            timestamp = self._timestamp(payload.get("bucket_end") or payload.get("timestamp"))
            expiry = str(payload.get("expiry") or "").strip()
            underlying = str(payload.get("underlying") or "NIFTY").strip().upper()
            spot_raw = self._opt_leg_val(payload, "spot")
            spot = self._opt_price_rupees(spot_raw) if spot_raw is not None else None
            if not expiry:
                skipped_no_expiry += 1
                self.logger.warning(
                    "OPTION_DB_FAILED | reason=missing_expiry bucket=%s rows_in_chain=%d",
                    payload.get("bucket_end"),
                    len(chain),
                )
                continue
            try:
                expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
            except (TypeError, ValueError):
                skipped_no_expiry += 1
                self.logger.warning(
                    "OPTION_DB_FAILED | reason=bad_expiry expiry=%r bucket=%s",
                    expiry,
                    payload.get("bucket_end"),
                )
                continue
            chain = filter_chain_to_radius(chain, spot, radius=STRIKE_RADIUS)
            if spot and spot > 0:
                strikes_in_chain = {
                    int(round(float(row.get("strike"))))
                    for row in chain
                    if isinstance(row, dict) and row.get("strike") is not None
                }
                if len(strikes_in_chain) < MIN_CHAIN_STRIKES:
                    self.logger.warning(
                        "options_data insert has only %d strikes for timestamp=%s "
                        "(expected ATM±%d = %d; spot=%s)",
                        len(strikes_in_chain),
                        payload.get("bucket_end"),
                        STRIKE_RADIUS,
                        MIN_CHAIN_STRIKES,
                        spot,
                    )
            allowed_keys = {
                f"{int(round(float(row.get('strike'))))}:{side}"
                for row in chain
                if isinstance(row, dict) and row.get("strike") is not None
                for side in ("CE", "PE")
                if isinstance(row.get(side), dict) and row.get(side)
            }
            option_candles = {
                k: v for k, v in option_candles.items() if k in allowed_keys
            }
            for crow in chain:
                if not isinstance(crow, dict):
                    continue
                try:
                    strike = int(round(float(crow.get("strike"))))
                except (TypeError, ValueError):
                    continue
                for side in ("CE", "PE"):
                    leg = crow.get(side)
                    if not isinstance(leg, dict) or not leg:
                        continue
                    rows_received += 1
                    candle_key = f"{strike}:{side}"
                    agg = option_candles.get(candle_key) if isinstance(option_candles.get(candle_key), dict) else None
                    if agg and not agg.get("is_empty") and agg.get("close"):
                        open_p = self._opt_price_rupees(agg.get("open"))
                        high_p = self._opt_price_rupees(agg.get("high"))
                        low_p = self._opt_price_rupees(agg.get("low"))
                        close_p = self._opt_price_rupees(agg.get("close"))
                        volume = agg.get("volume")
                        oi = agg.get("oi")
                    else:
                        ltp = self._opt_leg_val(leg, "ltp", "last_price", "close")
                        open_p = high_p = low_p = close_p = self._opt_price_rupees(ltp)
                        volume = self._opt_leg_val(leg, "volume", "traded_volume", "tradedVolume")
                        oi = self._opt_leg_val(leg, "oi", "open_interest", "openInterest")
                    if close_p is None or close_p <= 0:
                        skipped_no_price += 1
                        continue
                    open_p = open_p if open_p is not None else close_p
                    high_p = high_p if high_p is not None else close_p
                    low_p = low_p if low_p is not None else close_p
                    delta = self._opt_leg_val(leg, "delta")
                    gamma = self._opt_leg_val(leg, "gamma")
                    theta = self._opt_leg_val(leg, "theta")
                    vega = self._opt_leg_val(leg, "vega")
                    iv = self._opt_leg_val(leg, "iv")
                    symbol = self._grow_option_symbol(expiry_date, strike, side)
                    strike_rs = float(strike)
                    rows.append(
                        (
                            timestamp,
                            symbol,
                            expiry_date,
                            self._num(strike_rs),
                            side,
                            self._num(open_p),
                            self._num(high_p),
                            self._num(low_p),
                            self._num(close_p),
                            self._wire_int(volume, 1.0),
                            self._wire_int(oi, 1.0),
                            self._num(theta, digits=6),
                            self._num(delta, digits=6),
                            self._num(gamma, digits=6),
                            self._num(vega, digits=6),
                            self._num(iv, digits=6),
                            "OPT",
                            None,
                            float(close_p),
                            underlying,
                            self._opt_moneyness(side, int(round(strike_rs)), spot),
                            (float(strike_rs) - spot) if spot else None,
                        )
                    )

        self.logger.info(
            "OPTION_DB_INSERT | rows_received=%d rows_built=%d skipped_no_expiry=%d skipped_no_price=%d",
            rows_received, len(rows), skipped_no_expiry, skipped_no_price,
        )
        if not rows:
            return

        try:
            await conn.executemany(
                f"""
                INSERT INTO {self.options_data_table} (
                    "timestamp", symbol, expiry, strike, option_type,
                    open, high, low, close, volume, oi,
                    theta, delta, gamma, vega, iv,
                    instrument_type, iv_mid, ltp, underlying_symbol,
                    moneyness, spot_distance
                )
                VALUES (
                    $1::timestamp, $2, $3::date, $4, $5,
                    $6, $7, $8, $9, $10, $11,
                    $12, $13, $14, $15, $16,
                    $17, $18, $19, $20,
                    $21, $22
                )
                ON CONFLICT ("timestamp", symbol, strike, option_type) DO UPDATE SET
                    expiry = EXCLUDED.expiry,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    oi = EXCLUDED.oi,
                    theta = EXCLUDED.theta,
                    delta = EXCLUDED.delta,
                    gamma = EXCLUDED.gamma,
                    vega = EXCLUDED.vega,
                    iv = EXCLUDED.iv,
                    instrument_type = EXCLUDED.instrument_type,
                    ltp = EXCLUDED.ltp,
                    underlying_symbol = EXCLUDED.underlying_symbol,
                    moneyness = EXCLUDED.moneyness,
                    spot_distance = EXCLUDED.spot_distance
                """,
                rows,
            )
            self.logger.info("OPTION_DB_SUCCESS | rows_inserted=%d", len(rows))
        except Exception as exc:
            self.logger.exception("OPTION_DB_FAILED | error=%s", exc)
            raise

    async def _insert_market_ohlc_bundle(
        self,
        conn: asyncpg.Connection,
        payloads: list[dict[str, Any]],
    ) -> None:
        """Write NIFTY spot to market_ohlc; NIFTY50 cash to market_ohlc_3m only."""
        market_rows: list[tuple[Any, ...]] = []
        market_3m_rows: list[tuple[Any, ...]] = []
        created_at = datetime.now(IST).replace(tzinfo=None)
        for payload in payloads:
            timestamp = self._timestamp(payload.get("bucket_end") or payload.get("timestamp"))
            nifty = payload.get("nifty") if isinstance(payload.get("nifty"), dict) else {}
            stock_spots = payload.get("stock_spots") if isinstance(payload.get("stock_spots"), dict) else {}
            stock_eq_meta = (
                payload.get("stock_eq_meta") if isinstance(payload.get("stock_eq_meta"), dict) else {}
            )
            close = nifty.get("close")
            if close is not None and float(close) > 0:
                open_p = self._num(nifty.get("open"))
                high_p = self._num(nifty.get("high"))
                low_p = self._num(nifty.get("low"))
                close_p = self._num(close)
                volume = self._wire_int(nifty.get("volume"), 1.0)
                market_rows.append(
                    ("NIFTY", "3m", timestamp, open_p, high_p, low_p, close_p, volume, created_at)
                )
            for sym, candle in stock_spots.items():
                if not isinstance(candle, dict) or candle.get("is_empty"):
                    continue
                symbol = str(sym).strip().upper()
                if not symbol or float(candle.get("close") or 0) <= 0:
                    continue
                meta = stock_eq_meta.get(symbol) if isinstance(stock_eq_meta.get(symbol), dict) else {}
                open_p = self._num(candle.get("open"))
                high_p = self._num(candle.get("high"))
                low_p = self._num(candle.get("low"))
                close_p = self._num(candle.get("close"))
                volume = self._wire_int(candle.get("volume"), 1.0)
                market_3m_rows.append(
                    (
                        symbol,
                        meta.get("symbol_token"),
                        str(meta.get("exchange") or "NSE").strip().upper(),
                        str(meta.get("instrument_type") or "STOCK").strip().upper(),
                        "3m",
                        timestamp,
                        open_p,
                        high_p,
                        low_p,
                        close_p,
                        volume,
                        created_at,
                    )
                )

        if market_rows:
            await conn.executemany(
                f"""
                INSERT INTO {self.market_ohlc_table} (
                    symbol, interval, "timestamp", open, high, low, close, volume, created_at
                )
                VALUES ($1, $2, $3::timestamp, $4, $5, $6, $7, $8, $9::timestamp)
                """,
                market_rows,
            )
        if market_3m_rows:
            await conn.executemany(
                f"""
                INSERT INTO {self.market_ohlc_3m_table} (
                    symbol, symbol_token, exchange, instrument_type, interval,
                    candle_time, open, high, low, close, volume, created_at
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6::timestamp, $7, $8, $9, $10, $11, $12::timestamp
                )
                ON CONFLICT (symbol, candle_time) DO UPDATE SET
                    symbol_token = EXCLUDED.symbol_token,
                    exchange = EXCLUDED.exchange,
                    instrument_type = EXCLUDED.instrument_type,
                    interval = EXCLUDED.interval,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    created_at = EXCLUDED.created_at
                """,
                market_3m_rows,
            )
        self.logger.info(
            "MARKET_OHLC_INSERT | market_ohlc=%d market_ohlc_3m=%d",
            len(market_rows),
            len(market_3m_rows),
        )

    async def _ensure_table(self) -> None:
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._q(self.schema)};")
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.market_events_table} (
                  id BIGSERIAL PRIMARY KEY,
                  topic TEXT NOT NULL,
                  payload JSONB NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL
                );
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.order_book_table} (
                  id BIGSERIAL PRIMARY KEY,
                  "timestamp" TIMESTAMP NOT NULL,
                  atm NUMERIC(12,2),
                  strike NUMERIC(12,2) NOT NULL,
                  ce_avg_bid_qty NUMERIC(18,2),
                  ce_avg_ask_qty NUMERIC(18,2),
                  ce_total_buy_qty NUMERIC(18,2),
                  ce_total_sell_qty NUMERIC(18,2),
                  ce_delta NUMERIC(18,2),
                  ce_imbalance NUMERIC(10,4),
                  pe_avg_bid_qty NUMERIC(18,2),
                  pe_avg_ask_qty NUMERIC(18,2),
                  pe_total_buy_qty NUMERIC(18,2),
                  pe_total_sell_qty NUMERIC(18,2),
                  pe_delta NUMERIC(18,2),
                  pe_imbalance NUMERIC(10,4),
                  created_at TIMESTAMPTZ NOT NULL
                );
                """
            )
            await conn.execute(
                f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = '{self.schema}'
                          AND table_name = 'order_book_3m_strikes'
                          AND column_name = 'timestamp'
                          AND data_type = 'timestamp with time zone'
                    ) THEN
                        ALTER TABLE {self.order_book_table}
                        ALTER COLUMN "timestamp" TYPE TIMESTAMP
                        USING ("timestamp" AT TIME ZONE 'Asia/Kolkata');
                    END IF;
                END $$;
                """
            )
            await self._ensure_order_book_numeric_scales(conn)
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {self._q('ux_order_book_3m_timestamp_strike')}
                ON {self.order_book_table} ("timestamp", strike);
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self._q('idx_order_book_3m_timestamp')}
                ON {self.order_book_table} ("timestamp" DESC);
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.order_book_candle_table} (
                  id BIGSERIAL PRIMARY KEY,
                  "timestamp" TIMESTAMP NOT NULL UNIQUE,
                  atm NUMERIC(12,2),
                  exec_delta NUMERIC(18,2),
                  book_delta NUMERIC(18,2),
                  ask_removed NUMERIC(18,2),
                  bid_removed NUMERIC(18,2),
                  cum_exec_delta_30 NUMERIC(18,2),
                  cum_book_delta_30 NUMERIC(18,2),
                  imbalance NUMERIC(10,4),
                  strike_shift NUMERIC(12,2),
                  score NUMERIC(8,2),
                  regime TEXT,
                  created_at TIMESTAMPTZ NOT NULL
                );
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self._q('idx_order_book_3m_candles_timestamp')}
                ON {self.order_book_candle_table} ("timestamp" DESC);
                """
            )
            # Additive directional columns (CE/PE breakdown + net pressures).
            # Existing columns/semantics are untouched; old rows stay NULL.
            await conn.execute(
                f"""
                ALTER TABLE {self.order_book_candle_table}
                    ADD COLUMN IF NOT EXISTS ce_exec_delta   NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS pe_exec_delta   NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS ce_book_delta   NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS pe_book_delta   NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS ce_imbalance    NUMERIC(10,4),
                    ADD COLUMN IF NOT EXISTS pe_imbalance    NUMERIC(10,4),
                    ADD COLUMN IF NOT EXISTS ce_ask_removed  NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS ce_bid_removed  NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS pe_ask_removed  NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS pe_bid_removed  NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS bullish_pressure NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS bearish_pressure NUMERIC(18,2),
                    ADD COLUMN IF NOT EXISTS net_pressure     NUMERIC(18,2);
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.futures_data_table} (
                  id BIGSERIAL PRIMARY KEY,
                  "timestamp" TIMESTAMP NOT NULL,
                  open NUMERIC(18,2),
                  high NUMERIC(18,2),
                  low NUMERIC(18,2),
                  close NUMERIC(18,2),
                  volume NUMERIC(18,2),
                  oi NUMERIC(18,2),
                  symbol TEXT NOT NULL,
                  theta NUMERIC(18,6),
                  delta NUMERIC(18,6),
                  gamma NUMERIC(18,6),
                  vega NUMERIC(18,6),
                  instrument_type TEXT,
                  strike_price NUMERIC(12,2),
                  option_type TEXT,
                  expiry TEXT NOT NULL,
                  underlying_symbol TEXT
                );
                """
            )
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {self._q('ux_futures_data_symbol_expiry_timestamp')}
                ON {self.futures_data_table} (symbol, expiry, "timestamp");
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self._q('idx_futures_data_timestamp')}
                ON {self.futures_data_table} ("timestamp" DESC);
                """
            )
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.nifty50_stock_futures_table} (
                  id BIGSERIAL PRIMARY KEY,
                  "timestamp" TIMESTAMP NOT NULL,
                  symbol TEXT NOT NULL,
                  underlying_symbol TEXT,
                  expiry TIMESTAMP NOT NULL,
                  open NUMERIC(18,2),
                  high NUMERIC(18,2),
                  low NUMERIC(18,2),
                  close NUMERIC(18,2),
                  volume NUMERIC(18,2),
                  oi NUMERIC(18,2),
                  instrument_type TEXT
                );
                """
            )
            await conn.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {self._q('ux_nifty50_stock_futures_symbol_expiry_timestamp')}
                ON {self.nifty50_stock_futures_table} (symbol, expiry, "timestamp");
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self._q('idx_nifty50_stock_futures_timestamp')}
                ON {self.nifty50_stock_futures_table} ("timestamp" DESC);
                """
            )

    @staticmethod
    def _validate_identifier(value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"invalid PostgreSQL identifier: {value!r}")
        return value

    @staticmethod
    def _q(value: str) -> str:
        return f'"{value}"'

    @staticmethod
    def _num(value: Any, *, digits: int = 2) -> Decimal | None:
        if value is None:
            return None
        try:
            quant = Decimal("1").scaleb(-digits)
            return Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP)
        except (InvalidOperation, TypeError, ValueError):
            return None

    @staticmethod
    def _timestamp(value: Any) -> datetime:
        if isinstance(value, datetime):
            dt = value
        if isinstance(value, str):
            text = value.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(text)
            except ValueError:
                dt = datetime.now(timezone.utc)
        elif not isinstance(value, datetime):
            dt = datetime.now(timezone.utc)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return dt.astimezone(IST).replace(tzinfo=None)

    @staticmethod
    def _normalize_strike_rows(order_book: dict[str, Any], *, min_strikes: int = 21) -> list[dict[str, Any]]:
        """Ensure ATM ±10 (21 strikes at 50-pt step) before DB insert."""
        strikes = order_book.get("strikes") if isinstance(order_book.get("strikes"), list) else []
        rows = [row for row in strikes if isinstance(row, dict)]
        if len(rows) >= min_strikes:
            return rows

        atm_raw = order_book.get("atm")
        try:
            atm = int(float(atm_raw))
        except (TypeError, ValueError):
            return rows

        step = 50
        radius = 10  # ATM ±10
        by_strike = {int(float(row.get("strike"))): row for row in rows if row.get("strike") is not None}
        padded: list[dict[str, Any]] = []
        for offset in range(-radius, radius + 1):
            strike = atm + offset * step
            existing = by_strike.get(strike)
            if existing is not None:
                padded.append(existing)
            else:
                padded.append({"strike": strike, "ce": {}, "pe": {}})
        return padded

    @staticmethod
    def _scale(value: Any) -> float:
        try:
            scale = float(value)
        except (TypeError, ValueError):
            return 1.0
        return scale if scale > 0 else 1.0

    @staticmethod
    def _wire_float(value: Any, scale: float) -> float | None:
        if value is None:
            return None
        try:
            return float(value) * scale
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _wire_int(value: Any, scale: float) -> int | None:
        if value is None:
            return None
        try:
            return int(round(float(value) * scale))
        except (TypeError, ValueError):
            return None

    async def _ensure_order_book_numeric_scales(self, conn: asyncpg.Connection) -> None:
        two_digit_columns = [
            ("atm", "NUMERIC(12,2)"),
            ("strike", "NUMERIC(12,2)"),
            ("ce_avg_bid_qty", "NUMERIC(18,2)"),
            ("ce_avg_ask_qty", "NUMERIC(18,2)"),
            ("ce_total_buy_qty", "NUMERIC(18,2)"),
            ("ce_total_sell_qty", "NUMERIC(18,2)"),
            ("ce_delta", "NUMERIC(18,2)"),
            ("pe_avg_bid_qty", "NUMERIC(18,2)"),
            ("pe_avg_ask_qty", "NUMERIC(18,2)"),
            ("pe_total_buy_qty", "NUMERIC(18,2)"),
            ("pe_total_sell_qty", "NUMERIC(18,2)"),
            ("pe_delta", "NUMERIC(18,2)"),
        ]
        four_digit_columns = [
            ("ce_imbalance", "NUMERIC(10,4)"),
            ("pe_imbalance", "NUMERIC(10,4)"),
        ]
        expected = {
            **{column: (int(data_type.split("(")[1].split(",")[0]), 2) for column, data_type in two_digit_columns},
            **{column: (int(data_type.split("(")[1].split(",")[0]), 4) for column, data_type in four_digit_columns},
        }
        existing = await conn.fetch(
            """
            SELECT column_name, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = 'order_book_3m_strikes'
              AND column_name = ANY($2::text[])
            """,
            self.schema,
            list(expected),
        )
        if all(
            (
                row["column_name"] in expected
                and (row["numeric_precision"], row["numeric_scale"]) == expected[row["column_name"]]
            )
            for row in existing
        ) and len(existing) == len(expected):
            return

        for column, data_type in two_digit_columns:
            await conn.execute(
                f"""
                ALTER TABLE {self.order_book_table}
                ALTER COLUMN {self._q(column)} TYPE {data_type}
                USING ROUND({self._q(column)}::numeric, 2);
                """
            )
        for column, data_type in four_digit_columns:
            await conn.execute(
                f"""
                ALTER TABLE {self.order_book_table}
                ALTER COLUMN {self._q(column)} TYPE {data_type}
                USING ROUND({self._q(column)}::numeric, 4);
                """
            )
