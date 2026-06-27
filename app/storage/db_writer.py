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

            rows: list[tuple[str, str, datetime]] = []
            now = datetime.now(timezone.utc)
            for topic, payloads in batches.items():
                rows.extend((topic, json.dumps(payload), now) for payload in payloads)
            if rows:
                await conn.executemany(
                    f"INSERT INTO {self.market_events_table}(topic, payload, created_at) VALUES($1, $2::jsonb, $3)",
                    rows,
                )
        self.flushed_total += total_rows
        self.last_flush_error = None
        self.last_flush_at = datetime.now(timezone.utc)

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

        strike_count = len({row[2] for row in rows})
        if strike_count < 10:
            self.logger.warning(
                "order_book_3m_strikes insert has only %d strikes for timestamp=%s",
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
                created_at
            )
            VALUES (
                $1::timestamp, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
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
                created_at = EXCLUDED.created_at
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
    def _normalize_strike_rows(order_book: dict[str, Any], *, min_strikes: int = 10) -> list[dict[str, Any]]:
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
        radius = math.ceil((min_strikes - 1) / 2)
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
