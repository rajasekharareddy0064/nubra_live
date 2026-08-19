"""Backfill 3m candles into Postgres from Nubra historical_data().

Covers INDEX / NIFTY50 spot / NIFTY+stock FUT / NIFTY options (including
expired weeklies and monthlies — see
https://nubra.io/products/api/docs/guides/ExpiredOptions/).

Order-book tables are not in the historical API and are left untouched.

Usage (from repo root, needs local TOTP + Postgres):

    python scripts/backfill_historical_db.py
    python scripts/backfill_historical_db.py --from 2026-07-12 --to 2026-08-14
    python scripts/backfill_historical_db.py --dry-run --kinds INDEX,STOCK
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import zlib
from collections import defaultdict
from datetime import date, datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

sys.path.insert(0, ".")

from app.core.env_loader import load_project_env

load_project_env(".")

import logging

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    force=True,
)

from app.ingestion.input_patch import install_non_interactive_input_patch

install_non_interactive_input_patch(require_totp_secret=False)

from app.core.config import settings
from app.historical.expired import (
    fut_trading_symbol,
    grow_option_symbol,
    iter_weekdays,
    monthly_expiries_covering,
    nubra_option_symbol,
    option_expiries_for_session,
    parse_fut_symbol,
    strike_ladder,
)
from app.historical.normalize import to_bucket_end_index
from app.historical.universe import FUT_FIELDS, OHLC_FIELDS, UniverseGroup
from app.instruments.nifty50 import (
    NIFTY50_MASTER_ASSET_ALIASES,
    NIFTY50_SYMBOLS_SORTED,
    nifty50_canonical_symbol,
)
from app.realtime.interval_clock import is_nse_session_close_label
from app.realtime.options_chain import STRIKE_STEP, get_atm_strike, get_strike_range
from app.storage.db_writer import DBWriter

IST = ZoneInfo("Asia/Kolkata")
DEFAULT_FROM = date(2026, 7, 12)
OPT_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "cumulative_volume",
    "cumulative_oi",
    "theta",
    "delta",
    "gamma",
    "vega",
    "iv_mid",
]
INDEX_FIELDS = ["open", "high", "low", "close", "cumulative_volume"]


def _parse_args() -> argparse.Namespace:
    today = datetime.now(IST).date()
    parser = argparse.ArgumentParser(description="Backfill DB 3m candles from Nubra historical REST")
    parser.add_argument("--from", dest="date_from", default=DEFAULT_FROM.isoformat())
    parser.add_argument("--to", dest="date_to", default=today.isoformat())
    parser.add_argument(
        "--kinds",
        default="INDEX,STOCK,FUT,OPT",
        help="Comma list: INDEX,STOCK,FUT,OPT",
    )
    parser.add_argument(
        "--radius",
        type=int,
        default=0,
        help="OPT ATM strike radius (0 = settings.option_emit_radius, default 10)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print counts; do not write DB")
    parser.add_argument(
        "--no-replace",
        action="store_true",
        help="Do not delete existing rows in the date window before insert",
    )
    return parser.parse_args()


def _kinds(text: str) -> set[str]:
    return {p.strip().upper() for p in str(text).split(",") if p.strip()}


def _utc_window(start: date, end: date) -> tuple[str, str]:
    start_dt = datetime.combine(start, time(9, 15), tzinfo=IST)
    end_dt = datetime.combine(end, time(15, 31), tzinfo=IST)
    fmt = "%Y-%m-%dT%H:%M:%S.000Z"
    return (
        start_dt.astimezone(timezone.utc).strftime(fmt),
        end_dt.astimezone(timezone.utc).strftime(fmt),
    )


def _naive(ts: Any) -> datetime | None:
    try:
        stamp = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
    except Exception:
        return None
    if not isinstance(stamp, datetime):
        return None
    if stamp.tzinfo is not None:
        stamp = stamp.astimezone(IST).replace(tzinfo=None)
    return stamp.replace(second=0, microsecond=0)


def _session_rows(frame, *, alignment: str, interval: int, start: date, end: date):
    aligned = to_bucket_end_index(frame, alignment=alignment, interval_minutes=interval)
    if aligned is None or aligned.empty:
        return []
    rows = []
    for ts, rec in aligned.iterrows():
        stamp = _naive(ts)
        if stamp is None:
            continue
        if stamp.date() < start or stamp.date() > end:
            continue
        if not is_nse_session_close_label(stamp, interval_minutes=interval):
            continue
        rows.append((stamp, rec))
    return rows


def _f(rec, *keys: str) -> float | None:
    for key in keys:
        if hasattr(rec, "get"):
            val = rec.get(key)
        else:
            val = rec[key] if key in rec else None
        if val is None:
            continue
        try:
            out = float(val)
        except (TypeError, ValueError):
            continue
        if out == out:
            return out
    return None


def _candle(rec) -> dict[str, Any] | None:
    close = _f(rec, "close")
    if close is None or close <= 0:
        return None
    open_p = _f(rec, "open") or close
    high_p = _f(rec, "high") or close
    low_p = _f(rec, "low") or close
    return {
        "open": open_p,
        "high": high_p,
        "low": low_p,
        "close": close,
        "volume": _f(rec, "volume") or 0.0,
        "oi": _f(rec, "oi"),
        "is_empty": False,
    }


def _spot_fetch_symbol(index_symbol: str) -> str:
    return NIFTY50_MASTER_ASSET_ALIASES.get(index_symbol, index_symbol)


async def _delete_window(conn, writer: DBWriter, start: date, end: date, kinds: set[str]) -> None:
    schema = writer.schema
    if "INDEX" in kinds:
        n = await conn.execute(
            f'DELETE FROM "{schema}".market_ohlc '
            f"WHERE symbol = 'NIFTY' AND interval = '3m' "
            f'AND DATE("timestamp") BETWEEN $1 AND $2',
            start,
            end,
        )
        print(f"  deleted market_ohlc: {n}")
    if "STOCK" in kinds:
        n = await conn.execute(
            f'DELETE FROM "{schema}".market_ohlc_3m '
            f"WHERE DATE(candle_time) BETWEEN $1 AND $2",
            start,
            end,
        )
        print(f"  deleted market_ohlc_3m: {n}")
    if "FUT" in kinds:
        n = await conn.execute(
            f'DELETE FROM "{schema}".futures_data '
            f'WHERE DATE("timestamp") BETWEEN $1 AND $2',
            start,
            end,
        )
        print(f"  deleted futures_data: {n}")
        n = await conn.execute(
            f'DELETE FROM "{schema}".nifty50_stock_futures '
            f'WHERE DATE("timestamp") BETWEEN $1 AND $2',
            start,
            end,
        )
        print(f"  deleted nifty50_stock_futures: {n}")
    if "OPT" in kinds:
        n = await conn.execute(
            f'DELETE FROM "{schema}".options_data '
            f"WHERE (underlying_symbol = 'NIFTY' OR underlying_symbol IS NULL) "
            f'AND DATE("timestamp") BETWEEN $1 AND $2',
            start,
            end,
        )
        print(f"  deleted options_data: {n}")


def _day_hl(nifty_rows: list[tuple[datetime, Any]]) -> dict[date, tuple[float, float]]:
    by_day: dict[date, list[float]] = defaultdict(list)
    for stamp, rec in nifty_rows:
        close = _f(rec, "close")
        low = _f(rec, "low") or close
        high = _f(rec, "high") or close
        if close:
            by_day[stamp.date()].append(float(low or close))
            by_day[stamp.date()].append(float(high or close))
    out: dict[date, tuple[float, float]] = {}
    for day, vals in by_day.items():
        out[day] = (min(vals), max(vals))
    return out


def _build_option_universe(
    *,
    start: date,
    end: date,
    day_hl: dict[date, tuple[float, float]],
    radius: int,
    exchange: str,
) -> tuple[UniverseGroup | None, dict[str, dict[str, Any]]]:
    """Nubra OPT symbols for front weekly + live monthly, ATM±radius per session."""
    by_expiry: dict[tuple[date, str], tuple[float, float]] = {}
    for day in iter_weekdays(start, end):
        hl = day_hl.get(day)
        if not hl:
            continue
        for expiry, kind in option_expiries_for_session(day):
            key = (expiry, kind)
            prev = by_expiry.get(key)
            if prev is None:
                by_expiry[key] = hl
            else:
                by_expiry[key] = (min(prev[0], hl[0]), max(prev[1], hl[1]))
    meta: dict[str, dict[str, Any]] = {}
    symbols: list[str] = []
    for (expiry, kind), (lo, hi) in sorted(by_expiry.items()):
        weekly = kind == "weekly"
        for strike in strike_ladder(lo, hi, radius=radius):
            for side in ("CE", "PE"):
                fetch_sym = nubra_option_symbol(expiry, strike, side, weekly=weekly)
                if fetch_sym in meta:
                    continue
                meta[fetch_sym] = {
                    "expiry": expiry,
                    "strike": strike,
                    "side": side,
                    "weekly": weekly,
                    "grow": grow_option_symbol(expiry, strike, side),
                }
                symbols.append(fetch_sym)
    if not symbols:
        return None, {}
    print(
        f"option universe expiries={len(by_expiry)} symbols={len(symbols)} "
        f"(weekly+monthly, radius={radius})"
    )
    return UniverseGroup("OPT", exchange, symbols, list(OPT_FIELDS), meta={"legs": meta}), meta


def _fut_groups(start: date, end: date, exchange: str) -> tuple[UniverseGroup, dict[str, dict[str, Any]]]:
    expiries = [e for e in monthly_expiries_covering(start, end, extra_months=1) if e >= start]
    symbols: list[str] = []
    meta: dict[str, dict[str, Any]] = {}
    for exp in expiries:
        nifty_sym = fut_trading_symbol("NIFTY", exp)
        meta[nifty_sym] = {
            "expiry": exp.isoformat(),
            "expiry_dt": datetime.combine(exp, time(15, 30)),
            "underlying_symbol": "NIFTY",
            "instrument_type": "FUT",
        }
        symbols.append(nifty_sym)
        for index_sym in NIFTY50_SYMBOLS_SORTED:
            asset = _spot_fetch_symbol(index_sym)
            fut_sym = fut_trading_symbol(asset, exp)
            if fut_sym in meta:
                continue
            meta[fut_sym] = {
                "expiry": exp.isoformat(),
                "expiry_dt": datetime.combine(exp, time(15, 30)),
                "underlying_symbol": nifty50_canonical_symbol(index_sym),
                "instrument_type": "FUT",
            }
            symbols.append(fut_sym)
    print(f"fut universe months={[e.isoformat() for e in expiries]} symbols={len(symbols)}")
    fut_fields = [f for f in FUT_FIELDS if f != "tick_volume"]
    return UniverseGroup("FUT", exchange, symbols, fut_fields), meta


async def _insert_chunks(
    writer: DBWriter,
    conn,
    method,
    payloads: list[dict[str, Any]],
    *,
    size: int = 40,
    label: str = "",
) -> int:
    total_rows = len(payloads)
    if total_rows == 0:
        return 0
    n_chunks = (total_rows + size - 1) // size
    done = 0
    for i in range(0, total_rows, size):
        chunk = payloads[i : i + size]
        await method(conn, chunk)
        done += len(chunk)
        chunk_num = i // size + 1
        prefix = f"{label} " if label else ""
        print(f"  {prefix}chunk {chunk_num}/{n_chunks} ({done}/{total_rows})", flush=True)
    return done


async def main() -> None:
    args = _parse_args()
    start = date.fromisoformat(str(args.date_from))
    end = date.fromisoformat(str(args.date_to))
    if end < start:
        print("ERROR: --to is before --from")
        return
    kinds = _kinds(args.kinds)
    radius = int(args.radius) if args.radius else int(settings.option_emit_radius)
    interval = int(settings.candle_interval_minutes)
    exchange = settings.nubra_exchange
    price_scale = 100.0 if str(settings.nubra_env).upper() == "PROD" else 1.0
    start_iso, end_iso = _utc_window(start, end)
    print(
        f"backfill {start} -> {end} kinds={sorted(kinds)} env={settings.nubra_env} "
        f"schema={settings.db_schema} radius={radius} dry_run={args.dry_run}"
    )
    print(f"historical window {start_iso} -> {end_iso}")

    from app.historical.client import HistoricalClient

    client = HistoricalClient(
        env_name=settings.nubra_env,
        exchange=exchange,
        price_scale=price_scale,
        interval=f"{interval}m",
    )

    groups: list[UniverseGroup] = []
    if "INDEX" in kinds or "OPT" in kinds:
        groups.append(UniverseGroup("INDEX", exchange, ["NIFTY"], list(INDEX_FIELDS)))
    stock_alias_to_canonical = {v: k for k, v in NIFTY50_MASTER_ASSET_ALIASES.items()}
    if "STOCK" in kinds:
        fetch_spots = sorted({_spot_fetch_symbol(s) for s in NIFTY50_SYMBOLS_SORTED})
        groups.append(
            UniverseGroup(
                "STOCK",
                exchange,
                fetch_spots,
                [f for f in OHLC_FIELDS if f != "tick_volume"],
            )
        )
    fut_meta: dict[str, dict[str, Any]] = {}
    if "FUT" in kinds:
        fut_group, fut_meta = _fut_groups(start, end, exchange)
        groups.append(fut_group)

    print("fetching INDEX/STOCK/FUT …")
    fetched = await client.fetch(
        groups,
        interval=f"{interval}m",
        intra_day=False,
        real_time=False,
        start_date=start_iso,
        end_date=end_iso,
        progress=True,
    )
    alignment = fetched.policy.bar_alignment or "open"
    print(
        f"fetched symbols={len(fetched.frames)} requests={fetched.request_count} "
        f"errors={len(fetched.errors)} alignment={alignment}"
    )
    for err in fetched.errors[:12]:
        print("  fetch error:", err)
    if fetched.policy.notes:
        print("HIST_PROBE:", " | ".join(fetched.policy.notes))

    nifty_rows = _session_rows(
        fetched.frames.get("NIFTY"),
        alignment=alignment,
        interval=interval,
        start=start,
        end=end,
    )
    print(f"NIFTY session bars={len(nifty_rows)}")
    nifty_rec_by_ts = {ts: rec for ts, rec in nifty_rows}
    nifty_close_by_ts = {ts: _f(rec, "close") for ts, rec in nifty_rows}

    opt_meta: dict[str, dict[str, Any]] = {}
    if "OPT" in kinds:
        day_hl = _day_hl(nifty_rows)
        if not day_hl:
            print("WARNING: no NIFTY bars — option strikes cannot be built")
        else:
            opt_group, opt_meta = _build_option_universe(
                start=start,
                end=end,
                day_hl=day_hl,
                radius=radius,
                exchange=exchange,
            )
            if opt_group is not None:
                print(f"fetching OPT symbols={len(opt_group.symbols)} …")
                opt_fetched = await client.fetch(
                    [opt_group],
                    interval=f"{interval}m",
                    intra_day=False,
                    real_time=False,
                    start_date=start_iso,
                    end_date=end_iso,
                    progress=True,
                )
                fetched.frames.update(opt_fetched.frames)
                fetched.kinds.update(opt_fetched.kinds)
                fetched.request_count += opt_fetched.request_count
                fetched.errors.extend(opt_fetched.errors)
                print(
                    f"OPT fetched={len(opt_fetched.frames)} requests={opt_fetched.request_count} "
                    f"errors={len(opt_fetched.errors)}"
                )
                for err in opt_fetched.errors[:12]:
                    print("  opt fetch error:", err)

    ohlc_payloads: list[dict[str, Any]] = []
    if "INDEX" in kinds or "STOCK" in kinds:
        spots_by_ts: dict[datetime, dict[str, dict[str, Any]]] = defaultdict(dict)
        if "STOCK" in kinds:
            for symbol, frame in fetched.frames.items():
                if fetched.kinds.get(symbol) != "STOCK":
                    continue
                canonical = stock_alias_to_canonical.get(symbol, nifty50_canonical_symbol(symbol))
                for stamp, rec in _session_rows(
                    frame, alignment=alignment, interval=interval, start=start, end=end
                ):
                    candle = _candle(rec)
                    if candle:
                        spots_by_ts[stamp][canonical] = candle
        stamps = sorted(set(nifty_rec_by_ts) | set(spots_by_ts))
        for stamp in stamps:
            nifty_c = _candle(nifty_rec_by_ts[stamp]) if stamp in nifty_rec_by_ts else None
            spots = spots_by_ts.get(stamp) or {}
            if not nifty_c and not spots:
                continue
            ohlc_payloads.append(
                {
                    "bucket_end": stamp.isoformat(),
                    "nifty": nifty_c or {},
                    "stock_spots": spots,
                    "stock_eq_meta": {
                        sym: {
                            "symbol_token": str(zlib.crc32(sym.encode("utf-8")) & 0x7FFFFFFF),
                            "exchange": "NSE",
                            "instrument_type": "STOCK",
                        }
                        for sym in spots
                    },
                }
            )

    fut_payloads: list[dict[str, Any]] = []
    if "FUT" in kinds:
        futs_by_ts: dict[datetime, dict[str, dict[str, Any]]] = defaultdict(dict)
        stocks_by_ts: dict[datetime, dict[str, dict[str, Any]]] = defaultdict(dict)
        used_meta: dict[str, dict[str, Any]] = {}
        for symbol, frame in fetched.frames.items():
            if fetched.kinds.get(symbol) != "FUT":
                continue
            meta = dict(fut_meta.get(symbol) or {})
            parsed = parse_fut_symbol(symbol)
            if parsed and not meta:
                asset, exp = parsed
                meta = {
                    "expiry": exp.isoformat(),
                    "expiry_dt": datetime.combine(exp, time(15, 30)),
                    "underlying_symbol": nifty50_canonical_symbol(asset),
                    "instrument_type": "FUT",
                }
            if not meta:
                continue
            used_meta[symbol] = meta
            dest = futs_by_ts if symbol.startswith("NIFTY") else stocks_by_ts
            for stamp, rec in _session_rows(
                frame, alignment=alignment, interval=interval, start=start, end=end
            ):
                candle = _candle(rec)
                if candle:
                    candle["underlying_symbol"] = meta.get("underlying_symbol")
                    dest[stamp][symbol] = candle
        stamps = sorted(set(futs_by_ts) | set(stocks_by_ts))
        for stamp in stamps:
            fut_payloads.append(
                {
                    "bucket_end": stamp.isoformat(),
                    "price_scale": 1.0,
                    "futures": futs_by_ts.get(stamp) or {},
                    "stocks": stocks_by_ts.get(stamp) or {},
                    "fut_meta": used_meta,
                    "nifty50_underlyings": list(NIFTY50_SYMBOLS_SORTED),
                }
            )

    opt_payloads: list[dict[str, Any]] = []
    if "OPT" in kinds and opt_meta:
        # (timestamp, expiry) -> strike -> side -> candle + greeks
        buckets: dict[tuple[datetime, date], dict[int, dict[str, dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        for symbol, frame in fetched.frames.items():
            if fetched.kinds.get(symbol) != "OPT":
                continue
            info = opt_meta.get(symbol)
            if not info:
                continue
            expiry: date = info["expiry"]
            strike = int(info["strike"])
            side = str(info["side"]).upper()
            for stamp, rec in _session_rows(
                frame, alignment=alignment, interval=interval, start=start, end=end
            ):
                candle = _candle(rec)
                if not candle:
                    continue
                leg = {
                    "ltp": candle["close"],
                    "volume": candle.get("volume"),
                    "oi": candle.get("oi") or _f(rec, "oi"),
                    "delta": _f(rec, "delta"),
                    "gamma": _f(rec, "gamma"),
                    "theta": _f(rec, "theta"),
                    "vega": _f(rec, "vega"),
                    "iv": _f(rec, "iv_mid", "iv"),
                    "candle": candle,
                }
                buckets[(stamp, expiry)][strike][side] = leg
        for (stamp, expiry), by_strike in buckets.items():
            spot = nifty_close_by_ts.get(stamp)
            atm = get_atm_strike(spot, step=STRIKE_STEP) if spot else None
            allowed = (
                set(get_strike_range(atm, step=STRIKE_STEP, radius=radius))
                if atm is not None
                else set(by_strike)
            )
            chain = []
            option_candles: dict[str, Any] = {}
            for strike in sorted(by_strike):
                if strike not in allowed:
                    continue
                sides = by_strike[strike]
                row: dict[str, Any] = {"strike": strike}
                for side, leg in sides.items():
                    candle = leg.pop("candle")
                    row[side] = leg
                    option_candles[f"{strike}:{side}"] = candle
                chain.append(row)
            if not chain:
                continue
            opt_payloads.append(
                {
                    "bucket_end": stamp.isoformat(),
                    "expiry": expiry.isoformat(),
                    "underlying": "NIFTY",
                    "spot": spot,
                    "chain": chain,
                    "option_candles": option_candles,
                }
            )

    print(
        f"payloads ohlc={len(ohlc_payloads)} fut={len(fut_payloads)} opt={len(opt_payloads)}"
    )
    if args.dry_run:
        print("dry-run: skipping DB writes")
        return

    writer = DBWriter(settings.database_dsn, schema=settings.db_schema)
    await writer.connect()
    assert writer.pool is not None
    try:
        async with writer.pool.acquire() as conn:
            if not args.no_replace:
                print("replacing existing rows in window …")
                await _delete_window(conn, writer, start, end, kinds)
            if ohlc_payloads:
                print("writing market_ohlc …")
                n = await _insert_chunks(
                    writer, conn, writer._insert_market_ohlc_bundle, ohlc_payloads, label="market_ohlc"
                )
                print(f"wrote market_ohlc bundles={n}")
            if fut_payloads:
                print("writing futures_data …")
                n = await _insert_chunks(
                    writer, conn, writer._insert_futures_data, fut_payloads, label="futures_data"
                )
                print(f"wrote futures_data batches={n}")
                print("writing nifty50_stock_futures …")
                n = await _insert_chunks(
                    writer,
                    conn,
                    writer._insert_nifty50_stock_futures,
                    fut_payloads,
                    label="nifty50_stock_futures",
                )
                print(f"wrote nifty50_stock_futures batches={n}")
            if opt_payloads:
                print("writing options_data …")
                n = await _insert_chunks(
                    writer, conn, writer._insert_options_data, opt_payloads, label="options_data"
                )
                print(f"wrote options_data batches={n}")
    finally:
        await writer.close()
    print("backfill done")


if __name__ == "__main__":
    asyncio.run(main())
