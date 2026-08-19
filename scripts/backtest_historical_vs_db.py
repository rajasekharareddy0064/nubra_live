"""Backtest live 3m DB aggregates vs Nubra historical_data() for one session.

Usage (from repo root):

    python scripts/backtest_historical_vs_db.py
    python scripts/backtest_historical_vs_db.py --date 2026-08-13
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

sys.path.insert(0, ".")

from app.core.env_loader import load_project_env

load_project_env(".")
from app.ingestion.input_patch import install_non_interactive_input_patch

# Session tokens expire; historical_data() needs a live SDK client.
install_non_interactive_input_patch(require_totp_secret=False)

from app.core.config import settings

IST = ZoneInfo("Asia/Kolkata")
DEFAULT_DAY = date(2026, 8, 13)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare DB 3m candles vs historical REST for one date")
    parser.add_argument("--date", default=DEFAULT_DAY.isoformat(), help="Session date YYYY-MM-DD")
    parser.add_argument("--json", action="store_true", help="Print full JSON report")
    parser.add_argument(
        "--skip-hist",
        action="store_true",
        help="Only print DB coverage (no historical API calls)",
    )
    return parser.parse_args()


def _session_utc_iso(day: date) -> tuple[str, str]:
    start = datetime.combine(day, time(9, 15), tzinfo=IST)
    end = datetime.combine(day, time(15, 30), tzinfo=IST)
    fmt = "%Y-%m-%dT%H:%M:%S.000Z"
    return (
        start.astimezone(timezone.utc).strftime(fmt),
        (end + timedelta(minutes=1)).astimezone(timezone.utc).strftime(fmt),
    )


def _records_to_df(rows: list[Any], timestamp_col: str = "timestamp"):
    import pandas as pd

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    if timestamp_col in df.columns and timestamp_col != "timestamp":
        df = df.rename(columns={timestamp_col: "timestamp"})
    if "iv" in df.columns and "iv_mid" not in df.columns:
        df["iv_mid"] = df["iv"]
    return df


async def _load_db(day: date) -> dict[str, Any]:
    import asyncpg

    schema = settings.db_schema
    conn = await asyncpg.connect(settings.database_dsn, timeout=20)
    try:
        nifty = await conn.fetch(
            f"""
            SELECT symbol, "timestamp", open, high, low, close, volume
            FROM "{schema}".market_ohlc
            WHERE symbol = 'NIFTY' AND interval = '3m'
              AND DATE("timestamp") = $1
            ORDER BY "timestamp"
            """,
            day,
        )
        spots = await conn.fetch(
            f"""
            SELECT symbol, candle_time AS timestamp, open, high, low, close, volume
            FROM "{schema}".market_ohlc_3m
            WHERE instrument_type = 'STOCK'
              AND DATE(candle_time) = $1
            ORDER BY candle_time, symbol
            """,
            day,
        )
        futs = await conn.fetch(
            f"""
            SELECT symbol, "timestamp", open, high, low, close, volume, oi
            FROM "{schema}".futures_data
            WHERE DATE("timestamp") = $1
            ORDER BY "timestamp", symbol
            """,
            day,
        )
        stock_futs = await conn.fetch(
            f"""
            SELECT symbol, "timestamp", open, high, low, close, volume, oi
            FROM "{schema}".nifty50_stock_futures
            WHERE DATE("timestamp") = $1
            ORDER BY "timestamp", symbol
            """,
            day,
        )
        opts = await conn.fetch(
            f"""
            SELECT symbol, "timestamp", strike, option_type,
                   open, high, low, close, volume, oi,
                   delta, gamma, theta, vega, iv
            FROM "{schema}".options_data
            WHERE DATE("timestamp") = $1
            ORDER BY "timestamp", strike, option_type
            """,
            day,
        )
        counts = {
            "market_ohlc": len(nifty),
            "market_ohlc_3m": len(spots),
            "futures_data": len(futs),
            "nifty50_stock_futures": len(stock_futs),
            "options_data": len(opts),
        }
        return {
            "counts": counts,
            "nifty": _records_to_df(list(nifty)),
            "spots": _records_to_df(list(spots)),
            "futs": _records_to_df(list(futs)),
            "stock_futs": _records_to_df(list(stock_futs)),
            "opts": _records_to_df(list(opts)),
        }
    finally:
        await conn.close()


def _unique_symbols(df, column: str = "symbol") -> list[str]:
    if df is None or df.empty or column not in df.columns:
        return []
    return sorted({str(s).strip().upper() for s in df[column].tolist() if str(s).strip()})


def _option_join_frame(opts):
    import pandas as pd

    if opts is None or opts.empty:
        return opts, {}
    out = opts.copy()
    alias: dict[str, str] = {}
    keys: list[str] = []
    for row in out.itertuples(index=False):
        try:
            strike = int(float(row.strike))
        except (TypeError, ValueError):
            strike = 0
        side = str(getattr(row, "option_type", "") or "").upper()
        grow = str(getattr(row, "symbol", "") or "").strip().upper()
        join_key = f"{strike}:{side}" if strike and side in {"CE", "PE"} else grow
        keys.append(join_key)
        if grow:
            alias[grow] = join_key
    out["symbol"] = keys
    return out, alias


def _build_groups(db: dict[str, Any], exchange: str, extra_opt_symbols: list[str]):
    from app.historical.universe import FUT_FIELDS, OHLC_FIELDS, OPT_FIELDS, UniverseGroup
    from app.instruments.nifty50 import NIFTY50_MASTER_ASSET_ALIASES

    groups: list = []
    index_fields = ["open", "high", "low", "close", "cumulative_volume"]
    if not db["nifty"].empty:
        groups.append(UniverseGroup("INDEX", exchange, ["NIFTY"], index_fields))
    spots = _unique_symbols(db["spots"])
    fetch_spots = sorted({NIFTY50_MASTER_ASSET_ALIASES.get(s, s) for s in spots})
    if fetch_spots:
        groups.append(UniverseGroup("STOCK", exchange, fetch_spots, list(OHLC_FIELDS)))
    futs = _unique_symbols(db["futs"]) + _unique_symbols(db["stock_futs"])
    futs = sorted(set(futs))
    if futs:
        groups.append(UniverseGroup("FUT", exchange, futs, list(FUT_FIELDS)))
    opt_syms = _unique_symbols(db["opts"])
    for extra in extra_opt_symbols:
        if extra and extra not in opt_syms:
            opt_syms.append(extra)
    if opt_syms:
        groups.append(UniverseGroup("OPT", exchange, sorted(set(opt_syms)), list(OPT_FIELDS)))
    return groups, {v: k for k, v in NIFTY50_MASTER_ASSET_ALIASES.items()}


def _print_table(report: dict[str, Any]) -> None:
    summary = report.get("summary") or {}
    compared = summary.get("compared") or 0
    matched = summary.get("matched") or 0
    rate = (100.0 * matched / compared) if compared else 0.0
    print(
        f"  {report.get('table')}: db_rows={report.get('db_rows')} hist_rows={report.get('hist_rows')} "
        f"compared={compared} matched={matched} mismatched={summary.get('mismatched')} "
        f"missing_hist={summary.get('missing_hist')} missing_db={summary.get('missing_db')} "
        f"match_rate={rate:.1f}%"
    )
    for ex in report.get("examples") or []:
        if ex.get("status") == "missing_hist":
            print(f"    missing_hist {ex.get('symbol')} {ex.get('timestamp')}")
        else:
            print(
                f"    {ex.get('status')} {ex.get('symbol')} {ex.get('timestamp')} "
                f"db_close={ex.get('db_close')} hist_close={ex.get('hist_close')} "
                f"diffs={ex.get('diffs')}"
            )


async def main() -> None:
    args = _parse_args()
    day = date.fromisoformat(str(args.date))
    print(f"backtest date={day} env={settings.nubra_env} schema={settings.db_schema}")
    print(f"dsn_host={settings.db_host} db={settings.db_name}")

    try:
        db = await _load_db(day)
    except Exception as exc:
        print(f"ERROR: could not read Postgres: {exc}")
        return

    print("DB coverage:", db["counts"])
    if not any(db["counts"].values()):
        print(
            "No nubra-live 3m rows for this date. "
            "Aggregation cannot be checked (USE_DATABASE may have been off)."
        )
        return

    if not db["nifty"].empty:
        first = db["nifty"].iloc[0]
        last = db["nifty"].iloc[-1]
        print(
            f"NIFTY db bars={len(db['nifty'])} first={first['timestamp']} close={first['close']} "
            f"last={last['timestamp']} close={last['close']}"
        )

    if args.skip_hist:
        return

    from app.historical.client import HistoricalClient
    from app.historical.compare import compare_day_table

    # Local testing: universe comes from DB symbols. Skip instrument master
    # (GCS / SDK get_instruments) — that path is Cloud Run startup, not this check.
    price_scale = 100.0 if str(settings.nubra_env).upper() == "PROD" else 1.0
    extra_opt: list[str] = []
    opt_alias: dict[str, str] = {}
    opt_join, grow_alias = _option_join_frame(db["opts"])
    opt_alias.update(grow_alias)

    groups, stock_alias = _build_groups(db, settings.nubra_exchange, extra_opt)
    print("historical universe:", {g.kind: len(g.symbols) for g in groups})
    if not groups:
        print("No symbols to fetch.")
        return

    client = HistoricalClient(
        env_name=settings.nubra_env,
        exchange=settings.nubra_exchange,
        price_scale=price_scale,
        interval="3m",
    )
    start_iso, end_iso = _session_utc_iso(day)
    print(f"historical window {start_iso} -> {end_iso} intraDay=False")
    fetched = await client.fetch(
        groups,
        interval="3m",
        intra_day=False,
        real_time=False,
        start_date=start_iso,
        end_date=end_iso,
    )
    print(
        f"fetched symbols={len(fetched.frames)} requests={fetched.request_count} "
        f"errors={len(fetched.errors)} alignment={fetched.policy.bar_alignment} "
        f"volume_mode={fetched.policy.volume_mode}"
    )
    for err in fetched.errors[:10]:
        print("  fetch error:", err)
    if fetched.policy.notes:
        print("HIST_PROBE:", " | ".join(fetched.policy.notes))

    kinds = fetched.kinds
    frames_by_kind: dict[str, dict] = {"INDEX": {}, "STOCK": {}, "FUT": {}, "OPT": {}}
    for symbol, kind in kinds.items():
        frames_by_kind.setdefault(kind, {})[symbol] = fetched.frames[symbol]
    # NIFTY may be INDEX even if kinds missed it.
    if "NIFTY" in fetched.frames:
        frames_by_kind["INDEX"]["NIFTY"] = fetched.frames["NIFTY"]

    interval = int(settings.candle_interval_minutes)
    alignment = fetched.policy.bar_alignment or "close"
    reports = []

    if not db["nifty"].empty:
        reports.append(
            compare_day_table(
                table="market_ohlc",
                kind="INDEX",
                db=db["nifty"],
                hist_frames=frames_by_kind.get("INDEX") or {},
                alignment=alignment,
                interval_minutes=interval,
                price_abs_tol=settings.historical_index_price_abs_tol,
                volume_rel_tol=settings.historical_volume_rel_tol,
            )
        )
    if not db["spots"].empty:
        reports.append(
            compare_day_table(
                table="market_ohlc_3m",
                kind="STOCK",
                db=db["spots"],
                hist_frames=frames_by_kind.get("STOCK") or {},
                alignment=alignment,
                interval_minutes=interval,
                price_abs_tol=settings.historical_price_abs_tol,
                volume_rel_tol=settings.historical_volume_rel_tol,
                symbol_alias=stock_alias,
            )
        )
    fut_db = db["futs"]
    if not db["stock_futs"].empty:
        import pandas as pd

        fut_db = pd.concat([db["futs"], db["stock_futs"]], ignore_index=True) if not db["futs"].empty else db["stock_futs"]
    if fut_db is not None and not fut_db.empty:
        reports.append(
            compare_day_table(
                table="futures_data+nifty50_stock_futures",
                kind="FUT",
                db=fut_db,
                hist_frames=frames_by_kind.get("FUT") or {},
                alignment=alignment,
                interval_minutes=interval,
                price_abs_tol=settings.historical_price_abs_tol,
                volume_rel_tol=settings.historical_volume_rel_tol,
            )
        )
    if opt_join is not None and not opt_join.empty:
        reports.append(
            compare_day_table(
                table="options_data",
                kind="OPT",
                db=opt_join,
                hist_frames=frames_by_kind.get("OPT") or {},
                alignment=alignment,
                interval_minutes=interval,
                price_abs_tol=settings.historical_price_abs_tol,
                volume_rel_tol=settings.historical_volume_rel_tol,
                compare_greeks=True,
                symbol_alias=opt_alias,
            )
        )

    print("\n=== 3m aggregation vs historical REST ===")
    print(
        f"session bars 09:18-15:30 IST | INDEX close ±{settings.historical_index_price_abs_tol:g} | "
        f"other price ±{settings.historical_price_abs_tol:g}"
    )
    for report in reports:
        _print_table(report)

    if args.json:
        print(json.dumps({"date": day.isoformat(), "db": db["counts"], "tables": reports}, default=str, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
