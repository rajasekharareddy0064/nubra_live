"""Diff a closed live 3m candle against the matching historical bar."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from app.historical.normalize import pick_bar
from app.historical.universe import UniverseGroup, option_leg_map
from app.realtime.interval_clock import is_nse_session_close_label

PRICE_FIELDS = ("open", "high", "low", "close")
GREEK_FIELDS = ("delta", "gamma", "theta", "vega")
OUT_OF_SCOPE = ("order_book_3m_strikes", "order_book_3m_candles")


def _f(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:
        return None
    return f


def _close_enough(
    live: float | None,
    hist: float | None,
    *,
    abs_tol: float,
    rel_tol: float,
    abs_floor: float = 0.0,
) -> bool:
    if live is None and hist is None:
        return True
    if live is None or hist is None:
        return False
    delta = abs(live - hist)
    if delta <= abs_tol or delta <= abs_floor:
        return True
    denom = max(abs(live), abs(hist), 1e-9)
    return (delta / denom) <= rel_tol


def _series_to_dict(row: pd.Series | None) -> dict[str, Any]:
    if row is None:
        return {}
    out: dict[str, Any] = {}
    for key, val in row.items():
        if val is None or (isinstance(val, float) and val != val):
            continue
        try:
            if hasattr(val, "item"):
                val = val.item()
        except (ValueError, AttributeError):
            pass
        out[str(key)] = val
    return out


def _live_ohlc(block: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(block, dict):
        return {}
    return {
        "open": _f(block.get("open")),
        "high": _f(block.get("high")),
        "low": _f(block.get("low")),
        "close": _f(block.get("close")),
        "volume": _f(block.get("volume")),
        "cum_volume": _f(block.get("cum_volume")),
        "oi": _f(block.get("oi")),
        "l1bid": _f(block.get("l1bid") or block.get("bid") or block.get("best_bid")),
        "l1ask": _f(block.get("l1ask") or block.get("ask") or block.get("best_ask")),
        "delta": _f(block.get("delta")),
        "gamma": _f(block.get("gamma")),
        "theta": _f(block.get("theta")),
        "vega": _f(block.get("vega")),
        "iv_mid": _f(block.get("iv_mid") or block.get("iv")),
    }


def _status_for(diffs: list[str], live: dict[str, Any], hist: dict[str, Any]) -> str:
    if not live and not hist:
        return "missing_both"
    if not live:
        return "missing_live"
    if not hist:
        return "missing_hist"
    if not diffs:
        return "match"
    if any(d.startswith("price:") for d in diffs):
        return "price_mismatch"
    if any(d.startswith("volume:") or d.startswith("oi:") for d in diffs):
        return "volume_mismatch"
    return "mismatch"


def _diff_fields(
    live: dict[str, Any],
    hist: dict[str, Any],
    *,
    price_abs_tol: float,
    volume_rel_tol: float,
    compare_greeks: bool,
    compare_l1: bool,
) -> list[str]:
    diffs: list[str] = []
    for field in PRICE_FIELDS:
        lv, hv = live.get(field), hist.get(field)
        if lv is None and hv is None:
            continue
        if not _close_enough(lv, hv, abs_tol=price_abs_tol, rel_tol=0.0):
            diffs.append(f"price:{field} live={lv} hist={hv}")
    lv, hv = live.get("volume"), hist.get("volume")
    if lv is not None or hv is not None:
        if not _close_enough(lv, hv, abs_tol=1.0, rel_tol=volume_rel_tol, abs_floor=1.0):
            diffs.append(f"volume: live={lv} hist={hv}")
    lv, hv = live.get("oi"), hist.get("oi")
    if lv is not None or hv is not None:
        if not _close_enough(lv, hv, abs_tol=1.0, rel_tol=volume_rel_tol, abs_floor=1.0):
            diffs.append(f"oi: live={lv} hist={hv}")
    if compare_greeks:
        for field in GREEK_FIELDS:
            lv, hv = live.get(field), hist.get(field)
            if lv is None or hv is None:
                continue
            if not _close_enough(lv, hv, abs_tol=0.01, rel_tol=0.05):
                diffs.append(f"greek:{field} live={lv} hist={hv}")
        lv, hv = live.get("iv_mid"), hist.get("iv_mid")
        if lv is not None and hv is not None:
            if not _close_enough(lv, hv, abs_tol=0.01, rel_tol=0.05):
                diffs.append(f"iv: live={lv} hist={hv}")
    if compare_l1:
        for field in ("l1bid", "l1ask"):
            lv, hv = live.get(field), hist.get(field)
            if lv is None or hv is None:
                continue
            if not _close_enough(lv, hv, abs_tol=price_abs_tol, rel_tol=0.0):
                diffs.append(f"l1:{field} live={lv} hist={hv}")
    return diffs


def _option_live_rows(live: dict[str, Any]) -> dict[tuple[int, str], dict[str, Any]]:
    out: dict[tuple[int, str], dict[str, Any]] = {}
    options = live.get("options") if isinstance(live.get("options"), dict) else {}
    candles = options.get("candles") if isinstance(options, dict) else None
    if not isinstance(candles, dict):
        candles = live.get("option_candles") if isinstance(live.get("option_candles"), dict) else {}
    if isinstance(candles, dict):
        for key, candle in candles.items():
            text = str(key)
            if ":" in text:
                strike_s, side = text.split(":", 1)
            elif "_" in text:
                parts = text.rsplit("_", 1)
                strike_s, side = parts[0], parts[-1]
            else:
                continue
            try:
                strike = int(float(strike_s))
            except (TypeError, ValueError):
                continue
            side_u = str(side).upper()
            if side_u not in {"CE", "PE"}:
                continue
            out[(strike, side_u)] = _live_ohlc(candle if isinstance(candle, dict) else {})
    chain = options.get("chain") if isinstance(options, dict) else None
    if isinstance(chain, list):
        for row in chain:
            if not isinstance(row, dict):
                continue
            try:
                strike = int(row.get("strike") or 0)
            except (TypeError, ValueError):
                continue
            if strike <= 0:
                continue
            for side in ("CE", "PE"):
                leg = row.get(side)
                if not isinstance(leg, dict):
                    continue
                existing = out.setdefault((strike, side), {})
                # Chain is an end-of-bar snapshot: fill close/ltp/oi/greeks.
                ltp = _f(leg.get("ltp") or leg.get("last_price"))
                if existing.get("close") is None and ltp is not None:
                    existing["close"] = ltp
                if existing.get("oi") is None:
                    existing["oi"] = _f(leg.get("open_interest") if leg.get("open_interest") is not None else leg.get("oi"))
                if existing.get("volume") is None:
                    existing["volume"] = _f(leg.get("volume"))
                for g in GREEK_FIELDS:
                    if existing.get(g) is None:
                        existing[g] = _f(leg.get(g))
                if existing.get("iv_mid") is None:
                    existing["iv_mid"] = _f(leg.get("iv_mid") or leg.get("iv"))
                for bid_key in ("l1bid", "bid", "best_bid", "bid_price"):
                    if existing.get("l1bid") is None:
                        existing["l1bid"] = _f(leg.get(bid_key))
                for ask_key in ("l1ask", "ask", "best_ask", "ask_price"):
                    if existing.get("l1ask") is None:
                        existing["l1ask"] = _f(leg.get(ask_key))
    return out


def _naive_minute(value: Any) -> pd.Timestamp | None:
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is not None:
        from app.historical.normalize import IST

        ts = ts.tz_convert(IST).tz_localize(None)
    return ts.floor("min")


def hist_frames_to_long(
    frames: dict[str, pd.DataFrame],
    *,
    alignment: str,
    interval_minutes: int,
    symbol_alias: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Stack normalized historical frames with naive IST close timestamps."""
    from app.historical.normalize import to_bucket_end_index

    rows: list[dict[str, Any]] = []
    alias = symbol_alias or {}
    for symbol, frame in frames.items():
        aligned = to_bucket_end_index(frame, alignment=alignment, interval_minutes=interval_minutes)
        if aligned is None or aligned.empty:
            continue
        join_symbol = alias.get(symbol, symbol)
        for ts, rec in aligned.iterrows():
            item = rec.to_dict()
            item["symbol"] = join_symbol
            item["hist_symbol"] = symbol
            item["timestamp"] = _naive_minute(ts)
            rows.append(item)
    if not rows:
        return pd.DataFrame(columns=["symbol", "timestamp"])
    return pd.DataFrame(rows)


def _session_close_ok(value: Any, *, interval_minutes: int) -> bool:
    ts = _naive_minute(value)
    if ts is None:
        return False
    try:
        return is_nse_session_close_label(ts.to_pydatetime(), interval_minutes=interval_minutes)
    except Exception:
        return is_nse_session_close_label(ts, interval_minutes=interval_minutes)


def compare_day_table(
    *,
    table: str,
    kind: str,
    db: pd.DataFrame,
    hist_frames: dict[str, pd.DataFrame],
    alignment: str,
    interval_minutes: int,
    price_abs_tol: float,
    volume_rel_tol: float,
    compare_greeks: bool = False,
    symbol_alias: dict[str, str] | None = None,
    max_examples: int = 8,
    session_only: bool = True,
) -> dict[str, Any]:
    """Join a full session of DB 3m rows to historical bars on (symbol, timestamp)."""
    if db is None or db.empty:
        return {
            "table": table,
            "kind": kind,
            "db_rows": 0,
            "hist_rows": 0,
            "summary": {
                "matched": 0,
                "mismatched": 0,
                "missing_hist": 0,
                "missing_db": 0,
                "compared": 0,
            },
            "examples": [],
            "note": "no DB rows for this date",
        }

    work = db.copy()
    work["symbol"] = work["symbol"].astype(str).str.strip().str.upper()
    work["timestamp"] = work["timestamp"].map(_naive_minute)
    work = work.dropna(subset=["symbol", "timestamp"])
    if session_only:
        work = work[work["timestamp"].map(lambda t: _session_close_ok(t, interval_minutes=interval_minutes))]

    hist_long = hist_frames_to_long(
        hist_frames,
        alignment=alignment,
        interval_minutes=interval_minutes,
        symbol_alias=symbol_alias,
    )
    if not hist_long.empty:
        hist_long["symbol"] = hist_long["symbol"].astype(str).str.strip().str.upper()
        hist_long["timestamp"] = hist_long["timestamp"].map(_naive_minute)
        if session_only:
            hist_long = hist_long[
                hist_long["timestamp"].map(lambda t: _session_close_ok(t, interval_minutes=interval_minutes))
            ]

    merged = work.merge(
        hist_long,
        on=["symbol", "timestamp"],
        how="outer",
        suffixes=("_db", "_hist"),
        indicator=True,
    )

    compared = 0
    matched = 0
    mismatched = 0
    missing_hist = 0
    missing_db = 0
    examples: list[dict[str, Any]] = []
    close_err: list[tuple[float, dict[str, Any]]] = []

    for rec in merged.to_dict(orient="records"):
        origin = rec.get("_merge")
        if origin == "left_only":
            missing_hist += 1
            if len(examples) < max_examples:
                examples.append(
                    {
                        "status": "missing_hist",
                        "symbol": rec.get("symbol"),
                        "timestamp": str(rec.get("timestamp")),
                    }
                )
            continue
        if origin == "right_only":
            missing_db += 1
            continue

        live_block = {
            "open": _f(rec.get("open_db", rec.get("open"))),
            "high": _f(rec.get("high_db", rec.get("high"))),
            "low": _f(rec.get("low_db", rec.get("low"))),
            "close": _f(rec.get("close_db", rec.get("close"))),
            "volume": _f(rec.get("volume_db", rec.get("volume"))),
            "oi": _f(rec.get("oi_db", rec.get("oi"))),
            "delta": _f(rec.get("delta_db", rec.get("delta"))),
            "gamma": _f(rec.get("gamma_db", rec.get("gamma"))),
            "theta": _f(rec.get("theta_db", rec.get("theta"))),
            "vega": _f(rec.get("vega_db", rec.get("vega"))),
            "iv_mid": _f(rec.get("iv_db", rec.get("iv_mid_db", rec.get("iv")))),
        }
        hist_block = {
            "open": _f(rec.get("open_hist")),
            "high": _f(rec.get("high_hist")),
            "low": _f(rec.get("low_hist")),
            "close": _f(rec.get("close_hist")),
            "volume": _f(rec.get("volume_hist")),
            "oi": _f(rec.get("oi_hist")),
            "delta": _f(rec.get("delta_hist")),
            "gamma": _f(rec.get("gamma_hist")),
            "theta": _f(rec.get("theta_hist")),
            "vega": _f(rec.get("vega_hist")),
            "iv_mid": _f(rec.get("iv_mid_hist")),
        }
        # When hist columns were not suffixed (no overlap), fall back.
        if hist_block["close"] is None and rec.get("close_hist") is None:
            hist_block = {
                "open": _f(rec.get("open")),
                "high": _f(rec.get("high")),
                "low": _f(rec.get("low")),
                "close": _f(rec.get("close")),
                "volume": _f(rec.get("volume")),
                "oi": _f(rec.get("oi")),
                "delta": _f(rec.get("delta")),
                "gamma": _f(rec.get("gamma")),
                "theta": _f(rec.get("theta")),
                "vega": _f(rec.get("vega")),
                "iv_mid": _f(rec.get("iv_mid")),
            }

        diffs = _diff_fields(
            live_block,
            hist_block,
            price_abs_tol=price_abs_tol,
            volume_rel_tol=volume_rel_tol,
            compare_greeks=compare_greeks,
            compare_l1=False,
        )
        compared += 1
        status = "match" if not diffs else (
            "price_mismatch" if any(d.startswith("price:") for d in diffs) else "volume_mismatch"
        )
        if status == "match":
            matched += 1
        else:
            mismatched += 1
            lc = live_block.get("close")
            hc = hist_block.get("close")
            err = abs((lc or 0) - (hc or 0)) if lc is not None and hc is not None else 0.0
            item = {
                "status": status,
                "symbol": rec.get("symbol"),
                "timestamp": str(rec.get("timestamp")),
                "db_close": lc,
                "hist_close": hc,
                "db_volume": live_block.get("volume"),
                "hist_volume": hist_block.get("volume"),
                "diffs": diffs[:6],
            }
            close_err.append((err, item))

    close_err.sort(key=lambda x: x[0], reverse=True)
    for _, item in close_err[:max_examples]:
        examples.append(item)

    return {
        "table": table,
        "kind": kind,
        "db_rows": int(len(work)),
        "hist_rows": int(len(hist_long)),
        "db_symbols": int(work["symbol"].nunique()) if not work.empty else 0,
        "summary": {
            "matched": matched,
            "mismatched": mismatched,
            "missing_hist": missing_hist,
            "missing_db": missing_db,
            "compared": compared,
        },
        "examples": examples[: max_examples + 4],
        "session_only": session_only,
    }


def compare_closed_bar(
    *,
    live: dict[str, Any] | None,
    frames: dict[str, pd.DataFrame],
    kinds: dict[str, str],
    groups: list[UniverseGroup],
    bucket_end: datetime,
    alignment: str,
    interval_minutes: int,
    price_abs_tol: float,
    volume_rel_tol: float,
) -> dict[str, Any]:
    live = live if isinstance(live, dict) else {}
    legs = option_leg_map(groups)
    live_options = _option_live_rows(live)
    rows: list[dict[str, Any]] = []

    def add_row(
        symbol: str,
        kind: str,
        live_block: dict[str, Any],
        hist_row: pd.Series | None,
        *,
        extra: dict[str, Any] | None = None,
    ) -> None:
        hist = _series_to_dict(hist_row)
        compare_greeks = kind == "OPT"
        compare_l1 = bool(live_block.get("l1bid") or live_block.get("l1ask"))
        diffs = _diff_fields(
            live_block,
            hist,
            price_abs_tol=price_abs_tol,
            volume_rel_tol=volume_rel_tol,
            compare_greeks=compare_greeks,
            compare_l1=compare_l1,
        )
        rec: dict[str, Any] = {
            "symbol": symbol,
            "kind": kind,
            "status": _status_for(diffs, live_block, hist),
            "live": live_block,
            "hist": hist,
            "diffs": diffs,
        }
        if extra:
            rec.update(extra)
        rows.append(rec)

    nifty_live = _live_ohlc(live.get("nifty") if isinstance(live.get("nifty"), dict) else None)
    if not any(v is not None for v in nifty_live.values()):
        meta = live.get("meta") if isinstance(live.get("meta"), dict) else {}
        nifty_live = _live_ohlc(meta.get("index") if isinstance(meta, dict) else None)
    add_row("NIFTY", "INDEX", nifty_live, pick_bar(frames.get("NIFTY"), bucket_end, alignment=alignment, interval_minutes=interval_minutes))

    futures = live.get("futures") if isinstance(live.get("futures"), dict) else {}
    stocks_fut = live.get("stocks") if isinstance(live.get("stocks"), dict) else {}
    stock_spots = live.get("stock_spots") if isinstance(live.get("stock_spots"), dict) else {}

    hist_symbols = set(frames)
    live_fut_keys = set(futures) | set(stocks_fut)
    live_spot_keys = set(stock_spots)

    for symbol, kind in kinds.items():
        if kind == "INDEX":
            continue
        if kind == "OPT":
            meta = legs.get(symbol) or {}
            strike = int(meta.get("strike") or 0)
            side = str(meta.get("side") or "").upper()
            live_block = live_options.get((strike, side), {})
            add_row(
                symbol,
                "OPT",
                live_block,
                pick_bar(frames.get(symbol), bucket_end, alignment=alignment, interval_minutes=interval_minutes),
                extra={"strike": strike, "side": side},
            )
            continue
        if kind == "FUT":
            block = futures.get(symbol) if symbol in futures else stocks_fut.get(symbol)
            add_row(
                symbol,
                "FUT",
                _live_ohlc(block if isinstance(block, dict) else None),
                pick_bar(frames.get(symbol), bucket_end, alignment=alignment, interval_minutes=interval_minutes),
            )
            continue
        if kind == "STOCK":
            add_row(
                symbol,
                "STOCK",
                _live_ohlc(stock_spots.get(symbol) if isinstance(stock_spots.get(symbol), dict) else None),
                pick_bar(frames.get(symbol), bucket_end, alignment=alignment, interval_minutes=interval_minutes),
            )

    # Live-only symbols the historical fetch did not return.
    for symbol in sorted(live_fut_keys - hist_symbols):
        block = futures.get(symbol) if symbol in futures else stocks_fut.get(symbol)
        add_row(symbol, "FUT", _live_ohlc(block if isinstance(block, dict) else None), None)
    for symbol in sorted(live_spot_keys - hist_symbols):
        add_row(symbol, "STOCK", _live_ohlc(stock_spots.get(symbol) if isinstance(stock_spots.get(symbol), dict) else None), None)

    summary = {
        "matched": sum(1 for r in rows if r["status"] == "match"),
        "mismatched": sum(1 for r in rows if r["status"] not in {"match", "missing_live", "missing_hist", "missing_both"}),
        "missing_live": sum(1 for r in rows if r["status"] == "missing_live"),
        "missing_hist": sum(1 for r in rows if r["status"] == "missing_hist"),
        "rows": len(rows),
    }
    return {
        "bucket_end": bucket_end.isoformat() if hasattr(bucket_end, "isoformat") else str(bucket_end),
        "alignment": alignment,
        "summary": summary,
        "rows": rows,
        "out_of_scope": list(OUT_OF_SCOPE),
        "note": (
            "Order-book qty/delta/regime candles are not in historical_data(); "
            "L1 bid/ask is compared only when live exposes those prices."
        ),
    }
