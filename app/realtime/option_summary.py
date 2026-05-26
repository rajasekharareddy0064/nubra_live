from __future__ import annotations

from typing import Any


def _f(x: Any) -> float:
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def summarize_options_for_interval(
    option_chain_row: dict[str, Any],
    options_by_strike: dict[int, dict[str, Any]],
    prev_totals: dict[str, float],
) -> dict[str, Any]:
    """
    Total CE vs PE OI, OI change vs previous interval, volume, PCR.
    Prefers full chain row if present; else aggregates per-strike maps.
    """
    if not isinstance(option_chain_row, dict):
        option_chain_row = {}
    if not isinstance(options_by_strike, dict):
        options_by_strike = {}

    ce_oi = 0.0
    pe_oi = 0.0
    ce_vol = 0.0
    pe_vol = 0.0

    ce = (
        option_chain_row.get("ce")
        or option_chain_row.get("CE")
        or option_chain_row.get("calls")
        or option_chain_row.get("call_data")
        or []
    )
    pe = (
        option_chain_row.get("pe")
        or option_chain_row.get("PE")
        or option_chain_row.get("puts")
        or option_chain_row.get("put_data")
        or []
    )
    if isinstance(ce, dict):
        ce = [v for v in ce.values() if isinstance(v, dict)]
    if isinstance(pe, dict):
        pe = [v for v in pe.values() if isinstance(v, dict)]
    if not isinstance(ce, list):
        print("DEBUG option_summary bad ce type:", type(ce), ce)
        ce = []
    if not isinstance(pe, list):
        print("DEBUG option_summary bad pe type:", type(pe), pe)
        pe = []
    if ce or pe:
        for row in ce:
            if not isinstance(row, dict):
                print("DEBUG option_summary bad ce row type:", type(row), row)
                continue
            r = row or {}
            ce_oi += _f(r.get("open_interest") or r.get("oi"))
            ce_vol += _f(r.get("volume"))
        for row in pe:
            if not isinstance(row, dict):
                print("DEBUG option_summary bad pe row type:", type(row), row)
                continue
            r = row or {}
            pe_oi += _f(r.get("open_interest") or r.get("oi"))
            pe_vol += _f(r.get("volume"))
    else:
        for _strike, legs in options_by_strike.items():
            if not isinstance(legs, dict):
                print("DEBUG option_summary bad legs type:", type(legs), legs)
                continue
            ce_d = legs.get("CE") or {}
            pe_d = legs.get("PE") or {}
            ce_oi += _f(ce_d.get("open_interest") or ce_d.get("oi"))
            pe_oi += _f(pe_d.get("open_interest") or pe_d.get("oi"))
            ce_vol += _f(ce_d.get("volume"))
            pe_vol += _f(pe_d.get("volume"))

    prev_ce = _f(prev_totals.get("ce_oi"))
    prev_pe = _f(prev_totals.get("pe_oi"))
    pcr = (pe_oi / ce_oi) if ce_oi > 0 else None

    return {
        "total_ce_oi": ce_oi,
        "total_pe_oi": pe_oi,
        "ce_oi_change": ce_oi - prev_ce,
        "pe_oi_change": pe_oi - prev_pe,
        "ce_volume": ce_vol,
        "pe_volume": pe_vol,
        "put_call_ratio": round(pcr, 4) if pcr is not None else None,
    }


def stock_futures_strength(stock_futures: dict[str, dict[str, Any]], candles: dict[str, Any]) -> dict[str, Any]:
    """Aggregate strength: mean % move from candle open→close per symbol."""
    moves: list[float] = []
    total_oi = 0.0
    total_volume = 0.0
    for sym, data in stock_futures.items():
        oi = _f(data.get("oi") or data.get("open_interest"))
        volume = _f(data.get("volume") or data.get("traded_volume") or data.get("total_volume"))
        total_oi += oi
        total_volume += volume
        cnd = candles.get(sym) if isinstance(candles, dict) else None
        if isinstance(cnd, dict):
            o = _f(cnd.get("open"))
            cl = _f(cnd.get("close"))
            if o > 0:
                pct = (cl - o) / o * 100.0
                moves.append(pct)
    strength = sum(moves) / len(moves) if moves else None
    return {
        "average_pct_move": round(strength, 4) if strength is not None else None,
        "total_oi": total_oi,
        "total_volume": total_volume,
    }
