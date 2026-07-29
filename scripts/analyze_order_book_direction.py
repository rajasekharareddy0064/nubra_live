"""Directional analysis of exported order_book_3m_strikes data.

Applies the NEW CE/PE net directional logic (CE bullish, PE bearish) to each
3-min snapshot exported by export_order_book_strikes.py, so the fix can be
eyeballed against real rows.

Per timestamp:
    ce_exec   = Σ ce_delta                 (ce total_buy - total_sell)
    pe_exec   = Σ pe_delta
    net_exec  = ce_exec - pe_exec           (>0 bullish, <0 bearish)

    ce_book   = Σ (ce_avg_bid - ce_avg_ask)
    pe_book   = Σ (pe_avg_bid - pe_avg_ask)
    net_book  = ce_book - pe_book

    net_imbalance = (avg(ce_imbalance) - avg(pe_imbalance)) / 2

NOTE: order_book_3m_strikes has no ask_removed/bid_removed columns
(those live in order_book_3m_candles), so liquidity is not reconstructed here.

Run:  python scripts/analyze_order_book_direction.py [path_to_export.json]
"""
import json
import sys
from datetime import datetime
from pathlib import Path


def _f(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def label(net_exec: float, threshold: float = 5000.0) -> str:
    if net_exec > threshold:
        return "BULLISH"
    if net_exec < -threshold:
        return "BEARISH"
    return "NEUTRAL"


def analyze_snapshot(snap: dict) -> dict:
    ce_exec = pe_exec = 0.0
    ce_book = pe_book = 0.0
    ce_imb_vals: list[float] = []
    pe_imb_vals: list[float] = []
    strikes = snap.get("strikes", []) or []
    for row in strikes:
        ce = row.get("ce", {}) or {}
        pe = row.get("pe", {}) or {}
        ce_exec += _f(ce.get("delta"))
        pe_exec += _f(pe.get("delta"))
        ce_book += _f(ce.get("avg_bid_qty")) - _f(ce.get("avg_ask_qty"))
        pe_book += _f(pe.get("avg_bid_qty")) - _f(pe.get("avg_ask_qty"))
        if ce.get("imbalance") is not None:
            ce_imb_vals.append(_f(ce.get("imbalance")))
        if pe.get("imbalance") is not None:
            pe_imb_vals.append(_f(pe.get("imbalance")))

    net_exec = ce_exec - pe_exec
    net_book = ce_book - pe_book
    ce_imb = sum(ce_imb_vals) / len(ce_imb_vals) if ce_imb_vals else 0.0
    pe_imb = sum(pe_imb_vals) / len(pe_imb_vals) if pe_imb_vals else 0.0
    net_imbalance = (ce_imb - pe_imb) / 2.0

    # OLD (buggy) blind sum for comparison.
    old_exec_sum = ce_exec + pe_exec

    return {
        "timestamp": snap.get("timestamp"),
        "atm": snap.get("atm"),
        "strike_count": len(strikes),
        "ce_exec_delta": round(ce_exec, 2),
        "pe_exec_delta": round(pe_exec, 2),
        "net_exec": round(net_exec, 2),
        "ce_book_delta": round(ce_book, 2),
        "pe_book_delta": round(pe_book, 2),
        "net_book_delta": round(net_book, 2),
        "net_imbalance": round(net_imbalance, 4),
        "direction": label(net_exec),
        "old_exec_sum_ce_plus_pe": round(old_exec_sum, 2),
        "sign_flipped_vs_old": (old_exec_sum > 0) != (net_exec > 0),
    }


def main() -> None:
    default = None
    sample_dir = Path("sample_data")
    candidates = sorted(sample_dir.glob("order_book_3m_strikes_*.json"))
    if candidates:
        default = candidates[-1]
    in_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default
    if in_path is None or not in_path.exists():
        print("No export file found. Run export_order_book_strikes.py first.")
        sys.exit(1)

    data = json.loads(in_path.read_text(encoding="utf-8"))
    snapshots = data.get("snapshots", []) or []
    results = [analyze_snapshot(s) for s in snapshots]

    bullish = sum(1 for r in results if r["direction"] == "BULLISH")
    bearish = sum(1 for r in results if r["direction"] == "BEARISH")
    neutral = sum(1 for r in results if r["direction"] == "NEUTRAL")
    flipped = sum(1 for r in results if r["sign_flipped_vs_old"])

    out = {
        "source_file": str(in_path),
        "source_table": data.get("source_table"),
        "date": data.get("date"),
        "analyzed_at": datetime.now().isoformat(),
        "snapshot_count": len(results),
        "summary": {
            "bullish": bullish,
            "bearish": bearish,
            "neutral": neutral,
            "sign_flipped_vs_old_sum": flipped,
        },
        "snapshots": results,
    }
    out_path = in_path.with_name(in_path.stem.replace("strikes", "direction") + ".json")
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    # Console preview
    print(f"\nDirectional analysis of {in_path.name}")
    print("=" * 92)
    print(f"  {'Time':<8} {'ATM':>8} {'CE_exec':>12} {'PE_exec':>12} {'NET_exec':>12} {'Dir':<8} {'OLD(CE+PE)':>12} {'flip'}")
    print("  " + "-" * 88)
    for r in results:
        t = (r["timestamp"] or "")[11:16]
        print(
            f"  {t:<8} {r['atm']:>8} {r['ce_exec_delta']:>12} {r['pe_exec_delta']:>12} "
            f"{r['net_exec']:>12} {r['direction']:<8} {r['old_exec_sum_ce_plus_pe']:>12} "
            f"{'YES' if r['sign_flipped_vs_old'] else ''}"
        )
    print("  " + "-" * 88)
    print(f"  Snapshots: {len(results)} | BULLISH={bullish} BEARISH={bearish} NEUTRAL={neutral}")
    print(f"  Sign flipped vs old CE+PE sum: {flipped}")
    print(f"\nWrote summary -> {out_path}")


if __name__ == "__main__":
    main()
