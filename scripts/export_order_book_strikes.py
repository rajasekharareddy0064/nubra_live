"""Export today's trading_data.order_book_3m_strikes rows to a JSON file.

Produces a testing fixture grouped by candle timestamp, plus flat rows.

Run:  python scripts/export_order_book_strikes.py
Output: sample_data/order_book_3m_strikes_<today>.json
"""
import asyncio
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, ".")
from app.core.env_loader import load_project_env
from app.core.config import settings

load_project_env(".")
IST = ZoneInfo("Asia/Kolkata")


def _json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _to_ist(dt: datetime | None) -> datetime | None:
    """Return dt as an IST-aware datetime.

    The `timestamp` column is stored NAIVE but already in IST wall-clock,
    so we attach IST directly. `created_at` is TIMESTAMPTZ (UTC-aware), so
    we convert it to IST.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


async def main() -> None:
    import asyncpg

    conn = await asyncpg.connect(settings.database_dsn)
    schema = settings.db_schema
    today = datetime.now(IST).strftime("%Y-%m-%d")

    rows = await conn.fetch(
        f"""
        SELECT
            timestamp AS ts,
            atm, strike,
            ce_avg_bid_qty, ce_avg_ask_qty, ce_total_buy_qty, ce_total_sell_qty,
            ce_delta, ce_imbalance,
            pe_avg_bid_qty, pe_avg_ask_qty, pe_total_buy_qty, pe_total_sell_qty,
            pe_delta, pe_imbalance,
            created_at
        FROM "{schema}".order_book_3m_strikes
        WHERE DATE(timestamp) = '{today}'
        ORDER BY timestamp ASC, strike ASC
        """
    )
    await conn.close()

    flat: list[dict] = []
    grouped: dict[str, dict] = {}
    for r in rows:
        d = dict(r)
        # Normalize both timestamps to IST-aware so JSON shows +05:30.
        d["ts"] = _to_ist(d.get("ts"))
        d["created_at"] = _to_ist(d.get("created_at"))
        ts_key = d["ts"].isoformat() if d.get("ts") else "unknown"
        flat.append(d)
        bucket = grouped.setdefault(ts_key, {"timestamp": ts_key, "atm": d.get("atm"), "strikes": []})
        bucket["strikes"].append(
            {
                "strike": d.get("strike"),
                "ce": {
                    "avg_bid_qty": d.get("ce_avg_bid_qty"),
                    "avg_ask_qty": d.get("ce_avg_ask_qty"),
                    "total_buy_qty": d.get("ce_total_buy_qty"),
                    "total_sell_qty": d.get("ce_total_sell_qty"),
                    "delta": d.get("ce_delta"),
                    "imbalance": d.get("ce_imbalance"),
                },
                "pe": {
                    "avg_bid_qty": d.get("pe_avg_bid_qty"),
                    "avg_ask_qty": d.get("pe_avg_ask_qty"),
                    "total_buy_qty": d.get("pe_total_buy_qty"),
                    "total_sell_qty": d.get("pe_total_sell_qty"),
                    "delta": d.get("pe_delta"),
                    "imbalance": d.get("pe_imbalance"),
                },
            }
        )

    out = {
        "source_table": f"{schema}.order_book_3m_strikes",
        "date": today,
        "exported_at": datetime.now(IST).isoformat(),
        "total_rows": len(flat),
        "distinct_timestamps": len(grouped),
        "snapshots": list(grouped.values()),
        "flat_rows": flat,
    }

    out_dir = Path("sample_data")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"order_book_3m_strikes_{today}.json"
    out_path.write_text(json.dumps(out, indent=2, default=_json_default), encoding="utf-8")

    print(f"Wrote {len(flat)} rows across {len(grouped)} timestamps -> {out_path}")
    if not flat:
        print("NOTE: no rows for today — table may be empty (market closed / no ingestion).")


if __name__ == "__main__":
    asyncio.run(main())
