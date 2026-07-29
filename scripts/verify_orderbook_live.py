"""Read-only: show latest order_book_3m_candles rows (new directional cols)."""
import asyncio
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, ".")
from app.core.env_loader import load_project_env
from app.core.config import settings

load_project_env(".")
IST = ZoneInfo("Asia/Kolkata")


async def main() -> None:
    import asyncpg

    conn = await asyncpg.connect(settings.database_dsn)
    schema = settings.db_schema
    today = datetime.now(IST).date()
    now = datetime.now(IST).strftime("%H:%M:%S IST")
    print(f"Now: {now} | schema={schema} | date={today}")

    total = await conn.fetchval(
        f"SELECT COUNT(*) FROM \"{schema}\".order_book_3m_candles WHERE DATE(timestamp)=$1",
        today,
    )
    print(f"order_book_3m_candles rows today: {total}")

    rows = await conn.fetch(
        f"""
        SELECT timestamp AS ts, atm, exec_delta, book_delta, imbalance, score, regime,
               net_pressure, bullish_pressure, bearish_pressure,
               ce_exec_delta, pe_exec_delta
        FROM "{schema}".order_book_3m_candles
        WHERE DATE(timestamp)=$1
        ORDER BY timestamp DESC
        LIMIT 8
        """,
        today,
    )
    if rows:
        print(f"\n{'Time':<8} {'ATM':>8} {'net_exec':>10} {'net_book':>10} {'score':>6} {'regime':<12} {'net_press':>12} {'ce_exec':>10} {'pe_exec':>10}")
        for r in rows:
            t = r["ts"].strftime("%H:%M")
            print(
                f"{t:<8} {float(r['atm'] or 0):>8.0f} {float(r['exec_delta'] or 0):>10.0f} "
                f"{float(r['book_delta'] or 0):>10.0f} {float(r['score'] or 0):>6.1f} {str(r['regime']):<12} "
                f"{float(r['net_pressure'] or 0):>12.0f} {float(r['ce_exec_delta'] or 0):>10.0f} {float(r['pe_exec_delta'] or 0):>10.0f}"
            )
        newcols_filled = sum(1 for r in rows if r["net_pressure"] is not None)
        print(f"\nrows with NEW directional cols populated (of last {len(rows)}): {newcols_filled}")
    else:
        print("No candles yet today.")

    ob_count = await conn.fetchval(
        f"SELECT COUNT(*) FROM \"{schema}\".nifty50_stock_ohlc WHERE DATE(timestamp)=$1",
        today,
    )
    print(f"nifty50_stock_ohlc rows today: {ob_count}")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
