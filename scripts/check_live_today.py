"""Quick check: today's live DB rows from nubra-live."""
from __future__ import annotations

import asyncio
import sys
from datetime import date

sys.path.insert(0, ".")
from app.core.config import settings
from app.core.env_loader import load_project_env

load_project_env(".")


async def main() -> None:
    import asyncpg

    dsn = settings.database_dsn
    if not dsn:
        print("ERROR: no database_dsn configured")
        return
    conn = await asyncpg.connect(dsn, timeout=20)
    schema = settings.db_schema
    today = date.today()
    print(f"Schema={schema} date={today}")

    queries = {
        "market_ohlc (NIFTY)": f"""
            SELECT COUNT(*) AS n, MAX("timestamp") AS max_ts
            FROM "{schema}".market_ohlc
            WHERE DATE("timestamp") = $1 AND symbol = 'NIFTY'
        """,
        "market_ohlc_3m (STOCK spot)": f"""
            SELECT COUNT(*) AS n, COUNT(DISTINCT symbol) AS symbols, MAX(candle_time) AS max_ts
            FROM "{schema}".market_ohlc_3m
            WHERE DATE(candle_time) = $1 AND instrument_type = 'STOCK'
        """,
        "options_data": f"""
            SELECT COUNT(*) AS n, MAX("timestamp") AS max_ts
            FROM "{schema}".options_data
            WHERE DATE("timestamp") = $1
        """,
        "futures_data (NIFTY FUT)": f"""
            SELECT COUNT(*) AS n, MAX("timestamp") AS max_ts
            FROM "{schema}".futures_data
            WHERE DATE("timestamp") = $1 AND symbol LIKE 'NIFTY%FUT'
        """,
        "nifty50_stock_futures": f"""
            SELECT COUNT(*) AS n, MAX("timestamp") AS max_ts
            FROM "{schema}".nifty50_stock_futures
            WHERE DATE("timestamp") = $1
        """,
    }
    for label, q in queries.items():
        row = await conn.fetchrow(q, today)
        print(f"{label}: {dict(row)}")

    print("\n--- Latest NIFTY market_ohlc ---")
    for r in await conn.fetch(
        f"""
        SELECT symbol, "timestamp", open, high, low, close, volume
        FROM "{schema}".market_ohlc
        WHERE DATE("timestamp") = $1 AND symbol = 'NIFTY'
        ORDER BY "timestamp" DESC LIMIT 2
        """,
        today,
    ):
        print(dict(r))

    print("\n--- Latest spot stocks market_ohlc_3m (top 5 by time) ---")
    for r in await conn.fetch(
        f"""
        SELECT symbol, symbol_token, instrument_type, candle_time, open, close, volume
        FROM "{schema}".market_ohlc_3m
        WHERE DATE(candle_time) = $1 AND instrument_type = 'STOCK'
        ORDER BY candle_time DESC, symbol
        LIMIT 5
        """,
        today,
    ):
        print(dict(r))

    print("\n--- Latest options_data sample ---")
    for r in await conn.fetch(
        f"""
        SELECT symbol, "timestamp", open, high, low, close, volume, strike
        FROM "{schema}".options_data
        WHERE DATE("timestamp") = $1
        ORDER BY "timestamp" DESC LIMIT 2
        """,
        today,
    ):
        print(dict(r))

    # Price sanity: flag options with close > 5000 (likely paise bug)
    bad_opts = await conn.fetchval(
        f"""
        SELECT COUNT(*) FROM "{schema}".options_data
        WHERE DATE("timestamp") = $1 AND close > 5000
        """,
        today,
    )
    print(f"\noptions_data rows today with close > 5000 (suspicious): {bad_opts}")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
