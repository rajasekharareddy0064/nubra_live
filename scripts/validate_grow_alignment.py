"""Compare last Grow row vs first nubra-live row at the 2026-07-15 cutoff.

Table checks reflect nubra-live's target layout after Grow alignment:
  - market_ohlc        → NIFTY spot 3m only
  - market_ohlc_3m     → NIFTY50 spot stocks (instrument_type=STOCK), not NIFTY
  - nifty50_stock_ohlc → legacy; nubra-live no longer writes here
"""
import asyncio
import sys
from datetime import datetime

sys.path.insert(0, ".")
from app.core.env_loader import load_project_env
from app.core.config import settings

load_project_env(".")
CUTOFF = datetime(2026, 7, 15, 17, 17, 14)


async def main() -> None:
    import asyncpg

    conn = await asyncpg.connect(settings.database_dsn, timeout=15)
    schema = settings.db_schema
    specs = [
        ("futures_data", "AND symbol LIKE 'NIFTY%FUT'", "timestamp"),
        ("nifty50_stock_futures", "AND underlying_symbol = 'RELIANCE'", "timestamp"),
        ("options_data", "AND option_type = 'CE'", "timestamp"),
        ("market_ohlc", "AND symbol = 'NIFTY'", "timestamp"),
        (
            "market_ohlc_3m",
            "AND symbol = 'ICICIBANK' AND instrument_type = 'STOCK'",
            "candle_time",
        ),
        ("nifty50_stock_ohlc", "AND symbol = 'ICICIBANK'", "timestamp"),
    ]
    for table, extra, col in specs:
        print("=" * 90)
        print(f"TABLE: {schema}.{table}")
        grow = await conn.fetch(
            f'SELECT * FROM "{schema}".{table} WHERE {col} <= $1::timestamp {extra} ORDER BY {col} DESC LIMIT 1',
            CUTOFF,
        )
        live = await conn.fetch(
            f'SELECT * FROM "{schema}".{table} WHERE {col} > $1::timestamp {extra} ORDER BY {col} ASC LIMIT 1',
            CUTOFF,
        )
        print("LAST GROW:", dict(grow[0]) if grow else "(none)")
        print("FIRST LIVE:", dict(live[0]) if live else "(none)")
        if table == "nifty50_stock_ohlc":
            print("NOTE: nubra-live no longer writes nifty50_stock_ohlc; expect no new live rows.")
        if table == "market_ohlc_3m":
            print("NOTE: nubra-live writes NIFTY50 spot stocks only (no NIFTY) into market_ohlc_3m.")
        print()

    # Spot-stock coverage: how many NIFTY50 symbols have live rows today vs Grow baseline.
    spot_symbols = await conn.fetch(
        f"""
        SELECT symbol, COUNT(*) AS rows
        FROM "{schema}".market_ohlc_3m
        WHERE instrument_type = 'STOCK'
          AND candle_time > $1::timestamp
        GROUP BY symbol
        ORDER BY symbol
        """,
        CUTOFF,
    )
    print("=" * 90)
    print(f"LIVE market_ohlc_3m spot stocks after cutoff (count={len(spot_symbols)}):")
    for row in spot_symbols:
        print(f"  {row['symbol']}: {row['rows']} rows")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
