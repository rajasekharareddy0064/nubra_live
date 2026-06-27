"""Smoke test futures_3m DB inserts against existing tables."""
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.storage.db_writer import DBWriter

IST = ZoneInfo("Asia/Kolkata")


async def main() -> None:
    writer = DBWriter(settings.database_dsn, schema=settings.db_schema)
    await writer.connect()

    now = datetime.now(IST).replace(second=0, microsecond=0)
    minute = (now.minute // 3) * 3
    bucket_end = now.replace(minute=minute)
    expiry_dt = datetime(2026, 6, 30)

    bucket_end_naive = bucket_end.replace(tzinfo=None)
    payload = {
        "bucket_end": bucket_end_naive.isoformat(),
        "price_scale": 100.0,
        "futures": {
            "NIFTY26JUNFUT": {
                "open": 24071.9,
                "high": 24085.0,
                "low": 24070.0,
                "close": 24077.0,
                "volume": 3686670,
                "oi": 16678220,
                "is_empty": False,
            },
        },
        "stocks": {
            "ULTRACEMCO26JUNFUT": {
                "open": 11414.0,
                "high": 11429.0,
                "low": 11404.0,
                "close": 11416.0,
                "volume": 193350,
                "oi": 2513350,
                "is_empty": False,
                "underlying_symbol": "ULTRACEMCO",
            },
        },
        "fut_meta": {
            "NIFTY26JUNFUT": {
                "underlying_symbol": "NIFTY",
                "expiry": "2026-06-25",
                "expiry_dt": datetime(2026, 6, 25),
                "instrument_type": "FUT",
            },
            "ULTRACEMCO26JUNFUT": {
                "underlying_symbol": "ULTRACEMCO",
                "expiry": "2026-06-30",
                "expiry_dt": expiry_dt,
                "instrument_type": "FUT",
            },
        },
        "nifty50_underlyings": ["ULTRACEMCO", "RELIANCE"],
    }
    async with writer.pool.acquire() as conn:
        await writer._insert_futures_data(conn, [payload])
        await writer._insert_nifty50_stock_futures(conn, [payload])
        fut_rows = await conn.fetch(
            f"""
            SELECT symbol, expiry, timestamp, open, close, volume, oi, underlying_symbol
            FROM {writer.futures_data_table}
            WHERE timestamp >= $1::timestamp
            ORDER BY timestamp DESC, symbol
            LIMIT 5
            """,
            bucket_end_naive - timedelta(minutes=3),
        )
        stock_rows = await conn.fetch(
            f"""
            SELECT symbol, underlying_symbol, expiry, timestamp, open, close, volume, oi
            FROM {writer.nifty50_stock_futures_table}
            WHERE timestamp >= $1::timestamp
            ORDER BY timestamp DESC, symbol
            LIMIT 5
            """,
            bucket_end_naive - timedelta(minutes=3),
        )
    print("futures_data:")
    for row in fut_rows:
        print(dict(row))
    print("nifty50_stock_futures:")
    for row in stock_rows:
        print(dict(row))
    await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
