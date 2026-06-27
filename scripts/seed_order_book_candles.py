"""One-off seed for order_book_3m_candles sample rows."""
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
    samples = [
        {
            "bucket_end": bucket_end.isoformat(),
            "order_book": {
                "atm": 23500,
                "exec_delta": 3705,
                "book_delta": -350480,
                "ask_removed": 972790,
                "bid_removed": 913185,
                "cum_exec_delta_30": 3705,
                "cum_book_delta_30": -350480,
                "imbalance": -0.0129,
                "strike_shift": 0,
                "breakout_score": 44.83,
                "regime": "LOADING",
            },
        },
        {
            "bucket_end": (bucket_end - timedelta(minutes=3)).isoformat(),
            "order_book": {
                "atm": 23500,
                "exec_delta": 2100,
                "book_delta": -120000,
                "ask_removed": 500000,
                "bid_removed": 480000,
                "cum_exec_delta_30": 5805,
                "cum_book_delta_30": -470480,
                "imbalance": -0.0085,
                "strike_shift": 50,
                "breakout_score": 41.20,
                "regime": "RANGE",
            },
        },
    ]
    async with writer.pool.acquire() as conn:
        await writer._insert_order_book_3m_candles(conn, samples)
        rows = await conn.fetch(
            f"""
            SELECT timestamp, atm, exec_delta, book_delta, ask_removed, bid_removed,
                   cum_exec_delta_30, cum_book_delta_30, imbalance, strike_shift, score, regime
            FROM {writer.order_book_candle_table}
            ORDER BY timestamp DESC
            LIMIT 5
            """
        )
    for row in rows:
        print(dict(row))
    await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
