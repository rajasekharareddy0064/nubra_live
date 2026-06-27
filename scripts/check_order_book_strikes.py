import asyncio

import asyncpg

from app.core.config import settings


async def main() -> None:
    conn = await asyncpg.connect(settings.database_dsn)
    schema = settings.db_schema
    for table in ("order_book_3m_strikes", "order_book_3m_candles"):
        latest = await conn.fetchrow(
            f'SELECT MAX(timestamp) AS latest FROM "{schema}"."{table}"'
        )
        print(f"{table} latest:", dict(latest) if latest else None)
    candles = await conn.fetch(
        f"""
        SELECT c.timestamp, COUNT(s.strike) AS strike_count
        FROM "{schema}"."order_book_3m_candles" c
        LEFT JOIN "{schema}"."order_book_3m_strikes" s ON s.timestamp = c.timestamp
        GROUP BY c.timestamp
        ORDER BY c.timestamp DESC
        LIMIT 10
        """
    )
    print("Candles vs strike rows:")
    for row in candles:
        print(dict(row))
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
