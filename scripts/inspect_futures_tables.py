import asyncio

import asyncpg

from app.core.config import settings


async def main() -> None:
    conn = await asyncpg.connect(settings.database_dsn)
    schema = settings.db_schema
    for table in ("futures_data", "nifty50_stock_futures"):
        print(f"=== {schema}.{table} ===")
        rows = await conn.fetch(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            """,
            schema,
            table,
        )
        for row in rows:
            print(dict(row))
        idx = await conn.fetch(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = $1 AND tablename = $2
            """,
            schema,
            table,
        )
        for row in idx:
            print("IDX:", row["indexname"], row["indexdef"])
        sample = await conn.fetch(
            f'SELECT * FROM "{schema}"."{table}" ORDER BY 1 DESC LIMIT 2'
        )
        for row in sample:
            print("SAMPLE:", dict(row))
        print()
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
