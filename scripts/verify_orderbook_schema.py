"""Read-only check that the directional migration landed in the DB.

Confirms order_book_3m_candles has the new directional columns and that
nifty50_stock_ohlc exists. Safe (read-only, information_schema only).
"""
import asyncio
import sys

sys.path.insert(0, ".")
from app.core.env_loader import load_project_env
from app.core.config import settings

load_project_env(".")

NEW_COLS = [
    "ce_exec_delta", "pe_exec_delta", "ce_book_delta", "pe_book_delta",
    "ce_imbalance", "pe_imbalance", "ce_ask_removed", "ce_bid_removed",
    "pe_ask_removed", "pe_bid_removed", "bullish_pressure", "bearish_pressure",
    "net_pressure",
]


async def main() -> None:
    import asyncpg

    conn = await asyncpg.connect(settings.database_dsn)
    schema = settings.db_schema
    rows = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'order_book_3m_candles'
        ORDER BY ordinal_position
        """,
        schema,
    )
    cols = [r["column_name"] for r in rows]
    print(f"schema = {schema}")
    print(f"order_book_3m_candles columns ({len(cols)}):")
    print(" ", cols)
    missing = [c for c in NEW_COLS if c not in cols]
    print("new directional columns present:", "ALL ✅" if not missing else f"MISSING {missing}")

    ohlc = await conn.fetchval("SELECT to_regclass($1)", f"{schema}.nifty50_stock_ohlc")
    print("nifty50_stock_ohlc table exists:", "YES ✅" if ohlc is not None else "NO ❌")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
