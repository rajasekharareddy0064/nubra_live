"""Verify simulation DB inserts across all tables."""
import asyncio
import sys
sys.path.insert(0, ".")

from app.core.env_loader import load_project_env
from app.core.config import settings

load_project_env(".")


async def check():
    import asyncpg
    dsn = settings.database_dsn
    conn = await asyncpg.connect(dsn)
    schema = settings.db_schema

    print("\n=== futures_data (NIFTY futures) ===")
    rows = await conn.fetch(
        f'SELECT symbol, "timestamp", open, high, low, close, volume, oi, expiry, underlying_symbol '
        f'FROM "{schema}".futures_data ORDER BY "timestamp" DESC LIMIT 5'
    )
    for r in rows:
        print(f'  {r["symbol"]} | {r["timestamp"]} | O={r["open"]} H={r["high"]} L={r["low"]} C={r["close"]} V={r["volume"]} OI={r["oi"]} | expiry={r["expiry"]} underlying={r["underlying_symbol"]}')
    if not rows:
        print("  (empty)")
    print(f"  Total rows: {len(rows)}")

    print("\n=== nifty50_stock_futures ===")
    rows = await conn.fetch(
        f'SELECT symbol, "timestamp", open, high, low, close, volume, oi, underlying_symbol '
        f'FROM "{schema}".nifty50_stock_futures ORDER BY "timestamp" DESC LIMIT 5'
    )
    for r in rows:
        print(f'  {r["symbol"]} | {r["timestamp"]} | O={r["open"]} H={r["high"]} L={r["low"]} C={r["close"]} V={r["volume"]} OI={r["oi"]} | underlying={r["underlying_symbol"]}')
    if not rows:
        print("  (empty)")
    print(f"  Total rows: {len(rows)}")

    print("\n=== order_book_3m_strikes (option order book) ===")
    rows = await conn.fetch(
        f'SELECT "timestamp", atm, strike, ce_avg_bid_qty, ce_delta, pe_avg_bid_qty, pe_delta '
        f'FROM "{schema}".order_book_3m_strikes ORDER BY "timestamp" DESC LIMIT 10'
    )
    for r in rows:
        print(f'  {r["timestamp"]} | ATM={r["atm"]} Strike={r["strike"]} | CE_bid={r["ce_avg_bid_qty"]} CE_delta={r["ce_delta"]} | PE_bid={r["pe_avg_bid_qty"]} PE_delta={r["pe_delta"]}')
    if not rows:
        print("  (empty)")
    print(f"  Total rows: {len(rows)}")

    print("\n=== order_book_3m_candles ===")
    rows = await conn.fetch(
        f'SELECT "timestamp", atm, exec_delta, book_delta, imbalance, score, regime '
        f'FROM "{schema}".order_book_3m_candles ORDER BY "timestamp" DESC LIMIT 5'
    )
    for r in rows:
        print(f'  {r["timestamp"]} | ATM={r["atm"]} exec_d={r["exec_delta"]} book_d={r["book_delta"]} imb={r["imbalance"]} score={r["score"]} regime={r["regime"]}')
    if not rows:
        print("  (empty)")
    print(f"  Total rows: {len(rows)}")

    print("\n=== market_events (simulation) ===")
    rows = await conn.fetch(
        f"""SELECT topic, payload->>'ticks_processed' as ticks, created_at """
        f"""FROM "{schema}".market_events WHERE topic='simulation_candle' ORDER BY created_at DESC LIMIT 3"""
    )
    for r in rows:
        print(f'  {r["topic"]} | ticks={r["ticks"]} | {r["created_at"]}')
    if not rows:
        print("  (empty)")

    await conn.close()
    print("\n[DONE] Database verification complete.")


if __name__ == "__main__":
    asyncio.run(check())
