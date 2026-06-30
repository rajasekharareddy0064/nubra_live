"""Verify live market DB inserts across all tables â€” today's data."""
import asyncio
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, ".")
from app.core.env_loader import load_project_env
from app.core.config import settings

load_project_env(".")
IST = ZoneInfo("Asia/Kolkata")


async def check():
    import asyncpg
    dsn = settings.database_dsn
    conn = await asyncpg.connect(dsn)
    schema = settings.db_schema
    today = datetime.now(IST).strftime("%Y-%m-%d")
    now_str = datetime.now(IST).strftime("%H:%M:%S IST")

    print(f"\n{'='*70}")
    print(f"  LIVE MARKET DATA REPORT â€” {today}  (as of {now_str})")
    print(f"{'='*70}")

    # â”€â”€ futures_data (NIFTY futures 3-min candles) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n\n[ 1 ] NIFTY FUTURES â€” futures_data (3-min candles, today)")
    print("-" * 70)
    rows = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' as ts,
               symbol, expiry, underlying_symbol,
               ROUND(open::numeric/100.0, 2)   AS open_rs,
               ROUND(high::numeric/100.0, 2)   AS high_rs,
               ROUND(low::numeric/100.0, 2)    AS low_rs,
               ROUND(close::numeric/100.0, 2)  AS close_rs,
               ROUND(volume::numeric, 0)       AS volume,
               ROUND(oi::numeric, 0)           AS oi
        FROM "{schema}".futures_data
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '{today}'
          AND symbol LIKE 'NIFTY%FUT'
        ORDER BY timestamp DESC
        LIMIT 15
    """)
    if rows:
        print(f"  {'Time':<8} {'Symbol':<18} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} {'Volume':>10} {'OI':>12}")
        print(f"  {'-'*8} {'-'*18} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")
        for r in rows:
            ts = str(r['ts'])[-14:-3] if r['ts'] else '?'
            print(f"  {ts:<8} {r['symbol']:<18} {r['open_rs']:>10} {r['high_rs']:>10} {r['low_rs']:>10} {r['close_rs']:>10} {r['volume']:>10} {r['oi']:>12}")
        print(f"\n  Total rows today: {len(rows)}")
    else:
        print("  (no data yet today)")

    # â”€â”€ nifty50_stock_futures â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n\n[ 2 ] NIFTY 50 STOCK FUTURES â€” nifty50_stock_futures (3-min candles, today)")
    print("-" * 70)
    rows_sf = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' as ts,
               symbol, underlying_symbol,
               ROUND(open::numeric/100.0, 2)   AS open_rs,
               ROUND(high::numeric/100.0, 2)   AS high_rs,
               ROUND(low::numeric/100.0, 2)    AS low_rs,
               ROUND(close::numeric/100.0, 2)  AS close_rs,
               ROUND(volume::numeric, 0)       AS volume,
               ROUND(oi::numeric, 0)           AS oi
        FROM "{schema}".nifty50_stock_futures
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '{today}'
        ORDER BY timestamp DESC, symbol ASC
        LIMIT 30
    """)
    if rows_sf:
        # Show distinct timestamps to understand candle count
        distinct_ts = sorted({str(r['ts'])[-14:-3] for r in rows_sf}, reverse=True)
        print(f"  Candle timestamps: {', '.join(distinct_ts[:5])}")
        print(f"\n  {'Time':<8} {'Symbol':<22} {'Open':>10} {'Close':>10} {'Volume':>10} {'OI':>12}")
        print(f"  {'-'*8} {'-'*22} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")
        for r in rows_sf[:15]:
            ts = str(r['ts'])[-14:-3] if r['ts'] else '?'
            print(f"  {ts:<8} {r['symbol']:<22} {r['open_rs']:>10} {r['close_rs']:>10} {r['volume']:>10} {r['oi']:>12}")
        if len(rows_sf) > 15:
            print(f"  ... and {len(rows_sf)-15} more rows")
        # Count unique symbols
        syms = {r['symbol'] for r in rows_sf}
        print(f"\n  Total rows today: {len(rows_sf)} | Unique symbols: {len(syms)}")
    else:
        print("  (no data yet today)")

    # â”€â”€ order_book_3m_strikes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n\n[ 3 ] ORDER BOOK STRIKES â€” order_book_3m_strikes (today)")
    print("-" * 70)
    rows_ob = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' as ts,
               ROUND(atm::numeric, 2) as atm,
               ROUND(strike::numeric, 2) as strike,
               ROUND(ce_avg_bid_qty::numeric, 2) as ce_bid,
               ROUND(ce_avg_ask_qty::numeric, 2) as ce_ask,
               ROUND(ce_delta::numeric, 4) as ce_delta,
               ROUND(ce_imbalance::numeric, 4) as ce_imb,
               ROUND(pe_avg_bid_qty::numeric, 2) as pe_bid,
               ROUND(pe_avg_ask_qty::numeric, 2) as pe_ask,
               ROUND(pe_delta::numeric, 4) as pe_delta,
               ROUND(pe_imbalance::numeric, 4) as pe_imb
        FROM "{schema}".order_book_3m_strikes
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '{today}'
          AND ce_avg_bid_qty > 0
        ORDER BY timestamp DESC, strike ASC
        LIMIT 25
    """)
    if rows_ob:
        print(f"  {'Time':<8} {'ATM':>8} {'Strike':>8} {'CE_Bid':>10} {'CE_Ask':>10} {'CE_Î”':>8} {'PE_Bid':>10} {'PE_Ask':>10} {'PE_Î”':>8}")
        print(f"  {'-'*8} {'-'*8} {'-'*8} {'-'*10} {'-'*10} {'-'*8} {'-'*10} {'-'*10} {'-'*8}")
        for r in rows_ob:
            ts = str(r['ts'])[-14:-3] if r['ts'] else '?'
            print(f"  {ts:<8} {r['atm']:>8} {r['strike']:>8} {r['ce_bid']:>10} {r['ce_ask']:>10} {r['ce_delta']:>8} {r['pe_bid']:>10} {r['pe_ask']:>10} {r['pe_delta']:>8}")
        print(f"\n  Total rows today (with data): {len(rows_ob)}")
    else:
        # Check if any rows exist (even empty ones)
        count = await conn.fetchval(f"""
            SELECT COUNT(*) FROM "{schema}".order_book_3m_strikes
            WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '{today}'
        """)
        print(f"  Total rows today: {count} â€” but all have zero values (strikes not yet selected)")

    # â”€â”€ order_book_3m_candles â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n\n[ 4 ] ORDER BOOK CANDLES â€” order_book_3m_candles (today)")
    print("-" * 70)
    rows_obc = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' as ts,
               ROUND(atm::numeric, 2) as atm,
               ROUND(exec_delta::numeric, 2) as exec_d,
               ROUND(book_delta::numeric, 2) as book_d,
               ROUND(imbalance::numeric, 4) as imb,
               ROUND(score::numeric, 2) as score,
               regime
        FROM "{schema}".order_book_3m_candles
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '{today}'
          AND exec_delta != 0
        ORDER BY timestamp DESC
        LIMIT 10
    """)
    if rows_obc:
        print(f"  {'Time':<8} {'ATM':>8} {'ExecÎ”':>10} {'BookÎ”':>10} {'Imbal':>8} {'Score':>7} {'Regime'}")
        print(f"  {'-'*8} {'-'*8} {'-'*10} {'-'*10} {'-'*8} {'-'*7} {'-'*20}")
        for r in rows_obc:
            ts = str(r['ts'])[-14:-3] if r['ts'] else '?'
            print(f"  {ts:<8} {r['atm']:>8} {r['exec_d']:>10} {r['book_d']:>10} {r['imb']:>8} {r['score']:>7} {r['regime']}")
        print(f"\n  Rows with data today: {len(rows_obc)}")
    else:
        count = await conn.fetchval(f"""
            SELECT COUNT(*) FROM "{schema}".order_book_3m_candles
            WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '{today}'
        """)
        print(f"  Total candles today: {count} â€” but exec_delta=0 in all (no orderbook ticks yet)")

    # â”€â”€ Summary â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print(f"\n\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    cnt = {
        "futures_data": await conn.fetchval(f"SELECT COUNT(*) FROM \"{schema}\".futures_data WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'"),
        "nifty50_stock_futures": await conn.fetchval(f"SELECT COUNT(*) FROM \"{schema}\".nifty50_stock_futures WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'"),
        "order_book_3m_strikes": await conn.fetchval(f"SELECT COUNT(*) FROM \"{schema}\".order_book_3m_strikes WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'"),
        "order_book_3m_candles": await conn.fetchval(f"SELECT COUNT(*) FROM \"{schema}\".order_book_3m_candles WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'"),
    }
    for table, count in cnt.items():
        status = "[PASS]" if count > 0 else "[EMPTY]"
        print(f"  {status}  {table:<35} rows={count}")

    await conn.close()
    print("")


if __name__ == "__main__":
    asyncio.run(check())

