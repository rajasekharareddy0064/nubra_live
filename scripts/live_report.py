"""Quick live market data report."""
import asyncio, sys
sys.path.insert(0, ".")
from app.core.env_loader import load_project_env
from app.core.config import settings
load_project_env(".")
from datetime import datetime
from zoneinfo import ZoneInfo
IST = ZoneInfo("Asia/Kolkata")
today = datetime.now(IST).strftime("%Y-%m-%d")
now_str = datetime.now(IST).strftime("%H:%M:%S IST")

async def run():
    import asyncpg
    conn = await asyncpg.connect(settings.database_dsn)
    s = settings.db_schema

    print(f"\n{'='*65}")
    print(f"  LIVE MARKET REPORT — {today} at {now_str}")
    print(f"{'='*65}")

    # [1] NIFTY futures
    print("\n[1] NIFTY FUTURES (futures_data)")
    rows = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' as ts, symbol,
               ROUND(open::numeric/100,2) as o, ROUND(high::numeric/100,2) as h,
               ROUND(low::numeric/100,2) as l, ROUND(close::numeric/100,2) as c,
               ROUND(volume::numeric,0) as v, ROUND(oi::numeric,0) as oi
        FROM "{s}".futures_data
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'
          AND symbol LIKE 'NIFTY%FUT'
        ORDER BY timestamp DESC, symbol LIMIT 9
    """)
    if rows:
        print(f"  {'Time':<8} {'Symbol':<20} {'Open':>9} {'High':>9} {'Low':>9} {'Close':>9} {'Vol':>8} {'OI':>10}")
        for r in rows:
            ts = str(r['ts'])[11:16]
            print(f"  {ts:<8} {r['symbol']:<20} {r['o']:>9} {r['h']:>9} {r['l']:>9} {r['c']:>9} {r['v']:>8} {r['oi']:>10}")
        print(f"  rows={len(rows)}")
    else:
        print("  (empty)")

    # [2] Stock futures summary
    print("\n[2] STOCK FUTURES (nifty50_stock_futures)")
    sf_cnt = await conn.fetchrow(f"""
        SELECT COUNT(*) as cnt, COUNT(DISTINCT symbol) as syms,
               MAX(timestamp AT TIME ZONE 'Asia/Kolkata') as latest
        FROM "{s}".nifty50_stock_futures
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'
    """)
    print(f"  Total rows: {sf_cnt['cnt']}  |  Unique symbols: {sf_cnt['syms']}  |  Latest: {str(sf_cnt['latest'])[11:16] if sf_cnt['latest'] else 'N/A'}")

    sf = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' as ts, symbol,
               ROUND(close::numeric/100,2) as c,
               ROUND(volume::numeric,0) as v, ROUND(oi::numeric,0) as oi
        FROM "{s}".nifty50_stock_futures
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'
        ORDER BY timestamp DESC, symbol LIMIT 10
    """)
    if sf:
        print(f"  {'Time':<8} {'Symbol':<24} {'Close':>9} {'Vol':>8} {'OI':>10}")
        for r in sf:
            ts = str(r['ts'])[11:16]
            print(f"  {ts:<8} {r['symbol']:<24} {r['c']:>9} {r['v']:>8} {r['oi']:>10}")

    # [3] Order book strikes
    print("\n[3] ORDER BOOK STRIKES (order_book_3m_strikes)")
    ob_sum = await conn.fetchrow(f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN ce_avg_bid_qty > 0 THEN 1 ELSE 0 END) as with_data,
               COUNT(DISTINCT timestamp) as candles,
               MAX(ROUND(atm::numeric,2)) as latest_atm
        FROM "{s}".order_book_3m_strikes
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'
    """)
    print(f"  Total rows: {ob_sum['total']}  |  With data: {ob_sum['with_data']}  |  Candles: {ob_sum['candles']}  |  Latest ATM: {ob_sum['latest_atm']}")

    strikes = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' as ts,
               ROUND(atm::numeric,2) as atm, ROUND(strike::numeric,2) as strike,
               ROUND(ce_avg_bid_qty::numeric,0) as ce_bid,
               ROUND(pe_avg_bid_qty::numeric,0) as pe_bid,
               ROUND(ce_delta::numeric,4) as ce_d, ROUND(pe_delta::numeric,4) as pe_d
        FROM "{s}".order_book_3m_strikes
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'
          AND ce_avg_bid_qty > 0
        ORDER BY timestamp DESC, strike LIMIT 12
    """)
    if strikes:
        print(f"  {'Time':<8} {'ATM':>8} {'Strike':>8} {'CE_Bid':>8} {'CE_Δ':>8} {'PE_Bid':>8} {'PE_Δ':>8}")
        for r in strikes:
            ts = str(r['ts'])[11:16]
            print(f"  {ts:<8} {r['atm']:>8} {r['strike']:>8} {r['ce_bid']:>8} {r['ce_d']:>8} {r['pe_bid']:>8} {r['pe_d']:>8}")
    else:
        print("  (no rows with CE data yet)")

    # [4] Order book candles
    print("\n[4] ORDER BOOK CANDLES (order_book_3m_candles)")
    obc = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' as ts,
               ROUND(atm::numeric,2) as atm,
               ROUND(exec_delta::numeric,2) as ed, ROUND(book_delta::numeric,2) as bd,
               ROUND(score::numeric,2) as score, regime
        FROM "{s}".order_book_3m_candles
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')='{today}'
        ORDER BY timestamp DESC LIMIT 8
    """)
    if obc:
        print(f"  {'Time':<8} {'ATM':>8} {'ExecΔ':>10} {'BookΔ':>10} {'Score':>7} {'Regime'}")
        for r in obc:
            ts = str(r['ts'])[11:16]
            atm = r['atm'] if r['atm'] else 'N/A'
            print(f"  {ts:<8} {str(atm):>8} {r['ed']:>10} {r['bd']:>10} {r['score']:>7} {r['regime']}")
        non_zero = sum(1 for r in obc if r['ed'] != 0)
        print(f"  Total candles: {len(obc)}  |  With data: {non_zero}")
    else:
        print("  (empty)")

    print("")
    await conn.close()

asyncio.run(run())
