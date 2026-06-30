"""Check if options_data table is receiving live data."""
import asyncio
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, ".")
from app.core.env_loader import load_project_env
from app.core.config import settings

load_project_env(".")
IST = ZoneInfo("Asia/Kolkata")

async def check_options_data():
    import asyncpg
    dsn = settings.database_dsn
    conn = await asyncpg.connect(dsn)
    schema = settings.db_schema
    today = datetime.now(IST).strftime("%Y-%m-%d")
    now_str = datetime.now(IST).strftime("%H:%M:%S IST")
    
    print(f"\n{'='*70}")
    print(f"  OPTIONS DATA VERIFICATION — {today} (as of {now_str})")
    print(f"{'='*70}")
    
    # Check total rows today
    total_rows = await conn.fetchval(f"""
        SELECT COUNT(*) FROM "{schema}".options_data
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '{today}'
    """)
    
    print(f"\n[1] TOTAL ROWS TODAY: {total_rows}")
    
    if total_rows > 0:
        # Get latest rows
        rows = await conn.fetch(f"""
            SELECT timestamp AT TIME ZONE 'Asia/Kolkata' as ts,
                   symbol,
                   ROUND(strike::numeric/100.0, 2) as strike_rs,
                   option_type,
                   ROUND(ltp::numeric/100.0, 2) as ltp_rs,
                   volume,
                   oi,
                   ROUND(iv::numeric, 2) as iv,
                   ROUND(delta::numeric, 4) as delta,
                   ROUND(gamma::numeric, 4) as gamma,
                   ROUND(theta::numeric, 4) as theta,
                   ROUND(vega::numeric, 4) as vega,
                   expiry
            FROM "{schema}".options_data
            WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '{today}'
            ORDER BY timestamp DESC, strike ASC, option_type DESC
            LIMIT 20
        """)
        
        print(f"\n[2] SAMPLE RECORDS (latest first):")
        print(f"{'-'*120}")
        print(f"{'Time':<10} {'Symbol':<15} {'Strike':>8} {'Type':<3} {'LTP':>8} {'Vol':>8} {'OI':>12} {'IV':>6} {'Delta':>8} {'Gamma':>8}")
        print(f"{'-'*120}")
        
        for r in rows:
            ts = str(r['ts'])[-14:-3] if r['ts'] else '?'
            print(f"{ts:<10} {r['symbol']:<15} {r['strike_rs']:>8} {r['option_type']:>3} {r['ltp_rs']:>8.2f} {r['volume']:>8} {r['oi']:>12} {r['iv']:>6.1f} {r['delta']:>8.4f} {r['gamma']:>8.4f}")
        
        # Check distribution
        print(f"\n[3] DISTRIBUTION BY TIMESTAMP:")
        distribution = await conn.fetch(f"""
            SELECT DATE_TRUNC('minute', timestamp AT TIME ZONE 'Asia/Kolkata') as minute,
                   COUNT(*) as rows_per_minute,
                   COUNT(DISTINCT strike) as unique_strikes,
                   COUNT(DISTINCT option_type) as unique_types
            FROM "{schema}".options_data
            WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata') = '{today}'
            GROUP BY minute
            ORDER BY minute DESC
            LIMIT 10
        """)
        
        print(f"{'Time':<20} {'Rows':>10} {'Strikes':>10} {'Types':>10}")
        print(f"{'-'*50}")
        for r in distribution:
            time_str = str(r['minute'])[-16:-3] if r['minute'] else '?'
            print(f"{time_str:<20} {r['rows_per_minute']:>10} {r['unique_strikes']:>10} {r['unique_types']:>10}")
    
    else:
        print(f"\n[!] NO DATA FOUND in options_data table for today ({today})")
        
        # Check if table exists
        table_exists = await conn.fetchval(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{schema}' AND table_name = 'options_data'
            )
        """)
        
        print(f"\n[4] TABLE EXISTS: {table_exists}")
        
        if table_exists:
            # Check schema
            columns = await conn.fetch(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = 'options_data'
                ORDER BY ordinal_position
            """)
            
            print(f"\n[5] TABLE SCHEMA:")
            print(f"{'Column':<25} {'Type':<20} {'Nullable'}")
            print(f"{'-'*55}")
            for c in columns:
                print(f"{c['column_name']:<25} {c['data_type']:<20} {c['is_nullable']}")
    
    await conn.close()
    print(f"\n{'='*70}")
    print("  VERIFICATION COMPLETE")
    print(f"{'='*70}")

if __name__ == "__main__":
    asyncio.run(check_options_data())