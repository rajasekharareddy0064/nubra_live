import asyncio, sys
from datetime import datetime
from zoneinfo import ZoneInfo
sys.path.insert(0, ".")
from app.core.env_loader import load_project_env
from app.core.config import settings
load_project_env(".")
IST = ZoneInfo("Asia/Kolkata")

async def check():
    import asyncpg
    conn = await asyncpg.connect(settings.database_dsn)
    schema = settings.db_schema
    today = datetime.now(IST).date()
    cnt = await conn.fetchval(
        f"SELECT COUNT(*) FROM \"{schema}\".options_data "
        f"WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')=$1", today)
    print(f"options_data rows today: {cnt}")
    rows = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' AS ts, symbol, strike, option_type,
               close, ltp, volume, oi, delta, iv, moneyness, spot_distance, expiry
        FROM "{schema}".options_data
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')=$1
        ORDER BY timestamp DESC, strike ASC LIMIT 12
    """, today)
    for r in rows:
        ts = str(r['ts'])[-14:-3]
        print(f"  {ts} {r['symbol']:<22} {r['option_type']} K={r['strike']} ltp={r['ltp']} "
              f"vol={r['volume']} oi={r['oi']} delta={r['delta']} iv={r['iv']} "
              f"{r['moneyness']} dist={r['spot_distance']} exp={r['expiry']}")
    # distinct snapshot timestamps today
    tss = await conn.fetch(f"""
        SELECT timestamp AT TIME ZONE 'Asia/Kolkata' AS ts, COUNT(*) n
        FROM "{schema}".options_data
        WHERE DATE(timestamp AT TIME ZONE 'Asia/Kolkata')=$1
        GROUP BY 1 ORDER BY 1 DESC LIMIT 6
    """, today)
    print("\n  recent snapshots (ts -> rows):")
    for r in tss:
        print(f"    {str(r['ts'])[-14:-3]} -> {r['n']}")
    await conn.close()

asyncio.run(check())
