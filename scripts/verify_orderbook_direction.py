"""Validate CE/PE directional aggregation in OrderBookAggregator.

Feeds synthetic CE/PE orderbook ticks through the REAL public path
(update_option -> snapshot_and_reset) for the four required scenarios and
checks the sign of net_exec / net_pressure and the emitted metrics.

Run:  python scripts/verify_orderbook_direction.py
"""
import asyncio
import sys

sys.path.insert(0, ".")
from app.realtime.order_book import OrderBookAggregator

ATM = 24500
STEP = 50


def tick(*, buy=0.0, sell=0.0, bid=0.0, ask=0.0, bid_price=100.0, ask_price=101.0):
    """Build a minimal orderbook payload the aggregator understands."""
    return {
        "buy_qty": buy,
        "sell_qty": sell,
        "bid_qty": bid,
        "ask_qty": ask,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "volume": buy + sell,
    }


async def feed(agg, strike, side, payload):
    await agg.update_option(
        atm_source=ATM,
        strike=strike,
        option_type=side,
        payload=payload,
        bids=[],
        asks=[],
        price_scale=1.0,
        bucket_id="test",
    )


async def run_scenario(name, ce_ticks, pe_ticks):
    agg = OrderBookAggregator()
    # Feed several ticks so ask/bid "removed" (tick-to-tick decrease) registers.
    for i in range(3):
        for t in ce_ticks:
            await feed(agg, ATM, "CE", t)
        for t in pe_ticks:
            await feed(agg, ATM, "PE", t)
    snap = await agg.snapshot_and_reset(atm_source=ATM)
    d = snap["directional"]
    print(f"\n=== {name} ===")
    print(f"  net_exec       = {d['net_exec']:>12}   (ce={d['ce_exec_delta']} pe={d['pe_exec_delta']})")
    print(f"  net_book_delta = {d['net_book_delta']:>12}")
    print(f"  net_imbalance  = {d['net_imbalance']:>12}")
    print(f"  net_pressure   = {d['net_pressure']:>12}   (bull={d['bullish_pressure']} bear={d['bearish_pressure']})")
    print(f"  exec_delta(out)= {snap['exec_delta']:>12}   book_delta(out)={snap['book_delta']}")
    print(f"  score          = {snap['breakout_score']:>12}   regime={snap['regime']}")
    return snap, d


async def main():
    ok = True

    # Scenario 1: Heavy CE buying + Heavy PE selling -> bullish
    s1, d1 = await run_scenario(
        "S1 Heavy CE buying + PE selling (BULLISH)",
        ce_ticks=[tick(buy=90000, sell=1000, bid=50000, ask=10000)],
        pe_ticks=[tick(buy=1000, sell=90000, bid=10000, ask=50000)],
    )
    ok &= d1["net_exec"] > 0
    ok &= s1["exec_delta"] > 0

    # Scenario 2: Heavy PE buying + Heavy CE selling -> bearish
    s2, d2 = await run_scenario(
        "S2 Heavy PE buying + CE selling (BEARISH)",
        ce_ticks=[tick(buy=1000, sell=90000, bid=10000, ask=50000)],
        pe_ticks=[tick(buy=90000, sell=1000, bid=50000, ask=10000)],
    )
    ok &= d2["net_exec"] < 0
    ok &= s2["exec_delta"] < 0
    ok &= s2["regime"] in ("RANGE", "LOADING")

    # Scenario 3: CE and PE equally strong -> near zero
    s3, d3 = await run_scenario(
        "S3 CE == PE (NEUTRAL)",
        ce_ticks=[tick(buy=50000, sell=50000, bid=30000, ask=30000)],
        pe_ticks=[tick(buy=50000, sell=50000, bid=30000, ask=30000)],
    )
    ok &= abs(d3["net_exec"]) < 1e-6
    ok &= abs(d3["net_pressure"]) < 1e-6

    # Scenario 4: Market falls sharply -> net_exec negative, not positive
    s4, d4 = await run_scenario(
        "S4 Sharp fall: PE buying dominates (net_exec NEGATIVE)",
        ce_ticks=[tick(buy=5000, sell=60000, bid=8000, ask=40000)],
        pe_ticks=[tick(buy=120000, sell=2000, bid=70000, ask=9000)],
    )
    ok &= d4["net_exec"] < 0
    ok &= s4["exec_delta"] < 0

    print("\n" + "=" * 50)
    print("RESULT:", "ALL PASS ✅" if ok else "FAIL ❌")
    print("=" * 50)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
