"""One-shot historical_data() fetch, optionally diffed against the live app.

Usage (from repo root, market hours):

    python scripts/compare_historical_live.py
    python scripts/compare_historical_live.py --live-url http://127.0.0.1:8000
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta

sys.path.insert(0, ".")

from app.core.config import settings
from app.core.env_loader import load_project_env

load_project_env(".")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Nubra historical 3m bars and compare to live")
    parser.add_argument(
        "--live-url",
        default="",
        help="Base URL of the running nubra-live app (uses /realtime/candles/last)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full compare report as JSON",
    )
    return parser.parse_args()


async def _load_live_candle(base_url: str) -> dict | None:
    if not base_url:
        return None
    import urllib.error
    import urllib.request

    url = base_url.rstrip("/") + "/realtime/candles/last"

    def _get() -> dict | None:
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            print(f"live candle HTTP {exc.code}: {exc.reason}")
            return None
        except Exception as exc:  # noqa: BLE001
            print(f"live candle fetch failed: {exc}")
            return None

    return await asyncio.to_thread(_get)


async def main() -> None:
    args = _parse_args()
    from app.historical.client import HistoricalClient
    from app.historical.compare import compare_closed_bar
    from app.historical.universe import build_universe
    from app.instruments.manager import InstrumentManager
    from app.realtime.interval_clock import closed_bucket_start, market_tz

    print(f"env={settings.nubra_env} exchange={settings.nubra_exchange}")
    manager = InstrumentManager(
        env_name=settings.nubra_env,
        strike_radius=settings.strike_radius,
    )
    price_scale = float(manager.price_scale)
    client = HistoricalClient(
        env_name=settings.nubra_env,
        exchange=settings.nubra_exchange,
        price_scale=price_scale,
        interval=f"{int(settings.candle_interval_minutes)}m",
    )
    groups = build_universe(
        manager,
        nifty_price=settings.initial_nifty_price,
        exchange=settings.nubra_exchange,
    )
    print("universe:", {g.kind: len(g.symbols) for g in groups})
    fetched = await client.fetch(groups, intra_day=True, real_time=False)
    print(
        f"fetched symbols={len(fetched.frames)} requests={fetched.request_count} "
        f"errors={len(fetched.errors)} alignment={fetched.policy.bar_alignment} "
        f"volume_mode={fetched.policy.volume_mode}"
    )
    for err in fetched.errors[:8]:
        print("  fetch error:", err)

    tz = market_tz(settings.market_timezone)
    now = datetime.now(tz)
    interval = int(settings.candle_interval_minutes)
    bucket_end = closed_bucket_start(now, interval, tz) + timedelta(minutes=interval)
    live = await _load_live_candle(args.live_url)
    if live:
        print(f"live bucket_end={live.get('bucket_end')}")
    else:
        print("no live candle (pass --live-url to compare against the running app)")

    report = compare_closed_bar(
        live=live,
        frames=fetched.frames,
        kinds=fetched.kinds,
        groups=groups,
        bucket_end=bucket_end,
        alignment=fetched.policy.bar_alignment or "close",
        interval_minutes=interval,
        price_abs_tol=settings.historical_price_abs_tol,
        volume_rel_tol=settings.historical_volume_rel_tol,
    )
    summary = report.get("summary") or {}
    print(
        f"HIST_COMPARE | bucket={report.get('bucket_end')} matched={summary.get('matched')} "
        f"mismatched={summary.get('mismatched')} missing_live={summary.get('missing_live')} "
        f"missing_hist={summary.get('missing_hist')} rows={summary.get('rows')}"
    )
    nifty_row = next((r for r in report.get("rows") or [] if r.get("symbol") == "NIFTY"), None)
    if nifty_row:
        print("NIFTY live:", nifty_row.get("live"))
        print("NIFTY hist:", nifty_row.get("hist"))
        print("NIFTY status:", nifty_row.get("status"), nifty_row.get("diffs"))
    mismatches = [r for r in report.get("rows") or [] if r.get("status") not in {"match", "missing_live", "missing_hist"}]
    for row in mismatches[:15]:
        print(f"  {row.get('kind')} {row.get('symbol')} {row.get('status')} {row.get('diffs')}")
    if args.json:
        print(json.dumps(report, default=str, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
