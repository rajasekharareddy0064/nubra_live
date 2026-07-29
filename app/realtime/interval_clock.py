from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def market_tz(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def floor_to_interval(dt: datetime, interval_minutes: int, tz: ZoneInfo) -> datetime:
    """Bucket start in `tz` aligned to clock (e.g. 9:15, 9:18 for 3m)."""
    local = dt.astimezone(tz)
    total_min = local.hour * 60 + local.minute
    floored = (total_min // interval_minutes) * interval_minutes
    h, m = divmod(floored, 60)
    return local.replace(hour=h, minute=m, second=0, microsecond=0)


def closed_bucket_start(now: datetime, interval_minutes: int, tz: ZoneInfo) -> datetime:
    """
    Start timestamp of the interval that just completed at `now`.
    If now == 9:18:00 and interval==3, returns 9:15 (bucket [9:15,9:18)).
    """
    current_start = floor_to_interval(now, interval_minutes, tz)
    return current_start - timedelta(minutes=interval_minutes)


def seconds_until_next_boundary(now: datetime, interval_minutes: int, tz: ZoneInfo) -> float:
    current_start = floor_to_interval(now, interval_minutes, tz)
    next_start = current_start + timedelta(minutes=interval_minutes)
    delta = (next_start - now.astimezone(tz)).total_seconds()
    # Small post-boundary buffer so asyncio.sleep() undershoot does not wake
    # ~1ms early and cause closed_bucket_start to target the previous bar.
    return max(0.05, delta + 0.05)


def next_interval_boundary(now: datetime, interval_minutes: int, tz: ZoneInfo) -> datetime:
    """Wall-clock time when the current interval bucket closes."""
    current_start = floor_to_interval(now, interval_minutes, tz)
    return current_start + timedelta(minutes=interval_minutes)
