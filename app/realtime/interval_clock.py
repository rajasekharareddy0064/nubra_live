from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

# NSE cash / F&O regular session. 3m bars are emitted at bucket close, so the
# first written bar is [09:15, 09:18) and the last is [15:27, 15:30).
NSE_SESSION_OPEN = time(9, 15)
NSE_SESSION_CLOSE = time(15, 30)


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


def is_nse_cash_session_bar(bucket_start: datetime, bucket_end: datetime, tz: ZoneInfo) -> bool:
    """True if the closed bar lies fully inside 09:15–15:30 IST.

    Skips the 09:12 close ([09:12, 09:15)) and anything after 15:30.
    """
    start = bucket_start.astimezone(tz) if bucket_start.tzinfo else bucket_start.replace(tzinfo=tz)
    end = bucket_end.astimezone(tz) if bucket_end.tzinfo else bucket_end.replace(tzinfo=tz)
    open_dt = start.replace(
        hour=NSE_SESSION_OPEN.hour,
        minute=NSE_SESSION_OPEN.minute,
        second=0,
        microsecond=0,
    )
    close_dt = start.replace(
        hour=NSE_SESSION_CLOSE.hour,
        minute=NSE_SESSION_CLOSE.minute,
        second=0,
        microsecond=0,
    )
    return start >= open_dt and end <= close_dt


def is_nse_session_close_label(dt: datetime, *, interval_minutes: int = 3) -> bool:
    """True if a bar-close clock time is a regular-session close (09:18..15:30)."""
    minutes = int(dt.hour) * 60 + int(dt.minute)
    first_close = NSE_SESSION_OPEN.hour * 60 + NSE_SESSION_OPEN.minute + int(interval_minutes)
    last_close = NSE_SESSION_CLOSE.hour * 60 + NSE_SESSION_CLOSE.minute
    return first_close <= minutes <= last_close
