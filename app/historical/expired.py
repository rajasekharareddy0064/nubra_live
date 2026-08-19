"""Nubra expired-option and monthly FUT trading-symbol helpers.

Symbol rules: https://nubra.io/products/api/docs/guides/ExpiredOptions/
"""
from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Literal

MONTH_ABB = (
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
)
OCT_DEC_WEEKLY = {10: "O", 11: "N", 12: "D"}
ABB_TO_MONTH = {name: i + 1 for i, name in enumerate(MONTH_ABB)}


def last_weekday(year: int, month: int, weekday: int) -> date:
    """Last ``weekday`` (Mon=0 … Sun=6) of the month — NSE monthly F&O is Thursday."""
    last_day = calendar.monthrange(year, month)[1]
    d = date(year, month, last_day)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


def last_thursday(year: int, month: int) -> date:
    return last_weekday(year, month, 3)


def next_tuesday(day: date) -> date:
    """NIFTY weekly expiry (Tue). Same day if ``day`` is already Tuesday."""
    return day + timedelta(days=(1 - day.weekday()) % 7)


def iter_weekdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def iter_tuesdays(start: date, end: date):
    d = start
    d = next_tuesday(d)
    while d <= end:
        yield d
        d += timedelta(days=7)


def monthly_expiries_covering(start: date, end: date, *, extra_months: int = 1) -> list[date]:
    """Monthly (last-Thursday) expiries from the month before ``start`` through ``end`` + extra."""
    y, m = start.year, start.month
    if m == 1:
        y, m = y - 1, 12
    else:
        m -= 1
    ey, em = end.year, end.month
    for _ in range(max(0, extra_months)):
        if em == 12:
            ey, em = ey + 1, 1
        else:
            em += 1
    out: list[date] = []
    while (y, m) <= (ey, em):
        out.append(last_thursday(y, m))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def nubra_option_symbol(
    expiry: date,
    strike: int,
    side: str,
    *,
    weekly: bool = True,
    underlying: str = "NIFTY",
) -> str:
    """Trading symbol for ``historical_data(type=OPT)``, including expired contracts."""
    yy = int(expiry.year) % 100
    side_u = str(side).strip().upper()
    under = str(underlying).strip().upper() or "NIFTY"
    strike_i = int(strike)
    if not weekly:
        return f"{under}{yy:02d}{MONTH_ABB[expiry.month - 1]}{strike_i}{side_u}"
    if expiry.month >= 10:
        letter = OCT_DEC_WEEKLY[expiry.month]
        return f"{under}{yy:02d}{letter}{expiry.day:02d}{strike_i}{side_u}"
    return f"{under}{yy}{expiry.month}{expiry.day:02d}{strike_i}{side_u}"


def grow_option_symbol(expiry: date, strike: int, side: str, *, underlying: str = "NIFTY") -> str:
    """Match live ``DBWriter._grow_option_symbol`` (e.g. ``NIFTY2672124550CE``)."""
    yy = int(expiry.year) % 100
    return (
        f"{str(underlying).strip().upper()}{yy}{int(expiry.month)}"
        f"{int(expiry.day)}{int(strike)}{str(side).strip().upper()}"
    )


def fut_trading_symbol(asset: str, expiry: date) -> str:
    """Monthly FUT trading symbol, e.g. ``NIFTY26JULFUT``, ``TMPV26JULFUT``."""
    yy = int(expiry.year) % 100
    mon = MONTH_ABB[expiry.month - 1]
    return f"{str(asset).strip().upper()}{yy:02d}{mon}FUT"


def parse_fut_symbol(symbol: str) -> tuple[str, date] | None:
    text = str(symbol).strip().upper()
    if not text.endswith("FUT") or len(text) < 8:
        return None
    body = text[:-3]
    if len(body) < 5:
        return None
    mon = body[-3:]
    month = ABB_TO_MONTH.get(mon)
    if month is None:
        return None
    yy_s = body[-5:-3]
    if not yy_s.isdigit():
        return None
    asset = body[:-5]
    if not asset:
        return None
    year = 2000 + int(yy_s)
    return asset, last_thursday(year, month)


def round_strike(price: float, step: int = 50) -> int:
    return int(round(float(price) / step) * step)


def strike_ladder(low: float, high: float, *, radius: int, step: int = 50) -> list[int]:
    """ATM±radius around the day's range so a moving ATM stays covered."""
    lo = round_strike(low, step) - radius * step
    hi = round_strike(high, step) + radius * step
    if lo > hi:
        lo, hi = hi, lo
    return list(range(lo, hi + step, step))


Kind = Literal["weekly", "monthly"]


def option_expiries_for_session(day: date) -> list[tuple[date, Kind]]:
    """Front weekly (next Tuesday) plus the still-live monthly, if any."""
    weekly = next_tuesday(day)
    monthly = last_thursday(day.year, day.month)
    out: list[tuple[date, Kind]] = [(weekly, "weekly")]
    if monthly >= day:
        out.append((monthly, "monthly"))
    return out
