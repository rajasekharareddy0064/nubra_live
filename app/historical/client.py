"""Batched, rate-limited wrapper around Nubra ``MarketData.historical_data()``."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas as pd

from app.historical.normalize import (
    IST,
    NormalizationPolicy,
    normalize_frame,
    ns_to_ist,
    probe_policy,
)
from app.historical.universe import UniverseGroup

logger = logging.getLogger(__name__)

BATCH_SIZE = 5
RATE_LIMIT_PER_MINUTE = 50
MAX_RETRIES = 4

SERIES_ATTRS: dict[str, str] = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "cumulative_volume": "cum_volume",
    "tick_volume": "tick_volume",
    "cumulative_oi": "oi",
    "l1bid": "l1bid",
    "l1ask": "l1ask",
    "theta": "theta",
    "delta": "delta",
    "gamma": "gamma",
    "vega": "vega",
    "iv_mid": "iv_mid",
}


class RateLimiter:
    """Simple sliding-window limiter (historical quota is 60 req/min)."""

    def __init__(self, max_calls: int = RATE_LIMIT_PER_MINUTE, period_s: float = 60.0) -> None:
        self._max = max(1, int(max_calls))
        self._period = float(period_s)
        self._times: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                while self._times and now - self._times[0] >= self._period:
                    self._times.popleft()
                if len(self._times) < self._max:
                    self._times.append(now)
                    return
                wait = self._period - (now - self._times[0]) + 0.05
                await asyncio.sleep(max(0.05, wait))


@dataclass
class HistoricalFetch:
    frames: dict[str, pd.DataFrame] = field(default_factory=dict)
    kinds: dict[str, str] = field(default_factory=dict)
    policy: NormalizationPolicy = field(default_factory=NormalizationPolicy)
    errors: list[str] = field(default_factory=list)
    request_count: int = 0


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _interval_minutes(interval: str) -> int:
    text = str(interval or "3m").strip().lower()
    if text.endswith("m") and text[:-1].isdigit():
        return max(1, int(text[:-1]))
    if text.isdigit():
        return max(1, int(text))
    return 3


def _drop_invalid_field(fields: list[str], error_text: str) -> list[str] | None:
    """If the API rejected a field name, return fields without it."""
    import re

    text = str(error_text)
    match = re.search(r"invalid field\s+(\w+)", text, re.IGNORECASE)
    if not match:
        match = re.search(r"field\s+(\w+)\s+not found", text, re.IGNORECASE)
    if not match:
        return None
    bad = match.group(1).strip()
    cleaned = [f for f in fields if f != bad]
    if len(cleaned) == len(fields) or not cleaned:
        return None
    return cleaned


def _utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def session_window_utc(now: datetime | None = None) -> tuple[str, str]:
    """NSE cash session 09:15 IST → now (UTC ISO strings)."""
    local_now = now.astimezone(IST) if now is not None else datetime.now(IST)
    start = local_now.replace(hour=9, minute=15, second=0, microsecond=0)
    if local_now < start:
        start = start - timedelta(days=1)
    return _utc_iso(start), _utc_iso(local_now)


def _points(series: Any) -> list[tuple[int, Any]]:
    if series is None:
        return []
    try:
        iterable = list(series)
    except TypeError:
        return []
    out: list[tuple[int, Any]] = []
    for point in iterable:
        ts = getattr(point, "timestamp", None)
        val = getattr(point, "value", None)
        if ts is None and isinstance(point, dict):
            ts = point.get("timestamp")
            val = point.get("value")
        if ts is None:
            continue
        try:
            out.append((int(ts), val))
        except (TypeError, ValueError):
            continue
    return out


def chart_to_frame(chart: Any) -> pd.DataFrame:
    columns: dict[str, pd.Series] = {}
    for attr, col in SERIES_ATTRS.items():
        series = getattr(chart, attr, None)
        if series is None and isinstance(chart, dict):
            series = chart.get(attr)
        pts = _points(series)
        if not pts:
            continue
        ist_index = [ns_to_ist(ts) for ts, _ in pts]
        values = [val for _, val in pts]
        valid = [(i, v) for i, v in zip(ist_index, values) if i is not None]
        if not valid:
            continue
        idx = pd.DatetimeIndex([i for i, _ in valid], tz=IST)
        columns[col] = pd.Series([v for _, v in valid], index=idx)
    if not columns:
        return pd.DataFrame()
    df = pd.DataFrame(columns)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep="last")]
    return df


def parse_response(result: Any) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    payload = getattr(result, "result", None)
    if payload is None and isinstance(result, dict):
        payload = result.get("result")
    if not payload:
        return frames
    for chart_data in payload:
        values = getattr(chart_data, "values", None)
        if values is None and isinstance(chart_data, dict):
            values = chart_data.get("values")
        if not values:
            continue
        for item in values:
            if not isinstance(item, dict):
                continue
            for symbol, chart in item.items():
                sym = str(symbol).strip().upper()
                if not sym:
                    continue
                frames[sym] = chart_to_frame(chart)
    return frames


def _is_rate_limited(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "429" in text or "rate limit" in text or "too many" in text


class HistoricalClient:
    """Fetch historical OHLC/analytics for the live universe."""

    def __init__(
        self,
        *,
        env_name: str = "UAT",
        exchange: str = "NSE",
        price_scale: float = 100.0,
        interval: str = "3m",
        sdk_factory: Callable[[], Any] | None = None,
        rate_limit: int = RATE_LIMIT_PER_MINUTE,
    ) -> None:
        self._env_name = env_name
        self._exchange = str(exchange or "NSE").upper()
        self._price_scale = float(price_scale) or 1.0
        self._interval = interval
        self._sdk_factory = sdk_factory
        self._limiter = RateLimiter(max_calls=rate_limit)
        self._md: Any = None
        self.policy = NormalizationPolicy()

    def _market_data(self) -> Any:
        if self._md is not None:
            return self._md
        if self._sdk_factory is not None:
            self._md = self._sdk_factory()
            return self._md
        from nubra_python_sdk.marketdata.market_data import MarketData

        from app.ingestion.auth_client import get_authenticated_client

        client = get_authenticated_client(env_name=self._env_name, skip_refdata=True)
        self._md = MarketData(client)
        return self._md

    def _call_sync(self, request: dict[str, Any]) -> Any:
        return self._market_data().historical_data(request)

    async def _call_with_retry(self, request: dict[str, Any]) -> Any:
        last_exc: BaseException | None = None
        for attempt in range(MAX_RETRIES):
            await self._limiter.acquire()
            try:
                return await asyncio.to_thread(self._call_sync, request)
            except Exception as exc:  # noqa: BLE001 — SDK errors are opaque
                last_exc = exc
                if _is_rate_limited(exc) and attempt < MAX_RETRIES - 1:
                    wait = 2 ** attempt
                    logger.warning("historical_data rate-limited; retry in %ss: %s", wait, exc)
                    await asyncio.sleep(wait)
                    continue
                raise
        raise last_exc or RuntimeError("historical_data failed")

    async def fetch(
        self,
        groups: list[UniverseGroup],
        *,
        interval: str | None = None,
        intra_day: bool = True,
        start_date: str | None = None,
        end_date: str | None = None,
        real_time: bool = False,
        progress: bool = False,
    ) -> HistoricalFetch:
        interval = interval or self._interval
        if not start_date or not end_date:
            start_date, end_date = session_window_utc()
        out = HistoricalFetch(policy=self.policy)
        total_batches = sum(
            (len(group.symbols) + BATCH_SIZE - 1) // BATCH_SIZE for group in groups
        )
        batch_idx = 0
        for group in groups:
            for batch in _chunks(group.symbols, BATCH_SIZE):
                batch_idx += 1
                if progress:
                    lead = batch[0] if len(batch) == 1 else f"{batch[0]}…+{len(batch) - 1}"
                    print(
                        f"  {group.kind} batch {batch_idx}/{total_batches} ({lead})",
                        flush=True,
                    )
                request = {
                    "exchange": group.exchange or self._exchange,
                    "type": group.kind,
                    "values": batch,
                    "fields": list(group.fields),
                    "startDate": start_date,
                    "endDate": end_date,
                    "interval": interval,
                    "intraDay": bool(intra_day),
                    "realTime": bool(real_time),
                }
                try:
                    raw = await self._call_with_retry(request)
                    out.request_count += 1
                except Exception as exc:  # noqa: BLE001
                    text = str(exc)
                    dropped = _drop_invalid_field(request.get("fields") or [], text)
                    if dropped is not None:
                        request["fields"] = dropped
                        logger.warning("HIST_FETCH_RETRY | type=%s dropped invalid field | %s", group.kind, text)
                        try:
                            raw = await self._call_with_retry(request)
                            out.request_count += 1
                        except Exception as retry_exc:  # noqa: BLE001
                            msg = f"{group.kind} {batch}: {retry_exc}"
                            out.errors.append(msg)
                            logger.warning("HIST_FETCH_FAILED | %s", msg)
                            continue
                    else:
                        msg = f"{group.kind} {batch}: {exc}"
                        out.errors.append(msg)
                        logger.warning("HIST_FETCH_FAILED | %s", msg)
                        continue
                parsed = parse_response(raw)
                for symbol, frame in parsed.items():
                    out.frames[symbol] = normalize_frame(
                        frame,
                        instrument_type=group.kind,
                        price_scale=self._price_scale,
                        policy=out.policy,
                    )
                    out.kinds[symbol] = group.kind
                missing = [s for s in batch if s not in parsed]
                if missing:
                    logger.info("HIST_FETCH | type=%s missing_symbols=%s", group.kind, missing)
        if not self.policy.probed and out.frames:
            self.policy = probe_policy(
                out.frames,
                interval_minutes=_interval_minutes(interval),
                price_scale=self._price_scale,
            )
            out.policy = self.policy
        return out
