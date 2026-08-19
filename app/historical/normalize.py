"""Convert Nubra historical series to live-pipeline units (IST, rupees, interval volume)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from app.core.price_utils import normalize_price

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
_OPT_PAISE_THRESHOLD = 500.0
PRICE_COLS = ("open", "high", "low", "close", "l1bid", "l1ask")
GREEK_COLS = ("delta", "gamma", "theta", "vega", "iv_mid")


@dataclass
class NormalizationPolicy:
    """First-fetch calibration so we do not invent false mismatches."""

    bar_alignment: str = "close"  # "open" | "close" vs live bucket_end
    volume_mode: str = "cumulative_diff"  # "tick_volume" | "cumulative_diff"
    probed: bool = False
    notes: list[str] = field(default_factory=list)


def opt_price_rupees(value: Any) -> float | None:
    """Option premiums: paise if ``>= 500``, else already rupees (Grow/live rule)."""
    if value is None:
        return None
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if price != price:  # NaN
        return None
    if price <= 0:
        return None
    if price >= _OPT_PAISE_THRESHOLD:
        return price / 100.0
    return price


def ns_to_ist(timestamp_ns: Any) -> pd.Timestamp | None:
    """Nanosecond unix timestamp → timezone-aware IST pandas Timestamp."""
    if timestamp_ns is None:
        return None
    try:
        ts = pd.to_datetime(int(timestamp_ns), unit="ns", utc=True)
    except (TypeError, ValueError, OverflowError):
        return None
    if pd.isna(ts):
        return None
    return ts.tz_convert(IST)


def infer_bar_alignment(index: pd.DatetimeIndex, interval_minutes: int = 3) -> str:
    """Guess whether historical timestamps are bar open or bar close.

    NSE cash session starts 09:15 IST. A first stamp of 09:15 means open;
    09:18 means close (matching live ``bucket_end``).
    """
    if index is None or len(index) == 0:
        return "close"
    local = index
    if local.tz is None:
        local = local.tz_localize(IST)
    else:
        local = local.tz_convert(IST)
    session = local[(local.hour > 9) | ((local.hour == 9) & (local.minute >= 15))]
    if len(session) == 0:
        return "close"
    first = session[0]
    if first.hour == 9 and first.minute == 15:
        return "open"
    if first.hour == 9 and first.minute == 18:
        return "close"
    # Fall back to live convention (bars labeled by close time).
    _ = interval_minutes
    return "close"


def to_bucket_end_index(
    df: pd.DataFrame,
    *,
    alignment: str,
    interval_minutes: int,
) -> pd.DataFrame:
    """Rebase a historical frame index to naive IST bar-close minutes (live DB convention)."""
    if df is None or df.empty:
        return df
    out = df.copy()
    idx = out.index
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.to_datetime(idx)
        out.index = idx
    if idx.tz is None:
        idx = idx.tz_localize(IST)
    else:
        idx = idx.tz_convert(IST)
    idx = idx.floor("min")
    if alignment == "open":
        idx = idx + pd.Timedelta(minutes=int(interval_minutes))
    out.index = pd.DatetimeIndex(idx.tz_localize(None), name="timestamp")
    out = out[~out.index.duplicated(keep="last")]
    out.sort_index(inplace=True)
    return out


def target_timestamp(bucket_end: datetime, alignment: str, interval_minutes: int) -> datetime:
    """Historical bar timestamp that should match a live closed bucket."""
    end = bucket_end
    if end.tzinfo is None:
        end = end.replace(tzinfo=IST)
    else:
        end = end.astimezone(IST)
    if alignment == "open":
        return end - timedelta(minutes=interval_minutes)
    return end


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:
        return None
    return f


def convert_price(value: Any, *, instrument_type: str, price_scale: float) -> float | None:
    raw = _to_float(value)
    if raw is None:
        return None
    kind = str(instrument_type or "").upper()
    if kind == "OPT":
        return opt_price_rupees(raw)
    converted = normalize_price(
        raw, scale=float(price_scale) or 1.0, kind=kind, module="historical"
    )
    return converted


def interval_volume(cum: pd.Series, tick: pd.Series | None) -> tuple[pd.Series, str]:
    """Prefer ``tick_volume`` when present; otherwise ``diff`` cumulative_volume."""
    if tick is not None and tick.notna().any():
        filled = tick.astype(float)
        if (filled.fillna(0) > 0).any():
            return filled.fillna(0.0), "tick_volume"
    cum_f = cum.astype(float)
    vol = cum_f.diff().copy()
    if len(vol) and (pd.isna(vol.iloc[0]) or float(vol.iloc[0]) < 0):
        first = float(cum_f.iloc[0]) if pd.notna(cum_f.iloc[0]) else 0.0
        vol.iloc[0] = first
    vol = vol.clip(lower=0.0).fillna(0.0)
    return vol, "cumulative_diff"


def normalize_frame(
    df: pd.DataFrame,
    *,
    instrument_type: str,
    price_scale: float,
    policy: NormalizationPolicy | None = None,
) -> pd.DataFrame:
    """Scale prices, derive interval volume, keep cum_volume / oi as point-in-time."""
    if df is None or df.empty:
        return df
    out = df.copy()
    out.sort_index(inplace=True)
    kind = str(instrument_type or "").upper()
    for col in PRICE_COLS:
        if col not in out.columns:
            continue
        out[col] = out[col].map(lambda v, t=kind: convert_price(v, instrument_type=t, price_scale=price_scale))

    tick = out["tick_volume"] if "tick_volume" in out.columns else None
    if "cum_volume" in out.columns:
        vol, mode = interval_volume(out["cum_volume"], tick)
        out["volume"] = vol
        if policy is not None and not policy.probed:
            policy.volume_mode = mode
    elif tick is not None:
        out["volume"] = tick.astype(float).fillna(0.0)
        if policy is not None and not policy.probed:
            policy.volume_mode = "tick_volume"
    else:
        out["volume"] = 0.0

    if "oi" in out.columns:
        out["oi"] = out["oi"].map(_to_float)
    for col in GREEK_COLS:
        if col in out.columns:
            out[col] = out[col].map(_to_float)
    return out


def probe_policy(
    frames: dict[str, pd.DataFrame],
    *,
    interval_minutes: int,
    price_scale: float,
) -> NormalizationPolicy:
    """One-time log of timestamp alignment, price units, and volume mode."""
    policy = NormalizationPolicy()
    nifty = frames.get("NIFTY")
    sample = nifty if nifty is not None and not nifty.empty else None
    if sample is None:
        for df in frames.values():
            if df is not None and not df.empty:
                sample = df
                break
    if sample is None or sample.empty:
        policy.notes.append("no historical rows to probe")
        policy.probed = True
        return policy

    policy.bar_alignment = infer_bar_alignment(sample.index, interval_minutes)
    close = sample["close"].dropna() if "close" in sample.columns else pd.Series(dtype=float)
    raw_note = ""
    if not close.empty:
        last_close = float(close.iloc[-1])
        if last_close >= 100_000:
            raw_note = f"close={last_close} still looks like paise after scale={price_scale}"
            policy.notes.append(raw_note)
        else:
            policy.notes.append(f"close={last_close} rupees scale={price_scale}")
    vol_col = "tick_volume" if "tick_volume" in sample.columns and sample["tick_volume"].notna().any() else "cum_volume"
    if vol_col == "tick_volume":
        policy.volume_mode = "tick_volume"
    else:
        policy.volume_mode = "cumulative_diff"
    first_ts = sample.index[0]
    policy.notes.append(f"first_ts={first_ts} alignment={policy.bar_alignment} volume_mode={policy.volume_mode}")
    policy.probed = True
    logger.info("HIST_PROBE | %s", " | ".join(policy.notes))
    return policy


def pick_bar(
    df: pd.DataFrame,
    bucket_end: datetime,
    *,
    alignment: str,
    interval_minutes: int,
) -> pd.Series | None:
    """Return the historical row for the just-closed live bucket, or None."""
    if df is None or df.empty:
        return None
    target = target_timestamp(bucket_end, alignment, interval_minutes)
    target_ts = pd.Timestamp(target)
    if target_ts.tzinfo is None:
        target_ts = target_ts.tz_localize(IST)
    idx = df.index
    if idx.tz is None:
        idx = idx.tz_localize(IST)
        df = df.copy()
        df.index = idx
    # Exact second match first, then same minute, then nearest within 2 seconds.
    try:
        row = df.loc[target_ts]
        if isinstance(row, pd.DataFrame):
            return row.iloc[-1]
        return row
    except KeyError:
        pass
    if idx.tz is not None and target_ts.tzinfo is not None:
        target_ts = target_ts.tz_convert(idx.tz)
    minute_hits = idx.floor("min") == target_ts.floor("min")
    if bool(minute_hits.any()):
        positions = minute_hits.to_numpy().nonzero()[0]
        return df.iloc[int(positions[-1])]
    deltas = abs(idx.asi8 - int(target_ts.value))
    best_i = int(deltas.argmin())
    if int(deltas[best_i]) <= 2_000_000_000:  # 2 seconds in ns
        return df.iloc[best_i]
    return None
