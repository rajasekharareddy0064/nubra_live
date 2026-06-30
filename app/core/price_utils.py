"""
Global price normalization utility for Nubra feed.

Nubra PROD sends prices in PAISE (price_scale = 100).
All internal pipeline values and DB inserts use RUPEES.

Rule:
  - If a value >= 100_000, it is almost certainly in paise → divide by scale.
  - If a value < 100_000, it is already in rupees → return as-is.
  - Threshold of 100_000 covers NIFTY (min ~15,000 rupees = 1,500,000 paise)
    without accidentally converting small option premium values (e.g. ₹50).

This is the ONE place conversion happens. Never divide elsewhere.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Wire threshold: values >= this are treated as paise and divided by scale.
# NIFTY index minimum is ~15,000 → in paise = 1,500,000 (>> 100_000).
# Highest possible small-value option premium in rupees = ~9,999 (< 100_000).
_PAISE_THRESHOLD: float = 100_000.0


def normalize_price(
    value: Any,
    *,
    scale: float,
    module: str = "",
    debug: bool = False,
) -> float:
    """Convert a Nubra feed price to rupees, exactly once.

    Parameters
    ----------
    value  : raw feed value (paise or rupees)
    scale  : price_scale from InstrumentManager (100 on PROD, 1 on UAT)
    module : caller name for debug logs
    debug  : if True, emit conversion log

    Returns
    -------
    float price in RUPEES
    """
    if value is None:
        return 0.0
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0

    if scale <= 1.0:
        # UAT or uninitialized — no conversion needed
        if debug:
            logger.debug(
                "normalize_price | module=%s original=%s detected=RUPEES scale=%s converted=%s",
                module, f, scale, f,
            )
        return f

    # Detect unit: large values are paise, small values are already rupees
    if f >= _PAISE_THRESHOLD:
        result = f / scale
        if debug:
            logger.debug(
                "normalize_price | module=%s original=%s detected=PAISE scale=%s converted=%s",
                module, f, scale, result,
            )
        return result
    else:
        # Already in rupees (e.g. option premium of ₹150, or value already converted)
        if debug:
            logger.debug(
                "normalize_price | module=%s original=%s detected=RUPEES(small) scale=%s converted=%s",
                module, f, scale, f,
            )
        return f


def is_paise(value: Any, *, scale: float) -> bool:
    """Return True if value is likely in paise domain."""
    if scale <= 1.0:
        return False
    try:
        return float(value) >= _PAISE_THRESHOLD
    except (TypeError, ValueError):
        return False
