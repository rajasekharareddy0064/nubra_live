"""
Global price normalization utility for Nubra feed.

Nubra PROD sends prices in PAISE (price_scale = 100).
All internal pipeline values and DB inserts use RUPEES.

Kind-aware rules (PROD, scale=100):
  - STOCK / FUT: always divide by scale. Equities under ~₹1,000 arrive as
    e.g. 72900 paise — the 100_000 INDEX heuristic would leave them in paise.
  - INDEX: values >= 100_000 are paise (NIFTY ~15,000 rupees = 1,500,000 paise).
  - OPT: values >= 500 are paise; smaller values are already rupee premiums.

This is the ONE place conversion happens. Call it at the wire ingest edge
only — do not run STOCK/FUT conversion again on already-rupee candles.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# INDEX wire threshold: values >= this are treated as paise and divided by scale.
# NIFTY index minimum is ~15,000 → in paise = 1,500,000 (>> 100_000).
_PAISE_THRESHOLD: float = 100_000.0
# Option premiums: ₹5 in paise = 500. Live/Grow already-rupee premiums stay below this.
_OPT_PAISE_THRESHOLD: float = 500.0

_STOCK_FUT_KINDS = {"STOCK", "FUT", "EQ", "STOCK_FUT", "STOCK_SPOT"}
_OPT_KINDS = {"OPT", "OPTION"}


def normalize_price(
    value: Any,
    *,
    scale: float,
    kind: str = "",
    module: str = "",
    debug: bool = False,
) -> float:
    """Convert a Nubra feed price to rupees, exactly once.

    Parameters
    ----------
    value  : raw feed value (paise or rupees)
    scale  : price_scale from InstrumentManager (100 on PROD, 1 on UAT)
    kind   : INDEX | STOCK | FUT | OPT — selects the conversion heuristic
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
                "normalize_price | module=%s kind=%s original=%s detected=RUPEES scale=%s converted=%s",
                module, kind, f, scale, f,
            )
        return f

    kind_u = str(kind or "").upper()
    if kind_u in _STOCK_FUT_KINDS:
        result = f / scale
        if debug:
            logger.debug(
                "normalize_price | module=%s kind=%s original=%s detected=PAISE(always) scale=%s converted=%s",
                module, kind_u, f, scale, result,
            )
        return result

    threshold = _OPT_PAISE_THRESHOLD if kind_u in _OPT_KINDS else _PAISE_THRESHOLD
    if f >= threshold:
        result = f / scale
        if debug:
            logger.debug(
                "normalize_price | module=%s kind=%s original=%s detected=PAISE scale=%s converted=%s",
                module, kind_u, f, scale, result,
            )
        return result

    if debug:
        logger.debug(
            "normalize_price | module=%s kind=%s original=%s detected=RUPEES(small) scale=%s converted=%s",
            module, kind_u, f, scale, f,
        )
    return f


def is_paise(value: Any, *, scale: float, kind: str = "") -> bool:
    """Return True if value is likely in paise domain."""
    if scale <= 1.0:
        return False
    kind_u = str(kind or "").upper()
    if kind_u in _STOCK_FUT_KINDS:
        return True
    try:
        threshold = _OPT_PAISE_THRESHOLD if kind_u in _OPT_KINDS else _PAISE_THRESHOLD
        return float(value) >= threshold
    except (TypeError, ValueError):
        return False
