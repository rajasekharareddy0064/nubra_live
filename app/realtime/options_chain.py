"""
Real-time NIFTY option-chain processor.

Reconstructs an 11-strike option chain (5 ITM + 1 ATM + 5 OTM) from the
``options_by_strike`` dict that :class:`app.realtime.market_state.MarketStateStore`
keeps populated by per-ref orderbook + greeks ticks. Computes the
ML-friendly metrics used downstream (PCR, OI skew, max-OI strikes,
ATM CE/PE LTP, …) and provides a small throttled cache so the chain is
rebuilt at most every ``THROTTLE_MS`` milliseconds — *or* immediately
when the ATM strike rolls.

Public surface (matches the spec in the project brief):

* :func:`get_atm_strike` — round spot to the nearest 50.
* :func:`get_strike_range` — 11 strikes around ATM.
* :func:`build_option_chain` — reconstruct an ordered chain from
  ``options_by_strike``.
* :func:`compute_option_metrics` — aggregate ML metrics over a chain.
* :func:`update_candle_options` — stamp a chain + metrics into a
  ``candle_3m`` dict.
* :class:`OptionsChainBuilder` — stateful wrapper that throttles
  :func:`build_option_chain` and caches the latest result so multiple
  consumers (per-tick pipeline, REST endpoint, 3-minute scheduler) can
  read the same in-memory snapshot without re-aggregating.

Design notes
------------
* The ``options_by_strike`` dict is owned by the realtime pipeline and
  mutated from the asyncio loop only, so we read it without locks. We
  do shallow-copy the per-leg dicts when emitting the chain rows so
  downstream consumers (LiveHub broadcast, candle scheduler) don't see
  the dict change under their feet.
* ``step`` is hard-coded to 50 (NIFTY's listed strike spacing). If
  Nubra ever switches the contract spec, change ``STRIKE_STEP`` here
  and in :class:`app.instruments.manager.InstrumentManager`.
* Strike radius for the **chain view** (5) is intentionally narrower
  than the manager's **subscription radius** (default 15 → 31 strikes)
  — we subscribe to a wider window so we have data covering ATM
  drift, but only present the ATM +/- 5 slice to consumers.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

#: NIFTY listed strike spacing.
STRIKE_STEP: int = 50

#: How many strikes either side of ATM the chain view exposes.
#: 5 -> 5 ITM + 1 ATM + 5 OTM = 11 strikes.
STRIKE_RADIUS: int = 5

#: Minimum interval between chain rebuilds when the ATM hasn't moved.
#: Sub-second so the WebSocket / REST consumers see fresh OI as it
#: arrives, but coarse enough that a 1 kHz tick burst doesn't melt the
#: aggregator.
THROTTLE_MS: int = 500


__all__ = [
    "STRIKE_STEP",
    "STRIKE_RADIUS",
    "THROTTLE_MS",
    "get_atm_strike",
    "get_strike_range",
    "build_option_chain",
    "compute_option_metrics",
    "update_candle_options",
    "OptionsChainBuilder",
]


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _is_valid_spot(spot: Any) -> bool:
    """Return ``True`` if ``spot`` is a finite positive number."""
    if spot is None:
        return False
    try:
        f = float(spot)
    except (TypeError, ValueError):
        return False
    return f > 0.0 and f == f and f != float("inf") and f != float("-inf")


def _f(x: Any) -> float:
    """Coerce to float, returning 0.0 for None / unparseable."""
    if x is None:
        return 0.0
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _opt_oi(leg: dict[str, Any] | None) -> float:
    """Pull OI from a leg dict, tolerating both naming conventions."""
    if not leg:
        return 0.0
    return _f(leg.get("open_interest") if leg.get("open_interest") is not None else leg.get("oi"))


def _opt_ltp(leg: dict[str, Any] | None) -> Optional[float]:
    if not leg:
        return None
    val = leg.get("ltp")
    if val is None:
        val = leg.get("last_price")
    return None if val is None else _f(val)


# ---------------------------------------------------------------------------
# Public API: pure functions
# ---------------------------------------------------------------------------


def get_atm_strike(spot: float | int | None, *, step: int = STRIKE_STEP) -> Optional[int]:
    """Round ``spot`` to the nearest ``step`` (default 50).

    Returns ``None`` for missing / invalid spot so callers can
    short-circuit cleanly during the brief startup window before the
    first NIFTY index tick arrives.
    """
    if not _is_valid_spot(spot):
        return None
    return int(round(float(spot) / step) * step)  # type: ignore[arg-type]


def get_strike_range(
    atm: int,
    *,
    step: int = STRIKE_STEP,
    radius: int = STRIKE_RADIUS,
) -> list[int]:
    """Return ``2*radius + 1`` strikes centred on ``atm``, ascending."""
    return [int(atm + i * step) for i in range(-radius, radius + 1)]


def build_option_chain(
    options_by_strike: dict[int | str, dict[str, dict[str, Any]]],
    spot: float | int | None,
    *,
    step: int = STRIKE_STEP,
    radius: int = STRIKE_RADIUS,
) -> tuple[list[dict[str, Any]], Optional[int]]:
    """Reconstruct an ordered NIFTY option chain around ATM.

    Parameters
    ----------
    options_by_strike
        Live per-strike dict maintained by the ingestion pipeline.
        Strike keys may be ``int`` or ``str`` depending on which
        callsite you're reading from — both are accepted.
    spot
        Latest NIFTY spot price. ``None`` / ``<= 0`` returns ``([], None)``.
    step, radius
        Override the hard-coded NIFTY defaults if you ever need a
        different chain shape (e.g. BANKNIFTY uses step=100).

    Returns
    -------
    (chain_rows, atm_strike)
        ``chain_rows`` is a list of ``{"strike", "CE", "PE", "is_atm",
        "distance"}`` dicts in ascending strike order, **omitting any
        strike where neither CE nor PE has data yet**. ``atm_strike``
        is the rounded ATM (or ``None`` if spot was invalid).
    """
    atm = get_atm_strike(spot, step=step)
    if atm is None:
        return [], None

    spot_f = float(spot)  # type: ignore[arg-type]
    strikes = get_strike_range(atm, step=step, radius=radius)

    chain: list[dict[str, Any]] = []
    for strike in strikes:
        # Tolerate both int and str keys (different stages of the
        # pipeline serialize differently).
        leg = options_by_strike.get(strike)
        if leg is None:
            leg = options_by_strike.get(str(strike))  # type: ignore[arg-type]
        if not leg:
            continue
        ce = leg.get("CE")
        pe = leg.get("PE")
        if not ce and not pe:
            continue
        chain.append(
            {
                "strike": int(strike),
                "CE": dict(ce) if isinstance(ce, dict) else None,
                "PE": dict(pe) if isinstance(pe, dict) else None,
                "is_atm": int(strike) == atm,
                "distance": float(int(strike) - spot_f),
            }
        )
    return chain, atm


def compute_option_metrics(chain: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate ML-friendly metrics over a chain.

    Returns a dict with stable keys even when the chain is empty (all
    numeric metrics ``0.0``, all "strike" / "ltp" metrics ``None``)
    so downstream consumers can rely on the schema without conditionals.
    """
    empty = not chain

    total_ce_oi = 0.0
    total_pe_oi = 0.0
    max_ce_oi = -1.0
    max_ce_strike: Optional[int] = None
    max_pe_oi = -1.0
    max_pe_strike: Optional[int] = None
    atm_strike: Optional[int] = None
    atm_ce_ltp: Optional[float] = None
    atm_pe_ltp: Optional[float] = None

    for row in chain:
        try:
            strike = int(row["strike"])
        except (KeyError, TypeError, ValueError):
            continue
        ce = row.get("CE") if isinstance(row.get("CE"), dict) else None
        pe = row.get("PE") if isinstance(row.get("PE"), dict) else None

        ce_oi = _opt_oi(ce)
        pe_oi = _opt_oi(pe)
        total_ce_oi += ce_oi
        total_pe_oi += pe_oi

        if ce_oi > max_ce_oi:
            max_ce_oi = ce_oi
            max_ce_strike = strike
        if pe_oi > max_pe_oi:
            max_pe_oi = pe_oi
            max_pe_strike = strike

        if row.get("is_atm"):
            atm_strike = strike
            atm_ce_ltp = _opt_ltp(ce)
            atm_pe_ltp = _opt_ltp(pe)

    pcr: Optional[float]
    if total_ce_oi > 0:
        pcr = total_pe_oi / total_ce_oi
    else:
        pcr = None

    skew_denom = total_ce_oi + total_pe_oi
    oi_skew: Optional[float]
    if skew_denom > 0:
        oi_skew = (total_pe_oi - total_ce_oi) / skew_denom
    else:
        oi_skew = None

    return {
        "total_ce_oi": total_ce_oi,
        "total_pe_oi": total_pe_oi,
        "PCR": pcr,
        "atm_strike": atm_strike,
        "atm_ce_ltp": atm_ce_ltp,
        "atm_pe_ltp": atm_pe_ltp,
        "max_oi_strike_ce": None if max_ce_strike is None or max_ce_oi <= 0 else max_ce_strike,
        "max_oi_strike_pe": None if max_pe_strike is None or max_pe_oi <= 0 else max_pe_strike,
        "oi_skew": oi_skew,
        "rows": 0 if empty else len(chain),
    }


def update_candle_options(
    candle_3m: dict[str, Any],
    chain: list[dict[str, Any]],
    metrics: dict[str, Any],
) -> dict[str, Any]:
    """Stamp ``chain`` + ``metrics`` into ``candle_3m["options"]``.

    Mutates ``candle_3m`` in place **and** returns it so the call can
    be chained. Existing keys under ``options`` (e.g. ``summary``,
    ``by_strike`` from older code paths) are preserved.
    """
    options = candle_3m.setdefault("options", {})
    options["chain"] = chain
    options["metrics"] = metrics
    return candle_3m


# ---------------------------------------------------------------------------
# Stateful builder (throttle + ATM-change trigger + cached result)
# ---------------------------------------------------------------------------


class OptionsChainBuilder:
    """Throttled builder + cache for the live option chain.

    Multiple concurrent consumers (per-tick pipeline, REST endpoint,
    3-minute scheduler) share one instance so the aggregation cost is
    paid at most once per ``throttle_ms``. ATM rolls bypass the
    throttle and trigger an immediate rebuild — this is the single
    most important refresh trigger because the **set of strikes**
    rendered to the consumer changes when ATM moves.
    """

    def __init__(
        self,
        *,
        step: int = STRIKE_STEP,
        radius: int = STRIKE_RADIUS,
        throttle_ms: int = THROTTLE_MS,
    ) -> None:
        if step <= 0:
            raise ValueError(f"step must be positive (got {step})")
        if radius <= 0:
            raise ValueError(f"radius must be positive (got {radius})")
        if throttle_ms < 0:
            raise ValueError(f"throttle_ms must be non-negative (got {throttle_ms})")

        self._step = step
        self._radius = radius
        self._throttle_seconds = throttle_ms / 1000.0

        self._last_build_at: float = 0.0
        self._last_atm: Optional[int] = None
        self._last_chain: list[dict[str, Any]] = []
        self._last_metrics: dict[str, Any] = compute_option_metrics([])
        self._builds: int = 0  # diagnostic counter

    # --- introspection -----------------------------------------------------

    @property
    def latest_chain(self) -> list[dict[str, Any]]:
        return self._last_chain

    @property
    def latest_metrics(self) -> dict[str, Any]:
        return self._last_metrics

    @property
    def latest_atm(self) -> Optional[int]:
        return self._last_atm

    @property
    def builds(self) -> int:
        """Total number of times :meth:`maybe_rebuild` actually rebuilt."""
        return self._builds

    # --- main entry point --------------------------------------------------

    def maybe_rebuild(
        self,
        options_by_strike: dict[int | str, dict[str, dict[str, Any]]],
        spot: float | int | None,
        *,
        force: bool = False,
    ) -> tuple[list[dict[str, Any]], dict[str, Any], Optional[int]]:
        """Rebuild the chain if (a) ATM rolled, (b) throttle expired, or (c) ``force``.

        Otherwise return the cached snapshot. If ``spot`` is invalid
        the cached snapshot is returned unchanged — startup-time
        callers can keep invoking this safely before the first NIFTY
        tick arrives.
        """
        if not _is_valid_spot(spot):
            return self._last_chain, self._last_metrics, self._last_atm

        atm = get_atm_strike(spot, step=self._step)
        if atm is None:
            return self._last_chain, self._last_metrics, self._last_atm

        now = time.monotonic()
        atm_changed = atm != self._last_atm
        throttle_elapsed = (now - self._last_build_at) >= self._throttle_seconds

        if not (force or atm_changed or throttle_elapsed):
            return self._last_chain, self._last_metrics, self._last_atm

        chain, atm_resolved = build_option_chain(
            options_by_strike, spot, step=self._step, radius=self._radius
        )
        metrics = compute_option_metrics(chain)

        if atm_changed:
            logger.info(
                "Option chain rebuilt | atm_rolled %s -> %s | rows=%d",
                self._last_atm,
                atm_resolved,
                len(chain),
            )

        self._last_chain = chain
        self._last_metrics = metrics
        self._last_atm = atm_resolved
        self._last_build_at = now
        self._builds += 1

        return chain, metrics, atm_resolved

    def reset(self) -> None:
        """Drop the cache. Mostly useful in tests."""
        self._last_build_at = 0.0
        self._last_atm = None
        self._last_chain = []
        self._last_metrics = compute_option_metrics([])
        self._builds = 0
