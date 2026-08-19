from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from pandas.errors import EmptyDataError


def _to_month_key(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""

    # Common expiry formats handled defensively.
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d-%m-%Y", "%d%b%Y", "%Y-%m"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m")
        except ValueError:
            continue

    # pandas fallback for mixed formats.
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m")


def _is_internal_option_key(symbol: str) -> bool:
    """True for pipeline keys like ``NIFTY_24600_CE``, not exchange trading symbols."""
    parts = str(symbol or "").strip().upper().split("_")
    return (
        len(parts) == 3
        and parts[0] == "NIFTY"
        and parts[1].isdigit()
        and parts[2] in {"CE", "PE"}
    )


def _to_expiry_dt(value: object) -> pd.Timestamp:
    if value is None:
        return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.notna(parsed):
        return parsed.normalize()
    # Common integer-like expiry forms.
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%d-%m-%Y", "%d%b%Y"):
        try:
            return pd.Timestamp(datetime.strptime(text, fmt))
        except ValueError:
            continue
    return pd.NaT


@dataclass(frozen=True)
class SubscriptionDiff:
    added: list[int]
    removed: list[int]
    current: list[int]


from app.instruments.nifty50 import (
    DEFAULT_NIFTY50_SYMBOLS,
    NIFTY50_SYMBOL_COUNT,
    NIFTY50_SYMBOLS,
    nifty50_canonical_symbol,
    nifty50_master_assets,
)


class InstrumentManager:
    """
    Efficient instrument-token registry for live trading subscriptions.

    Design goals:
    - Load and normalize instrument master once.
    - Build indexed DataFrames/maps upfront.
    - Avoid repeated full-frame filtering in hot path.
    - Recompute option tokens only when ATM boundary moves.
    """

    def __init__(
        self,
        *,
        env_name: str = "UAT",
        use_env_creds: bool = True,
        local_cache_csv: str | None = "instrument_master_cache.csv",
        nifty50_symbols: list[str] | None = None,
        on_option_tokens_changed: Callable[[SubscriptionDiff], None] | None = None,
        instrument_fetcher: Callable[[], pd.DataFrame] | None = None,
        strike_radius: int = 15,
    ) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.env_name = env_name
        self.use_env_creds = use_env_creds
        self.local_cache_csv = local_cache_csv
        selected_symbols = nifty50_symbols or list(DEFAULT_NIFTY50_SYMBOLS)
        self.nifty50_symbols = {str(x).strip().upper() for x in selected_symbols if str(x).strip()}
        self.on_option_tokens_changed = on_option_tokens_changed
        self.instrument_fetcher = instrument_fetcher or self._default_instrument_fetcher
        # ATM ± N strikes (50-point steps). Legacy default was ±750 via fixed span.
        self._strike_radius = max(1, int(strike_radius))
        self._strike_step: int = 50
        self._strike_scale: int = 1
        self._option_symbol_by_ref: dict[int, str] = {}
        self._option_trading_symbol_by_ref: dict[int, str] = {}
        self._nifty_fut_refs: list[int] = []
        self._nifty_fut_contract_by_ref: dict[int, str] = {}
        self._nifty_fut_symbol_by_ref: dict[int, str] = {}
        self._fut_by_symbol: dict[str, dict[str, Any]] = {}
        self._stock_fut_refs: list[int] = []
        self._stock_fut_assets: set[str] = set()
        self._stock_eq_refs: list[int] = []
        self._stock_eq_assets: set[str] = set()
        self._stock_eq_by_symbol: dict[str, dict[str, Any]] = {}

        self.df: pd.DataFrame = self._load_instruments()
        self._prepare_indices()

        self._active_atm: int | None = None
        self._active_option_tokens: list[int] = []

    # ----------------------------
    # Public API
    # ----------------------------
    def get_nifty_index(self) -> int:
        if self._nifty_index_ref is None:
            raise LookupError("NIFTY index instrument not found")
        return self._nifty_index_ref

    def get_nifty_futures(self) -> int:
        if self._nifty_fut_ref is None:
            raise LookupError("NIFTY future not found")
        return self._nifty_fut_ref

    def get_nifty_futures_refs(self) -> list[int]:
        return list(self._nifty_fut_refs)

    def get_nifty_fut_contracts(self) -> dict[int, str]:
        """Map ``ref_id`` → ``"current"`` / ``"next"`` / ``"far"``."""
        return dict(self._nifty_fut_contract_by_ref)

    def get_nifty_fut_symbols(self) -> dict[int, str]:
        """Map ``ref_id`` → trading symbol (e.g. ``NIFTY26MAYFUT``)."""
        return dict(self._nifty_fut_symbol_by_ref)

    def get_fut_meta(self, symbol: str) -> dict[str, Any] | None:
        """Trading symbol → underlying, expiry, instrument_type."""
        return self._fut_by_symbol.get(str(symbol).strip().upper())

    def get_stock_futures(self) -> list[int]:
        return list(self._stock_fut_refs)

    def get_stock_equity(self) -> list[int]:
        return list(self._stock_eq_refs)

    def get_stock_eq_meta(self, symbol: str) -> dict[str, Any] | None:
        """NIFTY50 cash symbol → token / exchange metadata for market_ohlc_3m."""
        return self._stock_eq_by_symbol.get(nifty50_canonical_symbol(str(symbol).strip().upper()))

    def get_stock_spot_symbols(self) -> list[str]:
        """Canonical NIFTY50 cash symbols resolved from the instrument master."""
        return sorted(self._stock_eq_by_symbol)

    def get_stock_fut_trading_symbols(self) -> list[str]:
        """Nearest NIFTY50 stock-future trading symbols (e.g. ``RELIANCE26MAYFUT``)."""
        out: list[str] = []
        seen: set[str] = set()
        for ref in self._stock_fut_refs:
            rows = self.df[self.df["ref_id"] == ref]
            if rows.empty:
                continue
            sym = str(rows.iloc[0].get("symbol") or "").strip().upper()
            if sym and sym not in seen:
                seen.add(sym)
                out.append(sym)
        return out

    def get_option_trading_symbol_by_ref(self) -> dict[int, str]:
        """``ref_id`` → master trading symbol for historical_data() (not ``NIFTY_24600_CE``)."""
        return dict(self._option_trading_symbol_by_ref)

    def get_atm_option_legs(self, nifty_price: float | None = None) -> list[dict[str, Any]]:
        """ATM ± strike_radius option legs with historical-API trading symbols."""
        try:
            px = float(nifty_price) if nifty_price and float(nifty_price) > 0 else 0.0
        except (TypeError, ValueError):
            px = 0.0
        if px <= 0:
            px = float(self._active_atm or 0) or 22000.0
        maps = self.get_ref_maps(px)
        legs: list[dict[str, Any]] = []
        for ref_id, (strike, side) in (maps.get("option_by_ref") or {}).items():
            trading = self._option_trading_symbol_by_ref.get(int(ref_id), "")
            if not trading:
                continue
            legs.append(
                {
                    "ref_id": int(ref_id),
                    "symbol": trading,
                    "strike": int(strike),
                    "side": str(side).upper(),
                }
            )
        return legs

    @property
    def price_scale(self) -> int:
        """Multiplier between Nubra wire prices and rupees.

        Auto-detected in :meth:`_prepare_indices` from the option
        master's strike spacing. Typical values:

        * ``1``   — wire prices are already in rupees.
        * ``100`` — wire prices are in paise (NIFTY UAT default).

        ``WebSocket`` LTP / OHLC values must be divided by this scale
        before being shown to consumers, written into candles, or used
        as ``spot`` for ATM resolution. Strike keys in
        ``state.options_by_strike`` are already converted to rupees by
        :meth:`get_ref_maps`, so ATM/strike comparisons must happen in
        rupees too.
        """
        return max(1, int(self._strike_scale))

    def to_rupees(self, price: Any) -> float:
        from app.core.price_utils import normalize_price
        return normalize_price(price, scale=float(self.price_scale), kind="INDEX", module="manager")

    def get_option_tokens(self, atm: int) -> list[int]:
        center_rupee = self._nearest_50(atm)
        center = self._to_strike_domain(center_rupee)
        strikes = [center + i * self._strike_step for i in range(-self._strike_radius, self._strike_radius + 1)]
        tokens: list[int] = []
        for strike in strikes:
            pair = self._option_map.get(int(strike))
            if not pair:
                continue
            ce = pair.get("CE")
            pe = pair.get("PE")
            if ce is not None:
                tokens.append(ce)
            if pe is not None:
                tokens.append(pe)
        return tokens

    def get_option_subscription_payload(self, nifty_price: float | None = None) -> dict[str, Any]:
        """
        Output contract for option ref-id subscriptions (ATM ± strike_radius).
        """
        if nifty_price is not None:
            atm = self._nearest_50(nifty_price)
            tokens = sorted(set(self.get_option_tokens(atm)))
        else:
            if self._active_atm is None:
                raise LookupError("ATM not initialized; call update_atm() first")
            atm = self._active_atm
            tokens = sorted(set(self._active_option_tokens))

        symbols = [self._option_symbol_by_ref.get(int(t), f"NIFTY_{t}") for t in tokens]
        return {"atm": atm, "tokens": [int(t) for t in tokens], "symbols": symbols}

    def get_nifty_option_chain_key(self) -> str:
        """WebSocket key for Nubra *option chain* stream (not a trading symbol).

        Nubra expects ``ASSET:YYYYMMDD``, e.g. ``NIFTY:20260426`` — the same
        contract as in the REST/stream docs (`data_type="option"`). This is
        **not** the chain row format like ``NIFTY26APR24600PE`` shown in
        terminal UIs; those are per-leg symbols from the instrument master.
        """
        if not self._nifty_option_chain_key:
            raise LookupError("Current-month NIFTY option-chain key not found")
        return self._nifty_option_chain_key

    def get_option_expiry(self) -> str | None:
        """Selected NIFTY option expiry as ``YYYY-MM-DD`` (or ``None``).

        Derived from the resolved option-chain key ``NIFTY:YYYYMMDD``.
        Used by the options_data writer to satisfy the table's NOT NULL
        expiry column. Read-only; does not mutate any state.
        """
        key = self._nifty_option_chain_key or ""
        if ":" not in key:
            return None
        ymd = key.split(":", 1)[1].strip()
        if len(ymd) != 8 or not ymd.isdigit():
            return None
        return f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"

    def get_index_symbols(self) -> list[str]:
        return list(self._index_symbols)

    def get_ohlcv_symbols(self) -> list[str]:
        return ["NIFTY"]

    def update_atm(self, price: float) -> SubscriptionDiff | None:
        """
        Recalculate ATM and return subscription diff only when boundary moves >= 50.

        On the **first** call (``_active_atm is None``) we ALSO fire
        ``on_option_tokens_changed`` if it is registered. This handles
        the corner case where ``update_atm`` is the first method that
        learns the live spot price (e.g. when ``get_subscription_tokens``
        was bypassed). For the normal startup path,
        ``get_subscription_tokens`` pre-arms ``_active_atm`` with the
        bootstrap price so this branch reduces to a noop diff.
        """
        new_atm = self._nearest_50(price)
        if self._active_atm is None:
            new_tokens = sorted(self.get_option_tokens(new_atm))
            self._active_atm = new_atm
            self._active_option_tokens = new_tokens
            diff = SubscriptionDiff(added=new_tokens, removed=[], current=new_tokens)
            if self.on_option_tokens_changed is not None and new_tokens:
                self.on_option_tokens_changed(diff)
            return diff

        if abs(new_atm - self._active_atm) < 50:
            return None

        previous = set(self._active_option_tokens)
        current = set(self.get_option_tokens(new_atm))
        added = sorted(current - previous)
        removed = sorted(previous - current)
        now_sorted = sorted(current)

        self._active_atm = new_atm
        self._active_option_tokens = now_sorted

        diff = SubscriptionDiff(added=added, removed=removed, current=now_sorted)
        if self.on_option_tokens_changed is not None and (added or removed):
            self.on_option_tokens_changed(diff)
        self.logger.info(
            "ATM moved to %s; option diff added=%d removed=%d total=%d",
            new_atm,
            len(added),
            len(removed),
            len(now_sorted),
        )
        return diff

    def get_subscription_tokens(self, nifty_price: float) -> dict[str, list[int]]:
        atm = self._nearest_50(nifty_price)
        options = self.get_option_tokens(atm)
        if len(options) < 40:
            raise Exception(f"Invalid option tokens count: {len(options)}")
        nifty_futures = list(self._nifty_fut_refs)
        if not nifty_futures:
            self.logger.warning("No NIFTY futures token resolved; continuing without futures subscription")

        # Critical: record the ATM / token set we are about to subscribe
        # to so :meth:`update_atm` can correctly diff against the wire
        # state when the live spot moves away from `nifty_price`. Without
        # this, the first live update would silently record live state
        # without firing the resubscribe callback, leaving the wire
        # stuck on the bootstrap-price strike window.
        self._active_atm = atm
        self._active_option_tokens = sorted(options)

        self.logger.info(
            "Bootstrapped option subscription | atm=%s | tokens=%d | "
            "nifty_price_bootstrap=%s",
            atm,
            len(options),
            nifty_price,
        )

        return {
            "index": [self.get_nifty_index()],
            "nifty_futures": nifty_futures,
            "stock_futures": self.get_stock_futures(),
            "stock_equity": self.get_stock_equity(),
            "options": options,
        }

    def get_stream_subscriptions(
        self,
        nifty_price: float,
        *,
        include_ohlcv: bool = True,
        include_option_chain: bool = True,
    ) -> dict[str, list[str]]:
        """
        Build stream payloads in Nubra subscribe-compatible formats.
        """
        token_bundle = self.get_subscription_tokens(nifty_price=nifty_price)

        option_ref_ids = token_bundle["options"]
        orderbook_ref_ids = (
            token_bundle["nifty_futures"]
            + token_bundle["stock_futures"]
            + token_bundle.get("stock_equity", [])
            + option_ref_ids
        )

        # Keep output deterministic and compact.
        orderbook_ref_ids = sorted(set(orderbook_ref_ids))
        option_ref_ids = sorted(set(option_ref_ids))

        option_chain_keys: list[str] = []
        if include_option_chain:
            try:
                key = self.get_nifty_option_chain_key()
                if key:
                    option_chain_keys = [key]
            except LookupError:
                self.logger.warning("NIFTY option-chain WebSocket key unavailable (expiry not resolved)")

        return {
            "index_symbols": self.get_index_symbols(),
            # ``NIFTY:YYYYMMDD`` for ``data_type="option"`` (full chain feed).
            "option_chain_keys": option_chain_keys,
            "orderbook_ref_ids": [str(x) for x in orderbook_ref_ids],
            # Subscribe greeks for same ref_id universe to maximize OI availability.
            "greeks_ref_ids": [str(x) for x in orderbook_ref_ids],
            "ohlcv_symbols": self.get_ohlcv_symbols() if include_ohlcv else [],
        }

    def get_ref_maps(self, nifty_price: float) -> dict[str, Any]:
        """
        Classify orderbook ref_ids: NIFTY fut, stock fut symbols, option strike/side.
        """
        atm = self._nearest_50(nifty_price)
        # Build the option-strike window in the MASTER's strike domain
        # (paise on PROD, where _strike_scale=100 / _strike_step=5000),
        # exactly like get_option_tokens(). Using the raw rupee ATM with
        # _strike_window() here was the long-standing bug that left
        # option_by_ref empty (opt_map_size=0) even though the master held
        # hundreds of option strikes — every orderbook option tick then
        # fell through to the "unresolved ref_id" path and the order-book
        # aggregator stayed empty.
        center = self._to_strike_domain(atm)
        strikes = [
            center + i * self._strike_step
            for i in range(-self._strike_radius, self._strike_radius + 1)
        ]
        option_by_ref: dict[int, tuple[int, str]] = {}
        for strike in strikes:
            pair = self._option_map.get(int(strike))
            if not pair:
                continue
            ce = pair.get("CE")
            pe = pair.get("PE")
            # Emit the strike back in RUPEES (the domain the pipeline/aggregator
            # operate in): divide the strike-domain key by _strike_scale.
            strike_rupees = int(round(strike / max(self._strike_scale, 1)))
            if ce is not None:
                option_by_ref[int(ce)] = (strike_rupees, "CE")
            if pe is not None:
                option_by_ref[int(pe)] = (strike_rupees, "PE")

        stock_fut_symbols: dict[int, str] = {}
        for ref in self._stock_fut_refs:
            rows = self.df[self.df["ref_id"] == ref]
            if rows.empty:
                continue
            sym = rows.iloc[0].get("symbol") or rows.iloc[0].get("asset") or ""
            stock_fut_symbols[int(ref)] = str(sym).strip().upper()

        stock_eq_symbols: dict[int, str] = {}
        for ref in self._stock_eq_refs:
            for sym, meta in self._stock_eq_by_symbol.items():
                if meta.get("ref_id") == ref:
                    stock_eq_symbols[int(ref)] = sym
                    break

        return {
            "atm": atm,
            "nifty_fut_ref": self._nifty_fut_ref,
            "nifty_fut_refs": list(self._nifty_fut_refs),
            "nifty_fut_contract_by_ref": dict(self._nifty_fut_contract_by_ref),
            "nifty_fut_symbol_by_ref": dict(self._nifty_fut_symbol_by_ref),
            "stock_fut_symbols": stock_fut_symbols,
            "stock_eq_symbols": stock_eq_symbols,
            "option_by_ref": option_by_ref,
            "option_symbol_by_ref": {
                ref_id: self._option_symbol_by_ref.get(ref_id, f"NIFTY_{strike}_{side}")
                for ref_id, (strike, side) in option_by_ref.items()
            },
        }

    def reference_map_status(self, nifty_price: float | None = None) -> dict[str, Any]:
        """Health/validation snapshot of the reference maps.

        Returns master-load state plus the sizes of the three ref-id maps
        the pipeline needs to resolve every orderbook/greeks tick:
        ``opt_map`` (option ref→strike/side), ``fut_map`` (NIFTY future
        ref→symbol) and ``stock_map`` (stock future ref→symbol). The
        startup gate refuses to launch the 3-minute scheduler until all
        three are > 0 and all configured NIFTY50 stock futures are resolved.
        """
        master_loaded = self.df is not None and not self.df.empty
        try:
            px = nifty_price if (nifty_price and float(nifty_price) > 0) else 0.0
        except (TypeError, ValueError):
            px = 0.0
        if px <= 0:
            px = float(self._active_atm or 0) or 22000.0
        try:
            maps = self.get_ref_maps(px)
        except Exception:
            maps = {}
        expected_stock_count = len(self.nifty50_symbols) if self.nifty50_symbols else NIFTY50_SYMBOL_COUNT
        missing_stock_symbols = sorted(self.nifty50_symbols - self._stock_fut_assets)
        stock_count = len(self._stock_fut_refs)
        stock_map_size = len(maps.get("stock_fut_symbols") or {})
        return {
            "master_loaded": bool(master_loaded),
            "master_rows": int(len(self.df)) if master_loaded else 0,
            "option_count": len(self._option_map),
            "future_count": len(self._nifty_fut_symbol_by_ref),
            "stock_count": stock_count,
            "expected_stock_count": expected_stock_count,
            "missing_stock_symbols": missing_stock_symbols,
            "stock_count_ok": (
                stock_count == expected_stock_count
                and stock_map_size == expected_stock_count
                and not missing_stock_symbols
            ),
            "opt_map_size": len(maps.get("option_by_ref") or {}),
            "fut_map_size": len(maps.get("nifty_fut_symbol_by_ref") or {}),
            "stock_map_size": stock_map_size,
        }

    # ----------------------------
    # Loading and preprocessing
    # ----------------------------
    #: Maximum age of the on-disk instrument-master cache. Older than
    #: this and we refetch — otherwise a stale CSV silently hides newly
    #: listed expiries (e.g. only "current+next" appear when "far"
    #: should also be listed) and the user has to manually delete it.
    INSTRUMENT_CACHE_MAX_AGE_SECONDS: int = 24 * 60 * 60  # 24h

    def _cache_is_stale(self, cache_path: Path) -> bool:
        try:
            import time as _time

            age = _time.time() - cache_path.stat().st_mtime
        except OSError:
            return True
        return age > self.INSTRUMENT_CACHE_MAX_AGE_SECONDS

    def _load_instruments(self) -> pd.DataFrame:
        cache_path = Path(self.local_cache_csv) if self.local_cache_csv else None
        if cache_path and cache_path.exists() and self._cache_is_stale(cache_path):
            self.logger.info(
                "instrument cache %s is older than %ss; refetching to pick up "
                "newly listed expiries",
                cache_path,
                self.INSTRUMENT_CACHE_MAX_AGE_SECONDS,
            )
            try:
                cache_path.unlink()
            except OSError as exc:
                self.logger.warning("could not delete stale cache %s: %s", cache_path, exc)

        if cache_path and cache_path.exists():
            self.logger.info("loading instrument cache from %s", cache_path)
            try:
                df = pd.read_csv(cache_path)
                self.logger.info("instrument cache loaded rows=%s cols=%s", len(df), len(df.columns))
            except (EmptyDataError, pd.errors.ParserError, UnicodeDecodeError) as exc:
                self.logger.warning(
                    "instrument cache unreadable (%s). Refetching from Nubra and overwriting cache.",
                    exc,
                )
                df = pd.DataFrame()
            if df.empty:
                self.logger.warning(
                    "instrument cache at %s has 0 rows (truncated or corrupt). "
                    "Refetching from Nubra SDK.",
                    cache_path,
                )
                df = self.instrument_fetcher()
                self.logger.info("instrument master fetched rows=%s cols=%s", len(df), len(df.columns))
                if not df.empty and cache_path:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    df.to_csv(cache_path, index=False)
                    self.logger.info("instrument cache refreshed at %s", cache_path)
        else:
            self.logger.info("fetching instrument master from Nubra SDK")
            df = self.instrument_fetcher()
            self.logger.info("instrument master fetched rows=%s cols=%s", len(df), len(df.columns))
            if not df.empty and cache_path:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(cache_path, index=False)
                self.logger.info("instrument cache written to %s", cache_path)

        if df.empty:
            hint = (
                "Instrument master has no rows. Common causes: (1) a zero-byte or "
                "truncated instrument_master_cache.csv — delete it and restart; "
                "(2) Nubra SDK returned an empty DataFrame — check auth "
                "(PHONE_NO/MPIN/TOTP, NUBRA_ENV) and that "
                "InstrumentData().get_instruments_dataframe() works for this environment."
            )
            if cache_path and cache_path.exists():
                hint += f" Cache file: {cache_path.resolve()}"
            raise ValueError(hint)
        normalized = self._normalize_columns(df)
        self.logger.info("instrument master normalized rows=%s cols=%s", len(normalized), len(normalized.columns))
        return normalized

    def _default_instrument_fetcher(self) -> pd.DataFrame:
        """
        Resolve SDK loader with a defensive import strategy.
        The docs mention InstrumentData + get_instruments_dataframe().
        """
        from app.ingestion.auth_client import get_authenticated_client

        client = get_authenticated_client(env_name=self.env_name)

        # Primary documented path:
        # https://nubra.io/products/api/docs/python-sdk/get-instruments.html
        try:
            from nubra_python_sdk.refdata.instruments import InstrumentData

            helper = InstrumentData(client)
            df = helper.get_instruments_dataframe()
            # Empty frames are not usable; try fallbacks before giving up.
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df
        except Exception:
            pass

        # Fallback paths for older/alternate SDK layouts.
        candidates = [
            "nubra_python_sdk.refdata.instruments",
            "nubra_python_sdk.market_data.instrument_data",
            "nubra_python_sdk.market_data.instruments",
            "nubra_python_sdk.instruments.instrument_data",
        ]
        for module_path in candidates:
            try:
                module = __import__(module_path, fromlist=["InstrumentData"])
                instrument_cls = getattr(module, "InstrumentData", None)
                if instrument_cls is None:
                    continue
                helper = instrument_cls(client=client)
                df = helper.get_instruments_dataframe()
                if isinstance(df, pd.DataFrame) and not df.empty:
                    return df
            except Exception:
                continue

        raise ImportError(
            "Could not load a non-empty instrument master from Nubra: "
            "all InstrumentData paths returned empty or failed. "
            "Verify nubra_python_sdk version, auth, and that "
            "get_instruments_dataframe() returns rows in UAT/PROD."
        )

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        normalized = df.copy()
        normalized.columns = [c.strip().lower() for c in normalized.columns]
        self.logger.info("normalizing instrument columns")

        required = {"ref_id", "asset", "derivative_type", "expiry"}
        missing = required - set(normalized.columns)
        if missing:
            raise ValueError(f"instrument master missing required columns: {sorted(missing)}")

        if "option_type" not in normalized.columns:
            normalized["option_type"] = None
        if "strike_price" not in normalized.columns:
            normalized["strike_price"] = None
        if "asset_type" not in normalized.columns:
            normalized["asset_type"] = None

        normalized["asset"] = normalized["asset"].astype(str).str.upper().str.strip()
        normalized["derivative_type"] = normalized["derivative_type"].astype(str).str.upper().str.strip()
        normalized["option_type"] = normalized["option_type"].astype(str).str.upper().str.strip()
        normalized["asset_type"] = normalized["asset_type"].astype(str).str.upper().str.strip()
        normalized["ref_id"] = pd.to_numeric(normalized["ref_id"], errors="coerce").astype("Int64")
        normalized["strike_price"] = pd.to_numeric(normalized["strike_price"], errors="coerce")
        # Vectorized expiry parsing for large masters.
        expiry_raw = normalized["expiry"].astype(str).str.strip()
        expiry_raw = expiry_raw.where(normalized["expiry"].notna(), "")
        # Normalize numeric-like values emitted as strings such as "20260421.0".
        expiry_clean = expiry_raw.str.replace(r"\.0$", "", regex=True).str.replace("/", "-", regex=False)
        expiry_dt = pd.to_datetime(expiry_clean, errors="coerce")
        # Second pass for compact numeric dates like YYYYMMDD.
        needs_second_pass = expiry_dt.isna() & expiry_clean.str.match(r"^\d{8}$", na=False)
        if needs_second_pass.any():
            expiry_dt.loc[needs_second_pass] = pd.to_datetime(
                expiry_clean.loc[needs_second_pass], format="%Y%m%d", errors="coerce"
            )
        normalized["expiry_dt"] = expiry_dt.dt.normalize()
        normalized["expiry_month"] = normalized["expiry_dt"].dt.strftime("%Y-%m").fillna("")
        # Vectorized symbol resolution (faster than row-wise apply on large masters).
        symbol_series = pd.Series([""] * len(normalized), index=normalized.index, dtype="object")
        candidate_cols = ("symbol", "stock_name", "trading_symbol", "instrument_name", "indexname", "name", "token")
        for col in candidate_cols:
            if col not in normalized.columns:
                continue
            values = normalized[col].astype(str).str.upper().str.strip()
            values = values.where(normalized[col].notna(), "")
            symbol_series = symbol_series.mask((symbol_series == "") & (values != ""), values)
        asset_values = normalized["asset"].astype(str).str.upper().str.strip()
        symbol_series = symbol_series.mask(symbol_series == "", asset_values)
        normalized["symbol"] = symbol_series

        # Drop malformed rows once to avoid repeated guards later.
        normalized = normalized.dropna(subset=["ref_id"]).copy()
        normalized["ref_id"] = normalized["ref_id"].astype(int)
        return normalized

    def _prepare_indices(self) -> None:
        today = pd.Timestamp.now().normalize()
        df = self.df

        # NIFTY index (prefer non-derivative rows).
        index_candidates = df[(df["asset"] == "NIFTY") & (~df["derivative_type"].isin(["OPT", "FUT"]))]
        if index_candidates.empty:
            index_candidates = df[df["asset"] == "NIFTY"]
        self._nifty_index_ref = self._first_ref(index_candidates)

        # NIFTY futures: pick current/next/far expiries.
        nifty_fut = df[
            (df["asset"] == "NIFTY")
            & (df["derivative_type"] == "FUT")
        ].copy()
        nifty_fut = nifty_fut[nifty_fut["expiry_dt"].notna()].sort_values("expiry_dt")
        non_exp = nifty_fut[nifty_fut["expiry_dt"] >= today]
        candidate = non_exp if not non_exp.empty else nifty_fut
        refs: list[int] = []
        contract_by_ref: dict[int, str] = {}
        symbol_by_ref: dict[int, str] = {}
        labels = ["current", "next", "far"]
        idx = 0
        for exp, group in candidate.groupby("expiry_dt", dropna=False):
            if idx >= 3:
                break
            row = group.iloc[0]
            ref_id = int(row["ref_id"])
            refs.append(ref_id)
            contract_by_ref[ref_id] = labels[idx]
            sym = row.get("symbol") or row.get("asset") or ""
            symbol_by_ref[ref_id] = str(sym).strip().upper()
            idx += 1
        self._nifty_fut_refs = refs
        self._nifty_fut_ref = refs[0] if refs else None
        self._nifty_fut_contract_by_ref = contract_by_ref
        self._nifty_fut_symbol_by_ref = symbol_by_ref

        # Coverage diagnostics — NSE typically lists three monthly NIFTY
        # futures (current/next/far) at any given time. Fewer than 3 is
        # almost always a stale on-disk cache: see
        # ``INSTRUMENT_CACHE_MAX_AGE_SECONDS`` and ``_cache_is_stale``.
        nifty_fut_count = len(refs)
        if nifty_fut_count < 3:
            missing = ["current", "next", "far"][nifty_fut_count:]
            self.logger.warning(
                "Only %d NIFTY future contract(s) resolved (%s); missing %s. "
                "If the cache is older than %ss it is auto-refreshed at next "
                "boot, otherwise delete %s and restart.",
                nifty_fut_count,
                {labels[i]: symbol_by_ref.get(r) for i, r in enumerate(refs)},
                missing,
                self.INSTRUMENT_CACHE_MAX_AGE_SECONDS,
                self.local_cache_csv,
            )
        else:
            self.logger.info(
                "NIFTY futures resolved: %s",
                {contract_by_ref[r]: symbol_by_ref.get(r) for r in refs},
            )

        # Stock futures: one nearest non-expired contract per asset.
        stock_fut = df[
            (df["derivative_type"] == "FUT")
            & (df["asset_type"] == "STOCK_FO")
        ].copy()
        if self.nifty50_symbols:
            master_assets = nifty50_master_assets(self.nifty50_symbols)
            stock_fut = stock_fut[stock_fut["asset"].isin(master_assets)]
        stock_fut = self._prefer_non_expired_per_asset(stock_fut, today)
        stock_fut = stock_fut.drop_duplicates(subset=["asset"], keep="first")
        expected_count = len(self.nifty50_symbols) if self.nifty50_symbols else NIFTY50_SYMBOL_COUNT
        selected = stock_fut.head(expected_count)
        self._stock_fut_refs = [int(ref) for ref in selected["ref_id"].tolist()]
        self._stock_fut_assets = {
            nifty50_canonical_symbol(str(asset))
            for asset in selected["asset"].tolist()
            if str(asset).strip()
        }
        self._stock_fut_assets = {
            asset for asset in self._stock_fut_assets if asset in self.nifty50_symbols
        }
        if len(self._stock_fut_refs) != expected_count:
            missing = sorted(self.nifty50_symbols - self._stock_fut_assets)
            self.logger.warning(
                "NIFTY50 stock futures incomplete | resolved=%d expected=%d missing=%s",
                len(self._stock_fut_refs),
                expected_count,
                missing,
            )

        # NIFTY50 cash equities (spot) — one row per asset, derivative_type STOCK.
        stock_eq = df[
            (df["derivative_type"] == "STOCK")
            & (df["asset_type"] == "STOCKS")
        ].copy()
        if self.nifty50_symbols:
            eq_assets = nifty50_master_assets(self.nifty50_symbols)
            stock_eq = stock_eq[stock_eq["asset"].isin(eq_assets)]
        stock_eq = stock_eq.drop_duplicates(subset=["asset"], keep="first")
        self._stock_eq_refs = [int(ref) for ref in stock_eq["ref_id"].tolist()]
        self._stock_eq_assets = {
            nifty50_canonical_symbol(str(asset))
            for asset in stock_eq["asset"].tolist()
            if str(asset).strip()
        }
        self._stock_eq_assets = {a for a in self._stock_eq_assets if a in self.nifty50_symbols}
        self._stock_eq_by_symbol = {}
        for _, row in stock_eq.iterrows():
            asset = nifty50_canonical_symbol(str(row.get("asset") or "").strip().upper())
            if not asset:
                continue
            token = row.get("token")
            self._stock_eq_by_symbol[asset] = {
                "symbol": asset,
                "symbol_token": str(int(token)) if pd.notna(token) else None,
                "exchange": str(row.get("exchange") or "NSE").strip().upper(),
                "instrument_type": "STOCK",
                "ref_id": int(row["ref_id"]),
            }
        if len(self._stock_eq_refs) != expected_count:
            missing_eq = sorted(self.nifty50_symbols - self._stock_eq_assets)
            self.logger.warning(
                "NIFTY50 stock equity incomplete | resolved=%d expected=%d missing=%s",
                len(self._stock_eq_refs),
                expected_count,
                missing_eq,
            )

        # Resolve symbols for index stream (index + futures + spot equities).
        ref_to_symbol = (
            df[["ref_id", "symbol"]]
            .dropna(subset=["symbol"])
            .drop_duplicates(subset=["ref_id"], keep="first")
            .set_index("ref_id")["symbol"]
            .to_dict()
        )
        index_symbols: list[str] = ["NIFTY"]
        for fut_ref in self._nifty_fut_refs:
            symbol = ref_to_symbol.get(fut_ref)
            if symbol:
                index_symbols.append(symbol)
        for ref_id in self._stock_fut_refs:
            symbol = ref_to_symbol.get(ref_id)
            if symbol:
                index_symbols.append(symbol)
        for asset in sorted(self._stock_eq_assets):
            index_symbols.append(asset)
        self._index_symbols = sorted(set(index_symbols))

        # Option map: strike -> {CE: ref_id, PE: ref_id} for nearest non-expired NIFTY expiry.
        nifty_opt_all = df[
            (df["asset"] == "NIFTY")
            & (df["derivative_type"] == "OPT")
            & (df["asset_type"] == "INDEX_FO")
            & (df["option_type"].isin(["CE", "PE"]))
            & (df["strike_price"].notna())
        ].copy()
        if not nifty_opt_all.empty:
            # Ensure expiry is parseable even when master stores values like "20260421.0".
            raw = nifty_opt_all["expiry"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
            alt_dt = pd.to_datetime(raw, errors="coerce")
            needs_fmt = alt_dt.isna() & raw.str.match(r"^\d{8}$", na=False)
            if needs_fmt.any():
                alt_dt.loc[needs_fmt] = pd.to_datetime(raw.loc[needs_fmt], format="%Y%m%d", errors="coerce")
            nifty_opt_all.loc[:, "_effective_expiry_dt"] = nifty_opt_all["expiry_dt"].where(
                nifty_opt_all["expiry_dt"].notna(),
                alt_dt.dt.normalize(),
            )
        else:
            nifty_opt_all.loc[:, "_effective_expiry_dt"] = pd.NaT

        selected_expiry_dt: pd.Timestamp | None
        valid_exp = nifty_opt_all[nifty_opt_all["_effective_expiry_dt"].notna()].copy()
        if valid_exp.empty:
            selected_expiry_dt = None
        else:
            future_exp = valid_exp[valid_exp["_effective_expiry_dt"] >= today]
            if not future_exp.empty:
                selected_expiry_dt = pd.Timestamp(future_exp["_effective_expiry_dt"].min()).normalize()
            else:
                # If cache is stale, use latest available expiry instead of oldest expired.
                selected_expiry_dt = pd.Timestamp(valid_exp["_effective_expiry_dt"].max()).normalize()

        opt_keep = [c for c in ("strike_price", "option_type", "ref_id", "symbol") if c in df.columns]
        option_df = df[
            (df["asset"] == "NIFTY")
            & (df["derivative_type"] == "OPT")
            & (df["expiry_dt"] == selected_expiry_dt)
            & (df["option_type"].isin(["CE", "PE"]))
            & (df["strike_price"].notna())
        ][opt_keep].copy()

        # Fallback using effective expiry when normalized expiry_dt is missing/misaligned.
        if option_df.empty and selected_expiry_dt is not None and not nifty_opt_all.empty:
            fallback_keep = [c for c in opt_keep if c in nifty_opt_all.columns]
            option_df = nifty_opt_all[
                nifty_opt_all["_effective_expiry_dt"] == selected_expiry_dt
            ][fallback_keep].copy()

        option_df["strike_price"] = option_df["strike_price"].astype(int)
        grouped: dict[int, dict[str, int]] = {}
        option_symbol_by_ref: dict[int, str] = {}
        option_trading_symbol_by_ref: dict[int, str] = {}
        for row in option_df.itertuples(index=False):
            strike = int(row.strike_price)
            side = str(row.option_type).upper()
            ref_id = int(row.ref_id)
            grouped.setdefault(strike, {})[side] = ref_id
            strike_rupees = int(round(strike / max(self._strike_scale, 1)))
            option_symbol_by_ref[ref_id] = f"NIFTY_{strike_rupees}_{side}"
            master_sym = ""
            if hasattr(row, "symbol") and row.symbol is not None:
                master_sym = str(row.symbol).strip().upper()
                if master_sym in {"", "NAN", "NONE", "NAT"}:
                    master_sym = ""
            if not master_sym or _is_internal_option_key(master_sym):
                if selected_expiry_dt is not None and pd.notna(selected_expiry_dt):
                    exp = pd.Timestamp(selected_expiry_dt)
                    master_sym = (
                        f"NIFTY{int(exp.year) % 100}{int(exp.month)}{int(exp.day)}"
                        f"{strike_rupees}{side}"
                    )
            if master_sym:
                option_trading_symbol_by_ref[ref_id] = master_sym
        self._option_map = grouped
        self._option_symbol_by_ref = option_symbol_by_ref
        self._option_trading_symbol_by_ref = option_trading_symbol_by_ref
        self._available_strikes = sorted(grouped.keys())
        if len(self._available_strikes) >= 2:
            diffs = [
                b - a
                for a, b in zip(self._available_strikes[:-1], self._available_strikes[1:])
                if b > a
            ]
            if diffs:
                self._strike_step = min(diffs)
                self._strike_scale = max(1, int(round(self._strike_step / 50)))

        # Option-chain stream requires ASSET:EXPIRY key.
        option_chain_candidates = df[
            (df["asset"] == "NIFTY")
            & (df["derivative_type"] == "OPT")
            & (df["expiry_dt"] == selected_expiry_dt)
            & (df["expiry_dt"].notna())
        ].sort_values("expiry_dt")
        if option_chain_candidates.empty:
            if selected_expiry_dt is not None:
                self._nifty_option_chain_key = f"NIFTY:{pd.Timestamp(selected_expiry_dt).strftime('%Y%m%d')}"
            else:
                self._nifty_option_chain_key = ""
        else:
            expiry_value = option_chain_candidates.iloc[0]["expiry_dt"]
            expiry_key = pd.Timestamp(expiry_value).strftime("%Y%m%d")
            self._nifty_option_chain_key = f"NIFTY:{expiry_key}"

        self._fut_by_symbol = self._build_fut_by_symbol(df)

    # ----------------------------
    # Helpers
    # ----------------------------
    @staticmethod
    def _build_fut_by_symbol(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        fut = df[df["derivative_type"] == "FUT"]
        for _, row in fut.iterrows():
            sym = str(row.get("symbol") or "").strip().upper()
            if not sym:
                continue
            exp_dt = row.get("expiry_dt")
            expiry_text = ""
            expiry_py: datetime | None = None
            if pd.notna(exp_dt):
                ts = pd.Timestamp(exp_dt).normalize()
                expiry_text = ts.strftime("%Y-%m-%d")
                expiry_py = ts.to_pydatetime()
            asset = str(row.get("asset") or "").strip().upper()
            out[sym] = {
                "underlying_symbol": nifty50_canonical_symbol(asset),
                "expiry": expiry_text,
                "expiry_dt": expiry_py,
                "instrument_type": "FUT",
            }
        return out

    @staticmethod
    def _nearest_50(price: float) -> int:
        return int(round(float(price) / 50.0) * 50)

    def _resolve_atm_strike(self, price: float) -> int:
        """
        Map incoming NIFTY price to option strike domain.
        Handles feeds where index price may be scaled (e.g. 10x).
        """
        if not self._available_strikes:
            return self._nearest_50(price)
        raw = float(price)
        candidates = [raw, raw / 10.0, raw / 100.0, raw * 10.0]
        snapped = [self._nearest_50(x) for x in candidates if x > 0]
        min_s = min(self._available_strikes)
        max_s = max(self._available_strikes)
        in_range = [s for s in snapped if min_s <= s <= max_s]
        if in_range:
            target = in_range[0]
        else:
            target = min(snapped, key=lambda s: min(abs(s - min_s), abs(s - max_s)))
        # Snap to nearest actual listed strike in map.
        return min(self._available_strikes, key=lambda s: abs(s - target))

    @staticmethod
    def _first_ref(df: pd.DataFrame) -> int | None:
        if df.empty:
            return None
        return int(df.iloc[0]["ref_id"])

    def _strike_window(self, atm: int) -> list[int]:
        step = self._strike_step
        span = self._strike_radius * step
        low = atm - span
        high = atm + span
        return list(range(low, high + step, step))

    def _to_strike_domain(self, atm_rupee: int) -> int:
        raw = int(round(atm_rupee * self._strike_scale))
        step = max(1, self._strike_step)
        return int(round(raw / step) * step)

    @staticmethod
    def _prefer_non_expired(df: pd.DataFrame, today: pd.Timestamp) -> pd.DataFrame:
        if df.empty:
            return df
        with_expiry = df[df["expiry_dt"].notna()].copy()
        if with_expiry.empty:
            return df.sort_values("expiry").head(1)
        non_expired = with_expiry[with_expiry["expiry_dt"] >= today]
        if non_expired.empty:
            return with_expiry.sort_values("expiry_dt").head(1)
        nearest_dt = non_expired["expiry_dt"].min()
        return non_expired[non_expired["expiry_dt"] == nearest_dt].sort_values("expiry_dt")

    def _prefer_non_expired_per_asset(self, df: pd.DataFrame, today: pd.Timestamp) -> pd.DataFrame:
        if df.empty:
            return df
        rows: list[pd.Series] = []
        for _, asset_df in df.groupby("asset", dropna=False):
            picked = self._prefer_non_expired(asset_df.copy(), today)
            if not picked.empty:
                rows.append(picked.iloc[0])
        if not rows:
            return df.head(0)
        out = pd.DataFrame(rows)
        return out.sort_values(["asset", "expiry_dt"])

    @staticmethod
    def _extract_symbol(row: pd.Series) -> str:
        # Backward-compatible helper retained for external callers/tests.
        candidate_cols = ("symbol", "stock_name", "trading_symbol", "instrument_name", "indexname", "name", "token")
        for col in candidate_cols:
            if col in row and pd.notna(row[col]):
                val = str(row[col]).strip().upper()
                if val:
                    return val
        if "asset" in row and pd.notna(row["asset"]):
            return str(row["asset"]).strip().upper()
        return ""
