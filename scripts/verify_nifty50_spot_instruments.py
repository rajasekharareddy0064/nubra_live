"""Verify NIFTY50 cash (spot) instruments match InstrumentManager and Nubra SDK.

Cross-checks three resolution paths for each NIFTY50 constituent:
  1. InstrumentManager.get_stock_eq_meta()  (production path)
  2. SDK get_instrument_by_symbol()         (Nubra docs: trading symbol lookup)
  3. SDK get_instruments_by_pattern()       (Nubra docs: structured filter)

Usage:
  python scripts/verify_nifty50_spot_instruments.py
  python scripts/verify_nifty50_spot_instruments.py --cache instrument_master_cache.csv
  python scripts/verify_nifty50_spot_instruments.py --sdk --env PROD

Reference: https://nubra.io/products/api/docs/python-sdk-v3/get-instruments.html
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, ".")
from app.core.config import settings
from app.core.env_loader import load_project_env
from app.instruments.manager import InstrumentManager
from app.instruments.nifty50 import (
    NIFTY50_MASTER_ASSET_ALIASES,
    NIFTY50_SYMBOLS_SORTED,
    nifty50_canonical_symbol,
    nifty50_master_assets,
)


def _field(obj: Any, name: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _normalize_sdk_row(obj: Any) -> dict[str, Any] | None:
    if obj is None:
        return None
    ref_id = _field(obj, "ref_id")
    if ref_id is None:
        return None
    token = _field(obj, "token")
    return {
        "ref_id": int(ref_id),
        "symbol": str(_field(obj, "stock_name") or _field(obj, "asset") or "").strip().upper(),
        "asset": str(_field(obj, "asset") or "").strip().upper(),
        "derivative_type": str(_field(obj, "derivative_type") or "").strip().upper(),
        "asset_type": str(_field(obj, "asset_type") or "").strip().upper(),
        "symbol_token": str(int(token)) if token is not None and str(token).strip() else None,
        "exchange": str(_field(obj, "exchange") or "NSE").strip().upper(),
        "nubra_name": str(_field(obj, "nubra_name") or "").strip(),
    }


def _pattern_row_from_df(df: pd.DataFrame, index_symbol: str) -> dict[str, Any] | None:
    """Mirror SDK get_instruments_by_pattern filter on a local master DataFrame."""
    assets = nifty50_master_assets({index_symbol})
    subset = df[
        (df["derivative_type"].astype(str).str.upper() == "STOCK")
        & (df["asset_type"].astype(str).str.upper() == "STOCKS")
        & (df["asset"].astype(str).str.upper().isin(assets))
    ]
    if subset.empty:
        return None
    row = subset.iloc[0]
    token = row.get("token")
    asset = str(row.get("asset") or "").strip().upper()
    return {
        "ref_id": int(row["ref_id"]),
        "symbol": str(row.get("stock_name") or asset).strip().upper(),
        "asset": asset,
        "derivative_type": "STOCK",
        "asset_type": "STOCKS",
        "symbol_token": str(int(token)) if pd.notna(token) else None,
        "exchange": str(row.get("exchange") or "NSE").strip().upper(),
        "nubra_name": str(row.get("nubra_name") or "").strip(),
    }


def _load_sdk_helper(env_name: str):
    from app.ingestion.auth_client import get_authenticated_client
    from nubra_python_sdk.refdata.instruments import InstrumentData

    client = get_authenticated_client(env_name=env_name)
    return InstrumentData(client)


def _sdk_by_symbol(helper, symbol: str, exchange: str) -> dict[str, Any] | None:
    try:
        obj = helper.get_instrument_by_symbol(symbol, exchange=exchange)
    except TypeError:
        obj = helper.get_instrument_by_symbol(symbol)
    except Exception:
        return None
    row = _normalize_sdk_row(obj)
    if row and row["derivative_type"] == "STOCK" and row["asset_type"] == "STOCKS":
        return row
    return None


def _sdk_by_pattern(helper, index_symbol: str, exchange: str) -> dict[str, Any] | None:
    master_asset = NIFTY50_MASTER_ASSET_ALIASES.get(index_symbol, index_symbol)
    pattern = {
        "exchange": exchange,
        "asset": master_asset,
        "derivative_type": "STOCK",
        "asset_type": "STOCKS",
    }
    try:
        matches = helper.get_instruments_by_pattern([pattern])
    except Exception:
        return None
    if not matches:
        return None
    first = matches[0] if isinstance(matches, list) else matches
    if isinstance(first, list):
        first = first[0] if first else None
    return _normalize_sdk_row(first)


def _compare_rows(
    index_symbol: str,
    manager_meta: dict[str, Any] | None,
    pattern_row: dict[str, Any] | None,
    symbol_row: dict[str, Any] | None,
    sdk_pattern_row: dict[str, Any] | None,
) -> list[str]:
    issues: list[str] = []
    if manager_meta is None:
        issues.append("manager_missing")
        return issues

    canonical = nifty50_canonical_symbol(index_symbol)
    if manager_meta.get("symbol") != canonical:
        issues.append(f"manager_symbol={manager_meta.get('symbol')}!={canonical}")

    for label, row in (
        ("pattern_df", pattern_row),
        ("sdk_symbol", symbol_row),
        ("sdk_pattern", sdk_pattern_row),
    ):
        if row is None:
            if label.startswith("sdk_"):
                continue
            issues.append(f"{label}_missing")
            continue
        if int(row["ref_id"]) != int(manager_meta["ref_id"]):
            issues.append(f"{label}_ref_id={row['ref_id']}!={manager_meta['ref_id']}")
        mgr_token = manager_meta.get("symbol_token")
        row_token = row.get("symbol_token")
        if mgr_token and row_token and str(mgr_token) != str(row_token):
            issues.append(f"{label}_token={row_token}!={mgr_token}")
    return issues


def main() -> int:
    load_project_env(".")

    parser = argparse.ArgumentParser(description="Verify NIFTY50 spot instrument resolution")
    parser.add_argument(
        "--cache",
        default=settings.instrument_cache_file,
        help="Local instrument master CSV (default: instrument_master_cache.csv)",
    )
    parser.add_argument(
        "--env",
        default=settings.nubra_env,
        help="Nubra environment for live SDK checks (UAT/PROD)",
    )
    parser.add_argument(
        "--exchange",
        default=settings.nubra_exchange,
        help="Exchange passed to SDK lookups (default: NSE)",
    )
    parser.add_argument(
        "--sdk",
        action="store_true",
        help="Also cross-check live SDK get_instrument_by_symbol / get_instruments_by_pattern",
    )
    args = parser.parse_args()

    cache_path = Path(args.cache)
    if not cache_path.exists():
        print(f"ERROR: cache not found: {cache_path.resolve()}")
        return 1

    def _csv_fetcher() -> pd.DataFrame:
        return pd.read_csv(cache_path, low_memory=False)

    mgr = InstrumentManager(
        env_name=str(args.env).upper(),
        use_env_creds=False,
        local_cache_csv=None,
        instrument_fetcher=_csv_fetcher,
    )
    raw_df = _csv_fetcher()
    raw_df.columns = [c.strip().lower() for c in raw_df.columns]

    sdk_helper = None
    if args.sdk:
        try:
            sdk_helper = _load_sdk_helper(str(args.env).upper())
            print(f"SDK helper loaded (env={args.env}, exchange={args.exchange})")
        except Exception as exc:
            print(f"WARN: could not load SDK helper: {exc}")
            print("      Continuing with cache + InstrumentManager checks only.")

    print("=" * 90)
    print(f"NIFTY50 spot instrument verification | cache={cache_path.name} | manager_eq={len(mgr.get_stock_equity())}")
    print("=" * 90)

    ok = 0
    failed: list[tuple[str, list[str]]] = []
    for symbol in NIFTY50_SYMBOLS_SORTED:
        manager_meta = mgr.get_stock_eq_meta(symbol)
        pattern_row = _pattern_row_from_df(raw_df, symbol)
        symbol_row = sdk_pattern_row = None
        if sdk_helper is not None:
            symbol_row = _sdk_by_symbol(sdk_helper, symbol, args.exchange)
            if symbol_row is None:
                alias = NIFTY50_MASTER_ASSET_ALIASES.get(symbol)
                if alias:
                    symbol_row = _sdk_by_symbol(sdk_helper, alias, args.exchange)
            sdk_pattern_row = _sdk_by_pattern(sdk_helper, symbol, args.exchange)

        issues = _compare_rows(symbol, manager_meta, pattern_row, symbol_row, sdk_pattern_row)
        if issues:
            failed.append((symbol, issues))
            print(f"FAIL {symbol:14} {', '.join(issues)}")
            if manager_meta:
                print(f"       manager: ref_id={manager_meta.get('ref_id')} token={manager_meta.get('symbol_token')}")
            if pattern_row:
                print(f"       pattern: ref_id={pattern_row.get('ref_id')} asset={pattern_row.get('asset')}")
        else:
            ok += 1
            meta = manager_meta or {}
            print(
                f"OK   {symbol:14} ref_id={meta.get('ref_id')} "
                f"token={meta.get('symbol_token')} exchange={meta.get('exchange')}"
            )

    print("=" * 90)
    print(f"Summary: ok={ok}/{len(NIFTY50_SYMBOLS_SORTED)} failed={len(failed)}")
    if failed:
        missing = [sym for sym, issues in failed if "manager_missing" in issues]
        if missing:
            print(f"Missing from manager: {', '.join(missing)}")
        print("Fix: refresh instrument_master_cache.csv from SDK (delete stale cache and restart).")
        return 1

    if sdk_helper is None and not args.sdk:
        print("Tip: re-run with --sdk to cross-check live Nubra SDK lookups.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
