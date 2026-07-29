"""Canonical NIFTY 50 constituent symbols for stock-futures subscriptions."""
from __future__ import annotations

# Matches latest index constituent list (Apr 2026 weights); 49 names as provided.
NIFTY50_SYMBOLS: frozenset[str] = frozenset(
    {
        "ADANIENT",
        "ADANIPORTS",
        "APOLLOHOSP",
        "ASIANPAINT",
        "AXISBANK",
        "BAJAJ-AUTO",
        "BAJAJFINSV",
        "BAJFINANCE",
        "BEL",
        "BHARTIARTL",
        "CIPLA",
        "COALINDIA",
        "DRREDDY",
        "EICHERMOT",
        "ETERNAL",
        "GRASIM",
        "HCLTECH",
        "HDFCBANK",
        "HDFCLIFE",
        "HINDALCO",
        "HINDUNILVR",
        "ICICIBANK",
        "INDIGO",
        "INFY",
        "ITC",
        "JIOFIN",
        "JSWSTEEL",
        "KOTAKBANK",
        "LT",
        "M&M",
        "MARUTI",
        "MAXHEALTH",
        "NTPC",
        "ONGC",
        "POWERGRID",
        "RELIANCE",
        "SBILIFE",
        "SBIN",
        "SHRIRAMFIN",
        "SUNPHARMA",
        "TATACONSUM",
        "TATASTEEL",
        "TATAMOTORS",
        "TCS",
        "TECHM",
        "TITAN",
        "TRENT",
        "ULTRACEMCO",
        "WIPRO",
    }
)

NIFTY50_SYMBOL_COUNT: int = len(NIFTY50_SYMBOLS)

# Sorted list for deterministic subscription / DB payloads.
NIFTY50_SYMBOLS_SORTED: tuple[str, ...] = tuple(sorted(NIFTY50_SYMBOLS))

# Back-compat aliases used elsewhere in the codebase.
DEFAULT_NIFTY50_SYMBOLS = NIFTY50_SYMBOLS
NIFTY50_UNDERLYINGS = NIFTY50_SYMBOLS

# Latest index weights (%), same constituent set as NIFTY50_SYMBOLS.
NIFTY50_WEIGHTS: dict[str, float] = {
    "RELIANCE": 8.92,
    "BHARTIARTL": 6.17,
    "HDFCBANK": 5.89,
    "ICICIBANK": 5.33,
    "SBIN": 4.86,
    "TCS": 4.51,
    "BAJFINANCE": 3.40,
    "LT": 2.74,
    "HINDUNILVR": 2.47,
    "SUNPHARMA": 2.47,
    "INFY": 2.33,
    "MARUTI": 2.26,
    "TITAN": 2.24,
    "ADANIPORTS": 2.13,
    "M&M": 2.12,
    "ADANIENT": 2.11,
    "KOTAKBANK": 1.99,
    "AXISBANK": 1.98,
    "HCLTECH": 1.86,
    "ITC": 1.85,
    "ULTRACEMCO": 1.83,
    "NTPC": 1.73,
    "BAJAJ-AUTO": 1.62,
    "BAJAJFINSV": 1.60,
    "JSWSTEEL": 1.59,
    "ONGC": 1.56,
    "ETERNAL": 1.54,
    "BEL": 1.48,
    "POWERGRID": 1.38,
    "ASIANPAINT": 1.36,
    "COALINDIA": 1.31,
    "SHRIRAMFIN": 1.27,
    "TATASTEEL": 1.19,
    "EICHERMOT": 1.12,
    "GRASIM": 1.10,
    "HINDALCO": 1.09,
    "INDIGO": 1.06,
    "SBILIFE": 0.99,
    "WIPRO": 0.93,
    "TECHM": 0.83,
    "JIOFIN": 0.81,
    "TRENT": 0.81,
    "APOLLOHOSP": 0.67,
    "HDFCLIFE": 0.63,
    "TATAMOTORS": 0.62,
    "CIPLA": 0.61,
    "MAXHEALTH": 0.56,
    "TATACONSUM": 0.56,
    "DRREDDY": 0.49,
}

# NSE F&O `asset` may differ from index symbol (e.g. post-demerger TMPV).
NIFTY50_MASTER_ASSET_ALIASES: dict[str, str] = {
    "TATAMOTORS": "TMPV",
}


def nifty50_master_assets(symbols: frozenset[str] | set[str]) -> set[str]:
    """Expand index symbols to instrument-master asset names for FUT lookup."""
    assets = {str(s).strip().upper() for s in symbols if str(s).strip()}
    for symbol in list(assets):
        alias = NIFTY50_MASTER_ASSET_ALIASES.get(symbol)
        if alias:
            assets.add(alias)
    return assets


def nifty50_canonical_symbol(asset: str) -> str:
    """Map instrument-master asset back to index symbol when aliased."""
    asset_u = str(asset or "").strip().upper()
    for index_symbol, master_asset in NIFTY50_MASTER_ASSET_ALIASES.items():
        if asset_u == master_asset:
            return index_symbol
    return asset_u


if set(NIFTY50_WEIGHTS) != NIFTY50_SYMBOLS:
    missing_weights = sorted(NIFTY50_SYMBOLS - set(NIFTY50_WEIGHTS))
    extra_weights = sorted(set(NIFTY50_WEIGHTS) - NIFTY50_SYMBOLS)
    raise ValueError(
        "NIFTY50_WEIGHTS must cover exactly NIFTY50_SYMBOLS; "
        f"missing={missing_weights} extra={extra_weights}"
    )
