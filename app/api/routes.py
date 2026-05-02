from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException

from app.realtime.interval_clock import floor_to_interval, market_tz
from app.state.memory_store import MemoryStore
from app.state.redis_store import RedisStore

router = APIRouter()

Store = RedisStore | MemoryStore


def get_store() -> Store:
    # populated in app.main at startup
    from app.main import APP_STATE

    return APP_STATE["redis"]  # type: ignore[return-value]


@router.get("/realtime/snapshot")
async def get_realtime_snapshot() -> dict:
    from app.main import APP_STATE

    ms = APP_STATE.get("market_state")
    if ms is None:
        raise HTTPException(
            status_code=404,
            detail="realtime snapshot only when running without DB (use_database=false)",
        )
    return ms.snapshot()  # type: ignore[no-any-return]


@router.get("/state/{stream}/{key}")
async def get_stream_state(stream: str, key: str, store: Store = Depends(get_store)) -> dict:
    data = await store.hgetall(f"state:{stream}:{key}")
    if not data:
        raise HTTPException(status_code=404, detail="state not found")
    return data


@router.get("/signals/{symbol}")
async def get_latest_signal(symbol: str, store: Store = Depends(get_store)) -> dict:
    signal = await store.get_json(f"signals:latest:{symbol}")
    if not signal:
        raise HTTPException(status_code=404, detail="signal not found")
    return signal


@router.get("/candles/{symbol}/{interval}")
async def get_live_candle(symbol: str, interval: str, store: Store = Depends(get_store)) -> dict:
    data = await store.hgetall(f"state:ohlcv:{symbol}:{interval}")
    if not data:
        raise HTTPException(status_code=404, detail="candle not found")
    return data


@router.get("/realtime/candles/current")
async def get_current_candles() -> dict:
    """Snapshot of the in-progress (open) candle for the *current* bucket.

    The 3-minute scheduler in :func:`app.realtime.pipeline.run_interval_scheduler`
    only emits candles **at** bucket boundaries (e.g. at wall-clock
    12:06:00 it publishes the just-closed bar ``[12:03, 12:06)``). This
    endpoint exposes the **forming** bar — the one whose ``bucket_start``
    is ``floor_to_interval(now)`` — so a poller asking "what's the
    candle right now?" at 12:06:30 sees the 12:06 bar (with whatever
    ticks have arrived since 12:06:00), not the closed 12:03 bar.

    Returns ``{}`` shape mirroring the WebSocket ``candle_3m`` message
    (``bucket_start`` / ``bucket_end`` / ``meta.index`` /
    ``futures`` / ``stocks``) so clients can use one schema for both
    closed and open bars.
    """
    from app.core.config import settings
    from app.main import APP_STATE

    candle_board = APP_STATE.get("candles")
    if candle_board is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "in-memory candle board is only available in realtime mode "
                "(use_database=false). With Postgres enabled, query "
                "/candles/{symbol}/{interval} instead."
            ),
        )

    interval_minutes = int(settings.candle_interval_minutes)
    tz = market_tz(settings.market_timezone)
    now = datetime.now(tz)
    bucket_start = floor_to_interval(now, interval_minutes, tz)
    bucket_end = bucket_start + timedelta(minutes=interval_minutes)

    from app.realtime.pipeline import _get_underlying

    futures = {k: v.to_dict() for k, v in candle_board.futures.items()}  # type: ignore[union-attr]
    stocks = {k: v.to_dict() for k, v in candle_board.stock_futures.items()}  # type: ignore[union-attr]

    # Mirror the closed-bar enrichment from run_interval_scheduler so
    # this endpoint and the WebSocket candle_3m / candle_3m_open
    # broadcasts share the same schema.
    for sym, candle in stocks.items():
        candle["underlying_symbol"] = _get_underlying(sym)

    nifty_fut_contracts: dict[str, str] = {}
    ingestion = APP_STATE.get("ingestion")
    if ingestion is not None and getattr(ingestion, "instrument_manager", None) is not None:
        manager = ingestion.instrument_manager
        try:
            contract_by_ref = manager.get_nifty_fut_contracts()
            symbol_by_ref = manager.get_nifty_fut_symbols()
            for ref_id, label in contract_by_ref.items():
                sym = symbol_by_ref.get(ref_id)
                if sym:
                    nifty_fut_contracts[sym] = label
                    if sym in futures:
                        futures[sym]["contract"] = label
                        futures[sym]["underlying_symbol"] = "NIFTY"
        except Exception:
            pass

    market_state = APP_STATE.get("market_state")
    chain: list = []
    metrics: dict = {}
    if market_state is not None:
        chain = list(getattr(market_state, "option_chain_view", []) or [])
        metrics = dict(getattr(market_state, "option_metrics", {}) or {})

    return {
        "type": "candle_3m_open",
        "bucket_start": bucket_start.isoformat(),
        "bucket_end": bucket_end.isoformat(),
        "interval_minutes": interval_minutes,
        "now": now.isoformat(),
        "seconds_into_bucket": (now - bucket_start).total_seconds(),
        "futures": futures,
        "stocks": stocks,
        "options": {
            "chain": chain,
            "metrics": metrics,
        },
        "meta": {
            "index": candle_board.nifty.to_dict(),  # type: ignore[union-attr]
            "nifty_fut_contracts": nifty_fut_contracts,
        },
    }


@router.get("/debug/subscriptions")
async def get_active_subscriptions() -> dict:
    from app.main import APP_STATE

    ingestion = APP_STATE.get("ingestion")
    if ingestion is None:
        raise HTTPException(status_code=404, detail="ingestion not initialized")
    return getattr(ingestion, "last_subscriptions", {})


@router.get("/debug/auth-preflight")
async def get_auth_preflight() -> dict:
    from app.main import APP_STATE

    ingestion = APP_STATE.get("ingestion")
    if ingestion is not None:
        return getattr(ingestion, "auth_status", {})

    # If ingestion is disabled, still allow debug visibility.
    from app.ingestion.auth_preflight import auth_preflight_status

    return auth_preflight_status()


@router.get("/debug/options-subscription")
async def get_options_subscription() -> dict:
    from app.main import APP_STATE

    ingestion = APP_STATE.get("ingestion")
    if ingestion is None or getattr(ingestion, "instrument_manager", None) is None:
        raise HTTPException(status_code=404, detail="ingestion/instrument manager not initialized")

    manager = ingestion.instrument_manager
    price = getattr(ingestion, "initial_nifty_price", 0.0)
    try:
        return manager.get_option_subscription_payload(price)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"option subscription payload unavailable: {exc}") from exc


@router.get("/health/ws")
async def get_ws_health() -> dict:
    """Real-time health snapshot of the Nubra WebSocket manager."""
    from app.main import APP_STATE

    ingestion = APP_STATE.get("ingestion")
    if ingestion is None:
        return {
            "socket": "uninitialised",
            "auth": "n/a",
            "last_tick_seconds_ago": None,
            "tick_rate": 0.0,
        }
    health_fn = getattr(ingestion, "health", None)
    if health_fn is None:
        raise HTTPException(status_code=500, detail="ingestion does not expose health()")
    return health_fn()
