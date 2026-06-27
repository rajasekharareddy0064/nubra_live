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
            detail="market state store is not initialized on this server",
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
            detail="in-memory candle board is not initialized on this server",
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


@router.get("/debug/candle-scheduler")
async def get_candle_scheduler_debug() -> dict:
    from app.main import APP_STATE

    scheduler = APP_STATE.get("candle_scheduler")
    tasks = APP_STATE.get("tasks") or []
    task_state = []
    for task in tasks:
        name = getattr(task, "get_name", lambda: "")()
        if name == "Candle3mScheduler":
            task_state.append(
                {
                    "name": name,
                    "done": task.done(),
                    "cancelled": task.cancelled(),
                    "exception": repr(task.exception()) if task.done() and not task.cancelled() else None,
                }
            )

    hub_debug = {}
    hub = APP_STATE.get("hub")
    if hub is not None and hasattr(hub, "debug_snapshot"):
        hub_debug = hub.debug_snapshot()  # type: ignore[assignment, union-attr]

    candles = APP_STATE.get("candles")
    candle_debug = {}
    if candles is not None:
        candle_debug = {
            "open_index": candles.nifty.to_dict(),  # type: ignore[union-attr]
            "open_futures_count": len(candles.futures),  # type: ignore[union-attr]
            "open_stocks_count": len(candles.stock_futures),  # type: ignore[union-attr]
        }

    return {
        "scheduler": dict(scheduler) if isinstance(scheduler, dict) else {},
        "tasks": task_state,
        "hub": hub_debug,
        "candles": candle_debug,
    }


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


@router.post("/admin/shutdown")
async def admin_shutdown() -> dict:
    """Graceful shutdown: disconnect WS, flush DB, stop aggregation.

    Called by the market_stop Cloud Run Job before scaling to zero.
    Does not terminate the process — Cloud Run handles that when
    min-instances drops to 0 and there's no traffic.
    """
    import logging
    from app.main import APP_STATE

    logger = logging.getLogger("admin.shutdown")
    logger.info("ADMIN_SHUTDOWN_REQUESTED | reason=market_close")

    results = {}

    # 1. Stop ingestion (closes WebSocket)
    ingestion = APP_STATE.get("ingestion")
    if ingestion is not None:
        try:
            await ingestion.stop()
            results["websocket"] = "disconnected"
            logger.info("WEBSOCKET_DISCONNECTED | ingestion stopped")
        except Exception as exc:
            results["websocket"] = f"error: {exc}"
            logger.warning("WEBSOCKET_DISCONNECT_FAILED | %s", exc)
    else:
        results["websocket"] = "not_running"

    # 2. Flush database writer
    db_writer = APP_STATE.get("db_writer")
    if db_writer is not None and hasattr(db_writer, "flush"):
        try:
            await db_writer.flush()
            results["db_flush"] = "complete"
            logger.info("DB_FLUSH_COMPLETE | pending writes flushed")
        except Exception as exc:
            results["db_flush"] = f"error: {exc}"
            logger.warning("DB_FLUSH_FAILED | %s", exc)
    else:
        results["db_flush"] = "not_applicable"

    # 3. Cancel background tasks (aggregation, scheduler)
    tasks = APP_STATE.get("tasks", [])
    cancelled = 0
    for task in tasks:
        if not task.done():
            task.cancel()
            cancelled += 1
    results["tasks_cancelled"] = cancelled
    logger.info("TASKS_CANCELLED | count=%d", cancelled)

    APP_STATE["shutdown_requested"] = True
    logger.info("ADMIN_SHUTDOWN_COMPLETE | results=%s", results)
    return {"status": "shutdown_initiated", "details": results}


@router.get("/health/ingestion")
async def get_ingestion_health() -> dict:
    """Ingestion health with simulation mode support."""
    from app.main import APP_STATE
    from app.core.config import settings

    if settings.is_simulation:
        stats = APP_STATE.get("simulation_stats")
        if stats is None:
            return {"mode": "SIMULATION", "status": "STARTING"}
        if hasattr(stats, "to_dict"):
            return stats.to_dict()
        return {"mode": "SIMULATION", "status": "UNKNOWN"}

    ingestion = APP_STATE.get("ingestion")
    if ingestion is None:
        return {"mode": "LIVE", "status": "NOT_STARTED", "socket": "uninitialised"}

    health_fn = getattr(ingestion, "health", None)
    if health_fn is None:
        return {"mode": "LIVE", "status": "NO_HEALTH_FN"}

    ws_health = health_fn()
    return {"mode": "LIVE", "status": "RUNNING", **ws_health}
