import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, Awaitable

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response

from app.api.routes import router as rest_router
from app.api.ws import router as ws_router
from app.core.config import settings
from app.core.env_loader import load_project_env
from app.core.logging import setup_logging
from app.instruments.nifty50 import NIFTY50_SYMBOL_COUNT
from bootstrap_auth import bootstrap_auth

# --- Non-interactive bootstrap ----------------------------------------------
# Must run before any module imports nubra_python_sdk transitively.

load_project_env(".")
# ---------------------------------------------------------------------------

# Stable per-process identity. Cloud Run does not expose an instance id via
# env, so we mint one at import time. A correctly configured SINGLETON service
# must only ever show ONE of these UIDs across all logs for a given revision —
# that is our proof that no sibling instances were spawned.
INSTANCE_UID: str = uuid.uuid4().hex[:12]
K_REVISION: str = os.getenv("K_REVISION", "local")
K_SERVICE: str = os.getenv("K_SERVICE", "nubra-live")

from app.ingestion.auth_client import run_session_refresh_loop
from app.ingestion.nubra_socket import NubraIngestionService
from app.queue.broker import QueueBroker
from app.signals.engine import SignalEngine
from app.state.memory_store import MemoryStore
from app.state.redis_store import RedisStore
from app.storage.db_writer import DBWriter
from app.workers.greeks_worker import GreeksWorker
from app.workers.index_worker import IndexWorker
from app.workers.ohlcv_worker import OhlcvWorker
from app.workers.options_worker import OptionsWorker
from app.workers.orderbook_worker import OrderbookWorker


APP_STATE: dict[str, object] = {}


def _track_task(
    tasks: list[asyncio.Task[Any]],
    coro: Awaitable[Any],
    *,
    name: str,
    logger: logging.Logger,
) -> asyncio.Task[Any]:
    async def runner() -> None:
        logger.info("background task started: %s", name)
        try:
            await coro
        except asyncio.CancelledError:
            logger.info("background task cancelled: %s", name)
            raise
        except Exception:
            logger.exception("background task failed: %s", name)

    task = asyncio.create_task(runner(), name=name)
    tasks.append(task)
    return task


async def _start_nubra_background(
    ingestion: NubraIngestionService,
    *,
    env_name: str,
    logger: logging.Logger,
) -> None:
    APP_STATE["ingestion_status"] = {"state": "starting", "error": None}
    APP_STATE["ingestion"] = ingestion
    try:
        await ingestion.start()
        APP_STATE["ingestion_status"] = {"state": "ready", "error": None}
        logger.info("Nubra ingestion enabled")
    except Exception as exc:
        APP_STATE["ingestion_status"] = {
            "state": "error",
            "error": repr(exc),
        }
        logger.exception("Nubra ingestion startup failed: %s", exc)
        APP_STATE["ingestion_status"] = {"state": "error", "error": repr(exc)}
        logger.exception("Nubra ingestion startup failed")
        return

    await run_session_refresh_loop(
        env_name=env_name,
        on_refresh=ingestion.reconnect_websocket,
    )


async def _start_database_background(
    *,
    broker: QueueBroker,
    redis_store: RedisStore | MemoryStore,
    db_writer: DBWriter,
    logger: logging.Logger,
) -> None:
    child_tasks: list[asyncio.Task[Any]] = []
    APP_STATE["database_status"] = {"state": "connecting", "error": None}
    try:
        logger.info("background db connect begin")
        await asyncio.wait_for(db_writer.connect(), timeout=120.0)
        APP_STATE["database_status"] = {"state": "ready", "error": None}
        logger.info("background db connect complete")

        workers = [
            IndexWorker(broker, redis_store, db_writer),
            OptionsWorker(broker, redis_store, db_writer),
            OrderbookWorker(broker, redis_store, db_writer),
            GreeksWorker(broker, redis_store, db_writer),
            OhlcvWorker(broker, redis_store, db_writer),
        ]
        for worker in workers:
            child_tasks.append(
                asyncio.create_task(
                    worker.run_forever(),
                    name=f"{worker.__class__.__name__}.run",
                )
            )

        signal_engine = SignalEngine(redis_store=redis_store, symbols=["NIFTY"])
        child_tasks.append(asyncio.create_task(signal_engine.run_forever(), name="SignalEngine.run"))
        child_tasks.append(asyncio.create_task(db_writer.run_forever(), name="DBWriter.run"))

        await asyncio.gather(*child_tasks)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        APP_STATE["database_status"] = {"state": "error", "error": repr(exc)}
        logger.exception("database background startup/runtime failed")
    finally:
        for task in child_tasks:
            task.cancel()
        if child_tasks:
            await asyncio.gather(*child_tasks, return_exceptions=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    setup_logging(settings.log_level)
    load_project_env(".")
    logger = logging.getLogger("app.main")
    logger.info(
        "STARTUP_BEGIN | instance_uid=%s service=%s revision=%s mode=%s",
        INSTANCE_UID,
        K_SERVICE,
        K_REVISION,
        "simulation" if settings.is_simulation else ("live" if settings.enable_nubra_socket else "disabled"),
    )
    logger.info("startup begin: configuring lightweight app state")

    tasks: list[asyncio.Task[Any]] = []
    broker = QueueBroker(maxsize=settings.queue_maxsize)
    redis_store: RedisStore | MemoryStore = (
        RedisStore(settings.redis_url) if settings.use_redis else MemoryStore()
    )

    from app.realtime.candles import CandleBoard
    from app.realtime.hub import LiveHub
    from app.realtime.market_state import MarketStateStore
    from app.realtime.nifty_ohlc import NiftyOhlcAggregator
    from app.realtime.order_book import OrderBookAggregator
    from app.realtime.pipeline import RealtimePipeline, run_interval_scheduler

    hub = LiveHub()
    candles = CandleBoard()
    market_state = MarketStateStore()
    nifty_ohlc_aggregator = NiftyOhlcAggregator()
    order_book_aggregator = OrderBookAggregator()

    APP_STATE.clear()
    APP_STATE.update(
        {
            "broker": broker,
            "redis": redis_store,
            "hub": hub,
            "market_state": market_state,
            "candles": candles,
            "nifty_ohlc_aggregator": nifty_ohlc_aggregator,
            "order_book_aggregator": order_book_aggregator,
            "candle_scheduler": {},
            "startup_mode": "database" if settings.use_database else "realtime",
        }
    )

    realtime_broker = broker
    db_writer: DBWriter | None = None
    database_task_kwargs: dict[str, Any] | None = None
    if settings.use_database:
        realtime_broker = QueueBroker(maxsize=settings.queue_maxsize)
        db_writer = DBWriter(
            dsn=settings.database_dsn,
            batch_size=settings.db_batch_size,
            flush_interval_ms=settings.db_flush_interval_ms,
            schema=settings.db_schema,
        )
        APP_STATE["db_writer"] = db_writer
        APP_STATE["database_status"] = {"state": "scheduled", "error": None}
        database_task_kwargs = {
            "broker": broker,
            "redis_store": redis_store,
            "db_writer": db_writer,
            "logger": logger,
        }

    ingestion: NubraIngestionService | None = None
    if settings.is_simulation:
        # ── SIMULATION MODE ──────────────────────────────────────────
        # Replace live WebSocket with sample data replay.
        # The downstream pipeline (broker -> candles -> hub -> db) is identical.
        from app.simulation.simulator import MarketSimulator, SimulationStats

        APP_STATE["ingestion_status"] = {"state": "simulation", "error": None}
        APP_STATE["simulation_stats"] = SimulationStats()
        logger.info(
            "SIMULATION_MODE | speed=%sx data_dir=%s",
            settings.simulation_speed,
            settings.sample_data_dir,
        )

        simulator = MarketSimulator(
            broker=broker,
            speed=settings.simulation_speed,
            data_dir=settings.sample_data_dir,
        )
        APP_STATE["simulator"] = simulator

        async def _run_simulation() -> None:
            stats = await simulator.run()
            APP_STATE["simulation_stats"] = stats
            APP_STATE["ingestion_status"] = {
                "state": "simulation_complete",
                "error": None,
                "stats": stats.to_dict(),
            }

        _track_task(tasks, _run_simulation(), name="MarketSimulator", logger=logger)

    elif settings.enable_nubra_socket:
        # Auth bootstrap + ingestion startup are moved to a background task so
        # a failed / expired session does NOT crash the container at startup.
        # The API stays healthy; /health/ready exposes the auth state.
        APP_STATE["auth"] = {"auth_dir": None, "regenerated": False}
        APP_STATE["ingestion_status"] = {"state": "scheduled", "error": None}

        async def _boot_ingestion() -> None:
            """Bootstrap auth then start ingestion — all in background."""
            try:
                auth_result = await bootstrap_auth(env_name=settings.nubra_env)
                APP_STATE["auth"] = {
                    "auth_dir": auth_result.auth_dir,
                    "regenerated": auth_result.regenerated,
                }
            except Exception as exc:
                APP_STATE["auth"] = {"auth_dir": None, "regenerated": False}
                APP_STATE["ingestion_status"] = {"state": "auth_error", "error": repr(exc)}
                logger.error(
                    "Nubra auth bootstrap failed — ingestion disabled: %s", exc
                )
                return

            svc = NubraIngestionService(
                broker=broker,
                env_name=settings.nubra_env,
                exchange=settings.nubra_exchange,
                initial_nifty_price=settings.initial_nifty_price,
                strike_radius=settings.strike_radius,
                include_sdk_ohlcv=settings.subscribe_sdk_ohlcv,
                include_sdk_option_chain=settings.subscribe_sdk_option_chain,
                extra_brokers=(realtime_broker,) if settings.use_database else None,
            )
            APP_STATE["ingestion"] = svc
            nonlocal ingestion
            ingestion = svc
            pipeline._ingestion = svc  # wire ingestion into pipeline so _price_scale() returns the correct value
            await _start_nubra_background(
                svc,
                env_name=settings.nubra_env,
                logger=logger,
            )

        _track_task(tasks, _boot_ingestion(), name="NubraAuthAndIngestion", logger=logger)
    else:
        APP_STATE["ingestion_status"] = {"state": "disabled", "error": None}
        logger.warning("Nubra ingestion disabled; set ENABLE_NUBRA_SOCKET=true for live ticks")

    pipeline = RealtimePipeline(
        broker=realtime_broker,
        hub=hub,
        candles=candles,
        state=market_state,
        initial_nifty_price=settings.initial_nifty_price,
        ingestion=ingestion,
        nifty_ohlc_aggregator=nifty_ohlc_aggregator,
        order_book_aggregator=order_book_aggregator,
    )
    _track_task(tasks, pipeline.run_forever(), name="RealtimePipeline.run", logger=logger)

    # ── STARTUP GATE ──────────────────────────────────────────────────
    # The 3-minute scheduler must NOT run until the instrument master is
    # loaded and the three reference maps (opt/fut/stock) are populated.
    # Starting early is exactly what produced empty / zero order-book
    # snapshots: ticks arrive but every ref_id is unresolved, so the
    # aggregator stays empty and the scheduler writes zero rows.
    live_mode = settings.enable_nubra_socket and not settings.is_simulation

    async def _gated_interval_scheduler() -> None:
        gate_log = logging.getLogger("realtime.scheduler.gate")
        if live_mode:
            attempt = 0
            while True:
                attempt += 1
                ing = pipeline._ingestion
                state_status = APP_STATE.get("ingestion_status", {})
                ingestion_state = state_status.get("state") if isinstance(state_status, dict) else None
                if ing is None or getattr(ing, "instrument_manager", None) is None:
                    gate_log.info(
                        "STARTUP_GATE | uid=%s attempt=%d waiting reason=ingestion_not_ready ingestion_state=%s",
                        INSTANCE_UID, attempt, ingestion_state,
                    )
                    await asyncio.sleep(2.0)
                    continue
                ms = ing.reference_map_status(pipeline._last_nifty)
                if not ms["master_loaded"]:
                    gate_log.warning(
                        "STARTUP_GATE | uid=%s attempt=%d waiting reason=instrument_master_not_loaded",
                        INSTANCE_UID, attempt,
                    )
                    await asyncio.sleep(2.0)
                    continue
                if ms["opt_map_size"] <= 0 or ms["fut_map_size"] <= 0 or ms["stock_map_size"] <= 0:
                    gate_log.warning(
                        "STARTUP_GATE | uid=%s attempt=%d waiting reason=empty_maps "
                        "opt_map_size=%d fut_map_size=%d stock_map_size=%d "
                        "(option_count=%d future_count=%d stock_count=%d expected_stock_count=%d)",
                        INSTANCE_UID, attempt,
                        ms["opt_map_size"], ms["fut_map_size"], ms["stock_map_size"],
                        ms["option_count"], ms["future_count"], ms["stock_count"],
                        ms.get("expected_stock_count", NIFTY50_SYMBOL_COUNT),
                    )
                    await asyncio.sleep(2.0)
                    continue
                if not ms.get("stock_count_ok"):
                    gate_log.warning(
                        "STARTUP_GATE | uid=%s attempt=%d waiting reason=nifty50_incomplete "
                        "stock_count=%d stock_map_size=%d expected=%d missing=%s",
                        INSTANCE_UID,
                        attempt,
                        ms["stock_count"],
                        ms["stock_map_size"],
                        ms.get("expected_stock_count", NIFTY50_SYMBOL_COUNT),
                        ms.get("missing_stock_symbols") or [],
                    )
                    await asyncio.sleep(2.0)
                    continue

                # Maps are ready. Prime the pipeline's ref_maps now so the
                # very first orderbook tick resolves (don't wait for a NIFTY
                # spot tick to populate them lazily).
                pipeline.refresh_ref_maps()
                ws_connected = ingestion_state == "ready"
                gate_log.info(
                    "STARTUP_HEALTH | uid=%s | revision=%s | service=%s\n"
                    "  instrument_master_loaded=%s (rows=%d)\n"
                    "  option_count=%d | future_count=%d | stock_count=%d | expected_stock_count=%d\n"
                    "  opt_map_size=%d | fut_map_size=%d | stock_map_size=%d\n"
                    "  missing_stock_symbols=%s\n"
                    "  websocket_connected=%s | scheduler_started=True | gate_attempts=%d",
                    INSTANCE_UID, K_REVISION, K_SERVICE,
                    ms["master_loaded"], ms["master_rows"],
                    ms["option_count"], ms["future_count"], ms["stock_count"],
                    ms.get("expected_stock_count", NIFTY50_SYMBOL_COUNT),
                    ms["opt_map_size"], ms["fut_map_size"], ms["stock_map_size"],
                    ms.get("missing_stock_symbols") or [],
                    ws_connected, attempt,
                )
                APP_STATE["scheduler_gate"] = {
                    "started": True,
                    "instance_uid": INSTANCE_UID,
                    "revision": K_REVISION,
                    "attempts": attempt,
                    "websocket_connected": ws_connected,
                    **ms,
                }
                break
        else:
            gate_log.info(
                "STARTUP_GATE | uid=%s non-live mode — starting scheduler immediately", INSTANCE_UID
            )
            APP_STATE["scheduler_gate"] = {"started": True, "instance_uid": INSTANCE_UID, "mode": "non_live"}

        await run_interval_scheduler(
            hub,
            candles,
            market_state,
            interval_minutes=settings.candle_interval_minutes,
            tz_name=settings.market_timezone,
            debug_state=APP_STATE["candle_scheduler"],  # type: ignore[arg-type]
            nifty_ohlc_aggregator=nifty_ohlc_aggregator,
            order_book_aggregator=order_book_aggregator,
            db_writer=db_writer,
            instance_uid=INSTANCE_UID,
        )

    _track_task(
        tasks,
        _gated_interval_scheduler(),
        name="Candle3mScheduler",
        logger=logger,
    )

    if live_mode and settings.enable_historical_compare:
        from app.historical.poller import HistoricalComparePoller

        hist_poller = HistoricalComparePoller(hub)
        APP_STATE["historical_compare"] = hist_poller
        _track_task(
            tasks,
            hist_poller.run_forever(),
            name="HistoricalComparePoller",
            logger=logger,
        )
        logger.info("historical compare poller scheduled")

    if database_task_kwargs is not None:
        _track_task(
            tasks,
            _start_database_background(**database_task_kwargs),
            name="DatabaseRuntime",
            logger=logger,
        )

    APP_STATE["tasks"] = tasks
    logger.info("startup complete: app is accepting traffic; background tasks scheduled=%d", len(tasks))

    yield

    logger.info("shutdown begin")
    ingestion_obj = APP_STATE.get("ingestion")
    if ingestion_obj is not None:
        try:
            await ingestion_obj.stop()  # type: ignore[union-attr]
        except Exception:
            logger.exception("ingestion.stop raised")

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    dbw = APP_STATE.get("db_writer")
    if dbw is not None:
        try:
            await dbw.close()  # type: ignore[union-attr]
        except Exception:
            logger.exception("db_writer.close raised")

    rs = APP_STATE.get("redis")
    if rs is not None:
        await rs.close()  # type: ignore[union-attr]
    logger.info("shutdown complete")


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(rest_router)
app.include_router(ws_router)


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": settings.app_name,
        "docs": "/docs",
        "openapi": "/openapi.json",
        "health": "/health",
        "websocket": "/ws/live",
        "realtime_snapshot": "/realtime/snapshot",
    }


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/health")
async def health() -> Response:
    ingestion_status = APP_STATE.get("ingestion_status", {"state": "unknown"})
    ingestion_state = ingestion_status.get("state", "unknown")
    enable_nubra = getattr(settings, "enable_nubra_socket", False)

    # Return 503 when auth has failed and Nubra ingestion is required.
    if ingestion_state == "auth_error" and enable_nubra:
        return JSONResponse(
            status_code=503,
            content={
                "status": "degraded",
                "reason": "auth_error",
                "detail": ingestion_status.get("error"),
            },
        )
    return JSONResponse(status_code=200, content={"status": "ok"})


@app.get("/health/ready")
async def ready() -> dict[str, Any]:
    dbw = APP_STATE.get("db_writer")
    auth_state = APP_STATE.get("auth", {})
    ingestion_status = APP_STATE.get("ingestion_status", {"state": "unknown"})
    return {
        "status": "ok",
        "instance_uid": INSTANCE_UID,
        "revision": K_REVISION,
        "service": K_SERVICE,
        "startup_mode": APP_STATE.get("startup_mode"),
        "scheduler_gate": APP_STATE.get("scheduler_gate", {"started": False}),
        "ingestion": ingestion_status,
        "auth": {
            "auth_dir": auth_state.get("auth_dir"),
            "regenerated": auth_state.get("regenerated"),
            "state": ingestion_status.get("state"),
            "error": ingestion_status.get("error") if ingestion_status.get("state") == "auth_error" else None,
        },
        "database": APP_STATE.get("database_status", {"state": "not_used"}),
        "db_writer": dbw.stats() if dbw is not None and hasattr(dbw, "stats") else None,  # type: ignore[union-attr]
        "tasks": [
            {
                "name": task.get_name(),
                "done": task.done(),
                "cancelled": task.cancelled(),
            }
            for task in APP_STATE.get("tasks", [])  # type: ignore[union-attr]
        ],
    }


@app.get("/health/auth")
async def health_auth() -> Response:
    """Detailed auth diagnostics for ops."""
    ingestion_status = APP_STATE.get("ingestion_status", {"state": "unknown"})
    auth_state = APP_STATE.get("auth", {})
    ingestion_state = ingestion_status.get("state", "unknown")

    payload: dict[str, Any] = {
        "ingestion_state": ingestion_state,
        "ingestion_error": ingestion_status.get("error"),
        "auth_dir": auth_state.get("auth_dir"),
        "regenerated": auth_state.get("regenerated"),
        "enable_nubra_socket": getattr(settings, "enable_nubra_socket", False),
        "nubra_env": getattr(settings, "nubra_env", None),
        "x_device_id_set": bool(
            __import__("os").getenv("NUBRA_X_DEVICE_ID") or __import__("os").getenv("X_DEVICE_ID")
        ),
        "session_token_set": bool(
            __import__("os").getenv("NUBRA_SESSION_TOKEN") or __import__("os").getenv("SESSION_TOKEN")
        ),
        "totp_secret_set": bool(__import__("os").getenv("NUBRA_TOTP_SECRET")),
    }

    status_code = 503 if ingestion_state == "auth_error" else 200
    return JSONResponse(status_code=status_code, content=payload)
