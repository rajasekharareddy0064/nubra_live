import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Awaitable

from fastapi import FastAPI
from fastapi.responses import Response

from app.api.routes import router as rest_router
from app.api.ws import router as ws_router
from app.core.config import settings
from app.core.env_loader import load_project_env
from app.core.logging import setup_logging

# --- Non-interactive bootstrap ----------------------------------------------
# Must run before any module imports nubra_python_sdk transitively.
from app.ingestion.input_patch import install_non_interactive_input_patch

load_project_env(".")
install_non_interactive_input_patch(require_totp_secret=False)
# ---------------------------------------------------------------------------

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
        await asyncio.wait_for(ingestion.start(), timeout=45.0)
        APP_STATE["ingestion_status"] = {"state": "ready", "error": None}
        logger.info("Nubra ingestion enabled")
    except asyncio.TimeoutError:
        APP_STATE["ingestion_status"] = {
            "state": "timeout",
            "error": "ingestion.start exceeded 45s",
        }
        logger.exception("Nubra ingestion startup timed out")
        return
    except Exception as exc:
        APP_STATE["ingestion_status"] = {"state": "error", "error": repr(exc)}
        logger.exception("Nubra ingestion startup failed")
        return

    await run_session_refresh_loop(env_name=env_name)


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
        await asyncio.wait_for(db_writer.connect(), timeout=15.0)
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
    logger.info("startup begin: configuring lightweight app state")

    tasks: list[asyncio.Task[Any]] = []
    broker = QueueBroker(maxsize=settings.queue_maxsize)
    redis_store: RedisStore | MemoryStore = (
        RedisStore(settings.redis_url) if settings.use_redis else MemoryStore()
    )

    from app.realtime.candles import CandleBoard
    from app.realtime.hub import LiveHub
    from app.realtime.market_state import MarketStateStore
    from app.realtime.pipeline import RealtimePipeline, run_interval_scheduler

    hub = LiveHub()
    candles = CandleBoard()
    market_state = MarketStateStore()

    APP_STATE.clear()
    APP_STATE.update(
        {
            "broker": broker,
            "redis": redis_store,
            "hub": hub,
            "market_state": market_state,
            "candles": candles,
            "candle_scheduler": {},
            "startup_mode": "database" if settings.use_database else "realtime",
        }
    )

    realtime_broker = broker
    if settings.use_database:
        realtime_broker = QueueBroker(maxsize=settings.queue_maxsize)
        db_writer = DBWriter(
            dsn=settings.postgres_dsn,
            batch_size=settings.db_batch_size,
            flush_interval_ms=settings.db_flush_interval_ms,
        )
        APP_STATE["db_writer"] = db_writer
        APP_STATE["database_status"] = {"state": "scheduled", "error": None}
        _track_task(
            tasks,
            _start_database_background(
                broker=broker,
                redis_store=redis_store,
                db_writer=db_writer,
                logger=logger,
            ),
            name="DatabaseRuntime",
            logger=logger,
        )

    ingestion: NubraIngestionService | None = None
    if settings.enable_nubra_socket:
        ingestion = NubraIngestionService(
            broker=broker,
            env_name=settings.nubra_env,
            exchange=settings.nubra_exchange,
            initial_nifty_price=settings.initial_nifty_price,
            strike_radius=settings.strike_radius,
            include_sdk_ohlcv=settings.subscribe_sdk_ohlcv,
            include_sdk_option_chain=settings.subscribe_sdk_option_chain,
            extra_brokers=(realtime_broker,) if settings.use_database else None,
        )
        APP_STATE["ingestion"] = ingestion
        APP_STATE["ingestion_status"] = {"state": "scheduled", "error": None}
        _track_task(
            tasks,
            _start_nubra_background(
                ingestion,
                env_name=settings.nubra_env,
                logger=logger,
            ),
            name="NubraIngestionBootstrap",
            logger=logger,
        )
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
    )
    _track_task(tasks, pipeline.run_forever(), name="RealtimePipeline.run", logger=logger)
    _track_task(
        tasks,
        run_interval_scheduler(
            hub,
            candles,
            market_state,
            interval_minutes=settings.candle_interval_minutes,
            tz_name=settings.market_timezone,
            debug_state=APP_STATE["candle_scheduler"],  # type: ignore[arg-type]
        ),
        name="Candle3mScheduler",
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
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
async def ready() -> dict[str, Any]:
    return {
        "status": "ok",
        "startup_mode": APP_STATE.get("startup_mode"),
        "ingestion": APP_STATE.get("ingestion_status", {"state": "unknown"}),
        "database": APP_STATE.get("database_status", {"state": "not_used"}),
        "tasks": [
            {
                "name": task.get_name(),
                "done": task.done(),
                "cancelled": task.cancelled(),
            }
            for task in APP_STATE.get("tasks", [])  # type: ignore[union-attr]
        ],
    }
