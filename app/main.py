import asyncio

import logging

from contextlib import asynccontextmanager



from typing import Any

from fastapi import FastAPI
from fastapi.responses import Response



from app.api.routes import router as rest_router

from app.api.ws import router as ws_router

from app.core.config import settings

from app.core.env_loader import load_project_env

from app.core.logging import setup_logging

# --- Non-interactive bootstrap ----------------------------------------------
# Order matters here:
#   1. load_project_env(".") populates os.environ from .env so step 2 can
#      read NUBRA_TOTP_SECRET / PHONE_NO / MPIN.
#   2. install_non_interactive_input_patch() globally replaces builtins.input
#      with a TOTP auto-injecting / SMS-OTP-blocking guard. This MUST run
#      before any module that imports nubra_python_sdk (directly or
#      transitively, e.g. app.ingestion.auth_client / nubra_socket /
#      ws_manager) so the SDK's own input() calls are intercepted.
#   3. require_totp_secret=True makes the install raise NubraAuthError at
#      boot if the secret is missing, instead of letting the app start and
#      then explode at the first auth refresh.
from app.ingestion.input_patch import install_non_interactive_input_patch

load_project_env(".")
install_non_interactive_input_patch(require_totp_secret=True)
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





@asynccontextmanager

async def lifespan(_: FastAPI):

    setup_logging(settings.log_level)

    load_project_env(".")

    logger = logging.getLogger("app.main")

    logger.info("Lifespan: logging configured, env loaded")



    broker = QueueBroker(maxsize=settings.queue_maxsize)

    tasks: list[asyncio.Task] = []



    if settings.use_database:

        redis_store: RedisStore | MemoryStore

        if settings.use_redis:

            redis_store = RedisStore(redis_url=settings.redis_url)

        else:

            redis_store = MemoryStore()



        db_writer = DBWriter(

            dsn=settings.postgres_dsn,

            batch_size=settings.db_batch_size,

            flush_interval_ms=settings.db_flush_interval_ms,

        )

        logger.info("Lifespan: connecting to Postgres (dsn host=%s)", settings.postgres_dsn.split("@")[-1])

        await db_writer.connect()

        logger.info("Lifespan: Postgres pool ready")



        APP_STATE["broker"] = broker

        APP_STATE["redis"] = redis_store

        APP_STATE["db_writer"] = db_writer



        workers = [

            IndexWorker(broker, redis_store, db_writer),

            OptionsWorker(broker, redis_store, db_writer),

            OrderbookWorker(broker, redis_store, db_writer),

            GreeksWorker(broker, redis_store, db_writer),

            OhlcvWorker(broker, redis_store, db_writer),

        ]

        for worker in workers:

            tasks.append(asyncio.create_task(worker.run_forever(), name=f"{worker.__class__.__name__}.run"))



        signal_engine = SignalEngine(redis_store=redis_store, symbols=["NIFTY"])

        tasks.append(asyncio.create_task(signal_engine.run_forever(), name="SignalEngine.run"))

        tasks.append(asyncio.create_task(db_writer.run_forever(), name="DBWriter.run"))



        if settings.enable_nubra_socket:

            ingestion = NubraIngestionService(

                broker=broker,

                env_name=settings.nubra_env,

                exchange=settings.nubra_exchange,

                initial_nifty_price=settings.initial_nifty_price,

                strike_radius=settings.strike_radius,

                include_sdk_ohlcv=settings.subscribe_sdk_ohlcv,

                include_sdk_option_chain=settings.subscribe_sdk_option_chain,

            )

            await ingestion.start()

            APP_STATE["ingestion"] = ingestion

            tasks.append(asyncio.create_task(

                run_session_refresh_loop(env_name=settings.nubra_env),

                name="nubra-auth-refresh",

            ))

            logger.info("Nubra ingestion enabled")

        else:

            logger.warning("Nubra ingestion disabled; set ENABLE_NUBRA_SOCKET=true to enable live stream")

    else:

        # Realtime-only: in-memory state, tick fan-out, 3m candles — no Postgres.

        from app.realtime.candles import CandleBoard

        from app.realtime.hub import LiveHub

        from app.realtime.market_state import MarketStateStore

        from app.realtime.pipeline import RealtimePipeline, run_interval_scheduler



        redis_store = RedisStore(settings.redis_url) if settings.use_redis else MemoryStore()

        hub = LiveHub()

        candles = CandleBoard()

        market_state = MarketStateStore()



        APP_STATE["broker"] = broker

        APP_STATE["redis"] = redis_store

        APP_STATE["hub"] = hub

        APP_STATE["market_state"] = market_state

        APP_STATE["candles"] = candles



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

            )

            await ingestion.start()

            APP_STATE["ingestion"] = ingestion

            tasks.append(asyncio.create_task(

                run_session_refresh_loop(env_name=settings.nubra_env),

                name="nubra-auth-refresh",

            ))

            logger.info("Nubra ingestion enabled (realtime mode)")

        else:

            logger.warning("Nubra ingestion disabled; set ENABLE_NUBRA_SOCKET=true for live ticks")



        pipeline = RealtimePipeline(

            broker=broker,

            hub=hub,

            candles=candles,

            state=market_state,

            initial_nifty_price=settings.initial_nifty_price,

            ingestion=ingestion,

        )

        tasks.append(asyncio.create_task(pipeline.run_forever(), name="RealtimePipeline.run"))

        tasks.append(

            asyncio.create_task(

                run_interval_scheduler(

                    hub,

                    candles,

                    market_state,

                    interval_minutes=settings.candle_interval_minutes,

                    tz_name=settings.market_timezone,

                ),

                name="Candle3mScheduler",

            ),

        )



    APP_STATE["tasks"] = tasks

    logger.info("Lifespan: startup complete, accepting traffic")

    yield



    ingestion = APP_STATE.get("ingestion")

    if ingestion is not None:

        try:

            await ingestion.stop()  # type: ignore[union-attr]

        except Exception:

            logger.exception("Lifespan: ingestion.stop() raised")



    for task in tasks:

        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)



    if settings.use_database:

        dbw = APP_STATE.get("db_writer")

        if dbw is not None:

            await dbw.close()  # type: ignore[union-attr]

    rs = APP_STATE.get("redis")

    if rs is not None:

        await rs.close()  # type: ignore[union-attr]





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

