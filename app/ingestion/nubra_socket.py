"""
Nubra ingestion service: orchestrates auth + instruments + WebSocket
manager, fans market-data ticks into the internal :class:`QueueBroker`.

The transport-level concerns (reconnect, heartbeat, subscription
re-application, tick buffering, health metrics) live in
:class:`app.ingestion.ws_manager.WebSocketManager`. This module is the
business glue: it knows *what* to subscribe to (via
:class:`InstrumentManager`) and *where* ticks go (broker / event
envelopes).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import Any, Optional

from app.core.config import settings
from app.core.env_loader import load_project_env
from app.ingestion.auth_client import get_session_tokens
from app.ingestion.auth_preflight import assert_auth_preflight, auth_preflight_status
from app.ingestion.ws_manager import TickEvent, WebSocketManager
from app.instruments.manager import InstrumentManager
from app.queue.broker import QueueBroker
from app.queue.envelope import EventEnvelope


class NubraIngestionService:
    def __init__(
        self,
        broker: QueueBroker,
        env_name: str = "UAT",
        exchange: str = "NSE",
        initial_nifty_price: float = 22000.0,
        strike_radius: int = 15,
        include_sdk_ohlcv: bool = False,
        include_sdk_option_chain: bool = True,
        extra_brokers: Sequence[QueueBroker] | None = None,
    ) -> None:
        extras = tuple(extra_brokers or ())
        self._brokers: tuple[QueueBroker, ...] = (broker,) + extras
        self.broker = broker
        self.env_name = env_name
        self.exchange = exchange
        self.initial_nifty_price = initial_nifty_price
        self.strike_radius = strike_radius
        self.include_sdk_ohlcv = include_sdk_ohlcv
        self.include_sdk_option_chain = include_sdk_option_chain
        self.logger = logging.getLogger(self.__class__.__name__)

        self.ws_manager: Optional[WebSocketManager] = None
        self.instrument_manager: Optional[InstrumentManager] = None
        self.last_subscriptions: dict[str, list[str]] = {}
        self.auth_status: dict[str, Any] = {}
        self.last_start_phase: str = "init"
        self._sampled_option_logs = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self.last_start_phase = "load_env"
        self.logger.info("[start] phase=%s", self.last_start_phase)
        load_project_env(".")

        self.last_start_phase = "auth_preflight"
        self.logger.info("[start] phase=%s", self.last_start_phase)
        self.auth_status = auth_preflight_status()
        self.logger.info("Nubra auth preflight status: %s", self.auth_status)
        assert_auth_preflight()

        self.last_start_phase = "ws_manager_init"
        self.logger.info("[start] phase=%s env=%s exchange=%s", self.last_start_phase, self.env_name, self.exchange)
        self.ws_manager = WebSocketManager(
            env_name=self.env_name,
            on_tick=self._on_tick,
        )

        self.last_start_phase = "instrument_manager_init"
        self.logger.info("[start] phase=%s", self.last_start_phase)
        self.instrument_manager = InstrumentManager(
            env_name=self.env_name,
            use_env_creds=True,
            on_option_tokens_changed=self._on_option_tokens_changed,
            strike_radius=self.strike_radius,
        )

        self.last_start_phase = "ws_connect"
        self.logger.info("[start] phase=%s", self.last_start_phase)
        await self.ws_manager.start()

        # Surface session token presence for /debug logs.
        tokens = get_session_tokens(getattr(self.ws_manager, "_client", None))
        self.logger.info(
            "Nubra session ready (auth_token=%s, x_device_id=%s)",
            "set" if tokens.get("auth_token") else "missing",
            "set" if tokens.get("x_device_id") else "missing",
        )

        self.last_start_phase = "subscribe"
        self.logger.info("[start] phase=%s", self.last_start_phase)
        await self._subscribe_all()

        self.last_start_phase = "ready"
        self.logger.info("Nubra socket connected and subscribed")

    async def stop(self) -> None:
        if self.ws_manager is not None:
            await self.ws_manager.stop()
            self.ws_manager = None

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        if self.ws_manager is None:
            return {"socket": "uninitialised"}
        return self.ws_manager.health()

    # ------------------------------------------------------------------
    # Subscription orchestration
    # ------------------------------------------------------------------

    async def _subscribe_all(self) -> None:
        if self.ws_manager is None or self.instrument_manager is None:
            return

        subs = self.instrument_manager.get_stream_subscriptions(
            nifty_price=self.initial_nifty_price,
            include_ohlcv=self.include_sdk_ohlcv,
            include_option_chain=self.include_sdk_option_chain,
        )
        self.last_subscriptions = subs
        self.logger.info(
            "subscription sizes index=%s option_keys=%s orderbook=%s greeks=%s ohlcv=%s",
            len(subs["index_symbols"]),
            len(subs["option_chain_keys"]),
            len(subs["orderbook_ref_ids"]),
            len(subs["greeks_ref_ids"]),
            len(subs["ohlcv_symbols"]),
        )

        if subs["index_symbols"]:
            await self.ws_manager.subscribe(
                subs["index_symbols"], data_type="index", exchange=self.exchange
            )
        # Full option chain: ``ASSET:YYYYMMDD`` per Nubra realtime docs
        # (not the per-leg symbol ``NIFTY26APR24600PE`` form).
        if subs["option_chain_keys"]:
            await self.ws_manager.subscribe(
                subs["option_chain_keys"],
                data_type="option",
                exchange=self.exchange,
            )
        # Per-ref orderbook/greeks for ATM ± radius (complements chain stream).
        if subs["orderbook_ref_ids"]:
            await self.ws_manager.subscribe(
                subs["orderbook_ref_ids"], data_type="orderbook"
            )
        if subs["greeks_ref_ids"]:
            await self.ws_manager.subscribe(
                subs["greeks_ref_ids"], data_type="greeks", exchange=self.exchange
            )
        if subs["ohlcv_symbols"]:
            ohlcv_iv = f"{int(settings.candle_interval_minutes)}m"
            await self.ws_manager.subscribe(
                subs["ohlcv_symbols"],
                data_type="ohlcv",
                exchange=self.exchange,
                interval=ohlcv_iv,
            )
            self.logger.info("Nubra ohlcv subscribe interval=%s (matches candle_interval_minutes)", ohlcv_iv)

        self.logger.info("Active subscription payloads: %s", subs)

    def _on_option_tokens_changed(self, diff: Any) -> None:
        """Called from InstrumentManager (possibly a background thread)."""
        if self.ws_manager is None:
            return
        if not diff.added and not diff.removed:
            return

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running is not None:
            running.create_task(self._apply_option_diff(diff))
            return

        ws_loop = getattr(self.ws_manager, "_main_loop", None)
        if ws_loop is None:
            return
        ws_loop.call_soon_threadsafe(
            lambda: ws_loop.create_task(self._apply_option_diff(diff))
        )

    async def _apply_option_diff(self, diff: Any) -> None:
        if self.ws_manager is None:
            return
        if diff.removed:
            removed = [str(x) for x in diff.removed]
            await self.ws_manager.unsubscribe(removed, data_type="greeks", exchange=self.exchange)
            await self.ws_manager.unsubscribe(removed, data_type="orderbook")
        if diff.added:
            added = [str(x) for x in diff.added]
            await self.ws_manager.subscribe(added, data_type="greeks", exchange=self.exchange)
            await self.ws_manager.subscribe(added, data_type="orderbook")
        self.logger.info(
            "Option token diff applied: added=%s removed=%s",
            diff.added,
            diff.removed,
        )

    # ------------------------------------------------------------------
    # Tick → broker fan-out
    # ------------------------------------------------------------------

    def _on_tick(self, event: TickEvent) -> None:
        # ws_manager calls us on the main asyncio loop, so we can spawn
        # a task directly.
        if event.stream == "option" and self._sampled_option_logs < 3:
            self._sampled_option_logs += 1
            self.logger.info(
                "option payload sample keys=%s", sorted(event.payload.keys())
            )

        envelope = EventEnvelope(
            stream=event.stream, key=event.key, payload=event.payload
        )
        try:
            for b in self._brokers:
                asyncio.create_task(b.publish(envelope))
        except RuntimeError:
            # Defensive: should not happen, on_tick is invoked on the loop.
            self.logger.exception("broker.publish could not be scheduled")
