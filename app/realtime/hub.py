from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from typing import Any

from fastapi import WebSocket


def _sanitize_for_json(obj: Any) -> Any:
    """Make structures JSON-safe (NaN/Inf break ``json.dumps`` on some Python builds)."""
    if isinstance(obj, dict):
        return {str(k): _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def dumps_message(message: dict[str, Any]) -> str:
    return json.dumps(_sanitize_for_json(message), default=str, allow_nan=False)


class LiveHub:
    """Fan-out JSON messages to all connected /ws/live clients (in-process)."""

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._client_locks: dict[WebSocket, asyncio.Lock] = {}
        self._client_connected_at: dict[WebSocket, float] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._last_candle_3m: dict[str, Any] | None = None
        self._last_candle_3m_open: dict[str, Any] | None = None
        self._last_option_chain: dict[str, Any] | None = None
        self._candle_history: deque[dict[str, Any]] = deque(maxlen=50)
        self._message_counts: dict[str, int] = {}
        self._last_broadcast: dict[str, Any] = {}
        self._last_send_error: str | None = None

    @property
    def client_count(self) -> int:
        return len(self._clients)

    async def register(self, websocket: WebSocket) -> None:
        self._clients.add(websocket)
        self._client_locks[websocket] = asyncio.Lock()
        self._client_connected_at[websocket] = time.monotonic()
        self.logger.info("ws client connected clients=%d", self.client_count)

    def unregister(self, websocket: WebSocket) -> None:
        self._clients.discard(websocket)
        self._client_locks.pop(websocket, None)
        self._client_connected_at.pop(websocket, None)
        self.logger.info("ws client disconnected clients=%d", self.client_count)

    async def send_json_to(self, websocket: WebSocket, message: dict[str, Any]) -> None:
        await self._send_text(websocket, dumps_message(message))

    async def _send_text(self, websocket: WebSocket, text: str) -> None:
        lock = self._client_locks.get(websocket)
        if lock is None:
            raise RuntimeError("websocket is not registered")
        async with lock:
            await websocket.send_text(text)

    def debug_snapshot(self) -> dict[str, Any]:
        return {
            "client_count": self.client_count,
            "message_counts": dict(self._message_counts),
            "last_broadcast": dict(self._last_broadcast),
            "last_send_error": self._last_send_error,
            "has_last_candle_3m": self._last_candle_3m is not None,
            "has_last_candle_3m_open": self._last_candle_3m_open is not None,
            "has_last_option_chain": self._last_option_chain is not None,
            "candle_history_count": len(self._candle_history),
            "client_ages_seconds": [
                round(time.monotonic() - connected_at, 3)
                for connected_at in self._client_connected_at.values()
            ],
        }

    async def notify_client_connected(self, websocket: WebSocket) -> None:
        """Send handshake + last candle snapshots so UIs see data immediately."""
        from datetime import datetime

        from app.core.config import settings
        from app.realtime.interval_clock import (
            floor_to_interval,
            market_tz,
            seconds_until_next_boundary,
        )

        tz = market_tz(settings.market_timezone)
        now = datetime.now(tz)
        interval = int(settings.candle_interval_minutes)
        nxt = seconds_until_next_boundary(now, interval, tz)
        hello: dict[str, Any] = {
            "type": "ws_hello",
            "interval_minutes": interval,
            "timezone": settings.market_timezone,
            "current_bucket_start": floor_to_interval(now, interval, tz).isoformat(),
            "seconds_until_next_closed_candle": round(nxt, 3),
            "howto": (
                "Closed 3m bars are sent as JSON with type='candle_3m' only at each "
                "bucket boundary (IST). High-frequency updates use type='tick'. "
                "Filter messages by the 'type' field."
            ),
        }
        try:
            await self.send_json_to(websocket, hello)
            for candle in self._candle_history:
                await self.send_json_to(websocket, candle)
            if self._last_candle_3m_open is not None:
                await self.send_json_to(websocket, self._last_candle_3m_open)
            if self._last_option_chain is not None:
                await self.send_json_to(websocket, self._last_option_chain)
        except Exception:
            self.logger.exception("notify_client_connected failed")

    async def broadcast_json(self, message: dict[str, Any]) -> None:
        mtype = message.get("type")
        mtype_key = str(mtype or "unknown")
        self._message_counts[mtype_key] = self._message_counts.get(mtype_key, 0) + 1
        self._last_broadcast = {
            "type": mtype_key,
            "client_count": self.client_count,
            "bucket_start": message.get("bucket_start"),
            "bucket_end": message.get("bucket_end"),
        }
        if mtype == "candle_3m":
            self._last_candle_3m = dict(message)
            self._candle_history.append(dict(message))
        elif mtype == "candle_3m_open":
            self._last_candle_3m_open = dict(message)
        elif mtype == "option_chain":
            self._last_option_chain = dict(message)

        try:
            text = dumps_message(message)
        except (TypeError, ValueError) as exc:
            self.logger.exception("JSON encode failed for type=%s: %s", mtype, exc)
            return

        if mtype in ("candle_3m", "candle_3m_open") and not self._clients:
            self.logger.warning(
                "dropped %s broadcast (no /ws/live clients); connect before the "
                "boundary or poll GET /realtime/candles/current — bucket=%s",
                mtype,
                message.get("bucket_start"),
            )
        else:
            self.logger.info(
                "hub broadcast type=%s clients=%d bucket=%s",
                mtype_key,
                self.client_count,
                message.get("bucket_start"),
            )

        if not self._clients:
            return

        dead: list[WebSocket] = []
        for ws in list(self._clients):
            try:
                await self._send_text(ws, text)
            except Exception:
                self._last_send_error = f"{mtype_key}: send failed"
                dead.append(ws)
        for ws in dead:
            self.unregister(ws)
