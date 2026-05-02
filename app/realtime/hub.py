from __future__ import annotations

import json
import logging
import math
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
        self.logger = logging.getLogger(self.__class__.__name__)
        self._last_candle_3m: dict[str, Any] | None = None
        self._last_candle_3m_open: dict[str, Any] | None = None

    @property
    def client_count(self) -> int:
        return len(self._clients)

    async def register(self, websocket: WebSocket) -> None:
        self._clients.add(websocket)

    def unregister(self, websocket: WebSocket) -> None:
        self._clients.discard(websocket)

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
            await websocket.send_text(dumps_message(hello))
            if self._last_candle_3m is not None:
                await websocket.send_text(dumps_message(self._last_candle_3m))
            if self._last_candle_3m_open is not None:
                await websocket.send_text(dumps_message(self._last_candle_3m_open))
        except Exception:
            self.logger.exception("notify_client_connected failed")

    async def broadcast_json(self, message: dict[str, Any]) -> None:
        mtype = message.get("type")
        if mtype == "candle_3m":
            self._last_candle_3m = dict(message)
        elif mtype == "candle_3m_open":
            self._last_candle_3m_open = dict(message)

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

        if not self._clients:
            return

        dead: list[WebSocket] = []
        for ws in self._clients:
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._clients.discard(ws)