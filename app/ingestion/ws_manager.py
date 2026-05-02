"""
Resilient WebSocket manager for Nubra market data.

Wraps the SDK's :class:`NubraDataSocket` with the operational concerns
that production trading systems require:

* Async lifecycle (``async start`` / ``async stop``) suitable for FastAPI
  lifespan or any other supervisor.
* Watchdog-driven hard reconnect when ticks go stale (default >10s) — the
  SDK's own reconnect handles transient TCP drops, but a "connected but
  silent" socket is invisible to it.
* Subscription registry maintained by *us*. Persists across hard
  reconnects (which create a fresh socket instance and would otherwise
  lose the SDK's internal subscription set).
* Bounded async tick buffer (``asyncio.Queue``) for downstream
  consumers, with drop-oldest semantics under burst load.
* Thread-safe metrics + JSON-serialisable health snapshot for monitoring
  and ``/health`` endpoints.
* Exponential backoff with jitter (2s, 4s, 8s, 16s, 30s cap).
* Session-only auth integrated via :func:`app.ingestion.auth_client.get_authenticated_client`
  on every (re)connect. An expired session triggers a re-read of
  ``auth_data.db.*`` from disk — but **never** a TOTP / OTP login.
  Refreshing the session requires running ``setup_totp.py`` externally.
* Controlled session reload on token expiry: the SDK's internal
  reconnect is disabled (``reconnect=False``) because it would re-run
  the SDK's auth_flow which deletes the cache on a 401. Auth-shaped
  close frames or errors (``"token expired"``, ``"unauthorized"``,
  ``401``, …) are detected and trigger ``get_authenticated_client(
  force_refresh=True)`` followed by a clean socket rebuild. After
  ``max_auth_failures`` consecutive failures the manager enters a
  ``fatal`` state and stops reconnecting until the process is restarted,
  preventing loop storms while the supervisor / k8s liveness probe
  reacts to the missing session.
* Optional ``metrics_hook`` for Prometheus / external counters.

Public interface
----------------

    manager = WebSocketManager(env_name="UAT", on_tick=callback)
    await manager.start()
    await manager.subscribe(["NIFTY"], data_type="index", exchange="NSE")
    event = await manager.get_tick(timeout=1.0)
    snapshot = manager.health()
    await manager.stop()
"""

from __future__ import annotations

import asyncio
import collections
import logging
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable, Optional

from app.ingestion.auth_client import (
    NubraAuthError,
    get_authenticated_client,
    get_session_tokens,
)


SubscriptionKey = tuple[str, Optional[str], Optional[str]]
"""(data_type, exchange, interval)"""


# ---------------------------------------------------------------------------
# Auth-failure detection
# ---------------------------------------------------------------------------
#
# Substrings the Nubra SDK / aiohttp surfaces in close-frames or error
# objects when the bearer token has expired, the device-id has been
# revoked, or the server force-disconnects an unauthenticated socket.
# Matched case-insensitively against ``str(reason)`` / ``str(err)``.
#
# Keep this list conservative: any false positive triggers a full
# re-auth cycle, which is expensive (TOTP login + device handshake).
_AUTH_FAILURE_PATTERNS: tuple[str, ...] = (
    "token expired",
    "token has expired",
    "expired token",
    "session expired",
    "invalid token",
    "invalid session",
    "no token",
    "missing token",
    "unauthorized",
    "unauthenticated",
    "authentication failed",
    "auth failed",
    " 401",
    "(401",
    "code=401",
    "status=401",
    "http 401",
    "1008",  # WebSocket close code: policy violation (often auth)
    "4401",  # custom auth-rejection codes some gateways use
)


def _looks_like_auth_failure(text: Optional[str]) -> bool:
    """Heuristic: does this error/close-reason indicate token expiry?"""
    if not text:
        return False
    lower = text.lower()
    return any(pat in lower for pat in _AUTH_FAILURE_PATTERNS)


# ---------------------------------------------------------------------------
# Public data classes
# ---------------------------------------------------------------------------


@dataclass
class TickEvent:
    """Lightweight tick container delivered to consumers."""

    stream: str
    key: str
    payload: dict[str, Any]
    received_at: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Internal metrics
# ---------------------------------------------------------------------------


@dataclass
class _Metrics:
    ticks_total: int = 0
    ticks_dropped: int = 0
    reconnects: int = 0
    last_tick_monotonic: float = 0.0
    rate_window: collections.deque = field(default_factory=lambda: collections.deque(maxlen=4096))


# ---------------------------------------------------------------------------
# WebSocketManager
# ---------------------------------------------------------------------------


class WebSocketManager:
    """Resilient async owner of the Nubra market-data WebSocket."""

    DEFAULT_BUFFER_SIZE = 10_000
    DEFAULT_STALE_THRESHOLD_SECONDS = 10.0
    DEFAULT_WATCHDOG_INTERVAL_SECONDS = 2.0
    DEFAULT_BASE_BACKOFF_SECONDS = 2.0
    DEFAULT_MAX_BACKOFF_SECONDS = 30.0
    DEFAULT_CONNECT_TIMEOUT_SECONDS = 20.0
    DEFAULT_RATE_WINDOW_SECONDS = 60.0
    DEFAULT_AUTH_REFRESH_COOLDOWN_SECONDS = 3.0
    # Session-only mode: a session-reload can't recover from a missing /
    # expired auth_data.db.*, so we don't let the manager flap forever.
    # Three consecutive failures is enough confirmation that the supervisor
    # needs to step in and refresh the cache.
    DEFAULT_MAX_AUTH_FAILURES = 3

    def __init__(
        self,
        *,
        env_name: str = "UAT",
        on_tick: Optional[Callable[[TickEvent], None]] = None,
        on_async_tick: Optional[Callable[[TickEvent], Awaitable[None]]] = None,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        stale_threshold_seconds: float = DEFAULT_STALE_THRESHOLD_SECONDS,
        watchdog_interval_seconds: float = DEFAULT_WATCHDOG_INTERVAL_SECONDS,
        base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
        connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        rate_window_seconds: float = DEFAULT_RATE_WINDOW_SECONDS,
        auth_refresh_cooldown_seconds: float = DEFAULT_AUTH_REFRESH_COOLDOWN_SECONDS,
        max_auth_failures: int = DEFAULT_MAX_AUTH_FAILURES,
        metrics_hook: Optional[Callable[[dict[str, Any]], None]] = None,
        market_callbacks_extra: Optional[dict[str, Callable[[Any], None]]] = None,
    ) -> None:
        self.env_name = env_name
        self.on_tick = on_tick
        self.on_async_tick = on_async_tick
        self.metrics_hook = metrics_hook
        self.market_callbacks_extra = market_callbacks_extra or {}

        self.stale_threshold_seconds = stale_threshold_seconds
        self.watchdog_interval_seconds = watchdog_interval_seconds
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.connect_timeout_seconds = connect_timeout_seconds
        self.rate_window_seconds = rate_window_seconds
        self.auth_refresh_cooldown_seconds = auth_refresh_cooldown_seconds
        self.max_auth_failures = max_auth_failures

        self.logger = logging.getLogger(self.__class__.__name__)

        # Tick plumbing.
        self._tick_buffer: asyncio.Queue[TickEvent] = asyncio.Queue(maxsize=buffer_size)

        # State protected by _lock (touched from SDK background thread).
        self._lock = threading.RLock()
        self._subscriptions: dict[SubscriptionKey, set[str]] = {}
        self._metrics = _Metrics()

        self._socket: Any = None
        self._client: Any = None
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self._connect_event: Optional[asyncio.Event] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._consecutive_failures = 0
        self._auth_failure_count = 0
        self._closing = False
        self._fatal = False  # set when max_auth_failures is exceeded

        self._socket_state = "disconnected"  # connected | reconnecting | disconnected
        self._auth_state = "ok"              # ok | refreshing | error | fatal
        self._last_error: Optional[str] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._main_loop is not None:
            self.logger.debug("WebSocketManager.start called twice; ignoring")
            return

        self._main_loop = asyncio.get_running_loop()
        self._connect_event = asyncio.Event()
        self._closing = False
        self.logger.info("Starting WebSocketManager (env=%s)", self.env_name)

        await self._build_and_connect_socket(reason="initial")

        self._watchdog_task = asyncio.create_task(
            self._watchdog_loop(), name="ws-watchdog"
        )

    async def stop(self) -> None:
        self._closing = True
        self.logger.info("Stopping WebSocketManager")

        for task in (self._watchdog_task, self._reconnect_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        self._watchdog_task = None
        self._reconnect_task = None

        await self._teardown_socket()
        self._main_loop = None

    # ------------------------------------------------------------------
    # Subscription API
    # ------------------------------------------------------------------

    async def subscribe(
        self,
        symbols: Iterable[Any],
        *,
        data_type: str,
        exchange: Optional[str] = None,
        interval: Optional[str] = None,
    ) -> None:
        """Register a subscription and forward it to the live socket."""
        symbol_list = [str(s) for s in symbols]
        if not symbol_list:
            return

        key: SubscriptionKey = (data_type, exchange, interval)
        with self._lock:
            self._subscriptions.setdefault(key, set()).update(symbol_list)

        await self._send_subscribe(symbol_list, data_type=data_type, exchange=exchange, interval=interval)
        self.logger.info(
            "subscribe data_type=%s exchange=%s interval=%s count=%d",
            data_type,
            exchange,
            interval,
            len(symbol_list),
        )
        self._safe_metrics_hook({"event": "subscribe", "data_type": data_type, "count": len(symbol_list)})

    async def unsubscribe(
        self,
        symbols: Iterable[Any],
        *,
        data_type: str,
        exchange: Optional[str] = None,
    ) -> None:
        symbol_list = [str(s) for s in symbols]
        if not symbol_list:
            return

        with self._lock:
            for key, members in list(self._subscriptions.items()):
                if key[0] == data_type and key[1] == exchange:
                    members.difference_update(symbol_list)
                    if not members:
                        self._subscriptions.pop(key, None)

        if self._socket is None:
            return
        try:
            kwargs: dict[str, Any] = {"data_type": data_type}
            if exchange:
                kwargs["exchange"] = exchange
            await asyncio.to_thread(self._socket.unsubscribe, symbol_list, **kwargs)
            self.logger.info(
                "unsubscribe data_type=%s exchange=%s count=%d",
                data_type,
                exchange,
                len(symbol_list),
            )
        except Exception:
            self.logger.exception(
                "unsubscribe failed (continuing): data_type=%s count=%d",
                data_type,
                len(symbol_list),
            )

    # ------------------------------------------------------------------
    # Tick consumer API
    # ------------------------------------------------------------------

    async def get_tick(self, timeout: Optional[float] = None) -> Optional[TickEvent]:
        """Pop one tick from the buffer. Returns ``None`` on timeout."""
        try:
            if timeout is None:
                return await self._tick_buffer.get()
            return await asyncio.wait_for(self._tick_buffer.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def buffer_size(self) -> int:
        return self._tick_buffer.qsize()

    # ------------------------------------------------------------------
    # Health / metrics
    # ------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            last_age: Optional[float] = None
            if self._metrics.last_tick_monotonic > 0:
                last_age = max(0.0, now - self._metrics.last_tick_monotonic)
            sub_counts = {
                f"{k[0]}|{k[1] or '-'}|{k[2] or '-'}": len(v)
                for k, v in self._subscriptions.items()
            }
            session_tokens = (
                get_session_tokens(self._client) if self._client is not None else {}
            )
            return {
                "socket": self._socket_state,
                "last_tick_seconds_ago": last_age,
                "tick_rate": self._compute_rate_locked(),
                "auth": self._auth_state,
                "reconnects": self._metrics.reconnects,
                "consecutive_failures": self._consecutive_failures,
                "auth_failures": self._auth_failure_count,
                "max_auth_failures": self.max_auth_failures,
                "fatal": self._fatal,
                "ticks_total": self._metrics.ticks_total,
                "ticks_dropped": self._metrics.ticks_dropped,
                "buffer_size": self._tick_buffer.qsize(),
                "subscriptions": sub_counts,
                "session_present": bool(session_tokens.get("auth_token")),
                "last_error": self._last_error,
            }

    # ------------------------------------------------------------------
    # Internals — connection lifecycle
    # ------------------------------------------------------------------

    async def _build_and_connect_socket(
        self, *, reason: str, force_auth_refresh: bool = False
    ) -> None:
        self._socket_state = "reconnecting"
        self._auth_state = "refreshing"
        try:
            if force_auth_refresh:
                self.logger.warning(
                    "Token expired → reloading session from auth_data.db.* "
                    "(reason=%s, mode=session-only, no TOTP login)",
                    reason,
                )
            # In session-only mode this just re-reads auth_data.db.*; it
            # never performs a network login. If the on-disk session is
            # missing or expired, NubraAuthError surfaces and we surface
            # "Session missing — manual setup required" up the stack.
            self._client = await asyncio.to_thread(
                get_authenticated_client,
                env_name=self.env_name,
                force_refresh=force_auth_refresh,
            )
            self._auth_state = "ok"
            if force_auth_refresh:
                self.logger.info(
                    "Session refresh successful (re-read auth_data.db.*)"
                )
        except NubraAuthError as exc:
            self._auth_state = "error"
            self._auth_failure_count += 1
            self._last_error = f"auth: {exc}"
            self.logger.error(
                "Session missing — manual setup required "
                "(auth_failures=%d/%d): %s",
                self._auth_failure_count,
                self.max_auth_failures,
                exc,
            )
            raise

        from nubra_python_sdk.ticker import websocketdata

        socket = websocketdata.NubraDataSocket(
            client=self._client,
            on_market_data=self.market_callbacks_extra.get("market", self._cb_market),
            on_connect=self._cb_connect,
            on_close=self._cb_close,
            on_error=self._cb_error,
            on_index_data=self._cb_index,
            on_option_data=self._cb_option,
            on_orderbook_data=self._cb_with_refid("orderbook"),
            on_greeks_data=self._cb_with_refid("greeks"),
            on_ohlcv_data=self._cb_ohlcv,
            # We own reconnect + subscription persistence so that token
            # expiry can drive a controlled re-auth via ``auth_client``.
            # Letting the SDK reconnect itself defeats that: it reuses
            # the stale token / wrong device-id and storms the server.
            reconnect=False,
            persist_subscriptions=False,
        )

        assert self._connect_event is not None
        self._connect_event.clear()
        self._socket = socket

        self.logger.info("Connecting WebSocket (reason=%s)", reason)
        socket.connect()  # spawns daemon thread + async loop

        try:
            await asyncio.wait_for(
                self._connect_event.wait(), timeout=self.connect_timeout_seconds
            )
        except asyncio.TimeoutError:
            self._last_error = "connect_timeout"
            self.logger.error(
                "WebSocket connect timed out after %.1fs", self.connect_timeout_seconds
            )
            await self._teardown_socket()
            raise

        self._socket_state = "connected"
        self._consecutive_failures = 0
        # On a fully-successful (re)connect, clear auth-failure history.
        if self._auth_failure_count:
            self.logger.info(
                "Auth recovered after %d failed attempts; resetting counter",
                self._auth_failure_count,
            )
        self._auth_failure_count = 0
        # Seed last_tick so the watchdog gives us a grace period.
        with self._lock:
            self._metrics.last_tick_monotonic = time.monotonic()

        await self._reapply_subscriptions()
        self._safe_metrics_hook({"event": "connected", "reason": reason})
        if force_auth_refresh:
            self.logger.info("WebSocket restarted successfully after auth refresh")

    async def _teardown_socket(self) -> None:
        socket = self._socket
        self._socket = None
        if socket is None:
            return

        self.logger.info("Tearing down WebSocket")

        # 1) Stop any ping / keep-alive loops the SDK runs internally so
        #    the daemon thread exits its read-loop cleanly.
        for flag in ("keep_alive", "_keep_alive", "running", "_running", "_should_run"):
            try:
                if hasattr(socket, flag):
                    setattr(socket, flag, False)
            except Exception:  # noqa: BLE001
                self.logger.debug("Could not clear %s on socket", flag, exc_info=True)

        # 2) Best-effort: invoke whichever shutdown method the SDK exposes.
        for method_name in ("close", "disconnect", "stop", "shutdown"):
            method = getattr(socket, method_name, None)
            if not callable(method):
                continue
            try:
                result = await asyncio.to_thread(method)
                if asyncio.iscoroutine(result):
                    await result
                break  # one successful close is enough
            except Exception:  # noqa: BLE001
                self.logger.debug(
                    "socket.%s() raised during teardown", method_name, exc_info=True
                )

        # 3) Best-effort: close the inner aiohttp ClientSession to silence
        #    "Unclosed client session" warnings emitted by the SDK's loop.
        inner_session = (
            getattr(socket, "session", None)
            or getattr(socket, "_session", None)
            or getattr(socket, "client_session", None)
            or getattr(socket, "_client_session", None)
        )
        if inner_session is not None and not getattr(inner_session, "closed", True):
            try:
                inner_loop = (
                    getattr(socket, "_loop", None)
                    or getattr(socket, "loop", None)
                    or getattr(socket, "_event_loop", None)
                )
                if inner_loop is not None and inner_loop.is_running():
                    fut = asyncio.run_coroutine_threadsafe(
                        inner_session.close(), inner_loop
                    )
                    await asyncio.to_thread(fut.result, 5.0)
                else:
                    coro = inner_session.close()
                    if asyncio.iscoroutine(coro):
                        await coro
            except Exception:  # noqa: BLE001
                self.logger.debug(
                    "Failed to close inner aiohttp session", exc_info=True
                )

        # 4) Stop the SDK's private event loop if it is still running so
        #    the spawned daemon thread can exit (otherwise we leak both
        #    the loop and its aiohttp connector pool).
        inner_loop = (
            getattr(socket, "_loop", None)
            or getattr(socket, "loop", None)
            or getattr(socket, "_event_loop", None)
        )
        if inner_loop is not None and inner_loop.is_running():
            try:
                inner_loop.call_soon_threadsafe(inner_loop.stop)
            except Exception:  # noqa: BLE001
                self.logger.debug("Could not stop inner SDK loop", exc_info=True)

        self._socket_state = "disconnected"

    async def _hard_reconnect(
        self, reason: str, *, force_auth_refresh: bool = False
    ) -> None:
        if self._closing or self._fatal:
            return

        # Hard cap on re-auth failures: if we've failed N times in a row
        # to refresh the token, do NOT keep storming the auth server.
        # Surface the problem via health() and stop reconnecting until
        # the process is restarted (typically by the supervisor / k8s).
        if force_auth_refresh and self._auth_failure_count >= self.max_auth_failures:
            self.logger.error(
                "Session reload has failed %d times in a row (max=%d). "
                "Session missing — manual setup required. Disabling "
                "further reconnect attempts. last_error=%s. Refresh "
                "auth_data.db.* by running setup_totp.py from a real "
                "terminal, then restart this process.",
                self._auth_failure_count,
                self.max_auth_failures,
                self._last_error,
            )
            self._fatal = True
            self._auth_state = "fatal"
            self._socket_state = "disconnected"
            self._safe_metrics_hook(
                {"event": "auth_fatal", "failures": self._auth_failure_count}
            )
            return

        with self._lock:
            self._metrics.reconnects += 1
            attempt_no = self._metrics.reconnects

        self.logger.warning(
            "Hard reconnect triggered: reason=%s force_auth_refresh=%s (count=%d)",
            reason,
            force_auth_refresh,
            attempt_no,
        )

        await self._teardown_socket()

        backoff = min(
            self.base_backoff_seconds * (2 ** min(self._consecutive_failures, 4)),
            self.max_backoff_seconds,
        )
        # Up to ±25% jitter to spread clients on global outages.
        backoff += random.uniform(-backoff * 0.25, backoff * 0.25)
        backoff = max(0.5, backoff)

        # Auth-refresh reconnects also pay an additional cooldown so we
        # never burn through TOTP attempts in a tight loop when the
        # backend is misbehaving.
        if force_auth_refresh:
            backoff = max(backoff, self.auth_refresh_cooldown_seconds)

        self.logger.info(
            "Reconnect backoff %.2fs (attempt %d, force_auth_refresh=%s)",
            backoff,
            attempt_no,
            force_auth_refresh,
        )
        await asyncio.sleep(backoff)

        try:
            await self._build_and_connect_socket(
                reason=reason, force_auth_refresh=force_auth_refresh
            )
        except NubraAuthError as exc:
            # _build_and_connect_socket already incremented
            # _auth_failure_count and logged. Don't busy-loop; the
            # watchdog will re-trigger us, and the fatal-cap above
            # will bail us out cleanly if this keeps failing.
            self._last_error = f"reconnect-auth: {exc}"
        except Exception as exc:  # noqa: BLE001
            self._consecutive_failures += 1
            self._last_error = f"reconnect: {exc}"
            self.logger.exception("Reconnect attempt failed: %s", exc)
            # Don't busy-loop; let the watchdog re-trigger.

    def _schedule_hard_reconnect(
        self, reason: str, *, force_auth_refresh: bool = False
    ) -> None:
        loop = self._main_loop
        if loop is None or self._closing or self._fatal:
            return
        if self._reconnect_task and not self._reconnect_task.done():
            return  # already in flight

        def _spawn() -> None:
            self._reconnect_task = asyncio.create_task(
                self._hard_reconnect(reason, force_auth_refresh=force_auth_refresh),
                name="ws-hard-reconnect",
            )

        loop.call_soon_threadsafe(_spawn)

    async def _watchdog_loop(self) -> None:
        try:
            while not self._closing:
                await asyncio.sleep(self.watchdog_interval_seconds)
                if self._fatal:
                    # Fatal auth state — stay quiet; supervisor must restart us.
                    continue
                if self._socket_state != "connected":
                    continue
                with self._lock:
                    last = self._metrics.last_tick_monotonic
                age = time.monotonic() - last if last > 0 else 0.0
                if age > self.stale_threshold_seconds:
                    self.logger.warning(
                        "No ticks for %.1fs (>%.1fs threshold) — forcing hard reconnect",
                        age,
                        self.stale_threshold_seconds,
                    )
                    await self._hard_reconnect(reason="stale-ticks")
        except asyncio.CancelledError:
            self.logger.info("Watchdog cancelled")
            raise

    async def _reapply_subscriptions(self) -> None:
        with self._lock:
            snapshot = {k: set(v) for k, v in self._subscriptions.items()}

        for (data_type, exchange, interval), symbols in snapshot.items():
            if not symbols:
                continue
            try:
                await self._send_subscribe(
                    list(symbols),
                    data_type=data_type,
                    exchange=exchange,
                    interval=interval,
                )
                self.logger.info(
                    "Re-applied subscription data_type=%s exchange=%s count=%d",
                    data_type,
                    exchange,
                    len(symbols),
                )
            except Exception:
                self.logger.exception(
                    "Failed to re-apply subscription data_type=%s exchange=%s",
                    data_type,
                    exchange,
                )

    async def _send_subscribe(
        self,
        symbols: list[str],
        *,
        data_type: str,
        exchange: Optional[str],
        interval: Optional[str],
    ) -> None:
        if self._socket is None:
            self.logger.debug("subscribe skipped, socket not ready (data_type=%s)", data_type)
            return
        kwargs: dict[str, Any] = {"data_type": data_type}
        if exchange:
            kwargs["exchange"] = exchange
        if interval:
            kwargs["interval"] = interval
        try:
            await asyncio.to_thread(self._socket.subscribe, symbols, **kwargs)
        except Exception:
            self.logger.exception("subscribe failed for data_type=%s", data_type)

    # ------------------------------------------------------------------
    # SDK callbacks (run on the SDK's background thread)
    # ------------------------------------------------------------------

    def _cb_connect(self, _msg: Any) -> None:
        self.logger.info("WebSocket connected")
        loop = self._main_loop
        event = self._connect_event
        if loop and event:
            loop.call_soon_threadsafe(event.set)
        self._safe_metrics_hook({"event": "ws_connected"})

    def _cb_close(self, reason: Any) -> None:
        msg = str(reason)
        self.logger.warning("WebSocket closed: %s", msg)
        self._socket_state = "disconnected"
        self._safe_metrics_hook({"event": "ws_closed", "reason": msg})

        # SDK reconnect is disabled (we set reconnect=False), so every
        # close needs an explicit reconnect from us. Auth-shaped closes
        # additionally force a fresh TOTP login via auth_client.
        if _looks_like_auth_failure(msg):
            self._last_error = f"auth-close: {msg}"
            self.logger.warning(
                "Close reason looks auth-related → scheduling controlled re-auth"
            )
            self._schedule_hard_reconnect("token-expired", force_auth_refresh=True)
        else:
            self._schedule_hard_reconnect("ws-closed", force_auth_refresh=False)

    def _cb_error(self, err: Any) -> None:
        msg = str(err)
        self._last_error = msg
        self.logger.error("WebSocket error: %s", msg)
        self._safe_metrics_hook({"event": "ws_error", "error": msg})

        if _looks_like_auth_failure(msg):
            self.logger.warning(
                "WebSocket error looks auth-related → scheduling controlled re-auth"
            )
            self._schedule_hard_reconnect("token-expired", force_auth_refresh=True)

    def _cb_market(self, msg: Any) -> None:
        # Verbose; not promoted to a tick by default.
        self.logger.debug("market_data: %s", msg)

    def _cb_index(self, msg: Any) -> None:
        payload = _to_payload_dict(msg)
        key = str(payload.get("indexname", "unknown"))
        self._submit_tick("index", key, payload)

    def _cb_option(self, msg: Any) -> None:
        payload = _to_payload_dict(msg)
        key = f"{payload.get('asset', 'unknown')}:{payload.get('expiry', 'unknown')}"
        self._submit_tick("option", key, payload)

    def _cb_with_refid(self, stream: str) -> Callable[[Any], None]:
        def handler(msg: Any) -> None:
            payload = _to_payload_dict(msg)
            self._submit_tick(stream, str(_extract_ref_id(payload)), payload)

        return handler

    def _cb_ohlcv(self, msg: Any) -> None:
        payload = _to_payload_dict(msg)
        key = f"{payload.get('indexname', 'unknown')}:{payload.get('interval', 'unknown')}"
        self._submit_tick("ohlcv", key, payload)

    # ------------------------------------------------------------------
    # Tick plumbing
    # ------------------------------------------------------------------

    def _submit_tick(self, stream: str, key: str, payload: dict[str, Any]) -> None:
        event = TickEvent(stream=stream, key=key, payload=payload)
        loop = self._main_loop
        if loop is None or self._closing:
            return
        loop.call_soon_threadsafe(self._on_tick_main_loop, event)

    def _on_tick_main_loop(self, event: TickEvent) -> None:
        now = time.monotonic()
        with self._lock:
            self._metrics.ticks_total += 1
            self._metrics.last_tick_monotonic = now
            self._metrics.rate_window.append(now)

        # Bounded buffer with drop-oldest under burst.
        try:
            self._tick_buffer.put_nowait(event)
        except asyncio.QueueFull:
            try:
                self._tick_buffer.get_nowait()
                with self._lock:
                    self._metrics.ticks_dropped += 1
            except asyncio.QueueEmpty:
                pass
            try:
                self._tick_buffer.put_nowait(event)
            except asyncio.QueueFull:
                with self._lock:
                    self._metrics.ticks_dropped += 1

        if self.on_tick is not None:
            try:
                self.on_tick(event)
            except Exception:
                self.logger.exception("on_tick callback raised")

        if self.on_async_tick is not None:
            asyncio.create_task(self._dispatch_async(event), name="ws-async-tick")

    async def _dispatch_async(self, event: TickEvent) -> None:
        try:
            await self.on_async_tick(event)  # type: ignore[misc]
        except Exception:
            self.logger.exception("on_async_tick callback raised")

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def _compute_rate_locked(self) -> float:
        now = time.monotonic()
        window_start = now - self.rate_window_seconds
        # rate_window is bounded; iterate from right to find cutoff.
        timestamps = self._metrics.rate_window
        count = sum(1 for ts in timestamps if ts >= window_start)
        return count / self.rate_window_seconds

    def _safe_metrics_hook(self, payload: dict[str, Any]) -> None:
        if self.metrics_hook is None:
            return
        try:
            self.metrics_hook(payload)
        except Exception:
            self.logger.debug("metrics_hook raised", exc_info=True)


# ---------------------------------------------------------------------------
# SDK payload normalisation (lifted from the legacy ingestion service)
# ---------------------------------------------------------------------------


def _to_payload_dict(msg: Any) -> dict[str, Any]:
    if isinstance(msg, dict):
        return msg
    for method_name in ("model_dump", "dict", "to_dict"):
        method = getattr(msg, method_name, None)
        if callable(method):
            try:
                out = method()
                if isinstance(out, dict):
                    return out
            except Exception:
                pass
    if hasattr(msg, "__dict__"):
        out: dict[str, Any] = {}
        for k, v in vars(msg).items():
            if k.startswith("_"):
                continue
            out[k] = _serialize_value(v)
        if out:
            return out
    try:
        out = {}
        for name in dir(msg):
            if name.startswith("_"):
                continue
            try:
                value = getattr(msg, name)
            except Exception:
                continue
            if callable(value):
                continue
            out[name] = _serialize_value(value)
        if out:
            return out
    except Exception:
        pass
    return {"value": str(msg)}


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_serialize_value(x) for x in value]
    if isinstance(value, dict):
        return {str(k): _serialize_value(v) for k, v in value.items()}
    if hasattr(value, "__dict__"):
        return {
            k: _serialize_value(v)
            for k, v in vars(value).items()
            if not k.startswith("_")
        }
    return str(value)


def _extract_ref_id(payload: dict[str, Any]) -> Any:
    for key in (
        "ref_id",
        "refId",
        "refid",
        "instrument_token",
        "instrumentToken",
        "token",
    ):
        value = payload.get(key)
        if value is not None:
            return value
    return "unknown"
