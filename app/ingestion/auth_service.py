"""
Production-ready Nubra authentication service for Cloud Run.

Design
------
* On every startup: seed the shelve with NUBRA_X_DEVICE_ID from env,
  even when NUBRA_SESSION_TOKEN is expired. This is the critical fix
  for the "TOTP is not enabled" error — the SDK must reuse the enrolled
  device-id, not generate a fresh one.
* Session fast path: if NUBRA_SESSION_TOKEN in env is still valid,
  write full tokens to shelve and skip TOTP entirely.
* TOTP fallback: runs when session is expired/missing. Prunes stale
  bearers from shelve (keeps x-device-id), then calls SDK with
  totp_login=True. The input_patch injects TOTP code from env.
* Retry logic: up to 3 attempts with exponential backoff (2s, 4s, 8s).
* Invalid TOTP secret detection: if server rejects TOTP after injection,
  logs INVALID_TOTP_SECRET, logs actionable message, and raises
  NubraAuthError(fatal=True) so the container exits with code 1
  and Cloud Run restarts it (giving ops visibility).
* Session refresh loop: background task re-authenticates every
  REFRESH_INTERVAL seconds when session approaches expiry.
* WebSocket reconnect: called after successful session refresh.
* Structured log events: AUTH_START, GENERATING_TOTP, LOGIN_SUCCESS,
  LOGIN_FAILED, SESSION_REFRESH, SESSION_EXPIRED, INVALID_TOTP_SECRET,
  RETRY_LOGIN.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from typing import Any, Callable, Optional

from app.ingestion.auth_client import (
    _auth_dir,
    _is_jwt_expired,
    _short,
    _write_auth_shelve,
    ensure_auth_dir,
    get_authenticated_client,
    run_session_refresh_loop,
    session_is_expired,
)
from app.ingestion.auth_errors import NubraAuthError

logger = logging.getLogger(__name__)

__all__ = ["AuthService"]

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

#: How often the background refresh loop checks session expiry (seconds).
REFRESH_INTERVAL: float = 5 * 60

#: Phrases in error text that signal TOTP itself is misconfigured.
_FATAL_TOTP_PHRASES = (
    "totp is not enabled",
    "invalid_totp_secret",
    "totp rejected",
    "invalid totp",
)


# ---------------------------------------------------------------------------
# AuthService
# ---------------------------------------------------------------------------


class AuthService:
    """Lifecycle manager for the Nubra SDK authentication session.

    Parameters
    ----------
    env_name:
        The Nubra environment name (``"UAT"`` or ``"PROD"``).
    max_attempts:
        Maximum login attempts before giving up (default 3).
    base_backoff:
        Initial backoff in seconds; doubles on each retry (default 2.0).
    """

    def __init__(
        self,
        *,
        env_name: str,
        max_attempts: int = 3,
        base_backoff: float = 2.0,
        skip_refdata: bool = False,
    ) -> None:
        self._env_name = env_name
        self._max_attempts = max_attempts
        self._base_backoff = base_backoff
        self._skip_refdata = skip_refdata

        self._client: Optional[Any] = None
        self._authenticated_at: Optional[float] = None
        self._last_error: Optional[str] = None
        self._attempt_count: int = 0
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    def is_authenticated(self) -> bool:
        """Return True when a valid client is held in memory."""
        return self._client is not None

    def get_status(self) -> dict:
        """Return a status dict suitable for the /health/auth endpoint."""
        return {
            "authenticated": self.is_authenticated(),
            "env": self._env_name,
            "authenticated_at": self._authenticated_at,
            "last_error": self._last_error,
            "attempt_count": self._attempt_count,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _seed_device_id_always(self) -> None:
        """Seed x-device-id into the shelve unconditionally.

        This is THE critical fix for the Cloud Run "TOTP is not enabled" error.

        On every cold start the /tmp/auth directory is empty.
        bootstrap_session_from_env only writes the shelve when session_token
        is valid. When it is expired it bails, leaving the shelve empty.
        The SDK's __ensure_device_id then generates a fresh UUID — a device-id
        the server has NEVER seen — and /totp/login returns "TOTP is not enabled".

        By writing x-device-id here unconditionally (even when session is
        expired) we guarantee the SDK reuses the enrolled device-id for the
        TOTP login attempt, which the server recognises.
        """
        x_device_id = (
            os.getenv("NUBRA_X_DEVICE_ID") or os.getenv("X_DEVICE_ID") or ""
        ).strip()
        auth_token = (
            os.getenv("NUBRA_AUTH_TOKEN") or os.getenv("AUTH_TOKEN") or ""
        ).strip()

        if not x_device_id:
            logger.warning(
                "DEVICE_ID_MISSING | NUBRA_X_DEVICE_ID not set in env — "
                "SDK will generate a fresh device-id which may not be "
                "enrolled for TOTP. Run enroll_totp.py and set "
                "NUBRA_X_DEVICE_ID in Cloud Run secrets."
            )
            return

        try:
            base = ensure_auth_dir()
            payload: dict[str, str] = {"x-device-id": x_device_id}
            if auth_token:
                payload["auth_token"] = auth_token
            _write_auth_shelve(base, payload)
            logger.info(
                "DEVICE_ID_SEEDED | x_device_id=%s auth_token=%s",
                _short(x_device_id),
                _short(auth_token) if auth_token else "<not set>",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "DEVICE_ID_SEED_FAILED | Could not seed x-device-id: %s", exc
            )

    @staticmethod
    def _is_fatal_totp_error(exc: BaseException) -> bool:
        """Return True when the error indicates a structural TOTP problem."""
        msg = str(exc).lower()
        return any(phrase in msg for phrase in _FATAL_TOTP_PHRASES)

    # ------------------------------------------------------------------
    # authenticate
    # ------------------------------------------------------------------

    async def authenticate(self) -> Any:
        """Obtain an authenticated Nubra SDK client.

        Steps
        -----
        1. Seed x-device-id into the shelve (always — the critical fix).
        2. Try session fast path (NUBRA_SESSION_TOKEN still valid).
        3. TOTP fallback with retry + exponential backoff.
        4. Detect fatal TOTP errors and raise with clear guidance.

        Returns the SDK client on success.
        Raises NubraAuthError on unrecoverable failure.
        """
        async with self._lock:
            logger.info(
                "AUTH_START | env=%s max_attempts=%d",
                self._env_name,
                self._max_attempts,
            )

            # Step 1: always seed device-id so TOTP path uses enrolled device.
            self._seed_device_id_always()

            last_exc: Optional[BaseException] = None

            for attempt in range(1, self._max_attempts + 1):
                self._attempt_count += 1
                try:
                    logger.info(
                        "AUTH_ATTEMPT | attempt=%d/%d env=%s",
                        attempt,
                        self._max_attempts,
                        self._env_name,
                    )
                    client = await asyncio.to_thread(
                        get_authenticated_client,
                        env_name=self._env_name,
                        force_refresh=(attempt > 1),
                        skip_refdata=self._skip_refdata,
                    )
                    self._client = client
                    self._authenticated_at = time.time()
                    self._last_error = None
                    logger.info(
                        "LOGIN_SUCCESS | env=%s attempt=%d",
                        self._env_name,
                        attempt,
                    )
                    return client

                except NubraAuthError as exc:
                    last_exc = exc
                    self._last_error = str(exc)

                    if self._is_fatal_totp_error(exc):
                        logger.error(
                            "INVALID_TOTP_SECRET | %s\n"
                            "ACTION: TOTP secret is invalid or PROD enrollment has "
                            "changed. Run setup_totp.py once locally to generate a "
                            "new TOTP secret, then update the Cloud Run secret "
                            "NUBRA_TOTP_SECRET and redeploy.",
                            exc,
                        )
                        logger.error(
                            "LOGIN_FAILED | fatal=True env=%s — exiting with code 1 "
                            "so Cloud Run restarts the container",
                            self._env_name,
                        )
                        sys.exit(1)

                    if attempt >= self._max_attempts:
                        break

                    backoff = self._base_backoff * (2 ** (attempt - 1))
                    logger.warning(
                        "RETRY_LOGIN | attempt=%d/%d backoff=%.1fs error=%s",
                        attempt,
                        self._max_attempts,
                        backoff,
                        exc,
                    )
                    await asyncio.sleep(backoff)

            logger.error(
                "LOGIN_FAILED | env=%s attempts=%d error=%s",
                self._env_name,
                self._max_attempts,
                last_exc,
            )
            raise NubraAuthError(
                f"Authentication failed after {self._max_attempts} attempts: {last_exc}"
            ) from last_exc

    # ------------------------------------------------------------------
    # refresh_session
    # ------------------------------------------------------------------

    async def refresh_session(self) -> Any:
        """Force a session refresh, re-seeding device-id first.

        Returns the refreshed SDK client.
        Raises NubraAuthError on failure.
        """
        logger.info("SESSION_REFRESH | env=%s (forced)", self._env_name)

        # Re-seed device-id before every refresh attempt.
        self._seed_device_id_always()

        try:
            client = await asyncio.to_thread(
                get_authenticated_client,
                env_name=self._env_name,
                force_refresh=True,
                skip_refdata=self._skip_refdata,
            )
            self._client = client
            self._authenticated_at = time.time()
            self._last_error = None
            logger.info("SESSION_REFRESH | success env=%s", self._env_name)
            return client
        except NubraAuthError as exc:
            self._last_error = str(exc)
            logger.error("SESSION_REFRESH | failed env=%s error=%s", self._env_name, exc)
            raise

    # ------------------------------------------------------------------
    # run_refresh_loop
    # ------------------------------------------------------------------

    async def run_refresh_loop(
        self,
        *,
        on_refresh: Optional[Callable] = None,
        interval: float = REFRESH_INTERVAL,
    ) -> None:
        """Background loop: re-authenticate when the session approaches expiry.

        Parameters
        ----------
        on_refresh:
            Optional async or sync callback invoked after each successful
            refresh — typically ``NubraIngestionService.reconnect_websocket``.
        interval:
            How often (in seconds) to check whether the session needs
            refreshing. Defaults to ``REFRESH_INTERVAL`` (5 minutes).
        """
        logger.info(
            "SESSION_REFRESH_LOOP_START | env=%s interval=%.0fs",
            self._env_name,
            interval,
        )
        while True:
            try:
                await asyncio.sleep(interval)
                if not session_is_expired():
                    continue
                logger.info("SESSION_EXPIRED | triggering refresh env=%s", self._env_name)
                await self.refresh_session()
                if on_refresh is not None:
                    result = on_refresh()
                    if asyncio.iscoroutine(result):
                        await result
            except asyncio.CancelledError:
                logger.info("SESSION_REFRESH_LOOP_STOP | env=%s (cancelled)", self._env_name)
                raise
            except NubraAuthError as exc:
                logger.error(
                    "SESSION_REFRESH_LOOP | refresh failed env=%s: %s",
                    self._env_name,
                    exc,
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "SESSION_REFRESH_LOOP | unexpected error env=%s; will retry",
                    self._env_name,
                )
