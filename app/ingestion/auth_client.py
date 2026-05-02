"""
Nubra SDK authentication wrapper — session-first with controlled TOTP fallback.

Lifecycle
---------
1. **Session fast path (minimal login HTTP)**
   If ``auth_data.db.*`` holds a non-expired ``session_token`` plus
   ``auth_token`` and ``x-device-id``, we instantiate the SDK with its
   class-level ``FLAG`` pre-armed so ``__init__`` never calls
   ``auth_flow`` / ``__refresh_ref_data`` / ``__get_user_info``. The
   tokens are injected directly into ``token_data`` and ``HEADERS``.
   No stdin. **Reference data** for instruments (the class-level
   ``DF_REF_DATA_*`` used by ``get_instruments_dataframe()``) is then
   loaded with a single ``_get_instruments()`` call, because the
   session shortcut would otherwise skip that step. Logged as
   ``"Using cached session"``.

2. **TOTP fallback (single attempt)**
   When the cache is missing or the cached ``session_token`` JWT has
   expired, we go through the SDK's normal ``_login`` path with
   ``totp_login=True``. The :mod:`app.ingestion.input_patch` installed
   at process startup intercepts the SDK's ``input("🔐 Enter TOTP:")``
   prompt and injects a code generated from ``NUBRA_TOTP_SECRET``.

   Before constructing the SDK we **prune the stale bearer tokens**
   from the shelve while preserving ``x-device-id``. This is critical:
   when the SDK calls ``_verify_existing_session`` against an expired
   ``session_token`` the server returns 401, the SDK's
   ``__verify__mpin`` runs ``self.reset_tokens()`` which **wipes the
   shelve including the registered ``x-device-id``** — and a brand new
   device-id is then sent to ``/totp/login``, which the server has
   never seen, returning the misleading ``"TOTP is not enabled"``. By
   pruning only the bearers we keep the registered device alive, the
   SDK skips ``_verify_existing_session`` (because ``auth_token`` is
   now missing), and goes straight to ``_login``. Logged as
   ``"Refreshing session via TOTP"``.

3. **Hard failure**
   If the TOTP fallback fails (server says "TOTP is not enabled", the
   secret is wrong, the patch counter trips, etc.) we raise
   :class:`NubraAuthError` once and do not retry. The caller — usually
   :class:`app.ingestion.ws_manager.WebSocketManager` — counts the
   failure and eventually marks itself fatal so the supervisor surfaces
   the issue.

Public API
----------
``get_authenticated_client``  — get / build an authenticated client.
``ensure_session_fresh``      — ``get_authenticated_client(force_refresh=True)``.
``run_session_refresh_loop``  — async lifespan helper.
``reset_cached_client``       — drop the in-process cache.
``get_session_tokens``        — extract auth_token / session_token / x_device_id.
``bootstrap_session_from_env``— pre-write shelve from ``NUBRA_*`` env vars.
``NubraAuthError``            — see :mod:`app.ingestion.auth_errors`.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Optional

from app.core.env_loader import load_project_env
from app.ingestion.auth_errors import NubraAuthError

logger = logging.getLogger(__name__)


__all__ = [
    "NubraAuthError",
    "get_authenticated_client",
    "ensure_session_fresh",
    "run_session_refresh_loop",
    "reset_cached_client",
    "get_session_tokens",
    "bootstrap_session_from_env",
]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_AUTH_DB_PREFIX = "auth_data.db"
_AUTH_DB_EXTS = ("dat", "bak", "dir")
DEFAULT_REFRESH_LOOP_INTERVAL_SECONDS = 30 * 60

#: Match server / SDK error strings that mean the device-id is unknown
#: to the server's TOTP roster (re-enrolment required).
_TOTP_NOT_ENABLED_RE = re.compile(r"totp\s*(?:is\s*)?not\s*enabled", re.IGNORECASE)

#: Match SDK strings that mean we wandered into the SMS-OTP fallback.
_SMS_OTP_FALLBACK_RE = re.compile(r"send.*otp|verify.*otp|enter\s*otp", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Process-wide cache
# ---------------------------------------------------------------------------

_client_lock = threading.RLock()
_cached_client: Any = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _short(value: Optional[str], keep: int = 6) -> str:
    if not value:
        return "<none>"
    return f"{value[:keep]}…" if len(value) > keep else value


def _is_jwt_expired(token: Optional[str], leeway_seconds: int = 30) -> bool:
    """Decode a JWT (without signature verification) and check ``exp``."""
    if not token:
        return True
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return True
        payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
        exp = payload.get("exp")
        if exp is None:
            return False
        return time.time() + leeway_seconds >= float(exp)
    except (ValueError, binascii.Error, json.JSONDecodeError, UnicodeDecodeError):
        return True


def _shelve_path(base_dir: str | Path = ".") -> str:
    return str(Path(base_dir) / _AUTH_DB_PREFIX)


def _shelve_exists(base_dir: str | Path = ".") -> bool:
    base = Path(base_dir)
    return any((base / f"{_AUTH_DB_PREFIX}.{ext}").exists() for ext in _AUTH_DB_EXTS)


def _resolve_env_enum(env_name: str) -> Any:
    from nubra_python_sdk.start_sdk import NubraEnv

    mapping = {
        "DEV": NubraEnv.DEV,
        "STAGING": NubraEnv.STAGING,
        "UAT": NubraEnv.UAT,
        "PROD": NubraEnv.PROD,
    }
    enum_value = mapping.get(env_name.upper())
    if enum_value is None:
        raise NubraAuthError(
            f"Unknown NUBRA_ENV={env_name!r}. Expected one of: {sorted(mapping)}"
        )
    return enum_value


def _load_session_from_shelve(base_dir: str | Path = ".") -> dict[str, Optional[str]]:
    if not _shelve_exists(base_dir):
        return {"auth_token": None, "session_token": None, "x_device_id": None}
    try:
        import shelve

        with shelve.open(_shelve_path(base_dir), flag="r") as db:
            return {
                "auth_token": db.get("auth_token"),
                "session_token": db.get("session_token"),
                "x_device_id": db.get("x-device-id"),
            }
    except Exception as exc:  # noqa: BLE001
        raise NubraAuthError(
            f"Failed to read auth_data.db.* shelve in {Path(base_dir).resolve()}: {exc}"
        ) from exc


def get_session_tokens(client: Any) -> dict[str, Optional[str]]:
    """Extract ``auth_token`` / ``session_token`` / ``x_device_id`` from the SDK."""
    token_data = getattr(client, "token_data", {}) or {}
    headers = getattr(client, "HEADERS", {}) or {}

    auth_header = headers.get("Authorization") or ""
    bearer = auth_header[len("Bearer "):] if auth_header.startswith("Bearer ") else None

    return {
        "auth_token": token_data.get("auth_token"),
        "session_token": token_data.get("session_token") or bearer,
        "x_device_id": token_data.get("x-device-id") or headers.get("x-device-id"),
    }


def _prune_stale_bearers(base_dir: str | Path = ".") -> bool:
    """Remove expired ``auth_token`` / ``session_token`` from the shelve.

    The ``x-device-id`` is **always** preserved. Returns ``True`` if
    anything was actually deleted.

    Why this matters
    ----------------
    The SDK's ``_verify_existing_session`` calls ``/verifypin`` with the
    cached bearer. If the bearer is expired the server returns 401 and
    the SDK's ``__verify__mpin`` then calls ``self.reset_tokens()``,
    which **clears every key** in the shelve — including
    ``x-device-id``. The next ``__ensure_device_id`` therefore mints a
    brand-new uuid which the server has never seen and ``/totp/login``
    fails with "TOTP is not enabled". Pruning ahead of time avoids the
    whole 401 round-trip.
    """
    if not _shelve_exists(base_dir):
        return False
    base = Path(base_dir)
    try:
        import shelve

        changed = False
        with shelve.open(str(base / _AUTH_DB_PREFIX), flag="w", writeback=True) as db:
            session_token = db.get("session_token")
            auth_token = db.get("auth_token")
            if session_token and _is_jwt_expired(session_token):
                del db["session_token"]
                changed = True
            # auth_token is a UUID in the SDK schema, not a JWT, so we
            # cannot inspect its expiry; we drop it whenever
            # session_token was expired so the SDK skips
            # _verify_existing_session and goes straight to _login.
            if changed and auth_token is not None:
                del db["auth_token"]
            if changed:
                db.sync()
        if changed:
            logger.info(
                "Pruned stale bearer from auth_data.db.* (kept x-device-id)"
            )
        return changed
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not prune stale bearer from shelve: %s", exc)
        return False


def bootstrap_session_from_env(base_dir: str | Path = ".") -> bool:
    """Pre-write the SDK's shelve from env vars.

    Writes ``auth_data.db.*`` from ``NUBRA_AUTH_TOKEN`` /
    ``NUBRA_X_DEVICE_ID`` / ``NUBRA_SESSION_TOKEN`` when the cache is
    missing or incomplete. The ``session_token`` is only written if it
    is still valid; an expired one is dropped so callers cleanly see
    "session expired" instead of trying to reuse a dead token.

    Returns ``True`` if the shelve was (re-)written.
    """
    base = Path(base_dir)

    auth_token = (os.getenv("NUBRA_AUTH_TOKEN") or os.getenv("AUTH_TOKEN") or "").strip()
    x_device_id = (os.getenv("NUBRA_X_DEVICE_ID") or os.getenv("X_DEVICE_ID") or "").strip()
    session_token = (
        os.getenv("NUBRA_SESSION_TOKEN") or os.getenv("SESSION_TOKEN") or ""
    ).strip()

    if not x_device_id or not auth_token or not session_token:
        return False
    if _is_jwt_expired(session_token):
        logger.warning(
            "NUBRA_SESSION_TOKEN in env is expired; not writing it to "
            "auth_data.db. Refresh it via setup_totp.py / enroll_totp.py."
        )
        return False

    has_files = any((base / f"{_AUTH_DB_PREFIX}.{ext}").exists() for ext in _AUTH_DB_EXTS)
    needs_write = not has_files
    if has_files:
        try:
            import shelve as _shelve

            with _shelve.open(str(base / _AUTH_DB_PREFIX), flag="r") as db:
                if (
                    not db.get("x-device-id")
                    or not db.get("auth_token")
                    or _is_jwt_expired(db.get("session_token"))
                ):
                    needs_write = True
        except Exception:  # noqa: BLE001
            needs_write = True

    if not needs_write:
        return False

    try:
        import shelve

        with shelve.open(str(base / _AUTH_DB_PREFIX), flag="c", writeback=True) as db:
            db["auth_token"] = auth_token
            db["x-device-id"] = x_device_id
            db["session_token"] = session_token
            db.sync()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to bootstrap auth_data.db from env: %s", exc)
        return False

    logger.info(
        "Bootstrapped auth_data.db from env vars (x_device_id=%s, "
        "auth_token=%s, session_token=%s).",
        _short(x_device_id),
        _short(auth_token),
        _short(session_token),
    )
    return True


def reset_cached_client() -> None:
    global _cached_client
    with _client_lock:
        _cached_client = None


# ---------------------------------------------------------------------------
# Fast path: build a client straight from the shelve, no network
# ---------------------------------------------------------------------------


def _ensure_nubra_ref_master_if_empty(client: Any) -> None:
    """Populate ``InitNubraSdk.DF_REF_DATA_*`` when it is still empty.

    The session-fast path arms ``InitNubraSdk.FLAG`` **before**
    ``InitNubraSdk.__init__``, so the SDK skips ``__refresh_ref_data``
    — that is where ``_get_instruments()`` runs and fills the master.
    Without this call, ``InstrumentData(client).get_instruments_dataframe()``
    always returns an empty frame.

    The TOTP / full-login path already runs ``__refresh_ref_data`` in
    ``__init__``; this helper no-ops when ``DF_REF_DATA_NSE`` already
    has rows.
    """
    import pandas as pd
    from nubra_python_sdk.start_sdk import InitNubraSdk

    nse = InitNubraSdk.DF_REF_DATA_NSE
    if isinstance(nse, pd.DataFrame) and not nse.empty:
        return

    getter = getattr(client, "_get_instruments", None)
    if not callable(getter):
        raise NubraAuthError(
            "Nubra SDK client has no _get_instruments(); cannot load instrument master."
        )

    logger.info(
        "Instrument master empty — calling SDK _get_instruments() "
        "(session-fast InitNubraSdk.__init__ skips __refresh_ref_data)"
    )
    try:
        status = getter()
    except Exception as exc:
        logger.exception("Nubra _get_instruments failed: %s", exc)
        raise NubraAuthError(
            f"Failed to download instrument master from Nubra refdata API: {exc}"
        ) from exc

    nse2 = InitNubraSdk.DF_REF_DATA_NSE
    rows = len(nse2) if isinstance(nse2, pd.DataFrame) else 0
    logger.info(
        "Instrument master loaded | refdata_http=%s | nse_rows=%d",
        status,
        rows,
    )
    if rows == 0:
        raise NubraAuthError(
            "Instrument reference data is empty after calling the Nubra refdata API "
            f"(HTTP status {status}). Check NUBRA_ENV matches your enrolment, refresh "
            "auth_data.db.* (setup_totp.py / enroll_totp.py), and verify your session "
            "can access GET /refdata/refdata/<today>."
        )


def _try_build_session_only_client(env_name: str) -> Optional[Any]:
    """Construct the SDK from cached tokens without any HTTP.

    Returns ``None`` (not raises) when the shelve is missing /
    incomplete / expired so the caller can fall through to the TOTP
    refresh path. Genuinely unexpected errors (e.g. shelve corrupted)
    still raise :class:`NubraAuthError`.
    """
    if not _shelve_exists("."):
        logger.info("auth_data.db.* not present — falling through to TOTP login")
        return None

    tokens = _load_session_from_shelve(".")
    auth_token = tokens.get("auth_token")
    session_token = tokens.get("session_token")
    x_device_id = tokens.get("x_device_id")

    missing = [
        name for name, val in (
            ("auth_token", auth_token),
            ("session_token", session_token),
            ("x_device_id", x_device_id),
        ) if not val
    ]
    if missing:
        logger.info(
            "auth_data.db.* incomplete (missing %s) — falling through to TOTP login",
            missing,
        )
        return None

    if _is_jwt_expired(session_token):
        logger.info(
            "Cached session_token JWT expired — falling through to TOTP login"
        )
        return None

    from nubra_python_sdk.start_sdk import InitNubraSdk

    env_enum = _resolve_env_enum(env_name)

    # Pre-arm the SDK's class-level guard so __init__ becomes a pure
    # configuration step (no network, no input(), no shelve mutation).
    if hasattr(InitNubraSdk, "FLAG") and isinstance(InitNubraSdk.FLAG, dict):
        InitNubraSdk.FLAG["value"] = True

    client = InitNubraSdk(env=env_enum, totp_login=False, env_creds=False)

    client.token_data = {
        "auth_token": auth_token,
        "session_token": session_token,
        "x-device-id": x_device_id,
        "time_refdata": time.time(),
    }
    client.HEADERS["x-device-id"] = x_device_id
    client.HEADERS["Authorization"] = f"Bearer {session_token}"
    client.HEADERS["x-app-version"] = getattr(client, "VERSION", "")
    client.HEADERS["x-device-os"] = "sdk"
    client.HEADERS["Cookie"] = f"deviceId={x_device_id}"
    InitNubraSdk.BEARER_TOKEN = session_token

    return client


# ---------------------------------------------------------------------------
# Slow path: SDK login with patched input → TOTP auto-injected
# ---------------------------------------------------------------------------


def _classify_login_failure(captured: str, exc: BaseException) -> str:
    """Translate an SDK login crash into actionable guidance."""
    blob = (str(exc) + "\n" + (captured or "")).lower()

    if _TOTP_NOT_ENABLED_RE.search(blob):
        return (
            "Server reports 'TOTP is not enabled' for this account in "
            "the current NUBRA_ENV. Almost always one of:\n"
            "  • UAT vs PROD mismatch — set NUBRA_ENV to the env where "
            "TOTP was enrolled.\n"
            "  • The server has de-enrolled this device (commonly after "
            "the SDK's reset_tokens() wiped x-device-id). Re-enrol with "
            "  python enroll_totp.py --write-env\n"
            "  • TOTP was never enabled in this env. Run setup_totp.py."
        )

    if _SMS_OTP_FALLBACK_RE.search(blob):
        return (
            "SDK fell back to SMS-OTP, which the input patch blocks. "
            "This usually means TOTP was rejected and the SDK is "
            "retrying via the phone-OTP path. Run enroll_totp.py to "
            "re-establish TOTP."
        )

    if "maximum otp attempts exceeded" in blob:
        return (
            "SDK exhausted its 3 TOTP retries. Common causes: clock "
            "skew >30s on this host (fix NTP), a stale "
            "NUBRA_TOTP_SECRET (rotate via enroll_totp.py), or wrong "
            "NUBRA_ENV."
        )

    if "totp injection counter" in blob or "totp rejected" in blob:
        return (
            "Input patch broke the SDK retry loop after the first "
            "rejected TOTP — see the error above for the structural "
            "cause (env mismatch / de-enrolment / clock / secret)."
        )

    return ""


def _build_via_totp_login(env_name: str) -> Any:
    """Run the SDK login flow with the patched input injecting TOTP.

    Captures stdout for diagnostics, prunes stale bearers first to
    protect the registered ``x-device-id``, and resets the input
    patch's per-cycle TOTP counter so the 1-shot guard kicks in fresh.
    """
    import contextlib
    import io

    # Validate prerequisites up front so we fail with the right message
    # rather than letting the SDK return a half-built client.
    if not (os.getenv("NUBRA_TOTP_SECRET") or "").strip():
        raise NubraAuthError(
            "Refresh required (cached session is missing or expired) "
            "but NUBRA_TOTP_SECRET is not set. Either set it (output of "
            "enroll_totp.py) or refresh auth_data.db.* externally and "
            "restart."
        )

    # Reset the 1-shot TOTP guard so this cycle gets a clean budget.
    try:
        from app.ingestion.input_patch import (
            is_input_patch_installed,
            reset_totp_call_count,
        )

        if not is_input_patch_installed():
            raise NubraAuthError(
                "Input patch is not installed but session needs a TOTP "
                "refresh. install_non_interactive_input_patch() must be "
                "called at the very top of app/main.py before any SDK "
                "import."
            )
        reset_totp_call_count()
    except ImportError as exc:
        raise NubraAuthError(
            f"app.ingestion.input_patch is not importable: {exc}"
        ) from exc

    # Protect the registered x-device-id from the SDK's reset_tokens().
    _prune_stale_bearers(".")

    from nubra_python_sdk.start_sdk import InitNubraSdk

    env_enum = _resolve_env_enum(env_name)

    # Make sure FLAG is False so __init__ actually runs auth_flow.
    if hasattr(InitNubraSdk, "FLAG") and isinstance(InitNubraSdk.FLAG, dict):
        InitNubraSdk.FLAG["value"] = False

    logger.info(
        "Refreshing session via TOTP | env=%s | totp_login=True env_creds=True",
        env_name,
    )

    captured = io.StringIO()
    try:
        with contextlib.redirect_stdout(captured):
            client = InitNubraSdk(
                env=env_enum,
                totp_login=True,
                env_creds=True,
            )
    except NubraAuthError:
        # Already informative — re-raise verbatim.
        raise
    except Exception as exc:  # noqa: BLE001 — SDK raises bare Exception
        diag = _classify_login_failure(captured.getvalue(), exc)
        msg = f"Nubra TOTP login failed: {exc}"
        if diag:
            msg = f"{msg}\nDiagnosis:\n  {diag}"
        raise NubraAuthError(msg) from exc

    # Verify the SDK actually populated tokens — it has been seen to
    # silently return a tokenless client when reset_tokens() ran.
    tokens = get_session_tokens(client)
    if not tokens.get("session_token") or not tokens.get("x_device_id"):
        diag = _classify_login_failure(captured.getvalue(), RuntimeError("tokens missing"))
        raise NubraAuthError(
            "TOTP login returned a client without session_token / "
            "x_device_id (SDK silently bailed mid-login).\n"
            f"Diagnosis:\n  {diag or 'Inspect captured SDK output above.'}"
        )

    if _is_jwt_expired(tokens.get("session_token")):
        raise NubraAuthError(
            "TOTP login completed but the returned session_token is "
            "already expired — likely host clock drift."
        )

    logger.info(
        "Session refresh successful | env=%s | x_device_id=%s | "
        "session_token=%s",
        env_name,
        _short(tokens.get("x_device_id")),
        _short(tokens.get("session_token")),
    )
    return client


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_authenticated_client(
    *,
    env_name: str = "UAT",
    force_refresh: bool = False,
    **_legacy_kwargs: Any,
) -> Any:
    """Return an authenticated Nubra SDK client.

    Behaviour:

    * Returns the in-process cached client if its ``session_token`` is
      still valid (and ``force_refresh=False``).
    * Otherwise tries the session fast path
      (:func:`_try_build_session_only_client`) — no network.
    * Falls back to a single TOTP login
      (:func:`_build_via_totp_login`) when the cache is empty / stale.
    * Raises :class:`NubraAuthError` once on any unrecoverable failure.

    ``**_legacy_kwargs`` absorbs older parameters
    (``max_attempts`` / ``base_backoff_seconds`` /
    ``max_session_age_seconds``) so existing callers keep working.
    """
    global _cached_client

    with _client_lock:
        if _cached_client is not None and not force_refresh:
            tokens = get_session_tokens(_cached_client)
            if not _is_jwt_expired(tokens.get("session_token")):
                return _cached_client
            logger.info(
                "In-process cached session_token expired; rebuilding"
            )

        if force_refresh:
            logger.info("force_refresh=True — rebuilding Nubra client from disk / TOTP")
            _cached_client = None

        load_project_env(".")

        # Allow ops to seed the shelve from env vars before we read it.
        bootstrap_session_from_env(".")

        # 1) Fast path: cached session is still valid.
        client = _try_build_session_only_client(env_name=env_name)
        source = "auth_data.db.*"

        # 2) Slow path: TOTP login with patched input.
        if client is None:
            client = _build_via_totp_login(env_name=env_name)
            source = "totp-login"

        _ensure_nubra_ref_master_if_empty(client)

        _cached_client = client

        tokens = get_session_tokens(client)
        logger.info(
            "%s | env=%s | auth_token=%s | x_device_id=%s | "
            "session_token=%s | source=%s",
            "Using cached session" if source == "auth_data.db.*"
            else "Refreshed session via TOTP",
            env_name,
            _short(tokens.get("auth_token")),
            _short(tokens.get("x_device_id")),
            _short(tokens.get("session_token")),
            source,
        )
        return client


def ensure_session_fresh(
    *,
    env_name: str = "UAT",
    **_legacy_kwargs: Any,
) -> Any:
    """Force a fresh client build (re-reads shelve, retries TOTP if needed)."""
    return get_authenticated_client(env_name=env_name, force_refresh=True)


async def run_session_refresh_loop(
    *,
    env_name: str = "UAT",
    interval_seconds: float = DEFAULT_REFRESH_LOOP_INTERVAL_SECONDS,
    **_legacy_kwargs: Any,
) -> None:
    """Periodically re-validate the cached session.

    Re-reads ``auth_data.db.*`` (and triggers TOTP refresh if the cache
    is stale) every ``interval_seconds``. Failures are logged but do
    not crash the loop — the WebSocket manager surfaces them
    independently via its ``fatal`` state.

    Spawn from your FastAPI lifespan / async runner::

        tasks.append(asyncio.create_task(
            run_session_refresh_loop(env_name=settings.nubra_env),
            name="nubra-auth-refresh",
        ))
    """
    logger.info(
        "Starting Nubra session refresh loop (interval=%.0fs)",
        interval_seconds,
    )
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            await asyncio.to_thread(ensure_session_fresh, env_name=env_name)
        except asyncio.CancelledError:
            logger.info("Nubra session refresh loop cancelled")
            raise
        except NubraAuthError as exc:
            logger.error("Session refresh failed: %s", exc)
        except Exception:  # noqa: BLE001
            logger.exception("Session refresh loop iteration crashed; will retry")
