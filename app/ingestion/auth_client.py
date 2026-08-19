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
import contextlib
import json
import logging
import os
import re
import shutil
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
    "ensure_auth_dir",
    "session_is_expired",
]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_AUTH_DB_PREFIX = "auth_data.db"
_AUTH_DB_EXTS = ("dat", "bak", "dir")
DEFAULT_AUTH_DIR = Path("/tmp/auth")
DEFAULT_REFRESH_LOOP_INTERVAL_SECONDS = 5 * 60

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


def _auth_dir(base_dir: str | Path | None = None) -> Path:
    return Path(base_dir or os.getenv("NUBRA_AUTH_DIR") or DEFAULT_AUTH_DIR).resolve()


def ensure_auth_dir(base_dir: str | Path | None = None) -> Path:
    path = _auth_dir(base_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _shelve_path(base_dir: str | Path | None = None) -> str:
    return str(_auth_dir(base_dir) / _AUTH_DB_PREFIX)


def _shelve_exists(base_dir: str | Path | None = None) -> bool:
    base = _auth_dir(base_dir)
    return any((base / f"{_AUTH_DB_PREFIX}.{ext}").exists() for ext in _AUTH_DB_EXTS)


@contextlib.contextmanager
def _sdk_auth_workdir(base_dir: str | Path | None = None):
    target = ensure_auth_dir(base_dir)
    original = Path.cwd()
    os.chdir(target)
    try:
        yield target
    finally:
        os.chdir(original)


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


def _load_session_from_shelve(base_dir: str | Path | None = None) -> dict[str, Optional[str]]:
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
            f"Failed to read auth_data.db.* shelve in {_auth_dir(base_dir)}: {exc}"
        ) from exc


def _write_auth_shelve(
    base_dir: str | Path | None,
    values: dict[str, Optional[str]],
) -> None:
    import dbm.dumb
    import shelve

    base = ensure_auth_dir(base_dir)
    with shelve.Shelf(dbm.dumb.open(str(base / _AUTH_DB_PREFIX), "n"), writeback=True) as db:
        for key, value in values.items():
            if value:
                db[key] = value
        db.sync()
    bak = base / f"{_AUTH_DB_PREFIX}.bak"
    dir_file = base / f"{_AUTH_DB_PREFIX}.dir"
    if dir_file.exists() and not bak.exists():
        shutil.copyfile(dir_file, bak)


def _persist_client_tokens(base_dir: str | Path | None, client: Any) -> None:
    tokens = get_session_tokens(client)
    _write_auth_shelve(
        base_dir,
        {
            "auth_token": tokens.get("auth_token"),
            "session_token": tokens.get("session_token"),
            "x-device-id": tokens.get("x_device_id"),
        },
    )


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


def _prune_stale_bearers(base_dir: str | Path | None = None) -> bool:
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
    base = _auth_dir(base_dir)
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


def bootstrap_session_from_env(base_dir: str | Path | None = None) -> bool:
    """Pre-write the SDK's shelve from env vars.

    Writes ``auth_data.db.*`` from ``NUBRA_AUTH_TOKEN`` /
    ``NUBRA_X_DEVICE_ID`` / ``NUBRA_SESSION_TOKEN`` when the cache is
    missing or incomplete. The ``session_token`` is only written if it
    is still valid; an expired one is dropped so callers cleanly see
    "session expired" instead of trying to reuse a dead token.

    Returns ``True`` if the shelve was (re-)written.
    """
    base = ensure_auth_dir(base_dir)

    auth_token = (os.getenv("NUBRA_AUTH_TOKEN") or os.getenv("AUTH_TOKEN") or "").strip()
    x_device_id = (os.getenv("NUBRA_X_DEVICE_ID") or os.getenv("X_DEVICE_ID") or "").strip()
    session_token = (
        os.getenv("NUBRA_SESSION_TOKEN") or os.getenv("SESSION_TOKEN") or ""
    ).strip()

    if not x_device_id and not auth_token and not session_token:
        return False

    if session_token and _is_jwt_expired(session_token):
        logger.warning(
            "SESSION_EXPIRED | NUBRA_SESSION_TOKEN expired — seeding "
            "x-device-id only to preserve enrolled device binding for TOTP login"
        )
        # Do not clobber a still-valid shelve written by setup_totp.py.
        try:
            import shelve as _shelve

            db_path = str(base / _AUTH_DB_PREFIX)
            if any((base / f"{_AUTH_DB_PREFIX}.{ext}").exists() for ext in _AUTH_DB_EXTS):
                with _shelve.open(db_path, flag="r") as db:
                    cached = db.get("session_token")
                    cached_device = db.get("x-device-id")
                if cached and not _is_jwt_expired(cached):
                    logger.info(
                        "Keeping valid auth_data.db session (env NUBRA_SESSION_TOKEN is stale)"
                    )
                    return False
                if cached_device:
                    return False
        except Exception:  # noqa: BLE001
            pass
        # Still seed device-id so TOTP login reuses the enrolled device.
        # Without this, SDK generates a fresh UUID → server says "TOTP is not enabled".
        if x_device_id:
            _write_auth_shelve(base, {"x-device-id": x_device_id})
            logger.info("DEVICE_ID_SEEDED | x_device_id=%s", _short(x_device_id))
        return False

    if not x_device_id or not auth_token or not session_token:
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
        _write_auth_shelve(
            base,
            {
                "auth_token": auth_token,
                "x-device-id": x_device_id,
                "session_token": session_token,
            },
        )
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


def session_is_expired(*, base_dir: str | Path | None = None) -> bool:
    """Return True when the on-disk session is missing, incomplete, or expired."""
    try:
        tokens = _load_session_from_shelve(base_dir)
    except NubraAuthError:
        return True
    return (
        not tokens.get("auth_token")
        or not tokens.get("x_device_id")
        or _is_jwt_expired(tokens.get("session_token"))
    )


# ---------------------------------------------------------------------------
# Fast path: build a client straight from the shelve, no network
# ---------------------------------------------------------------------------


def _ensure_nubra_ref_master_if_empty(client: Any) -> None:
    """Populate ``InitNubraSdk.DF_REF_DATA_*`` when it is still empty.

    Uses a three-level fallback:
      1. Google Cloud Storage cache (fastest, <5s)
      2. Local CSV bundled in the Docker image (<10s)
      3. Nubra SDK _get_instruments() with 60s timeout + 3 retries

    The SDK's session-fast path arms ``InitNubraSdk.FLAG`` before
    ``__init__``, skipping ``__refresh_ref_data``. This function fills
    the gap without blocking startup indefinitely.
    """
    import pandas as pd
    from nubra_python_sdk.start_sdk import InitNubraSdk

    nse = InitNubraSdk.DF_REF_DATA_NSE
    if isinstance(nse, pd.DataFrame) and not nse.empty:
        return

    from app.storage.instrument_cache import load_instrument_master

    logger.info(
        "Instrument master empty — loading via three-level cache "
        "(GCS → local CSV → Nubra SDK)"
    )

    try:
        df = load_instrument_master(client)
    except RuntimeError as exc:
        logger.error("Instrument master load failed: %s", exc)
        raise NubraAuthError(str(exc)) from exc

    # Inject into the SDK's class-level storage so downstream code
    # (InstrumentData, InstrumentManager) sees the data.
    InitNubraSdk.DF_REF_DATA_NSE = df

    rows = len(df) if isinstance(df, pd.DataFrame) else 0
    logger.info("Instrument master populated | rows=%d", rows)

    if rows == 0:
        raise NubraAuthError(
            "Instrument reference data is empty after all fallback levels. "
            "Upload a valid instrument_master_cache.csv to "
            "gs://stock-anaysis-cache/ or check Nubra API access."
        )


def _try_build_session_only_client(
    env_name: str,
    base_dir: str | Path | None = None,
) -> Optional[Any]:
    """Construct the SDK from cached tokens without any HTTP.

    Returns ``None`` (not raises) when the shelve is missing /
    incomplete / expired so the caller can fall through to the TOTP
    refresh path. Genuinely unexpected errors (e.g. shelve corrupted)
    still raise :class:`NubraAuthError`.
    """
    if not _shelve_exists(base_dir):
        logger.info("auth_data.db.* not present — falling through to TOTP login")
        return None

    tokens = _load_session_from_shelve(base_dir)
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


def _build_via_totp_login(
    env_name: str,
    base_dir: str | Path | None = None,
) -> Any:
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
    _prune_stale_bearers(base_dir)

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
        with _sdk_auth_workdir(base_dir), contextlib.redirect_stdout(captured):
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

    _persist_client_tokens(base_dir, client)

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
    skip_refdata: bool = False,
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

    Parameters
    ----------
    skip_refdata:
        If True, skip loading the instrument reference master after
        authentication. Used by the auth-refresh job which only needs
        to validate the session, not load instruments (which may be
        empty on weekends/holidays when markets are closed).

    ``**_legacy_kwargs`` absorbs older parameters
    (``max_attempts`` / ``base_backoff_seconds`` /
    ``max_session_age_seconds``) so existing callers keep working.
    """
    global _cached_client

    with _client_lock:
        auth_dir = ensure_auth_dir()
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
        bootstrap_session_from_env(auth_dir)

        # 1) Fast path: cached session is still valid.
        client = _try_build_session_only_client(env_name=env_name, base_dir=auth_dir)
        source = "auth_data.db.*"

        # 2) Slow path: TOTP login with patched input.
        if client is None:
            client = _build_via_totp_login(env_name=env_name, base_dir=auth_dir)
            source = "totp-login"

        if not skip_refdata:
            _ensure_nubra_ref_master_if_empty(client)
        else:
            logger.info("skip_refdata=True — skipping instrument master load (auth-only mode)")

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
    on_refresh: Optional[Any] = None,
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
            if not session_is_expired():
                continue
            await asyncio.to_thread(ensure_session_fresh, env_name=env_name)
            logger.info("SESSION_REFRESHED")
            if on_refresh is not None:
                result = on_refresh()
                if asyncio.iscoroutine(result):
                    await result
        except asyncio.CancelledError:
            logger.info("Nubra session refresh loop cancelled")
            raise
        except NubraAuthError as exc:
            logger.error("Session refresh failed: %s", exc)
        except Exception:  # noqa: BLE001
            logger.exception("Session refresh loop iteration crashed; will retry")
