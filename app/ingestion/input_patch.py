"""
Non-interactive input patch for the Nubra SDK.

The SDK calls :func:`input` at several points in its auth flow
(`__prompt_and_verify_totp`, `_login`, `__verify__mpin`,
`__prompt_and_verify_otp`, …). Inside Docker / systemd / cron there is
no usable stdin: any of those calls would block forever waiting on a
closed pipe and the supervisor would eventually kill the container.

This module installs a single global replacement for
:data:`builtins.input` that:

* **Auto-injects TOTP** by generating a 6-digit code from
  ``NUBRA_TOTP_SECRET`` via :mod:`pyotp` whenever the SDK prints
  ``"🔐 Enter TOTP:"`` (or any prompt containing the word "totp").
* **Auto-injects PHONE_NO / MPIN / PASSWORD** from the corresponding
  environment variables, so the SDK never falls into a stdin read for
  routine credentials.
* **Hard-blocks SMS OTP** (``"🔐 Enter OTP:"`` and friends): SMS-OTP is
  unsafe inside containers (no phone in the loop) and means TOTP login
  has not happened — we raise :class:`NubraAuthError` immediately so
  the caller can fix the upstream config rather than mask the failure.
* **One-shot guard for TOTP**: the SDK's `__prompt_and_verify_totp`
  retries up to 3 times by default. If the server keeps rejecting the
  code after the first injection, retrying with a freshly generated
  TOTP will *not* help — the underlying issue is structural (UAT vs
  PROD mismatch, TOTP not enrolled server-side, wrong secret, clock
  skew, …). After the first injection the patch refuses to inject
  again and raises with actionable guidance, breaking the SDK's retry
  loop dead so we don't pile up server-side failures.
* **Validates the secret at install time** by generating a sample
  TOTP. A typo'd / corrupted ``NUBRA_TOTP_SECRET`` fails fast at
  process startup rather than later at the first auth call.

Public API
----------
``install_non_interactive_input_patch``
    Idempotent installer. Call **before** any ``import`` of
    ``nubra_python_sdk`` or ``app.ingestion.auth_client``.
``uninstall_non_interactive_input_patch``
    Test-only helper to restore the original :func:`input`.
``reset_totp_call_count``
    Reset the per-login-cycle TOTP injection counter. The auth client
    calls this immediately before each ``InitNubraSdk(...)`` so a
    successful retry does not leak quota from the previous cycle.
``is_input_patch_installed`` / ``get_totp_call_count``
    Diagnostics for tests and the ``/health`` endpoint.

Integration
-----------
At the very top of ``app/main.py`` (BEFORE importing the SDK or
``auth_client``)::

    from app.core.env_loader import load_project_env
    from app.ingestion.input_patch import install_non_interactive_input_patch

    load_project_env(".")
    install_non_interactive_input_patch()
"""

from __future__ import annotations

import builtins
import logging
import os
import threading
from typing import Any, Callable, Optional

from app.ingestion.auth_errors import NubraAuthError

logger = logging.getLogger(__name__)


__all__ = [
    "install_non_interactive_input_patch",
    "uninstall_non_interactive_input_patch",
    "is_input_patch_installed",
    "reset_totp_call_count",
    "get_totp_call_count",
]


# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

#: How many times the patched ``input`` will inject a fresh TOTP per
#: login cycle. The Nubra SDK retries up to 3× on its own; we cap at 1
#: because all retry-worthy failures (network glitch, momentary 5xx)
#: deserve a fresh `auth_flow`, not a fresh TOTP from the same secret.
DEFAULT_MAX_TOTP_INJECTIONS_PER_CYCLE: int = 1


# ---------------------------------------------------------------------------
# Module state (idempotent install + per-cycle TOTP counter)
# ---------------------------------------------------------------------------

_install_lock = threading.Lock()
_installed: bool = False
_original_input: Optional[Callable[..., str]] = None

_totp_lock = threading.Lock()
_totp_call_count: int = 0


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def is_input_patch_installed() -> bool:
    """Return ``True`` after :func:`install_non_interactive_input_patch` has run."""
    return _installed


def get_totp_call_count() -> int:
    """Number of TOTPs injected since the last :func:`reset_totp_call_count`."""
    return _totp_call_count


def reset_totp_call_count() -> None:
    """Reset the per-cycle TOTP injection counter.

    Call this once **immediately before** each fresh SDK login attempt.
    Exposing the reset (rather than auto-resetting on every input call)
    is what makes the 1-shot guard actually break the SDK's 3-retry
    loop: the SDK calls ``input`` three times in a row, the patch sees
    the counter rising each time, and the second call raises.
    """
    global _totp_call_count
    with _totp_lock:
        _totp_call_count = 0
    logger.debug("TOTP injection counter reset to 0")


# ---------------------------------------------------------------------------
# Prompt classification
# ---------------------------------------------------------------------------


_TOTP_PROMPT_TOKENS = ("totp",)
_SMS_OTP_PROMPT_TOKENS = ("otp",)  # only matched when no TOTP token is present
_PHONE_PROMPT_TOKENS = ("phone", "mobile")
_MPIN_PROMPT_TOKENS = ("mpin",)
_PASSWORD_PROMPT_TOKENS = ("password", "passwd", "current password", "new password")


def _classify(prompt: str) -> str:
    """Coarse classifier for SDK prompts.

    Returns one of ``totp`` / ``sms_otp`` / ``phone`` / ``mpin`` /
    ``password`` / ``unknown``. The ordering matters: ``totp`` is
    checked **before** ``sms_otp`` because the substring "otp" appears
    inside "totp".
    """
    p = (prompt or "").lower()
    if any(tok in p for tok in _TOTP_PROMPT_TOKENS):
        return "totp"
    if any(tok in p for tok in _SMS_OTP_PROMPT_TOKENS):
        return "sms_otp"
    if any(tok in p for tok in _PHONE_PROMPT_TOKENS):
        return "phone"
    if any(tok in p for tok in _MPIN_PROMPT_TOKENS):
        return "mpin"
    if any(tok in p for tok in _PASSWORD_PROMPT_TOKENS):
        return "password"
    return "unknown"


def _short(prompt: str) -> str:
    return (prompt or "").strip().replace("\n", " ")[:120]


# ---------------------------------------------------------------------------
# TOTP generation
# ---------------------------------------------------------------------------


def _read_totp_secret() -> str:
    secret = (os.getenv("NUBRA_TOTP_SECRET") or "").strip()
    if not secret:
        raise NubraAuthError(
            "NUBRA_TOTP_SECRET is not set. The Nubra SDK is asking for a "
            "TOTP but the input patch has nothing to inject. Set "
            "NUBRA_TOTP_SECRET in .env (or the container env) using the "
            "value emitted by setup_totp.py / enroll_totp.py."
        )
    return secret


def _generate_totp(secret: Optional[str] = None) -> str:
    """Generate the current TOTP for ``NUBRA_TOTP_SECRET``.

    Imported lazily so simply importing this module does not pull
    :mod:`pyotp` into processes that never actually exercise auth
    (e.g. unit tests of unrelated modules).
    """
    secret = secret or _read_totp_secret()
    try:
        import pyotp
    except ImportError as exc:
        raise NubraAuthError(
            "pyotp is not installed but is required for non-interactive "
            "TOTP login. Add 'pyotp' to requirements.txt and reinstall."
        ) from exc

    try:
        return pyotp.TOTP(secret).now()
    except Exception as exc:  # noqa: BLE001 — pyotp raises bare Exception subclasses
        raise NubraAuthError(
            f"Failed to generate TOTP from NUBRA_TOTP_SECRET: {exc}. "
            "The secret is likely malformed (must be base32). Re-run "
            "enroll_totp.py to obtain a fresh secret."
        ) from exc


# ---------------------------------------------------------------------------
# The patched ``input``
# ---------------------------------------------------------------------------


def _patched_input(prompt: str = "") -> str:
    """Replacement for :func:`builtins.input`.

    * TOTP prompts: inject a freshly generated code (1× per cycle).
    * Phone / MPIN / Password prompts: inject from env.
    * SMS-OTP prompts: raise :class:`NubraAuthError`.
    * Unknown prompts: raise :class:`NubraAuthError`.
    """
    global _totp_call_count

    kind = _classify(prompt)
    short = _short(prompt)

    if kind == "totp":
        with _totp_lock:
            _totp_call_count += 1
            attempt = _totp_call_count

        if attempt > DEFAULT_MAX_TOTP_INJECTIONS_PER_CYCLE:
            msg = (
                f"TOTP rejected — the Nubra server rejected the first "
                f"injected TOTP and the SDK is retrying (attempt={attempt}). "
                "Refusing to inject again; this is almost always one of:\n"
                "  • UAT vs PROD environment mismatch (NUBRA_ENV does not "
                "match the env where TOTP was enrolled)\n"
                "  • TOTP not enabled for this account in the current "
                "NUBRA_ENV — run setup_totp.py / enroll_totp.py from a "
                "real terminal\n"
                "  • Wrong NUBRA_TOTP_SECRET (rotated server-side)\n"
                "  • Host clock skew >30s — fix NTP\n"
                f"Prompt: {short!r}"
            )
            logger.error(msg)
            raise NubraAuthError(msg)

        try:
            code = _generate_totp()
        except NubraAuthError:
            raise

        logger.info(
            "Auto-injecting TOTP (attempt=%d/%d, prompt=%r)",
            attempt,
            DEFAULT_MAX_TOTP_INJECTIONS_PER_CYCLE,
            short,
        )
        return code

    if kind == "sms_otp":
        msg = (
            f"SMS OTP prompt blocked (prompt={short!r}). The Nubra SDK "
            "fell back to the phone-OTP flow, which means TOTP login "
            "was not attempted (or was rejected before reaching TOTP). "
            "Inside Docker this would hang waiting on a closed stdin — "
            "failing fast instead. Likely cause: stale auth_data.db.* "
            "with no x-device-id, or totp_login=False. Run setup_totp.py "
            "/ enroll_totp.py from a real terminal to repair."
        )
        logger.error(msg)
        raise NubraAuthError(msg)

    if kind == "phone":
        phone = (os.getenv("PHONE_NO") or "").strip()
        if not phone:
            raise NubraAuthError(
                f"SDK requested phone number (prompt={short!r}) but "
                "PHONE_NO is not set in env. Set PHONE_NO in .env."
            )
        logger.info("Auto-injecting PHONE_NO (prompt=%r)", short)
        return phone

    if kind == "mpin":
        mpin = (os.getenv("MPIN") or "").strip()
        if not mpin:
            raise NubraAuthError(
                f"SDK requested MPIN (prompt={short!r}) but MPIN is not "
                "set in env. Set MPIN in .env."
            )
        logger.info("Auto-injecting MPIN (prompt=%r)", short)
        return mpin

    if kind == "password":
        pw = (os.getenv("PASSWORD") or "").strip()
        if not pw:
            raise NubraAuthError(
                f"SDK requested password (prompt={short!r}) but PASSWORD "
                "is not set in env. This usually means insti_login was "
                "triggered unexpectedly."
            )
        logger.info("Auto-injecting PASSWORD (prompt=%r)", short)
        return pw

    msg = (
        f"Unrecognized interactive prompt blocked (prompt={short!r}). "
        "The non-interactive Nubra runtime never expects unknown input(). "
        "If this prompt is legitimate, extend app.ingestion.input_patch._classify."
    )
    logger.error(msg)
    raise NubraAuthError(msg)


# ---------------------------------------------------------------------------
# Public install / uninstall
# ---------------------------------------------------------------------------


def install_non_interactive_input_patch(
    *,
    require_totp_secret: bool = True,
    validate_totp_secret: bool = True,
    **_legacy_kwargs: Any,
) -> None:
    """Globally replace :data:`builtins.input` with :func:`_patched_input`.

    Idempotent — a second call is a no-op (and does not re-validate the
    secret, so rotating the secret + reinstalling requires calling
    :func:`uninstall_non_interactive_input_patch` first).

    Parameters
    ----------
    require_totp_secret
        If ``True`` (default), raise :class:`NubraAuthError` when
        ``NUBRA_TOTP_SECRET`` is missing. Set to ``False`` only if you
        have a fully populated ``auth_data.db.*`` and never expect the
        SDK to fall back to TOTP login (the patch will still raise on
        TOTP prompts in that case — failing fast is the goal).
    validate_totp_secret
        If ``True`` (default), generate a sample TOTP at install time
        to confirm the secret is well-formed base32 and pyotp is
        importable. A bad secret therefore fails at process start
        rather than at the first SDK login.
    """
    global _installed, _original_input

    with _install_lock:
        if _installed:
            logger.debug("Input patch already installed; skipping")
            return

        secret = (os.getenv("NUBRA_TOTP_SECRET") or "").strip()

        if require_totp_secret and not secret:
            raise NubraAuthError(
                "NUBRA_TOTP_SECRET is required to install the input "
                "patch but is not set. Either set it in .env (the value "
                "is emitted by setup_totp.py / enroll_totp.py) or call "
                "install_non_interactive_input_patch(require_totp_secret"
                "=False) if you intend to operate purely from a cached "
                "auth_data.db.* with no TOTP fallback."
            )

        if validate_totp_secret and secret:
            try:
                code = _generate_totp(secret)
            except NubraAuthError:
                raise
            if not (code and code.isdigit() and 6 <= len(code) <= 8):
                raise NubraAuthError(
                    f"Sample TOTP generated from NUBRA_TOTP_SECRET looks "
                    f"invalid (got {code!r}). The secret is malformed."
                )
            logger.info(
                "NUBRA_TOTP_SECRET validated (sample TOTP=%s****, "
                "secret_len=%d, max_injections_per_cycle=%d)",
                code[:2] if code else "",
                len(secret),
                DEFAULT_MAX_TOTP_INJECTIONS_PER_CYCLE,
            )

        _original_input = builtins.input
        builtins.input = _patched_input  # type: ignore[assignment]
        _installed = True

        logger.info(
            "Non-interactive input patch installed "
            "(TOTP auto-injection: %s, max=%d/cycle, SMS-OTP blocked, "
            "phone/MPIN auto-filled from env)",
            "enabled" if secret else "DISABLED (no NUBRA_TOTP_SECRET)",
            DEFAULT_MAX_TOTP_INJECTIONS_PER_CYCLE,
        )


def uninstall_non_interactive_input_patch() -> None:
    """Restore the original :data:`builtins.input`. Test-only helper."""
    global _installed, _original_input

    with _install_lock:
        if not _installed:
            return
        if _original_input is not None:
            builtins.input = _original_input
        _original_input = None
        _installed = False
        logger.info("Non-interactive input patch uninstalled")
