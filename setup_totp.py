#!/usr/bin/env python
"""
One-time interactive TOTP verification for the Nubra SDK.

Run this **outside Docker** on a machine with a real terminal. It does
exactly one thing:

    Build the Nubra SDK with ``totp_login=True`` and let YOU type the
    current 6-digit code from your authenticator app at the SDK's
    "🔐 Enter TOTP:" prompt.

If the login succeeds, it confirms that TOTP is correctly enabled for
your account in the chosen ``NUBRA_ENV``. Once confirmed, the Docker
runtime can use the same flow non-interactively via the patched
``builtins.input`` and ``NUBRA_TOTP_SECRET`` (see :mod:`app.ingestion.auth_client`).

Important
---------
* TOTP must be enabled **separately** for UAT and PROD. Run this script
  once per environment you intend to use.
* This script intentionally does NOT install the project's
  non-interactive input patch — it needs real stdin.
* If the server replies "TOTP is not enabled", that means TOTP was
  never enrolled for this account in this environment. Enable it via
  the Nubra web/mobile portal (security settings → enable TOTP / 2FA),
  then re-run this script to verify.

Pre-requisites
--------------
``.env`` must contain at least::

    PHONE_NO=<your Nubra phone number>
    MPIN=<your Nubra MPIN>
    NUBRA_ENV=UAT       # or PROD

Usage
-----
::

    python setup_totp.py
    python setup_totp.py --env UAT
    python setup_totp.py --env PROD
"""

from __future__ import annotations

import argparse
import contextlib
import io
import os
import sys
from typing import Any

# Do NOT import app.ingestion.input_patch here — this script must keep
# stdin interactive so the user can type the TOTP at the SDK's prompt.
from app.core.env_loader import load_project_env


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bar(title: str = "") -> None:
    line = "=" * 78
    if title:
        print(f"\n{line}\n  {title}\n{line}")
    else:
        print(line)


def _resolve_env_enum(env_name: str) -> Any:
    from nubra_python_sdk.start_sdk import NubraEnv

    mapping = {
        "DEV": NubraEnv.DEV,
        "STAGING": NubraEnv.STAGING,
        "UAT": NubraEnv.UAT,
        "PROD": NubraEnv.PROD,
    }
    if env_name.upper() not in mapping:
        raise SystemExit(
            f"[!] Unknown env {env_name!r}; expected one of {sorted(mapping)}"
        )
    return mapping[env_name.upper()]


class _Tee:
    """File-like writer that fans out to every wrapped stream.

    Used to mirror stdout to both the terminal (so the user sees the
    SDK's own prompts and error messages) and an in-memory buffer (so
    we can post-process server error text after a failed login).
    """

    def __init__(self, *streams: Any) -> None:
        self._streams = streams

    def write(self, data: str) -> int:
        for s in self._streams:
            try:
                s.write(data)
            except Exception:  # noqa: BLE001
                pass
        return len(data)

    def flush(self) -> None:
        for s in self._streams:
            try:
                s.flush()
            except Exception:  # noqa: BLE001
                pass


def _diagnose_failure(captured: str, env_name: str) -> str:
    """Translate captured SDK stdout into actionable guidance."""
    lower = (captured or "").lower()

    if "totp is not enabled" in lower or "totp not enabled" in lower:
        return (
            f"Nubra server reports TOTP is NOT enabled in env={env_name}.\n"
            "  This means TOTP was never enrolled for this account in this\n"
            "  environment. Fix:\n"
            "    1. Open the Nubra web/mobile portal logged in as this account.\n"
            "    2. Go to Security / 2FA settings and enable TOTP\n"
            "       (scan the QR code with Google Authenticator / Authy).\n"
            "    3. Re-run this script to verify the new enrolment.\n"
            "  Remember: TOTP must be enabled separately on UAT and PROD."
        )

    if "maximum otp attempts exceeded" in lower:
        return (
            "The SDK's 3-attempt TOTP retry budget was exhausted. Likely\n"
            "  causes:\n"
            "    • The 6-digit code was stale (>30 s old) — type quicker.\n"
            "    • System clock on this machine drifted — make sure NTP is\n"
            "      enabled.\n"
            "    • You typed the wrong code, or used an authenticator entry\n"
            "      bound to a different account / environment."
        )

    if "totp verification failed" in lower:
        return (
            "Nubra rejected the TOTP code(s) you entered. Most likely:\n"
            "    • Wrong account / wrong environment selected.\n"
            "    • Authenticator entry was set up for a different secret\n"
            "      than what is currently active on the server."
        )

    return ""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="One-time interactive TOTP verification for Nubra.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--env",
        default=None,
        help="Nubra environment to verify (UAT, PROD, STAGING, DEV). "
             "Defaults to NUBRA_ENV from .env, then UAT.",
    )
    args = parser.parse_args()

    load_project_env(".")

    env_name = (args.env or os.getenv("NUBRA_ENV") or "UAT").upper()
    phone = (os.getenv("PHONE_NO") or "").strip()
    mpin = (os.getenv("MPIN") or "").strip()

    _bar(f"Nubra TOTP verification | env={env_name}")
    print(f"  Phone : {phone or '<NOT SET>'}")
    print(f"  MPIN  : {'set' if mpin else '<NOT SET>'}\n")

    print("[!] TOTP must be enabled separately for each environment.")
    print(f"[!] You are verifying: {env_name}\n")

    if not phone or not mpin:
        print("[X] PHONE_NO and MPIN must be set in .env before running this.")
        return 2

    print("Instructions:")
    print("  1. Open your authenticator app (Google Authenticator / Authy).")
    print(f"  2. Find the entry for your Nubra account in {env_name}.")
    print("  3. When the SDK prompts '🔐 Enter TOTP:', type the current")
    print("     6-digit code shown by the app and press Enter.")
    print("  4. The SDK allows up to 3 attempts before giving up.")
    print()

    # ---------- Run the SDK login ------------------------------------------
    from nubra_python_sdk.start_sdk import InitNubraSdk

    env_enum = _resolve_env_enum(env_name)

    capture = io.StringIO()
    client: Any = None
    try:
        with contextlib.redirect_stdout(_Tee(sys.stdout, capture)):
            # totp_login=True activates the "🔐 Enter TOTP:" prompt;
            # env_creds=True auto-fills phone + MPIN so only TOTP is
            # interactive.
            client = InitNubraSdk(
                env=env_enum,
                totp_login=True,
                env_creds=True,
            )
    except Exception as exc:  # noqa: BLE001 — SDK raises bare Exception
        print()
        _bar("TOTP verification FAILED")
        print(f"  Error: {exc}\n")
        diag = _diagnose_failure(capture.getvalue(), env_name)
        if diag:
            print("  Diagnosis:\n")
            for line in diag.splitlines():
                print(f"  {line}")
        return 1

    # ---------- Success ----------------------------------------------------
    token_data = getattr(client, "token_data", {}) or {}
    headers = getattr(client, "HEADERS", {}) or {}

    auth_token = token_data.get("auth_token") or ""
    x_device_id = token_data.get("x-device-id") or headers.get("x-device-id") or ""
    session_token = token_data.get("session_token") or ""
    if not session_token:
        bearer = headers.get("Authorization") or ""
        if bearer.startswith("Bearer "):
            session_token = bearer[len("Bearer "):]

    print()
    _bar("TOTP successfully enabled for this environment")
    print(f"  env         : {env_name}")
    print(f"  phone       : {phone}")
    print(f"  auth_token  : {auth_token[:24]}…" if auth_token else "  auth_token  : <missing>")
    print(f"  x_device_id : {x_device_id[:24]}…" if x_device_id else "  x_device_id : <missing>")
    print(f"  session_tok : {session_token[:24]}…" if session_token else "  session_tok : <missing>")
    print()
    print(f"TOTP login is confirmed working in {env_name}.")

    # ---------- Docker portability snippet ---------------------------------
    # The Nubra server binds x-device-id ↔ phone during enrolment. A fresh
    # container generates a different device-id and gets rejected. Either
    # volume-mount auth_data.db.* OR export the tokens below.
    _bar("Docker / cross-machine portability snippet")
    print("Nubra server binds x-device-id to phone during enrolment, so a")
    print("fresh container with a brand-new device-id will be rejected by")
    print("/totp/login — even though TOTP works fine on this machine.")
    print()
    print("Two ways to make Docker reuse this enrolment:")
    print()
    print("Option A — volume-mount the shelve cache:")
    print("  Add to docker-compose.yml (or `docker run -v`):")
    print("    volumes:")
    print("      - ./auth_data.db.dat:/app/auth_data.db.dat")
    print("      - ./auth_data.db.bak:/app/auth_data.db.bak")
    print("      - ./auth_data.db.dir:/app/auth_data.db.dir")
    print()
    print("Option B — export these env vars to your container (recommended):")
    print("  Copy-paste into your docker-compose env: / .env / Kubernetes secret:")
    print()
    if auth_token and x_device_id:
        print("    # ---- Nubra session bootstrap (rotate periodically) ----")
        print(f"    NUBRA_AUTH_TOKEN={auth_token}")
        print(f"    NUBRA_X_DEVICE_ID={x_device_id}")
        if session_token:
            print(f"    NUBRA_SESSION_TOKEN={session_token}")
        print(f"    NUBRA_ENV={env_name}")
        print(f"    NUBRA_TOTP_SECRET=<your existing secret>")
        print("    # -------------------------------------------------------")
    else:
        print("    [!] Could not extract auth_token/x_device_id from the SDK.")
        print("        Inspect auth_data.db.* in this directory manually.")
    print()
    print("On startup the container's app.ingestion.auth_client will detect")
    print("these env vars, pre-write auth_data.db.*, and the SDK will skip")
    print("/totp/login entirely — no more 'TOTP is not enabled' error.")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
