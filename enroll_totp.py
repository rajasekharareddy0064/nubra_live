#!/usr/bin/env python
"""
One-time TOTP **enrolment** for the Nubra SDK.

Use this script when:

* TOTP has never been enabled for the account in this environment, OR
* The Nubra server has de-enrolled the device (you see ``"TOTP is not
  enabled"`` from ``/totp/login`` even though TOTP "worked yesterday"),
  OR
* The cached ``auth_data.db.*`` is missing or stale and a fresh
  enrolment is required.

Run from a **real terminal** on a trusted machine. Never inside Docker.

Steps (interactive)
-------------------

1.  Wipes any existing ``auth_data.db.*`` so the SDK does a fresh
    phone-OTP login (this is the ONLY way to obtain a server-recognised
    device binding).
2.  Asks Nubra to send an SMS OTP to your phone. You type the code at
    the SDK's ``"📱 Enter OTP:"`` prompt.
3.  Once logged in, calls ``totp_generate_secret()`` to ask the server
    for a fresh TOTP secret. The new secret + a manual-entry string
    are printed.
4.  You add the entry to your authenticator app (Google Authenticator
    / Authy / 1Password / …) — scan the QR or paste the secret.
5.  You type the current 6-digit code at the next prompt; the script
    calls ``totp_enable()`` to register your authenticator with the
    server.
6.  Prints the env-var snippet to copy-paste into ``.env`` (or your
    Docker/K8s secret manager) so the runtime can use session-only
    auth.

After success, ``app.ingestion.auth_client.get_authenticated_client``
will load the cached session without ever calling /totp/login.

Usage
-----
::

    python enroll_totp.py
    python enroll_totp.py --env UAT
    python enroll_totp.py --env PROD
    python enroll_totp.py --write-env       # auto-update .env on success
"""

from __future__ import annotations

import argparse
import contextlib
import glob
import io
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional

# IMPORTANT: do NOT import app.ingestion.input_patch here. This script
# needs real interactive stdin so the user can type SMS OTP and TOTP at
# the SDK's prompts.
from app.core.env_loader import load_project_env


# ---------------------------------------------------------------------------
# Terminal helpers
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
    """File-like writer that fans out to every wrapped stream."""

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


def _wipe_existing_cache() -> list[str]:
    """Remove any pre-existing auth_data.db.* so the SDK starts clean."""
    removed = []
    for path in glob.glob("auth_data.db*"):
        try:
            os.remove(path)
            removed.append(path)
        except OSError as exc:
            print(f"[!] Could not remove {path}: {exc}")
    return removed


# ---------------------------------------------------------------------------
# Secret extraction
# ---------------------------------------------------------------------------

# ``totp_generate_secret`` returns either a JSON blob with a ``secret`` /
# ``otpauth_url`` field or a free-form string containing the secret. We
# accept both.

_OTPAUTH_RE = re.compile(r"otpauth://totp/[^?\s\"']+\?[^\s\"']+")
_BASE32_RE = re.compile(r"\b([A-Z2-7]{16,})\b")


def _extract_secret(blob: str) -> tuple[Optional[str], Optional[str]]:
    """Return ``(base32_secret, otpauth_url)`` from arbitrary SDK output."""
    if not blob:
        return None, None

    # 1) Try strict JSON first.
    try:
        import json

        data = json.loads(blob)
        if isinstance(data, dict):
            secret = (
                data.get("secret")
                or data.get("totp_secret")
                or (data.get("data") or {}).get("secret")
                or (data.get("data") or {}).get("totp_secret")
            )
            url = (
                data.get("otpauth_url")
                or data.get("url")
                or (data.get("data") or {}).get("otpauth_url")
            )
            if secret or url:
                return secret, url
    except (ValueError, TypeError):
        pass

    # 2) Embedded otpauth:// URL anywhere in the blob.
    url_match = _OTPAUTH_RE.search(blob)
    url = url_match.group(0) if url_match else None
    secret = None
    if url:
        m = re.search(r"[?&]secret=([A-Z2-7]+)", url, re.IGNORECASE)
        if m:
            secret = m.group(1).upper()

    # 3) Bare base32 token (fallback).
    if not secret:
        m = _BASE32_RE.search(blob)
        if m:
            secret = m.group(1)

    return secret, url


# ---------------------------------------------------------------------------
# .env writer
# ---------------------------------------------------------------------------

_ENV_KEYS = (
    "NUBRA_ENV",
    "NUBRA_AUTH_TOKEN",
    "NUBRA_X_DEVICE_ID",
    "NUBRA_SESSION_TOKEN",
    "NUBRA_TOTP_SECRET",
)


def _update_env_file(updates: dict[str, str], path: str = ".env") -> None:
    """Idempotently update / append the given keys in ``.env``."""
    p = Path(path)
    existing = p.read_text(encoding="utf-8") if p.exists() else ""
    lines = existing.splitlines()
    seen = {k: False for k in updates}
    out: list[str] = []

    for line in lines:
        stripped = line.lstrip()
        # Match commented or live key=value lines for our targeted keys.
        for k in updates:
            if (
                stripped.startswith(f"{k}=")
                or stripped.startswith(f"# {k}=")
                or stripped.startswith(f"#{k}=")
            ):
                out.append(f"{k}={updates[k]}")
                seen[k] = True
                break
        else:
            out.append(line)

    appended = [f"{k}={v}" for k, v in updates.items() if not seen[k]]
    if appended:
        if out and out[-1].strip():
            out.append("")
        out.append("# --- Nubra session bootstrap (auto-written by enroll_totp.py) ---")
        out.extend(appended)
        out.append("# ----------------------------------------------------------------")

    p.write_text("\n".join(out) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="One-time interactive TOTP enrolment for Nubra.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--env",
        default=None,
        help="Nubra environment (UAT, PROD, STAGING, DEV). Defaults to "
             "NUBRA_ENV from .env, then UAT.",
    )
    parser.add_argument(
        "--write-env",
        action="store_true",
        help="On success, write NUBRA_ENV / NUBRA_AUTH_TOKEN / "
             "NUBRA_X_DEVICE_ID / NUBRA_SESSION_TOKEN / NUBRA_TOTP_SECRET "
             "into .env (creates / updates the file in place).",
    )
    parser.add_argument(
        "--keep-cache",
        action="store_true",
        help="Skip wiping auth_data.db.* before login. Use only if you "
             "are intentionally re-running enrolment with a known-good "
             "device-id already on disk.",
    )
    args = parser.parse_args()

    load_project_env(".")

    env_name = (args.env or os.getenv("NUBRA_ENV") or "UAT").upper()
    phone = (os.getenv("PHONE_NO") or "").strip()
    mpin = (os.getenv("MPIN") or "").strip()

    _bar(f"Nubra TOTP ENROLMENT | env={env_name}")
    print(f"  Phone : {phone or '<NOT SET>'}")
    print(f"  MPIN  : {'set' if mpin else '<NOT SET>'}")
    print()

    if not phone or not mpin:
        print("[X] PHONE_NO and MPIN must be set in .env before running this.")
        return 2

    print("This will:")
    print("  1. Wipe any existing auth_data.db.*")
    print("  2. Trigger a fresh SMS-OTP login (you'll get an SMS).")
    print("  3. Generate a NEW TOTP secret and ask you to enable it.")
    print("  4. Print env-var snippet (or update .env directly with --write-env).")
    print()
    print("[!] Use a real terminal — Docker / cron will not work.")
    print(f"[!] TOTP is environment-scoped. You are enrolling: {env_name}")
    print()

    confirm = input("Type 'yes' to proceed: ").strip().lower()
    if confirm != "yes":
        print("[X] Aborted.")
        return 1

    # 1) Clean slate so the SDK does a real phone-OTP login.
    if args.keep_cache:
        print("\n[i] --keep-cache set; existing auth_data.db.* left alone.")
    else:
        removed = _wipe_existing_cache()
        if removed:
            print(f"\n[i] Removed: {', '.join(removed)}")
        else:
            print("\n[i] No existing auth_data.db.* to remove.")

    # 2) Phone-OTP login. Capture stdout to give a useful error if it fails.
    from nubra_python_sdk.start_sdk import InitNubraSdk

    env_enum = _resolve_env_enum(env_name)

    capture_login = io.StringIO()
    print()
    _bar("Step 1/3: Phone-OTP login")
    print("Watch this terminal — you will see prompts from the SDK:")
    print("    📱 Enter OTP: <type the SMS code>")
    print()

    try:
        with contextlib.redirect_stdout(_Tee(sys.stdout, capture_login)):
            client = InitNubraSdk(
                env=env_enum,
                totp_login=False,   # force SMS-OTP path
                env_creds=True,     # auto-fill phone + MPIN from env
            )
    except Exception as exc:  # noqa: BLE001 — SDK raises bare Exception
        print()
        _bar("Phone-OTP login FAILED")
        print(f"  Error: {exc}")
        return 1

    auth_token = (getattr(client, "token_data", {}) or {}).get("auth_token")
    x_device_id = (getattr(client, "token_data", {}) or {}).get("x-device-id") or (
        getattr(client, "HEADERS", {}) or {}
    ).get("x-device-id")
    if not auth_token or not x_device_id:
        _bar("Phone-OTP login: tokens missing after construction")
        print("  Inspect auth_data.db.* manually; the SDK reported success but")
        print("  did not store auth_token / x-device-id.")
        return 1

    print(f"\n[OK] Phone-OTP login succeeded. x-device-id={x_device_id[:24]}…")

    # 3) Generate a new TOTP secret on the server.
    print()
    _bar("Step 2/3: Generate TOTP secret")

    capture_secret = io.StringIO()
    try:
        with contextlib.redirect_stdout(_Tee(sys.stdout, capture_secret)):
            blob = client.totp_generate_secret()
    except Exception as exc:  # noqa: BLE001
        _bar("totp_generate_secret FAILED")
        print(f"  Error: {exc}")
        return 1

    raw_blob = (str(blob) if blob is not None else "") + "\n" + capture_secret.getvalue()
    secret, otpauth_url = _extract_secret(raw_blob)
    if not secret:
        _bar("Could not extract secret from server response")
        print("  Raw response (truncated):")
        print(raw_blob[:600])
        return 1

    print()
    print("[!] Add this TOTP entry to your authenticator app NOW:")
    print(f"      Account label : Nubra-{env_name}")
    print(f"      Secret        : {secret}")
    if otpauth_url:
        print(f"      otpauth URL   : {otpauth_url}")
    print()
    print("    Most apps support manual-entry of the base32 secret if you")
    print("    don't have a QR code reader handy.")
    print()
    input("Press Enter once you've added the entry to your authenticator…")

    # 4) Enable TOTP server-side. The SDK's totp_enable() prompts for
    #    MPIN + TOTP itself; we re-implement the equivalent so we can
    #    pre-fill MPIN from env and only ask for the TOTP code.
    print()
    _bar("Step 3/3: Enable TOTP on the server")

    enable_attempts = 3
    for attempt in range(1, enable_attempts + 1):
        totp_code = input(
            f"  [{attempt}/{enable_attempts}] Type the current 6-digit TOTP "
            f"code from your authenticator: "
        ).strip()
        if not totp_code:
            print("  (empty input)")
            continue
        try:
            # ``_enable_totp`` is the underlying method ``totp_enable``
            # wraps; we call it directly so we can supply mpin + totp
            # without re-prompting.
            response = client._enable_totp(totp=totp_code, mpin=mpin)
            print(f"  Server response: {response}")
            break
        except Exception as exc:  # noqa: BLE001
            remaining = enable_attempts - attempt
            print(
                f"  [!] totp_enable failed ({remaining} attempts left): {exc}"
            )
            if remaining == 0:
                _bar("TOTP enrolment failed")
                print("  All 3 attempts exhausted. Possible causes:")
                print("    • Wrong code typed.")
                print("    • Authenticator clock drift (~30s window).")
                print("    • Authenticator entry uses a different secret.")
                return 1

    # 5) Re-fetch tokens (the SDK may have rotated session_token during enable).
    token_data = getattr(client, "token_data", {}) or {}
    headers = getattr(client, "HEADERS", {}) or {}
    auth_token = token_data.get("auth_token") or auth_token
    x_device_id = token_data.get("x-device-id") or headers.get("x-device-id") or x_device_id
    session_token = token_data.get("session_token") or ""
    if not session_token:
        bearer = headers.get("Authorization") or ""
        if bearer.startswith("Bearer "):
            session_token = bearer[len("Bearer "):]

    # ---------- Success summary ----------
    print()
    _bar("TOTP enrolment SUCCESSFUL")
    print(f"  env           : {env_name}")
    print(f"  phone         : {phone}")
    print(f"  auth_token    : {auth_token[:24]}…")
    print(f"  x_device_id   : {x_device_id[:24]}…")
    print(
        f"  session_token : {session_token[:24]}…"
        if session_token
        else "  session_token : <missing>"
    )
    print(f"  TOTP secret   : {secret}")
    print()
    print("auth_data.db.* in this directory now holds the live session.")

    # ---------- .env update / printout ----------
    updates = {
        "NUBRA_ENV": env_name,
        "NUBRA_AUTH_TOKEN": auth_token,
        "NUBRA_X_DEVICE_ID": x_device_id,
        "NUBRA_TOTP_SECRET": secret,
    }
    if session_token:
        updates["NUBRA_SESSION_TOKEN"] = session_token

    if args.write_env:
        try:
            _update_env_file(updates, ".env")
            print()
            _bar(".env updated")
            for k in _ENV_KEYS:
                if k in updates:
                    print(f"  {k}=<set>")
        except Exception as exc:  # noqa: BLE001
            print(f"\n[!] Failed to update .env: {exc}")
            print("    Falling back to print-only mode below.")
            args.write_env = False

    if not args.write_env:
        print()
        _bar("Copy-paste into .env (or your Docker secret manager)")
        for k, v in updates.items():
            print(f"    {k}={v}")
        print()
        print("Re-run with --write-env to update .env automatically.")

    print()
    _bar("Next steps")
    print("  • Local dev:   start the app — it will read auth_data.db.* directly.")
    print("  • Docker:      mount this directory's auth_data.db.* as a volume,")
    print("                 OR copy the env-var block above into your container env.")
    print("  • Refresh:     re-run this script whenever NUBRA_SESSION_TOKEN expires")
    print("                 (~24h). The TOTP secret stays valid until you rotate it.")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
