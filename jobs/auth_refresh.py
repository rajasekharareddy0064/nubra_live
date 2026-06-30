#!/usr/bin/env python
"""
Cloud Run Job: Nubra Authentication Refresh

Scheduled via Cloud Scheduler (weekdays 05:30 IST).
This job's ONLY responsibility is refreshing the Nubra session token
and writing the updated tokens back to GCP Secret Manager.

It does NOT:
  - Start the WebSocket
  - Subscribe to market data
  - Aggregate candles
  - Connect to Cloud SQL for market data
  - Start the API server or frontend WebSocket

Entry point: python jobs/auth_refresh.py

Reuses existing AuthService, auth_client, and input_patch from the
main application without modification.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time

# Ensure project root is on PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.env_loader import load_project_env
from app.core.logging import setup_logging
from app.ingestion.auth_client import (
    _is_jwt_expired,
    ensure_auth_dir,
    get_session_tokens,
    session_is_expired,
)
from app.ingestion.auth_errors import NubraAuthError
from app.ingestion.auth_service import AuthService
from app.ingestion.input_patch import install_non_interactive_input_patch

logger = logging.getLogger("jobs.auth_refresh")

# ---------------------------------------------------------------------------
# GCP Secret Manager integration
# ---------------------------------------------------------------------------

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "stock-anaysis")

# Map: Secret Manager secret name -> env var name
SECRET_MAP = {
    "nubra-auth-token": "NUBRA_AUTH_TOKEN",
    "nubra-x-device-id": "NUBRA_X_DEVICE_ID",
    "nubra-session-token": "NUBRA_SESSION_TOKEN",
}


def _get_secret_client():
    """Lazy import of google.cloud.secretmanager."""
    try:
        from google.cloud import secretmanager
        return secretmanager.SecretManagerServiceClient()
    except ImportError:
        logger.error(
            "google-cloud-secret-manager not installed. "
            "Add it to requirements.txt: google-cloud-secret-manager"
        )
        sys.exit(1)


def read_secret(client, secret_id: str) -> str:
    """Read the latest version of a secret from Secret Manager."""
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8")
    except Exception as exc:
        logger.error("Failed to read secret '%s': %s", secret_id, exc)
        return ""


def write_secret(client, secret_id: str, value: str) -> bool:
    """Add a new version to an existing secret in Secret Manager."""
    parent = f"projects/{PROJECT_ID}/secrets/{secret_id}"
    try:
        client.add_secret_version(
            request={
                "parent": parent,
                "payload": {"data": value.encode("utf-8")},
            }
        )
        logger.info("SECRET_UPDATED | secret=%s", secret_id)
        return True
    except Exception as exc:
        logger.error("SECRET_UPDATE_FAILED | secret=%s error=%s", secret_id, exc)
        return False


# ---------------------------------------------------------------------------
# Session validation
# ---------------------------------------------------------------------------


def _check_session_valid() -> bool:
    """Return True if the current NUBRA_SESSION_TOKEN is still valid."""
    token = (os.getenv("NUBRA_SESSION_TOKEN") or "").strip()
    if not token:
        logger.info("SESSION_TOKEN_MISSING | no NUBRA_SESSION_TOKEN in env")
        return False
    if _is_jwt_expired(token, leeway_seconds=300):  # 5 min leeway
        logger.info("SESSION_EXPIRED | token is expired or expires within 5 min")
        return False
    logger.info("SESSION_VALID | token is still valid")
    return True


# ---------------------------------------------------------------------------
# Main job logic
# ---------------------------------------------------------------------------


async def run_auth_refresh() -> int:
    """Execute the auth refresh job.

    Returns 0 on success, 1 on failure.
    """
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    load_project_env(".")
    logger.info("AUTH_REFRESH_JOB_START | project=%s", PROJECT_ID)

    env_name = os.getenv("NUBRA_ENV", "PROD")

    # Step 1: Read current secrets from Secret Manager
    sm_client = _get_secret_client()
    logger.info("READING_SECRETS | Reading current tokens from Secret Manager")

    for secret_id, env_var in SECRET_MAP.items():
        value = read_secret(sm_client, secret_id)
        if value:
            os.environ[env_var] = value
            logger.info(
                "SECRET_LOADED | %s=%s...%s",
                env_var,
                value[:6] if len(value) > 10 else "***",
                value[-4:] if len(value) > 10 else "",
            )
        else:
            logger.warning("SECRET_EMPTY | %s has no value", secret_id)

    # Also load TOTP credentials (needed for fallback)
    for secret_id in ("PHONE_NO", "MPIN", "NUBRA_TOTP_SECRET"):
        value = read_secret(sm_client, secret_id)
        if value:
            os.environ[secret_id] = value

    # Step 2: Check if session is still valid
    if _check_session_valid():
        logger.info("AUTH_REFRESH_JOB_COMPLETE | status=SESSION_VALID no_refresh_needed")
        return 0

    # Step 3: Session expired -> authenticate
    logger.info("AUTHENTICATING | Session expired, performing TOTP login")

    ensure_auth_dir()
    install_non_interactive_input_patch(require_totp_secret=True)

    svc = AuthService(env_name=env_name, max_attempts=3, base_backoff=2.0, skip_refdata=True)

    try:
        client = await svc.authenticate()
    except NubraAuthError as exc:
        logger.error("AUTH_REFRESH_FAILED | %s", exc)
        return 1

    # Step 4: Extract new tokens from the authenticated client
    tokens = get_session_tokens(client)
    new_auth_token = tokens.get("auth_token") or ""
    new_session_token = tokens.get("session_token") or ""
    new_device_id = tokens.get("x_device_id") or ""

    if not new_session_token:
        logger.error("AUTH_REFRESH_FAILED | No session_token after login")
        return 1

    if _is_jwt_expired(new_session_token):
        logger.error("AUTH_REFRESH_FAILED | New session_token is already expired")
        return 1

    logger.info(
        "NEW_TOKENS_OBTAINED | auth_token=%s session_token=%s device_id=%s",
        new_auth_token[:8] + "..." if new_auth_token else "<none>",
        new_session_token[:12] + "..." if new_session_token else "<none>",
        new_device_id[:8] + "..." if new_device_id else "<none>",
    )

    # Step 5: Write updated tokens back to Secret Manager
    logger.info("UPDATING_SECRETS | Writing new tokens to Secret Manager")

    updates = {
        "nubra-session-token": new_session_token,
        "nubra-auth-token": new_auth_token,
    }
    # Only update device-id if it changed
    current_device_id = (os.getenv("NUBRA_X_DEVICE_ID") or "").strip()
    if new_device_id and new_device_id != current_device_id:
        updates["nubra-x-device-id"] = new_device_id
        logger.info(
            "DEVICE_ID_CHANGED | old=%s new=%s",
            current_device_id[:8] + "..." if current_device_id else "<none>",
            new_device_id[:8] + "...",
        )

    all_ok = True
    for secret_id, value in updates.items():
        if value:
            ok = write_secret(sm_client, secret_id, value)
            if not ok:
                all_ok = False

    # Step 6: Verify the new session works
    logger.info("VERIFYING_AUTH | Confirming new session is valid")
    os.environ["NUBRA_SESSION_TOKEN"] = new_session_token
    if not _check_session_valid():
        logger.error("AUTH_REFRESH_FAILED | Post-update verification failed")
        return 1

    if all_ok:
        logger.info(
            "AUTH_REFRESH_JOB_COMPLETE | status=SUCCESS secrets_updated=%d",
            len(updates),
        )
        return 0
    else:
        logger.error("AUTH_REFRESH_JOB_COMPLETE | status=PARTIAL_FAILURE")
        return 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    return asyncio.run(run_auth_refresh())


if __name__ == "__main__":
    sys.exit(main())
