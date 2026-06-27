"""
Startup bootstrap for Nubra authentication.

Delegates to AuthService which:
  - Seeds the shelve with NUBRA_X_DEVICE_ID before any TOTP attempt
  - Retries TOTP login with exponential backoff
  - Calls sys.exit(1) on fatal TOTP errors so Cloud Run restarts the container
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any

from app.core.env_loader import load_project_env
from app.ingestion.auth_client import ensure_auth_dir, session_is_expired
from app.ingestion.auth_errors import NubraAuthError
from app.ingestion.auth_service import AuthService
from app.ingestion.input_patch import install_non_interactive_input_patch

logger = logging.getLogger(__name__)

REQUIRED_SECRET_ENV = ("PHONE_NO", "MPIN", "NUBRA_TOTP_SECRET")


@dataclass(frozen=True)
class AuthBootstrapResult:
    client: Any
    auth_dir: str
    regenerated: bool


def _validate_required_env() -> None:
    missing = [
        name
        for name in REQUIRED_SECRET_ENV
        if not (os.getenv(name) or "").strip().lstrip("\ufeff").replace(" ", "")
    ]
    if missing:
        raise NubraAuthError(
            "Missing required Nubra auth environment variables: "
            + ", ".join(missing)
        )


async def bootstrap_auth(
    *,
    env_name: str = "UAT",
    max_attempts: int = 3,
    base_backoff_seconds: float = 2.0,
    # Legacy params kept for call-site compatibility
    max_backoff_seconds: float = 30.0,
) -> AuthBootstrapResult:
    """Create or reuse the Nubra auth session before realtime startup.

    Uses AuthService which always seeds the shelve with NUBRA_X_DEVICE_ID
    from env — the critical fix for the "TOTP is not enabled" error in
    Cloud Run cold starts.
    """
    logger.info("AUTH_BOOTSTRAP_STARTED | env=%s", env_name)
    load_project_env(".")
    _validate_required_env()
    auth_dir = ensure_auth_dir()
    install_non_interactive_input_patch(require_totp_secret=True)

    needs_regeneration = session_is_expired(base_dir=auth_dir)

    svc = AuthService(
        env_name=env_name,
        max_attempts=max_attempts,
        base_backoff=base_backoff_seconds,
    )

    try:
        client = await svc.authenticate()
    except NubraAuthError as exc:
        msg = str(exc)
        # Detect fatal TOTP configuration errors and give clear ops guidance.
        if "INVALID_TOTP_SECRET" in msg or "TOTP is not enabled" in msg:
            logger.error(
                "INVALID_TOTP_SECRET | %s\n"
                "ACTION: TOTP secret is invalid or PROD enrollment has changed. "
                "Run setup_totp.py once locally to generate a new TOTP secret, "
                "then update the Cloud Run secret NUBRA_TOTP_SECRET and redeploy.",
                msg,
            )
            sys.exit(1)
        raise

    logger.info(
        "AUTH_BOOTSTRAP_COMPLETE | env=%s regenerated=%s auth_dir=%s",
        env_name,
        needs_regeneration,
        auth_dir,
    )
    return AuthBootstrapResult(
        client=client,
        auth_dir=str(auth_dir),
        regenerated=needs_regeneration,
    )
