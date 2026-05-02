"""
Auth preflight checks for the session-only Nubra flow.

Authentication now requires one of the following to be true at startup:

1. ``auth_data.db.*`` shelve files exist in the working directory
   (typically mounted as a Docker volume from a machine that ran
   ``setup_totp.py`` interactively).
2. ``NUBRA_AUTH_TOKEN`` + ``NUBRA_X_DEVICE_ID`` + ``NUBRA_SESSION_TOKEN``
   env vars are present so :func:`bootstrap_session_from_env` can
   pre-write the shelve.

TOTP secrets / MPIN / phone are no longer required at runtime — they
were only needed by the (now-removed) auto-login flow.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


AUTH_CACHE_FILES = ("auth_data.db.bak", "auth_data.db.dat", "auth_data.db.dir")
SESSION_ENV_KEYS = ("NUBRA_AUTH_TOKEN", "NUBRA_X_DEVICE_ID", "NUBRA_SESSION_TOKEN")


def auth_preflight_status(base_dir: str | Path = ".") -> dict[str, Any]:
    """Inspect on-disk + env state used by session-only auth."""
    root = Path(base_dir)
    cache_files = {name: (root / name).exists() for name in AUTH_CACHE_FILES}
    session_env = {name: bool(os.getenv(name)) for name in SESSION_ENV_KEYS}

    has_cache = any(cache_files.values())
    has_session_env = all(session_env.values())

    # Either is sufficient: the env-var path will be used to write the
    # shelve at startup if no cache files exist yet.
    auth_ready = has_cache or has_session_env

    guidance = (
        "Session-only auth requires either:\n"
        "  (a) auth_data.db.* shelve files in the working directory "
        "(mount as a Docker volume from a machine that ran "
        "setup_totp.py), OR\n"
        "  (b) NUBRA_AUTH_TOKEN + NUBRA_X_DEVICE_ID + NUBRA_SESSION_TOKEN "
        "env vars (setup_totp.py prints a copy-paste snippet on success)."
    )

    return {
        "auth_ready": auth_ready,
        "has_cache_files": has_cache,
        "has_session_env": has_session_env,
        "cache_files": cache_files,
        "session_keys": session_env,
        "guidance": "" if auth_ready else guidance,
    }


def assert_auth_preflight(base_dir: str | Path = ".") -> dict[str, Any]:
    status = auth_preflight_status(base_dir=base_dir)
    if not status["auth_ready"]:
        raise RuntimeError(status["guidance"])
    return status
