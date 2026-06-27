from __future__ import annotations

import os
from pathlib import Path
from typing import Any


AUTH_CACHE_FILES = ("auth_data.db.bak", "auth_data.db.dat", "auth_data.db.dir")
SESSION_ENV_KEYS = ("NUBRA_AUTH_TOKEN", "NUBRA_X_DEVICE_ID", "NUBRA_SESSION_TOKEN")
BOOTSTRAP_ENV_KEYS = ("PHONE_NO", "MPIN", "NUBRA_TOTP_SECRET")


def _auth_dir(base_dir: str | Path | None = None) -> Path:
    return Path(base_dir or os.getenv("NUBRA_AUTH_DIR") or "/tmp/auth")


def auth_preflight_status(base_dir: str | Path | None = None) -> dict[str, Any]:
    root = _auth_dir(base_dir)
    cache_files = {name: (root / name).exists() for name in AUTH_CACHE_FILES}
    session_env = {name: bool(os.getenv(name)) for name in SESSION_ENV_KEYS}
    bootstrap_env = {name: bool(os.getenv(name)) for name in BOOTSTRAP_ENV_KEYS}

    has_cache = any(cache_files.values())
    has_session_env = all(session_env.values())
    has_bootstrap_env = all(bootstrap_env.values())
    auth_ready = has_cache or has_session_env or has_bootstrap_env

    return {
        "auth_ready": auth_ready,
        "auth_dir": str(root),
        "has_cache_files": has_cache,
        "has_session_env": has_session_env,
        "has_bootstrap_env": has_bootstrap_env,
        "cache_files": cache_files,
        "session_keys": session_env,
        "bootstrap_keys": bootstrap_env,
        "guidance": ""
        if auth_ready
        else (
            "Nubra auth requires existing auth_data.db.* files in "
            f"{root}, session token env vars, or PHONE_NO + MPIN + "
            "NUBRA_TOTP_SECRET for automatic TOTP login."
        ),
    }


def assert_auth_preflight(base_dir: str | Path | None = None) -> dict[str, Any]:
    status = auth_preflight_status(base_dir=base_dir)
    if not status["auth_ready"]:
        raise RuntimeError(status["guidance"])
    return status
