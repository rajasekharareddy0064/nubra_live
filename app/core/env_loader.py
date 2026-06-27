from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv


def load_project_env(project_root: str | Path = ".") -> bool:
    """
    Load .env file into process environment.
    Returns True if .env file exists and was loaded.
    
    In Cloud Run, .env won't exist and env vars come from the runtime.
    This is expected and not an error.
    """
    env_path = Path(project_root) / ".env"
    if not env_path.exists():
        logging.getLogger(__name__).debug(
            ".env file not found at %s (expected in Cloud Run, using runtime env vars)",
            env_path
        )
        return False

    load_dotenv(dotenv_path=env_path, override=False)
    logging.getLogger(__name__).info("Loaded environment from %s", env_path)
    return True
