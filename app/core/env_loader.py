from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv


def load_project_env(project_root: str | Path = ".") -> bool:
    """
    Load .env file into process environment.
    Returns True if .env file exists and was loaded.
    """
    env_path = Path(project_root) / ".env"
    if not env_path.exists():
        return False

    load_dotenv(dotenv_path=env_path, override=False)
    logging.getLogger(__name__).info("Loaded environment from %s", env_path)
    return True
