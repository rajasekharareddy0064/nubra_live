from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from app.core.env_loader import load_project_env
from app.ingestion.auth_preflight import auth_preflight_status


def _check_sdk_import() -> dict:
    try:
        from nubra_python_sdk.refdata.instruments import InstrumentData  # noqa: F401

        return {"ok": True, "path": "nubra_python_sdk.refdata.instruments.InstrumentData"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _check_instrument_cache(path: str = "instrument_master_cache.csv") -> dict:
    p = Path(path)
    if not p.exists():
        return {"exists": False, "readable": False, "rows": 0, "columns": []}

    try:
        df = pd.read_csv(p)
    except Exception as exc:
        return {"exists": True, "readable": False, "error": str(exc), "rows": 0, "columns": []}

    required = {"ref_id", "asset", "derivative_type", "expiry"}
    cols = {c.strip().lower() for c in df.columns}
    return {
        "exists": True,
        "readable": True,
        "rows": int(len(df)),
        "columns": sorted(cols),
        "has_required_columns": required.issubset(cols),
    }


def main() -> None:
    load_project_env(".")

    report = {
        "env_loaded": True,
        "auth_preflight": auth_preflight_status(),
        "sdk_import": _check_sdk_import(),
        "instrument_cache": _check_instrument_cache(),
    }

    # Simple overall verdict for operators.
    report["overall_ok"] = bool(
        report["auth_preflight"]["auth_ready"] and report["sdk_import"]["ok"]
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
