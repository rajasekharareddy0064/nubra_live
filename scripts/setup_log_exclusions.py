#!/usr/bin/env python3
"""Apply Cloud Logging exclusions on the project _Default sink.

Reduces ingestion cost for nubra-live health probes and high-volume app logs.

Usage:
    python scripts/setup_log_exclusions.py
    python scripts/setup_log_exclusions.py --project stock-anaysis --service nubra-live
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from typing import Any


def gcloud_bin() -> str:
    for candidate in ("gcloud.cmd", "gcloud"):
        path = shutil.which(candidate)
        if path:
            return path
    raise RuntimeError("gcloud CLI not found on PATH")


def run_gcloud(*args: str) -> Any:
    cmd = [gcloud_bin(), *args, "--format=json"]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"gcloud failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr.strip()}"
        )
    if not proc.stdout.strip():
        return None
    return json.loads(proc.stdout)


def describe_sink(project: str) -> dict[str, Any]:
    return run_gcloud("logging", "sinks", "describe", "_Default", f"--project={project}")


def add_exclusion(project: str, *, name: str, description: str, log_filter: str) -> None:
    spec = f"name={name},description={description},filter={log_filter}"
    subprocess.run(
        [
            gcloud_bin(),
            "logging",
            "sinks",
            "update",
            "_Default",
            f"--project={project}",
            f"--add-exclusion={spec}",
        ],
        check=True,
    )
    print(f"Applied exclusion: {name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Configure nubra-live log exclusions")
    parser.add_argument("--project", default="stock-anaysis")
    parser.add_argument("--service", default="nubra-live")
    args = parser.parse_args()

    service = args.service
    base = f'resource.type="cloud_run_revision" resource.labels.service_name="{service}"'

    exclusions = [
        (
            "nubra-live-health-checks",
            "Drop Cloud Run request logs for nubra-live health endpoints",
            f"{base} (httpRequest.requestUrl=\"/health\" OR httpRequest.requestUrl=\"/health/ready\" "
            f'OR httpRequest.requestUrl="/health/auth" OR httpRequest.requestUrl="/health/ws" '
            f'OR httpRequest.requestUrl="/health/ingestion")',
        ),
        (
            "nubra-live-noisy-app-logs",
            "Drop high-volume nubra-live app logs if LOG_LEVEL drifts to INFO",
            f'{base} (textPayload=~"order_book aggregate" OR textPayload=~"hub broadcast type=" '
            f'OR textPayload=~"DB_FLUSH_" OR textPayload=~"SNAPSHOT_HEALTH" OR textPayload=~"emit candle_3m" '
            f'OR textPayload=~"SNAPSHOT_RESET" OR textPayload=~"QUEUE_ENQUEUE" OR textPayload=~"ORDER_BOOK_3M")',
        ),
    ]

    existing = {item.get("name") for item in describe_sink(args.project).get("exclusions", [])}

    for name, description, log_filter in exclusions:
        if name in existing:
            print(f"Already present, skipping: {name}")
            continue
        add_exclusion(args.project, name=name, description=description, log_filter=log_filter)

    print("\nCurrent exclusions on _Default:")
    for item in describe_sink(args.project).get("exclusions", []):
        print(f"  - {item.get('name')}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(exc.returncode) from exc
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc
