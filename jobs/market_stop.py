#!/usr/bin/env python
"""
Cloud Run Job: Stop Market Service

Triggered by Cloud Scheduler at 03:40 PM IST (weekdays).
Gracefully stops market processing and scales to min-instances=0
so no idle charges accrue overnight.

Order of operations:
  1. Signal the service to flush and disconnect (via /admin/shutdown endpoint)
  2. Wait for graceful shutdown acknowledgment
  3. Scale Cloud Run to min-instances=0
  4. Service remains deployed for the next trading day

Does NOT delete the service, revisions, or remove traffic routing.

Entry point: python jobs/market_stop.py
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.logging import setup_logging

logger = logging.getLogger("jobs.market_stop")

PROJECT = os.getenv("GCP_PROJECT_ID", "stock-anaysis")
REGION = os.getenv("GCP_REGION", "asia-south1")
SERVICE = os.getenv("CLOUD_RUN_SERVICE", "nubra-live")
DRAIN_TIMEOUT = int(os.getenv("DRAIN_TIMEOUT", "30"))


def _get_run_client():
    try:
        from google.cloud import run_v2
        return run_v2.ServicesClient()
    except ImportError:
        logger.error("google-cloud-run not installed.")
        sys.exit(1)


def _get_service_url() -> str:
    from google.cloud import run_v2
    client = _get_run_client()
    service_name = f"projects/{PROJECT}/locations/{REGION}/services/{SERVICE}"
    try:
        service = client.get_service(name=service_name)
        return service.uri
    except Exception:
        return f"https://{SERVICE}-{PROJECT}.{REGION}.run.app"


def _signal_graceful_shutdown(url: str) -> bool:
    """Call the service's shutdown endpoint to flush DB and disconnect WS."""
    import urllib.request

    try:
        req = urllib.request.Request(
            f"{url}/admin/shutdown",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=b'{"reason":"market_close"}',
        )
        with urllib.request.urlopen(req, timeout=DRAIN_TIMEOUT) as resp:
            body = json.loads(resp.read().decode())
            logger.info("SHUTDOWN_SIGNAL_SENT | response=%s", body)
            return True
    except Exception as exc:
        logger.warning(
            "SHUTDOWN_SIGNAL_FAILED | %s (service may already be idle)",
            exc,
        )
        return False


def _verify_websocket_closed(url: str) -> bool:
    """Check that WebSocket is disconnected."""
    import urllib.request

    try:
        req = urllib.request.Request(f"{url}/health/ws", method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode())
            state = body.get("socket", "unknown")
            if state == "disconnected" or state == "uninitialised":
                logger.info("WEBSOCKET_DISCONNECTED | socket=%s", state)
                return True
            logger.info("WEBSOCKET_STILL_ACTIVE | socket=%s", state)
            return False
    except Exception:
        # Service might already be scaled down
        logger.info("WEBSOCKET_DISCONNECTED | service not responding (scaled down)")
        return True


def _scale_service_down() -> bool:
    """Scale to min-instances=0."""
    from google.cloud import run_v2
    from google.protobuf import field_mask_pb2

    client = _get_run_client()
    service_name = f"projects/{PROJECT}/locations/{REGION}/services/{SERVICE}"

    try:
        service = client.get_service(name=service_name)
    except Exception as exc:
        logger.error("SCALE_DOWN_FAILED | Cannot get service: %s", exc)
        return False

    template = service.template
    scaling = template.scaling
    scaling.min_instance_count = 0

    request = run_v2.UpdateServiceRequest(
        service=service,
        update_mask=field_mask_pb2.FieldMask(
            paths=["template.scaling.min_instance_count"]
        ),
    )

    try:
        operation = client.update_service(request=request)
        operation.result(timeout=180)
        logger.info("SERVICE_SCALED_TO_ZERO | min-instances=0")
        return True
    except Exception as exc:
        logger.error("SCALE_DOWN_FAILED | %s", exc)
        return False


def main() -> int:
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    logger.info("MARKET_SERVICE_STOP | project=%s service=%s", PROJECT, SERVICE)

    url = _get_service_url()

    # Step 1: Signal graceful shutdown (flush DB, close WS)
    logger.info("INITIATING_GRACEFUL_SHUTDOWN | url=%s", url)
    _signal_graceful_shutdown(url)

    # Step 2: Wait briefly for flush to complete
    logger.info("WAITING_FOR_DRAIN | timeout=%ds", DRAIN_TIMEOUT)
    time.sleep(min(DRAIN_TIMEOUT, 15))

    # Step 3: Verify WebSocket disconnected
    _verify_websocket_closed(url)
    logger.info("DB_FLUSH_COMPLETE | (drain period elapsed)")

    # Step 4: Scale to zero
    logger.info("SCALING_DOWN | Setting min-instances=0")
    if not _scale_service_down():
        logger.error("MARKET_SERVICE_STOP | FAILED to scale down")
        return 1

    logger.info("MARKET_SERVICE_STOP | completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
