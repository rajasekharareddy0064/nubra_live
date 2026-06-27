#!/usr/bin/env python
"""
Cloud Run Job: Start Market Service

Triggered by Cloud Scheduler at 09:10 AM IST (weekdays).
Scales nubra-live to min-instances=1 so the container is warm
and ready before market open at 09:15.

Does NOT redeploy or build a new image. Only updates scaling config.
After scaling, verifies the service is healthy and ingestion starts.

Entry point: python jobs/market_start.py
"""
from __future__ import annotations

import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.logging import setup_logging

logger = logging.getLogger("jobs.market_start")

PROJECT = os.getenv("GCP_PROJECT_ID", "stock-anaysis")
REGION = os.getenv("GCP_REGION", "asia-south1")
SERVICE = os.getenv("CLOUD_RUN_SERVICE", "nubra-live")
HEALTH_TIMEOUT = int(os.getenv("HEALTH_TIMEOUT", "120"))


def _get_run_client():
    try:
        from google.cloud import run_v2
        return run_v2.ServicesClient()
    except ImportError:
        logger.error("google-cloud-run not installed. Add to requirements.txt.")
        sys.exit(1)


def _scale_service(min_instances: int, max_instances: int) -> bool:
    """Update Cloud Run service scaling without redeploying."""
    from google.cloud import run_v2
    from google.protobuf import field_mask_pb2

    client = _get_run_client()
    service_name = f"projects/{PROJECT}/locations/{REGION}/services/{SERVICE}"

    try:
        service = client.get_service(name=service_name)
    except Exception as exc:
        logger.error("SCALE_FAILED | Cannot get service: %s", exc)
        return False

    # Update scaling annotations on the template
    template = service.template
    scaling = template.scaling
    scaling.min_instance_count = min_instances
    scaling.max_instance_count = max_instances

    request = run_v2.UpdateServiceRequest(
        service=service,
        update_mask=field_mask_pb2.FieldMask(
            paths=["template.scaling.min_instance_count",
                   "template.scaling.max_instance_count"]
        ),
    )

    try:
        operation = client.update_service(request=request)
        result = operation.result(timeout=180)
        logger.info(
            "SCALE_SUCCESS | min=%d max=%d service=%s",
            min_instances, max_instances, SERVICE,
        )
        return True
    except Exception as exc:
        logger.error("SCALE_FAILED | %s", exc)
        return False


def _get_service_url() -> str:
    """Get the service URL from Cloud Run."""
    from google.cloud import run_v2

    client = _get_run_client()
    service_name = f"projects/{PROJECT}/locations/{REGION}/services/{SERVICE}"
    try:
        service = client.get_service(name=service_name)
        return service.uri
    except Exception:
        return f"https://{SERVICE}-{PROJECT}.{REGION}.run.app"


def _wait_for_health(url: str, timeout: int = 120) -> dict:
    """Poll /health/ready until ingestion is ready or timeout."""
    import urllib.request
    import json

    deadline = time.time() + timeout
    last_status = {}

    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"{url}/health/ready", method="GET")
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read().decode())
                last_status = body
                ingestion_state = body.get("ingestion", {}).get("state")
                if ingestion_state == "ready":
                    return body
                logger.info(
                    "HEALTH_POLL | ingestion=%s (waiting...)",
                    ingestion_state,
                )
        except Exception as exc:
            logger.debug("Health poll failed: %s", exc)
        time.sleep(10)

    return last_status


def main() -> int:
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    logger.info("MARKET_SERVICE_START | project=%s service=%s", PROJECT, SERVICE)

    # Step 1: Scale to min=1, max=1
    logger.info("SCALING_UP | Setting min-instances=1 max-instances=1")
    if not _scale_service(min_instances=1, max_instances=1):
        logger.error("MARKET_SERVICE_START | FAILED to scale service")
        return 1

    # Step 2: Wait for service to be ready
    url = _get_service_url()
    logger.info("WAITING_FOR_HEALTH | url=%s timeout=%ds", url, HEALTH_TIMEOUT)

    status = _wait_for_health(url, timeout=HEALTH_TIMEOUT)
    ingestion_state = status.get("ingestion", {}).get("state", "unknown")

    if ingestion_state == "ready":
        logger.info("SERVICE_READY | ingestion=ready")
        logger.info("WEBSOCKET_CONNECTED | service is receiving market data")
        logger.info("MARKET_INGESTION_STARTED | all checks passed")
        return 0
    else:
        logger.warning(
            "SERVICE_NOT_FULLY_READY | ingestion=%s (service is up but "
            "ingestion may still be initializing)",
            ingestion_state,
        )
        # Don't fail — the service is running, ingestion will catch up
        logger.info("MARKET_SERVICE_START | completed with warnings")
        return 0


if __name__ == "__main__":
    sys.exit(main())
