"""
Three-level instrument master cache with GCS backing.

Priority:
  Level 1: Google Cloud Storage (gs://BUCKET/instrument_master_cache.csv)
  Level 2: Local CSV bundled in the Docker image
  Level 3: Nubra SDK _get_instruments() with timeout + retry

This eliminates the startup hang caused by Nubra's refdata API
being slow/down/returning 440.
"""
from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Configuration via environment
CACHE_BUCKET = os.getenv("INSTRUMENT_CACHE_BUCKET", "stock-anaysis-cache")
CACHE_FILE = os.getenv("INSTRUMENT_CACHE_FILE", "instrument_master_cache.csv")
DOWNLOAD_TIMEOUT = int(os.getenv("INSTRUMENT_DOWNLOAD_TIMEOUT", "60"))
LOCAL_CACHE_PATH = Path(os.getenv("INSTRUMENT_LOCAL_CACHE", "instrument_master_cache.csv"))

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="instrument-io")


# ---------------------------------------------------------------------------
# GCS operations
# ---------------------------------------------------------------------------

def _get_storage_client():
    """Lazy import of google.cloud.storage."""
    try:
        from google.cloud import storage
        return storage.Client()
    except ImportError:
        logger.debug("google-cloud-storage not installed; GCS cache unavailable")
        return None
    except Exception as exc:
        logger.warning("GCS client init failed: %s", exc)
        return None


def instrument_cache_exists_gcs() -> bool:
    """Check if instrument cache exists in Cloud Storage."""
    client = _get_storage_client()
    if client is None:
        return False
    try:
        bucket = client.bucket(CACHE_BUCKET)
        blob = bucket.blob(CACHE_FILE)
        return blob.exists()
    except Exception as exc:
        logger.debug("GCS exists check failed: %s", exc)
        return False


def download_instrument_cache_gcs() -> Optional[pd.DataFrame]:
    """Download instrument master from Cloud Storage.

    Returns DataFrame or None if unavailable.
    """
    logger.info("CHECKING_CLOUD_STORAGE | bucket=%s file=%s", CACHE_BUCKET, CACHE_FILE)
    client = _get_storage_client()
    if client is None:
        logger.info("CLOUD_STORAGE_NOT_FOUND | GCS client unavailable")
        return None

    try:
        bucket = client.bucket(CACHE_BUCKET)
        blob = bucket.blob(CACHE_FILE)
        if not blob.exists():
            logger.info("CLOUD_STORAGE_NOT_FOUND | file does not exist in bucket")
            return None

        logger.info("CLOUD_STORAGE_FOUND | downloading %s/%s", CACHE_BUCKET, CACHE_FILE)
        start = time.time()
        content = blob.download_as_bytes()
        elapsed = time.time() - start

        import io
        df = pd.read_csv(io.BytesIO(content))
        logger.info(
            "CLOUD_STORAGE_FOUND | loaded rows=%d cols=%d in %.1fs",
            len(df), len(df.columns), elapsed,
        )
        return df if not df.empty else None
    except Exception as exc:
        logger.warning("CLOUD_STORAGE_NOT_FOUND | download failed: %s", exc)
        return None


def upload_instrument_cache_gcs(df: pd.DataFrame) -> bool:
    """Upload instrument master CSV to Cloud Storage."""
    logger.info("UPLOAD_TO_GCS | bucket=%s file=%s rows=%d", CACHE_BUCKET, CACHE_FILE, len(df))
    client = _get_storage_client()
    if client is None:
        logger.warning("UPLOAD_TO_GCS | skipped (no GCS client)")
        return False

    try:
        bucket = client.bucket(CACHE_BUCKET)
        # Create bucket if it doesn't exist
        if not bucket.exists():
            logger.info("UPLOAD_TO_GCS | creating bucket %s", CACHE_BUCKET)
            bucket = client.create_bucket(CACHE_BUCKET, location="asia-south1")

        blob = bucket.blob(CACHE_FILE)
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        blob.upload_from_string(csv_bytes, content_type="text/csv")
        logger.info("UPLOAD_TO_GCS | success size=%d bytes", len(csv_bytes))
        return True
    except Exception as exc:
        logger.warning("UPLOAD_TO_GCS | failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# SDK download with timeout
# ---------------------------------------------------------------------------

def _download_from_sdk_sync(client: Any) -> int:
    """Call SDK _get_instruments() synchronously. Returns HTTP status."""
    getter = getattr(client, "_get_instruments", None)
    if not callable(getter):
        raise RuntimeError("SDK client has no _get_instruments()")
    return getter()


def download_from_nubra_sdk(
    client: Any,
    *,
    timeout: int = DOWNLOAD_TIMEOUT,
    max_retries: int = 3,
) -> Optional[pd.DataFrame]:
    """Download instrument master from Nubra SDK with timeout and retry.

    Returns the DataFrame from InitNubraSdk.DF_REF_DATA_NSE, or None.
    """
    from nubra_python_sdk.start_sdk import InitNubraSdk

    for attempt in range(1, max_retries + 1):
        logger.info(
            "DOWNLOADING_FROM_NUBRA | attempt=%d/%d timeout=%ds",
            attempt, max_retries, timeout,
        )
        try:
            future = _executor.submit(_download_from_sdk_sync, client)
            status = future.result(timeout=timeout)

            nse = InitNubraSdk.DF_REF_DATA_NSE
            if isinstance(nse, pd.DataFrame) and not nse.empty:
                logger.info(
                    "DOWNLOAD_SUCCESS | attempt=%d http_status=%s rows=%d",
                    attempt, status, len(nse),
                )
                return nse
            else:
                logger.warning(
                    "DOWNLOADING_FROM_NUBRA | attempt=%d returned empty (status=%s)",
                    attempt, status,
                )
        except FuturesTimeout:
            logger.error(
                "DOWNLOAD_TIMEOUT | attempt=%d/%d timed out after %ds",
                attempt, max_retries, timeout,
            )
            future.cancel()
        except Exception as exc:
            logger.error(
                "DOWNLOADING_FROM_NUBRA | attempt=%d/%d failed: %s",
                attempt, max_retries, exc,
            )

        if attempt < max_retries:
            backoff = 2 ** attempt
            logger.info("DOWNLOADING_FROM_NUBRA | retrying in %ds", backoff)
            time.sleep(backoff)

    logger.error(
        "DOWNLOADING_FROM_NUBRA | all %d attempts failed", max_retries,
    )
    return None


# ---------------------------------------------------------------------------
# Main three-level loader
# ---------------------------------------------------------------------------

def load_instrument_master(
    client: Any,
    *,
    local_path: Path = LOCAL_CACHE_PATH,
) -> pd.DataFrame:
    """Load instrument master using three-level fallback.

    Priority:
      1. Google Cloud Storage cache
      2. Local CSV file (bundled in Docker image)
      3. Nubra SDK _get_instruments() with timeout + retry

    Returns a non-empty DataFrame or raises RuntimeError.
    """
    logger.info("INSTRUMENT_LOAD_START | bucket=%s local=%s", CACHE_BUCKET, local_path)
    start = time.time()

    # --- Level 1: Cloud Storage ---
    df = download_instrument_cache_gcs()
    if df is not None and not df.empty:
        elapsed = time.time() - start
        logger.info(
            "INSTRUMENT_LOAD_COMPLETE | source=cloud_storage rows=%d time=%.1fs",
            len(df), elapsed,
        )
        # Also save locally for next time
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(local_path, index=False)
        except Exception:
            pass
        return df

    # --- Level 2: Local CSV ---
    logger.info("LOADING_LOCAL_CACHE | path=%s", local_path)
    if local_path.exists():
        try:
            df = pd.read_csv(local_path)
            if not df.empty:
                elapsed = time.time() - start
                logger.info(
                    "LOCAL_CACHE_FOUND | rows=%d cols=%d time=%.1fs",
                    len(df), len(df.columns), elapsed,
                )
                # Upload to GCS for faster next startup
                _executor.submit(upload_instrument_cache_gcs, df)
                logger.info("INSTRUMENT_LOAD_COMPLETE | source=local_cache rows=%d", len(df))
                return df
        except Exception as exc:
            logger.warning("LOCAL_CACHE_NOT_FOUND | read failed: %s", exc)
    else:
        logger.info("LOCAL_CACHE_NOT_FOUND | file does not exist at %s", local_path)

    # --- Level 3: Nubra SDK (with timeout) ---
    df = download_from_nubra_sdk(client, timeout=DOWNLOAD_TIMEOUT, max_retries=3)
    if df is not None and not df.empty:
        elapsed = time.time() - start
        logger.info(
            "INSTRUMENT_LOAD_COMPLETE | source=nubra_sdk rows=%d time=%.1fs",
            len(df), elapsed,
        )
        # Save locally
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(local_path, index=False)
            logger.info("Saved instrument cache to %s", local_path)
        except Exception as exc:
            logger.warning("Failed to save local cache: %s", exc)
        # Upload to GCS
        _executor.submit(upload_instrument_cache_gcs, df)
        return df

    # All levels failed
    elapsed = time.time() - start
    raise RuntimeError(
        f"INSTRUMENT_LOAD_FAILED | All three levels failed after {elapsed:.1f}s. "
        f"GCS bucket={CACHE_BUCKET}, local={local_path}, SDK timed out or returned empty. "
        "The service will start without instrument data — ingestion will not work."
    )
