"""After each IST 3m close, fetch historical intraDay bars and diff vs live."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

from app.core.config import settings
from app.historical.client import HistoricalClient
from app.historical.compare import compare_closed_bar
from app.historical.universe import build_universe
from app.realtime.hub import LiveHub
from app.realtime.interval_clock import (
    closed_bucket_start,
    is_nse_cash_session_bar,
    market_tz,
    next_interval_boundary,
    seconds_until_next_boundary,
)

logger = logging.getLogger(__name__)


class HistoricalComparePoller:
    """Background task: wait for bar close + settle, then compare."""

    def __init__(
        self,
        hub: LiveHub,
        *,
        interval_minutes: int | None = None,
        tz_name: str | None = None,
        settle_seconds: float | None = None,
        price_abs_tol: float | None = None,
        volume_rel_tol: float | None = None,
        client: HistoricalClient | None = None,
    ) -> None:
        self._hub = hub
        self._interval = int(interval_minutes or settings.candle_interval_minutes)
        self._tz_name = tz_name or settings.market_timezone
        self._settle = float(
            settle_seconds if settle_seconds is not None else settings.historical_compare_settle_seconds
        )
        self._price_abs_tol = float(
            price_abs_tol if price_abs_tol is not None else settings.historical_price_abs_tol
        )
        self._volume_rel_tol = float(
            volume_rel_tol if volume_rel_tol is not None else settings.historical_volume_rel_tol
        )
        self._client = client
        self._last_report: dict[str, Any] | None = None
        self._last_bucket_end: str | None = None

    @property
    def last_report(self) -> dict[str, Any] | None:
        return self._last_report

    def _make_client(self, price_scale: float) -> HistoricalClient:
        if self._client is not None:
            return self._client
        return HistoricalClient(
            env_name=settings.nubra_env,
            exchange=settings.nubra_exchange,
            price_scale=price_scale,
            interval=f"{self._interval}m",
        )

    def _instrument_manager(self) -> Any | None:
        from app.main import APP_STATE

        ingestion = APP_STATE.get("ingestion")
        return getattr(ingestion, "instrument_manager", None) if ingestion is not None else None

    async def _wait_for_manager(self) -> Any:
        while True:
            manager = self._instrument_manager()
            if manager is not None and getattr(manager, "df", None) is not None:
                try:
                    if not manager.df.empty:
                        return manager
                except Exception:
                    pass
            logger.info("HIST_COMPARE_GATE | waiting for instrument manager")
            await asyncio.sleep(2.0)

    async def run_forever(self) -> None:
        tz = market_tz(self._tz_name)
        manager = await self._wait_for_manager()
        price_scale = float(getattr(manager, "price_scale", 100) or 100)
        client = self._make_client(price_scale)
        logger.info(
            "HIST_COMPARE_START | interval=%sm settle=%ss price_scale=%s",
            self._interval,
            self._settle,
            price_scale,
        )
        while True:
            loop_wake = datetime.now(tz)
            delay = seconds_until_next_boundary(loop_wake, self._interval, tz)
            target = next_interval_boundary(loop_wake, self._interval, tz)
            await asyncio.sleep(delay)
            while datetime.now(tz) < target:
                await asyncio.sleep(0.01)
            if self._settle > 0:
                await asyncio.sleep(self._settle)
            now = datetime.now(tz)
            bucket_start = closed_bucket_start(now, self._interval, tz)
            bucket_end = bucket_start + timedelta(minutes=self._interval)
            if not is_nse_cash_session_bar(bucket_start, bucket_end, tz):
                continue
            bucket_key = bucket_end.isoformat()
            if bucket_key == self._last_bucket_end:
                continue
            try:
                report = await self.compare_bucket(bucket_end, manager=manager, client=client)
                self._last_report = report
                self._last_bucket_end = bucket_key
                summary = report.get("summary") or {}
                logger.info(
                    "HIST_COMPARE | bucket=%s matched=%s mismatched=%s missing_live=%s "
                    "missing_hist=%s rows=%s requests=%s errors=%s alignment=%s",
                    bucket_key,
                    summary.get("matched"),
                    summary.get("mismatched"),
                    summary.get("missing_live"),
                    summary.get("missing_hist"),
                    summary.get("rows"),
                    report.get("request_count"),
                    report.get("error_count"),
                    report.get("alignment"),
                )
                mismatches = [
                    r for r in report.get("rows") or []
                    if r.get("status") not in {"match", "missing_live", "missing_hist"}
                ]
                for row in mismatches[:12]:
                    logger.warning(
                        "HIST_MISMATCH | %s %s %s diffs=%s",
                        row.get("kind"),
                        row.get("symbol"),
                        row.get("status"),
                        row.get("diffs"),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("HIST_COMPARE_FAILED | bucket=%s", bucket_key)

    async def compare_bucket(
        self,
        bucket_end: datetime,
        *,
        manager: Any | None = None,
        client: HistoricalClient | None = None,
        live: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        manager = manager or self._instrument_manager()
        if manager is None:
            raise RuntimeError("instrument manager is not ready")
        price_scale = float(getattr(manager, "price_scale", 100) or 100)
        client = client or self._make_client(price_scale)
        nifty_px = None
        live_msg = live if live is not None else self._hub.last_candle_3m
        if isinstance(live_msg, dict):
            nifty_block = live_msg.get("nifty") if isinstance(live_msg.get("nifty"), dict) else {}
            nifty_px = nifty_block.get("close")
            if nifty_px is None:
                meta = live_msg.get("meta") if isinstance(live_msg.get("meta"), dict) else {}
                index = meta.get("index") if isinstance(meta, dict) else {}
                nifty_px = index.get("close") if isinstance(index, dict) else None
        groups = build_universe(
            manager,
            nifty_price=float(nifty_px) if nifty_px else None,
            exchange=settings.nubra_exchange,
        )
        fetched = await client.fetch(groups, interval=f"{self._interval}m", intra_day=True, real_time=False)
        alignment = fetched.policy.bar_alignment or "close"
        report = compare_closed_bar(
            live=live_msg,
            frames=fetched.frames,
            kinds=fetched.kinds,
            groups=groups,
            bucket_end=bucket_end,
            alignment=alignment,
            interval_minutes=self._interval,
            price_abs_tol=self._price_abs_tol,
            volume_rel_tol=self._volume_rel_tol,
        )
        report["request_count"] = fetched.request_count
        report["error_count"] = len(fetched.errors)
        report["fetch_errors"] = fetched.errors[:20]
        report["policy"] = {
            "bar_alignment": fetched.policy.bar_alignment,
            "volume_mode": fetched.policy.volume_mode,
            "notes": list(fetched.policy.notes),
        }
        report["universe"] = {g.kind: len(g.symbols) for g in groups}
        live_end = None
        if isinstance(live_msg, dict):
            live_end = live_msg.get("bucket_end")
        report["live_bucket_end"] = live_end
        if live_end:
            logger.info(
                "HIST_COMPARE_ALIGN | closed_bucket_end=%s live_bucket_end=%s",
                bucket_end.isoformat(),
                live_end,
            )
        return report
