"""
Market Data Simulator for offline pipeline testing.

Replays sample JSON data through the exact same QueueBroker -> Pipeline
-> Candles -> DB Writer -> Hub path used in production. Only the data
source changes: sample files instead of live Nubra WebSocket.

Enable with: SIMULATION_MODE=true or MARKET_MODE=SIMULATION

Configurable replay speed via SIMULATION_SPEED:
  1.0  = realtime (1 tick per original interval)
  5.0  = 5x speed
  10.0 = 10x speed
  0.0  = instant (no delay between ticks)
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.queue.broker import QueueBroker
from app.queue.envelope import EventEnvelope

logger = logging.getLogger("simulation")


class SimulationStats:
    """Accumulates metrics during simulation replay."""

    def __init__(self) -> None:
        self.ticks_processed: int = 0
        self.candles_created: int = 0
        self.db_rows_written: int = 0
        self.frontend_messages: int = 0
        self.errors: list[str] = []
        self.start_time: float = 0.0
        self.end_time: float = 0.0
        self.streams: dict[str, int] = {}

    def record_tick(self, stream: str) -> None:
        self.ticks_processed += 1
        self.streams[stream] = self.streams.get(stream, 0) + 1

    def to_dict(self) -> dict[str, Any]:
        duration = self.end_time - self.start_time if self.end_time else 0
        return {
            "mode": "SIMULATION",
            "ticks_processed": self.ticks_processed,
            "candles_created": self.candles_created,
            "db_rows_written": self.db_rows_written,
            "frontend_messages": self.frontend_messages,
            "errors": len(self.errors),
            "error_details": self.errors[:10],
            "duration_seconds": round(duration, 2),
            "streams": self.streams,
            "status": "SUCCESS" if not self.errors else "PARTIAL",
        }


class SampleDataLoader:
    """Loads sample JSON files from the sample_data/ directory."""

    def __init__(self, data_dir: str = "sample_data") -> None:
        self.data_dir = Path(data_dir)

    def load_file(self, filename: str) -> list[dict[str, Any]]:
        path = self.data_dir / filename
        if not path.exists():
            logger.warning("Sample file not found: %s", path)
            return []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "records" in data:
            return data["records"]
        return [data]

    def load_all(self) -> list[dict[str, Any]]:
        """Load all sample files, merge into a single timeline."""
        all_records: list[dict[str, Any]] = []

        file_stream_map = {
            "nifty_ticks.json": "index",
            "futures_ticks.json": "index",
            "option_chain.json": "option",
            "order_book.json": "orderbook",
            "greeks.json": "greeks",
        }

        for filename, default_stream in file_stream_map.items():
            records = self.load_file(filename)
            for record in records:
                if "stream" not in record:
                    record["stream"] = default_stream
                all_records.append(record)

        # Also load any additional .json files in sample_data/
        for path in self.data_dir.glob("*.json"):
            if path.name not in file_stream_map:
                records = self.load_file(path.name)
                for record in records:
                    if "stream" not in record:
                        record["stream"] = "index"
                    all_records.append(record)

        # Sort by timestamp if available
        def sort_key(r: dict) -> float:
            ts = r.get("timestamp") or r.get("ts") or 0
            if isinstance(ts, str):
                try:
                    return float(ts)
                except ValueError:
                    return 0.0
            return float(ts)

        all_records.sort(key=sort_key)
        logger.info(
            "LOADING_SAMPLE_DATA | files=%d records=%d",
            len(file_stream_map),
            len(all_records),
        )
        return all_records

    def available_files(self) -> list[str]:
        if not self.data_dir.exists():
            return []
        return [f.name for f in self.data_dir.glob("*.json")]


class MarketSimulator:
    """Replays sample data through the production pipeline.

    The simulator publishes EventEnvelope objects to the same QueueBroker
    that the live NubraIngestionService would use. Downstream processing
    (RealtimePipeline, CandleBoard, DBWriter, LiveHub) is identical.
    """

    def __init__(
        self,
        broker: QueueBroker,
        *,
        speed: float = 1.0,
        data_dir: str = "sample_data",
    ) -> None:
        self.broker = broker
        self.speed = speed
        self.loader = SampleDataLoader(data_dir)
        self.stats = SimulationStats()
        self._running = False

    async def run(self) -> SimulationStats:
        """Load sample data and replay through the broker."""
        logger.info(
            "SIMULATION_STARTED | speed=%sx data_dir=%s",
            self.speed,
            self.loader.data_dir,
        )
        self.stats.start_time = time.time()
        self._running = True

        records = self.loader.load_all()
        if not records:
            logger.error("No sample data found in %s", self.loader.data_dir)
            self.stats.errors.append("No sample data files found")
            self.stats.end_time = time.time()
            return self.stats

        logger.info(
            "PROCESSING_TICKS | total_records=%d speed=%sx",
            len(records),
            self.speed,
        )

        prev_ts: float = 0.0

        for i, record in enumerate(records):
            if not self._running:
                break

            stream = record.get("stream", "index")
            key = record.get("key") or record.get("symbol") or "NIFTY"
            payload = record.get("payload") or record.get("data") or record

            # Calculate delay between records for speed simulation
            if self.speed > 0:
                curr_ts = float(record.get("timestamp") or record.get("ts") or 0)
                if prev_ts > 0 and curr_ts > prev_ts:
                    delay = (curr_ts - prev_ts) / self.speed
                    if delay > 0 and delay < 5:  # cap at 5s max wait
                        await asyncio.sleep(delay)
                prev_ts = curr_ts

            # Create envelope and publish to broker (same as live)
            try:
                envelope = EventEnvelope(
                    stream=stream,
                    key=str(key),
                    payload=payload if isinstance(payload, dict) else {"value": payload},
                )
                await self.broker.publish(envelope)
                self.stats.record_tick(stream)
            except Exception as exc:
                self.stats.errors.append(f"tick {i}: {exc}")
                logger.warning("Simulation tick %d failed: %s", i, exc)

            # Log progress every 10000 ticks
            if (i + 1) % 10000 == 0:
                logger.info(
                    "PROCESSING_TICKS | progress=%d/%d streams=%s",
                    i + 1,
                    len(records),
                    self.stats.streams,
                )

        self.stats.end_time = time.time()
        self._running = False

        logger.info(
            "SIMULATION_COMPLETED | ticks=%d duration=%.2fs streams=%s errors=%d",
            self.stats.ticks_processed,
            self.stats.end_time - self.stats.start_time,
            self.stats.streams,
            len(self.stats.errors),
        )
        return self.stats

    def stop(self) -> None:
        self._running = False
