import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import asyncpg


class DBWriter:
    def __init__(self, dsn: str, batch_size: int = 500, flush_interval_ms: int = 1000) -> None:
        self.dsn = dsn
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_ms / 1000.0
        self.queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=200000)
        self.pool: asyncpg.Pool | None = None
        self.logger = logging.getLogger(self.__class__.__name__)

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)
        await self._ensure_table()

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def enqueue(self, topic: str, payload: dict[str, Any]) -> None:
        await self.queue.put((topic, payload))

    async def run_forever(self) -> None:
        while True:
            await self.flush_once()
            await asyncio.sleep(self.flush_interval_s)

    async def flush_once(self) -> None:
        if not self.pool:
            return

        batches: dict[str, list[dict[str, Any]]] = defaultdict(list)
        total_rows = 0
        while total_rows < self.batch_size and not self.queue.empty():
            topic, payload = await self.queue.get()
            batches[topic].append(payload)
            total_rows += 1
            self.queue.task_done()

        if not batches:
            return

        async with self.pool.acquire() as conn:
            rows: list[tuple[str, str, datetime]] = []
            now = datetime.now(timezone.utc)
            for topic, payloads in batches.items():
                rows.extend((topic, json.dumps(payload), now) for payload in payloads)
            await conn.executemany(
                "INSERT INTO market_events(topic, payload, created_at) VALUES($1, $2::jsonb, $3)",
                rows,
            )

    async def _ensure_table(self) -> None:
        if not self.pool:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_events (
                  id BIGSERIAL PRIMARY KEY,
                  topic TEXT NOT NULL,
                  payload JSONB NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL
                );
                """
            )
