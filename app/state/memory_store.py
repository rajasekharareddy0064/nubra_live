from __future__ import annotations

import json
from typing import Any


class MemoryStore:
    """Minimal RedisStore-compatible API for dev / no-Redis mode."""

    def __init__(self) -> None:
        self._hash: dict[str, dict[str, str]] = {}
        self._kv: dict[str, str] = {}

    async def close(self) -> None:
        return None

    async def hset(self, key: str, mapping: dict[str, Any], ttl: int | None = None) -> None:
        if not mapping:
            return
        bucket = self._hash.setdefault(key, {})
        for k, v in mapping.items():
            bucket[k] = json.dumps(v) if isinstance(v, (dict, list)) else str(v)

    async def hgetall(self, key: str) -> dict[str, Any]:
        raw = self._hash.get(key) or {}
        out: dict[str, Any] = {}
        for k, v in raw.items():
            try:
                out[k] = json.loads(v)
            except json.JSONDecodeError:
                out[k] = v
        return out

    async def set_json(self, key: str, value: dict[str, Any], ttl: int | None = None) -> None:
        self._kv[key] = json.dumps(value)

    async def get_json(self, key: str) -> dict[str, Any] | None:
        val = self._kv.get(key)
        if not val:
            return None
        return json.loads(val)

    async def publish(self, channel: str, payload: dict[str, Any]) -> None:
        return None

    async def xadd(self, stream: str, payload: dict[str, Any], maxlen: int = 10000) -> None:
        return None

    def client(self) -> None:
        return None
