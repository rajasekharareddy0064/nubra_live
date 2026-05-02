import json
from typing import Any

from redis.asyncio import Redis


class RedisStore:
    def __init__(self, redis_url: str) -> None:
        self._redis = Redis.from_url(redis_url, decode_responses=True)

    async def close(self) -> None:
        await self._redis.aclose()

    async def hset(self, key: str, mapping: dict[str, Any], ttl: int | None = None) -> None:
        if not mapping:
            return
        await self._redis.hset(key, mapping={k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in mapping.items()})
        if ttl:
            await self._redis.expire(key, ttl)

    async def hgetall(self, key: str) -> dict[str, Any]:
        data = await self._redis.hgetall(key)
        return data or {}

    async def set_json(self, key: str, value: dict[str, Any], ttl: int | None = None) -> None:
        await self._redis.set(key, json.dumps(value))
        if ttl:
            await self._redis.expire(key, ttl)

    async def get_json(self, key: str) -> dict[str, Any] | None:
        val = await self._redis.get(key)
        if not val:
            return None
        return json.loads(val)

    async def publish(self, channel: str, payload: dict[str, Any]) -> None:
        await self._redis.publish(channel, json.dumps(payload))

    async def xadd(self, stream: str, payload: dict[str, Any], maxlen: int = 10000) -> None:
        await self._redis.xadd(stream, {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in payload.items()}, maxlen=maxlen, approximate=True)

    def client(self) -> Redis:
        return self._redis
