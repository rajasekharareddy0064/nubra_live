from app.workers.base import BaseWorker


class IndexWorker(BaseWorker):
    stream_name = "index"

    async def handle(self, key: str, payload: dict) -> None:
        state = {
            "symbol": key,
            "exchange": payload.get("exchange"),
            "index_value": payload.get("index_value"),
            "changepercent": payload.get("changepercent"),
            "volume": payload.get("volume"),
            "timestamp": payload.get("timestamp"),
        }
        await self.redis.hset(f"state:index:{key}", state, ttl=30)
        await self.redis.publish("pubsub:state", {"stream": "index", "key": key, "state": state})
        await self.db_writer.enqueue("index_ticks", state)
