from app.workers.base import BaseWorker


class GreeksWorker(BaseWorker):
    stream_name = "greeks"

    async def handle(self, key: str, payload: dict) -> None:
        state = {
            "ref_id": key,
            "delta": payload.get("delta"),
            "gamma": payload.get("gamma"),
            "theta": payload.get("theta"),
            "vega": payload.get("vega"),
            "iv": payload.get("iv"),
            "open_interest": payload.get("open_interest"),
            "timestamp": payload.get("timestamp"),
        }
        await self.redis.hset(f"state:greeks:{key}", state, ttl=10)
        await self.redis.publish("pubsub:state", {"stream": "greeks", "key": key, "state": state})
        await self.db_writer.enqueue("greeks_ticks", state)
