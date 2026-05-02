from app.workers.base import BaseWorker


class OhlcvWorker(BaseWorker):
    stream_name = "ohlcv"

    async def handle(self, key: str, payload: dict) -> None:
        close_price = payload.get("close") or 0
        high_price = payload.get("high") or 0
        breakout_level = max(close_price, high_price)

        state = {
            "symbol_interval": key,
            "symbol": payload.get("indexname"),
            "interval": payload.get("interval"),
            "open": payload.get("open"),
            "high": high_price,
            "low": payload.get("low"),
            "close": close_price,
            "bucket_volume": payload.get("bucket_volume"),
            "breakout_level": breakout_level,
            "timestamp": payload.get("timestamp"),
        }
        redis_key = f"state:ohlcv:{payload.get('indexname')}:{payload.get('interval')}"
        await self.redis.hset(redis_key, state, ttl=120)
        await self.redis.publish("pubsub:state", {"stream": "ohlcv", "key": key, "state": state})
        await self.db_writer.enqueue("ohlcv_ticks", state)
