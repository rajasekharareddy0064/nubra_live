from app.workers.base import BaseWorker


class OrderbookWorker(BaseWorker):
    stream_name = "orderbook"

    async def handle(self, key: str, payload: dict) -> None:
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        bid_qty = sum((row or {}).get("quantity") or 0 for row in bids[:5])
        ask_qty = sum((row or {}).get("quantity") or 0 for row in asks[:5])
        imbalance = (bid_qty - ask_qty) / max((bid_qty + ask_qty), 1)

        state = {
            "ref_id": key,
            "last_traded_price": payload.get("last_traded_price"),
            "last_traded_quantity": payload.get("last_traded_quantity"),
            "volume": payload.get("volume"),
            "imbalance_5": imbalance,
            "timestamp": payload.get("timestamp"),
        }
        await self.redis.hset(f"state:orderbook:{key}", state, ttl=10)
        await self.redis.publish("pubsub:state", {"stream": "orderbook", "key": key, "state": state})
        await self.db_writer.enqueue("orderbook_ticks", {"key": key, "payload": payload})
