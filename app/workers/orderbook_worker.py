from app.realtime.order_book import _levels_quantity
from app.workers.base import BaseWorker


def _book_levels(payload: dict, *keys: str) -> list:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


class OrderbookWorker(BaseWorker):
    stream_name = "orderbook"

    async def handle(self, key: str, payload: dict) -> None:
        core = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        bids = _book_levels(core, "bids", "buy", "buy_levels", "buyLevels")
        asks = _book_levels(core, "asks", "sell", "sell_levels", "sellLevels")
        bid_qty = _levels_quantity(bids, "quantity", "qty", "bid_qty", "bidQty", "size")
        ask_qty = _levels_quantity(asks, "quantity", "qty", "ask_qty", "askQty", "size")
        imbalance = (bid_qty - ask_qty) / max((bid_qty + ask_qty), 1)

        state = {
            "ref_id": key,
            "last_traded_price": core.get("last_traded_price") or core.get("lastTradedPrice") or core.get("ltp"),
            "last_traded_quantity": core.get("last_traded_quantity")
            or core.get("lastTradedQuantity")
            or core.get("ltq"),
            "volume": core.get("volume") or core.get("traded_volume") or core.get("tradedVolume"),
            "imbalance_5": imbalance,
            "timestamp": core.get("timestamp") or core.get("exchange_timestamp") or core.get("exchangeTimestamp"),
        }
        await self.redis.hset(f"state:orderbook:{key}", state, ttl=10)
        await self.redis.publish("pubsub:state", {"stream": "orderbook", "key": key, "state": state})
        await self.db_writer.enqueue("orderbook_ticks", {"key": key, "payload": payload})
