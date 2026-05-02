import asyncio
import logging

from app.state.redis_store import RedisStore


class SignalEngine:
    def __init__(self, redis_store: RedisStore, symbols: list[str]) -> None:
        self.redis = redis_store
        self.symbols = symbols
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run_forever(self, interval_s: float = 0.5) -> None:
        while True:
            for symbol in self.symbols:
                await self.evaluate(symbol)
            await asyncio.sleep(interval_s)

    async def evaluate(self, symbol: str) -> None:
        index_state = await self.redis.hgetall(f"state:index:{symbol}")
        ohlcv_state = await self.redis.hgetall(f"state:ohlcv:{symbol}:5m")

        if not index_state or not ohlcv_state:
            return

        close_px = float(ohlcv_state.get("close", 0) or 0)
        breakout_lvl = float(ohlcv_state.get("breakout_level", 0) or 0)
        breakout = close_px >= breakout_lvl and breakout_lvl > 0

        # Lightweight placeholders for cross-stream factors. These keys can be
        # mapped to exact chain/ref ids through an instrument registry module.
        option_state = await self.redis.hgetall(f"state:optionchain:{symbol}:active")
        orderbook_state = await self.redis.hgetall(f"state:orderbook:{symbol}:active")
        greeks_state = await self.redis.hgetall(f"state:greeks:{symbol}:active")

        oi_up = float(option_state.get("oi_change", 0) or 0) > 0
        delta_up = float(greeks_state.get("delta", 0) or 0) > 0.35
        imbalance = float(orderbook_state.get("imbalance_5", 0) or 0)
        imbalance_up = imbalance > 0.2

        score = 0.35 * int(breakout) + 0.25 * int(oi_up) + 0.20 * int(delta_up) + 0.20 * int(imbalance_up)
        if score < 0.7:
            return

        signal = {
            "symbol": symbol,
            "side": "LONG",
            "score": score,
            "reason": "breakout+oi+delta+imbalance",
        }
        await self.redis.set_json(f"signals:latest:{symbol}", signal, ttl=120)
        await self.redis.xadd(f"signals:history:{symbol}", signal, maxlen=10000)
        await self.redis.publish("pubsub:signals", signal)
