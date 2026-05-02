from app.workers.base import BaseWorker


class OptionsWorker(BaseWorker):
    stream_name = "option"

    async def handle(self, key: str, payload: dict) -> None:
        ce = payload.get("ce") or []
        pe = payload.get("pe") or []
        ce_oi = sum((row or {}).get("open_interest") or 0 for row in ce)
        pe_oi = sum((row or {}).get("open_interest") or 0 for row in pe)

        state = {
            "chain_key": key,
            "asset": payload.get("asset"),
            "expiry": payload.get("expiry"),
            "atm": payload.get("at_the_money_strike"),
            "current_price": payload.get("current_price"),
            "ce_oi_total": ce_oi,
            "pe_oi_total": pe_oi,
            "oi_change": pe_oi - ce_oi,
        }
        redis_key = f"state:optionchain:{payload.get('asset')}:{payload.get('expiry')}"
        await self.redis.hset(redis_key, state, ttl=15)
        await self.redis.publish("pubsub:state", {"stream": "option", "key": key, "state": state})
        await self.db_writer.enqueue("option_chain_ticks", {"key": key, "payload": payload})
