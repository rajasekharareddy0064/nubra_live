import logging
from abc import ABC, abstractmethod

from app.queue.broker import QueueBroker
from app.state.redis_store import RedisStore
from app.storage.db_writer import DBWriter


class BaseWorker(ABC):
    stream_name: str

    def __init__(self, broker: QueueBroker, redis_store: RedisStore, db_writer: DBWriter) -> None:
        self.broker = broker
        self.redis = redis_store
        self.db_writer = db_writer
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run_forever(self) -> None:
        while True:
            event = await self.broker.consume()
            try:
                if event.stream == self.stream_name:
                    await self.handle(event.key, event.payload)
            except Exception as exc:  # pragma: no cover - guardrail path
                self.logger.exception("worker failed: %s", exc)
            finally:
                self.broker.task_done()

    @abstractmethod
    async def handle(self, key: str, payload: dict) -> None:
        raise NotImplementedError
