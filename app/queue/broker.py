import asyncio

from app.queue.envelope import EventEnvelope


class QueueBroker:
    def __init__(self, maxsize: int = 100000) -> None:
        self._queue: asyncio.Queue[EventEnvelope] = asyncio.Queue(maxsize=maxsize)

    async def publish(self, event: EventEnvelope) -> None:
        await self._queue.put(event)

    async def consume(self) -> EventEnvelope:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    def qsize(self) -> int:
        return self._queue.qsize()
