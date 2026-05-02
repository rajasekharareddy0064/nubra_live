import asyncio
import logging

from app.core.env_loader import load_project_env
from app.ingestion.nubra_socket import NubraIngestionService
from app.queue.broker import QueueBroker


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    load_project_env(".")
    print("[check] starting ingestion startup check")
    service = NubraIngestionService(QueueBroker())
    try:
        await asyncio.wait_for(service.start(), timeout=120)
    except asyncio.TimeoutError:
        print(f"[check] TIMEOUT while phase={service.last_start_phase}")
        raise
    except Exception as exc:
        print(f"[check] FAILED at phase={service.last_start_phase} err={type(exc).__name__}: {exc}")
        raise
    print("INGESTION_START_OK")
    print(f"[check] phase={service.last_start_phase}")
    await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
