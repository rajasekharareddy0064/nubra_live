import asyncio

from app.core.env_loader import load_project_env
from app.ingestion.nubra_socket import NubraIngestionService
from app.queue.broker import QueueBroker


async def main() -> None:
    load_project_env(".")
    service = NubraIngestionService(QueueBroker(), env_name="UAT", exchange="NSE")
    try:
        await service.start()
        print("UAT_START_OK")
    except Exception as exc:
        print(f"UAT_START_ERR={type(exc).__name__}: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
