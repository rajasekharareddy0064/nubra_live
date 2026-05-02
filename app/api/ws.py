import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws/live")
async def live_ws(websocket: WebSocket) -> None:
    from app.main import APP_STATE

    hub = APP_STATE.get("hub")
    if hub is not None:
        await websocket.accept()
        await hub.register(websocket)
        await hub.notify_client_connected(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            hub.unregister(websocket)
        return

    redis = APP_STATE.get("redis")
    if redis is None:
        await websocket.close(code=1011)
        return

    redis_client = redis.client()
    if redis_client is None:
        await websocket.close(code=1011)
        return

    pubsub = redis_client.pubsub()

    await websocket.accept()
    await pubsub.subscribe("pubsub:signals", "pubsub:state")
    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("data"):
                await websocket.send_text(message["data"])
            await asyncio.sleep(0.01)
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe("pubsub:signals", "pubsub:state")
        await pubsub.aclose()
