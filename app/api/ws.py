import asyncio
import json
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()
logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 15.0


@router.websocket("/ws/live")
async def live_ws(websocket: WebSocket) -> None:
    from app.main import APP_STATE

    hub = APP_STATE.get("hub")
    if hub is not None:
        registered = False
        connected_at = time.monotonic()
        try:
            await websocket.accept()
            logger.info("ws/live accepted")
            await hub.register(websocket)
            registered = True
            await hub.notify_client_connected(websocket)
            while True:
                try:
                    raw = await asyncio.wait_for(
                        websocket.receive_text(),
                        timeout=HEARTBEAT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    await hub.send_json_to(
                        websocket,
                        {
                            "type": "ws_ping",
                            "server_time": time.time(),
                            "uptime_seconds": round(time.monotonic() - connected_at, 3),
                        },
                    )
                    logger.debug("ws/live heartbeat sent")
                    continue

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    msg = {"type": "text", "value": raw}
                logger.debug("ws/live received type=%s", msg.get("type"))

                if msg.get("type") == "ping":
                    await hub.send_json_to(
                        websocket,
                        {
                            "type": "pong",
                            "server_time": time.time(),
                            "client_time": msg.get("client_time"),
                        },
                    )
        except WebSocketDisconnect as exc:
            logger.info("ws/live disconnected code=%s clients=%s", exc.code, getattr(hub, "client_count", None))
        except Exception:
            logger.exception("ws/live loop failed")
            try:
                await websocket.close(code=1011)
            except Exception:
                pass
        finally:
            if registered:
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
