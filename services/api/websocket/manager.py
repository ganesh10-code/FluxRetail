"""
WebSocket connection manager.

Manages a pool of active WebSocket connections and broadcasts messages.

Broadcast strategy:
  - Raw events are broadcast immediately (message_type: 'event')
  - KPI aggregates are broadcast every 2 seconds (message_type: 'kpi')
    This is handled by the kafka consumer's periodic aggregation task.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import WebSocket

logger = structlog.get_logger(__name__)


class ConnectionManager:
    """Manages all active WebSocket connections."""

    def __init__(self) -> None:
        self._connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections.append(websocket)
        logger.info("websocket_connected", total=len(self._connections))

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self._connections:
            self._connections.remove(websocket)
        logger.info("websocket_disconnected", total=len(self._connections))

    async def broadcast_event(self, event_dict: dict[str, Any]) -> None:
        """Immediately broadcast a raw retail event to all clients."""
        message = json.dumps({
            "message_type": "event",
            "payload": event_dict,
        })
        await self._broadcast(message)

    async def broadcast_kpi(self, kpi_dict: dict[str, Any]) -> None:
        """Broadcast a compact KPI update to all clients."""
        message = json.dumps({
            "message_type": "kpi",
            "payload": {
                **kpi_dict,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
        })
        await self._broadcast(message)

    async def _broadcast(self, message: str) -> None:
        dead_connections: list[WebSocket] = []
        for ws in list(self._connections):
            try:
                await ws.send_text(message)
            except Exception:
                dead_connections.append(ws)

        for ws in dead_connections:
            self.disconnect(ws)

    @property
    def active_connections(self) -> int:
        return len(self._connections)


# Module-level singleton shared across the application
ws_manager = ConnectionManager()
