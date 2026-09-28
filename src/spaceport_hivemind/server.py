"""The Hivemind WebSocket service."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from .coordinator import Coordinator, CoordinatorConfig
from .model import Planet

LOG = logging.getLogger(__name__)


@dataclass
class HivemindServer:
    coordinator: Coordinator
    shared_key: str | None = None

    def __post_init__(self) -> None:
        self.clients: dict[str, ServerConnection] = {}
        self._lock = asyncio.Lock()

    async def handler(self, socket: ServerConnection) -> None:
        station_id: str | None = None
        try:
            hello = await self._receive_json(socket)
            if hello.get("type") != "hello" or not isinstance(hello.get("station_id"), str):
                await socket.close(code=4001, reason="expected hello")
                return
            if self.shared_key is not None and hello.get("shared_key") != self.shared_key:
                await socket.close(code=4003, reason="unauthorized")
                return
            station_id = hello["station_id"]
            async with self._lock:
                old = self.clients.get(station_id)
                self.clients[station_id] = socket
                if old is not None and old is not socket:
                    await old.close(code=4002, reason="superseded by newer bridge")
            await self._send_json(socket, {"type": "welcome", "station_id": station_id})
            LOG.info("Hivemind bridge online: %s", station_id)
            async for raw in socket:
                message = self._decode(raw)
                if message.get("station_id") != station_id:
                    await socket.close(code=4003, reason="station identity changed")
                    return
                if message.get("type") == "observation":
                    planet = Planet.from_observation(message["state"])
                    if planet.station_id != station_id:
                        await socket.close(code=4003, reason="observation station mismatch")
                        return
                    async with self._lock:
                        commands = self.coordinator.ingest(planet)
                        await self._dispatch(commands)
                elif message.get("type") == "command_ack":
                    LOG.info("%s acknowledged %s: %s", station_id, message.get("command_id"), message.get("status"))
                elif message.get("type") == "game_result":
                    LOG.info("%s game result: %s", station_id, message.get("result", {}).get("code"))
                else:
                    await socket.close(code=4001, reason="unknown hive message")
                    return
        except ConnectionClosed:
            pass
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            LOG.warning("Invalid hive message from %s: %s", station_id or "unknown", error)
            await socket.close(code=4001, reason="invalid hive message")
        except Exception:
            # A bug in coordination must be visible in the server log rather
            # than surfacing to every bridge as an unexplained WebSocket 1011.
            LOG.exception("Unhandled Hivemind error for %s", station_id or "unknown")
            await socket.close(code=1011, reason="internal hive error; inspect server log")
        finally:
            if station_id is not None and self.clients.get(station_id) is socket:
                self.clients.pop(station_id, None)
                LOG.info("Hivemind bridge offline: %s", station_id)

    async def _dispatch(self, commands: list[Any]) -> None:
        for command in commands:
            socket = self.clients.get(command.station_id)
            if socket is None:
                LOG.warning("Cannot dispatch %s; %s is offline", command.command_id, command.station_id)
                continue
            try:
                await self._send_json(socket, command.as_message())
                LOG.info("Dispatched %s to %s", command.action["kind"], command.station_id)
            except ConnectionClosed:
                LOG.warning("Cannot dispatch %s; %s disconnected", command.command_id, command.station_id)

    @staticmethod
    def _decode(raw: str | bytes) -> dict[str, Any]:
        if not isinstance(raw, str):
            raise ValueError("Hivemind protocol uses text JSON frames")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("Hivemind message must be a JSON object")
        return value

    async def _receive_json(self, socket: ServerConnection) -> dict[str, Any]:
        return self._decode(await socket.recv())

    @staticmethod
    async def _send_json(socket: ServerConnection, value: dict[str, Any]) -> None:
        await socket.send(json.dumps(value, separators=(",", ":"), sort_keys=True))


async def run(
    host: str,
    port: int,
    shared_key: str | None,
    station_count: int = 9,
    reserve_ticks: int = 4,
) -> None:
    server = HivemindServer(
        Coordinator(CoordinatorConfig(expected_station_count=station_count, reserve_ticks=reserve_ticks)),
        shared_key,
    )
    async with serve(server.handler, host, port, max_size=2**20):
        LOG.info("Hivemind listening on ws://%s:%d", host, port)
        await asyncio.Future()


def main() -> None:
    parser = argparse.ArgumentParser(description="Spaceport Bazaar central Hivemind")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--shared-key", default=None, help="Require this bridge secret")
    parser.add_argument("--station-count", type=int, default=9, help="Number of bridges required before planning (default: 9)")
    parser.add_argument("--reserve-ticks", type=int, default=4, help="Future upkeep ticks retained by the coordinator (default: 4)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(run(args.host, args.port, args.shared_key, args.station_count, args.reserve_ticks))
