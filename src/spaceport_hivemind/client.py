"""Thin dual-WebSocket bridge: Bazaar binary protocol in, Hive JSON out."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from google.protobuf.json_format import MessageToDict
from websockets.asyncio.client import ClientConnection, connect

from .game import BazaarConnection, GameConfig

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class HiveConfig:
    endpoint: str
    shared_key: str | None = None


class HivemindClient:
    """Forwards facts and executes explicit commands, without a policy layer."""

    def __init__(self, game: GameConfig, hive: HiveConfig) -> None:
        self.game_config = game
        self.hive_config = hive
        self.run_id: str | None = None
        self.pb: Any | None = None

    async def run(self) -> None:
        station_id = self.game_config.station_id
        LOG.info("%s connecting to Bazaar at %s", station_id, self.game_config.endpoint)
        async with BazaarConnection(self.game_config) as game:
            LOG.info("%s connected to Bazaar", station_id)
            LOG.info("%s connecting to Hivemind at %s", station_id, self.hive_config.endpoint)
            async with connect(self.hive_config.endpoint, max_size=2**20) as hive:
                self.pb = game.pb
                await self._send_hive(hive, {"type": "hello", "station_id": station_id, "shared_key": self.hive_config.shared_key})
                welcome = await self._receive_hive(hive)
                if welcome.get("type") != "welcome":
                    raise RuntimeError("Hivemind did not accept this bridge")
                LOG.info("%s connected to Hivemind", station_id)
                first = await game.receive()
                if first.WhichOneof("message") != "state":
                    raise RuntimeError("Expected Bazaar initial state")
                self.run_id = first.state.run_id
                await game.send_ready(first.state.run_id, first.state.snapshot_sequence)
                await self._send_observation(hive, first.state)
                await asyncio.gather(self._copy_game_to_hive(game, hive), self._copy_hive_to_game(game, hive))

    async def _copy_game_to_hive(self, game: BazaarConnection, hive: ClientConnection) -> None:
        while True:
            message = await game.receive()
            kind = message.WhichOneof("message")
            if kind == "state":
                if self.run_id != message.state.run_id:
                    self.run_id = message.state.run_id
                    await game.send_ready(message.state.run_id, message.state.snapshot_sequence)
                self.run_id = message.state.run_id
                await self._send_observation(hive, message.state)
            elif kind == "result":
                await self._send_hive(hive, {"type": "game_result", "station_id": self.game_config.station_id, "result": self._to_dict(message.result)})
            elif kind == "protocol_error" and message.protocol_error.close_session:
                raise RuntimeError("Bazaar closed this game session")

    async def _copy_hive_to_game(self, game: BazaarConnection, hive: ClientConnection) -> None:
        while True:
            command = await self._receive_hive(hive)
            if command.get("type") != "command":
                continue
            command_id = str(command.get("command_id", ""))
            try:
                if command.get("station_id") != self.game_config.station_id or command.get("run_id") != self.run_id:
                    raise ValueError("command addressed to a different game session")
                await game.send_action(command["action"], self.run_id or "", command_id)
                status = "sent"
            except (KeyError, TypeError, ValueError, RuntimeError) as error:
                LOG.warning("Rejected hive command %s: %s", command_id, error)
                status = f"rejected: {error}"
            await self._send_hive(hive, {"type": "command_ack", "station_id": self.game_config.station_id, "command_id": command_id, "status": status})

    async def _send_observation(self, hive: ClientConnection, state: Any) -> None:
        data = self._to_dict(state)
        data["station_id"] = self.game_config.station_id
        await self._send_hive(hive, {"type": "observation", "station_id": self.game_config.station_id, "state": data})

    @staticmethod
    def _to_dict(message: Any) -> dict[str, Any]:
        return MessageToDict(message, preserving_proto_field_name=True, use_integers_for_enums=False)

    @staticmethod
    async def _send_hive(hive: ClientConnection, value: dict[str, Any]) -> None:
        await hive.send(json.dumps(value, separators=(",", ":"), sort_keys=True))

    @staticmethod
    async def _receive_hive(hive: ClientConnection) -> dict[str, Any]:
        raw = await hive.recv()
        if not isinstance(raw, str):
            raise RuntimeError("Hivemind sent a non-JSON frame")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise RuntimeError("Hivemind message must be an object")
        return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Spaceport Bazaar Hivemind bridge")
    parser.add_argument("--endpoint", default="ws://127.0.0.1:3001/ws", help="Bazaar game endpoint")
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--station", required=True)
    parser.add_argument("--hivemind-endpoint", default="ws://127.0.0.1:8765")
    parser.add_argument("--hivemind-key", default=None, help="Override hivemind_key from the credentials JSON")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    try:
        credentials = json.loads(args.credentials.read_text())
        shared_key = args.hivemind_key if args.hivemind_key is not None else credentials.get("hivemind_key")
        if shared_key is not None and (not isinstance(shared_key, str) or not shared_key.strip()):
            parser.error("hivemind_key must be a nonempty string")
    except (OSError, ValueError, AttributeError):
        parser.error("Unable to read credentials JSON object")
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(message)s")
    game = GameConfig.from_credentials(args.credentials, args.endpoint, args.station)
    asyncio.run(HivemindClient(game, HiveConfig(args.hivemind_endpoint, shared_key)).run())
