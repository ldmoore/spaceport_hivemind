"""The Bazaar wire boundary used by a policy-free Hivemind bridge."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from websockets.asyncio.client import connect

SUBPROTOCOL = "bazaar.protobuf.v2"
PROTOCOL_VERSION = "2.0"


def protobuf_module() -> Any:
    try:
        from .generated import bazaar_pb2
    except ImportError as error:
        raise RuntimeError("Bazaar bindings are missing; install dev dependencies and run `make generate`.") from error
    return bazaar_pb2


@dataclass(frozen=True)
class GameConfig:
    endpoint: str
    station_id: str
    token: str

    @classmethod
    def from_credentials(cls, path: Path, endpoint: str, station_id: str) -> "GameConfig":
        payload = json.loads(path.read_text())
        for player in payload.get("players", []):
            if player.get("station_id") == station_id and isinstance(player.get("token"), str):
                return cls(endpoint, station_id, player["token"])
        raise ValueError(f"No token for station {station_id!r} in {path}")


class BazaarConnection:
    def __init__(self, config: GameConfig) -> None:
        self.config = config
        self.pb = protobuf_module()
        self.socket: Any | None = None

    async def __aenter__(self) -> "BazaarConnection":
        self.socket = await connect(self.config.endpoint, additional_headers={"Authorization": f"Bearer {self.config.token}"}, subprotocols=[SUBPROTOCOL])
        if self.socket.subprotocol != SUBPROTOCOL:
            await self.socket.close()
            raise RuntimeError("Bazaar did not confirm its required subprotocol")
        return self

    async def __aexit__(self, *_: object) -> None:
        if self.socket is not None:
            await self.socket.close()

    async def receive(self) -> Any:
        if self.socket is None:
            raise RuntimeError("Game connection is not open")
        raw = await self.socket.recv()
        if not isinstance(raw, bytes):
            raise RuntimeError("Bazaar sent text instead of Protobuf")
        message = self.pb.ServerMessage()
        message.ParseFromString(raw)
        return message

    async def send_ready(self, run_id: str, snapshot_sequence: int) -> None:
        message = self.pb.ClientMessage()
        message.ready.type = self.pb.READY_TYPE_READY
        message.ready.protocol_version = PROTOCOL_VERSION
        message.ready.run_id = run_id
        message.ready.ready = True
        message.ready.snapshot_sequence = snapshot_sequence
        await self._send(message)

    async def send_action(self, action: dict[str, Any], run_id: str, command_id: str) -> None:
        """Encode an instruction exactly as received; no strategy validation occurs here."""
        message = self.pb.ClientMessage()
        kind = action.get("kind")
        request_id = f"hive-{command_id}-{uuid4().hex}"
        if kind == "offer":
            body = message.offer
            body.type = self.pb.OFFER_COMMAND_TYPE_OFFER
            body.body.recipient_id = str(action["recipient_id"])
            self._copy_bundle(action["give"], body.body.give)
            self._copy_bundle(action["receive"], body.body.receive)
            body.body.expires_tick = int(action["expires_tick"])
        elif kind == "accept":
            body = message.accept
            body.type = self.pb.ACCEPT_TYPE_ACCEPT
            body.body.offer_id = str(action["offer_id"])
        elif kind == "withdraw":
            body = message.withdraw
            body.type = self.pb.WITHDRAW_TYPE_WITHDRAW
            body.body.object_id = str(action["offer_id"])
        else:
            raise ValueError(f"Unsupported command kind: {kind!r}")
        body.protocol_version = PROTOCOL_VERSION
        body.run_id = run_id
        body.request_id = request_id
        await self._send(message)

    async def _send(self, message: Any) -> None:
        if self.socket is None:
            raise RuntimeError("Game connection is not open")
        await self.socket.send(message.SerializeToString())

    @staticmethod
    def _copy_bundle(source: Any, destination: Any) -> None:
        if not isinstance(source, dict):
            raise ValueError("Bundle must be an object")
        for resource in ("water", "food", "components"):
            amount = source.get(resource, 0)
            if not isinstance(amount, int) or amount < 0:
                raise ValueError(f"Invalid {resource} bundle amount")
            setattr(destination, resource, amount)
