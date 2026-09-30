"""Small multi-station Bazaar server for exercising the Hivemind end to end.

This is intentionally a demo, not a replacement for the authoritative Bazaar
server.  It speaks the same Protobuf-over-WebSocket boundary used by the
bridges and implements the offer/accept path that the Coordinator relies on.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import secrets
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from websockets.asyncio.server import ServerConnection, serve

from spaceport_hivemind.game import PROTOCOL_VERSION, SUBPROTOCOL, protobuf_module
from .scenario import Scenario

LOG = logging.getLogger(__name__)
RESOURCES = ("water", "food", "components")
UPKEEP = {"water": 2, "food": 1, "components": 1}


@dataclass
class GameStation:
    station_id: str
    token: str
    inventory: dict[str, int]
    specialty: str
    ready: bool = False
    health: int = 100
    failed_once: bool = False
    first_failure_tick: int | None = None
    last_production: dict[str, int] = field(default_factory=lambda: dict.fromkeys(RESOURCES, 0))
    last_unmet_upkeep: dict[str, int] = field(default_factory=lambda: dict.fromkeys(RESOURCES, 0))
    fully_supplied_ticks: int = 0
    shortage_ticks: int = 0
    current_shortage_streak: int = 0
    longest_shortage_streak: int = 0
    produced_total: dict[str, int] = field(default_factory=lambda: dict.fromkeys(RESOURCES, 0))
    consumed_total: dict[str, int] = field(default_factory=lambda: dict.fromkeys(RESOURCES, 0))
    unmet_total: dict[str, int] = field(default_factory=lambda: dict.fromkeys(RESOURCES, 0))
    imported_total: dict[str, int] = field(default_factory=lambda: dict.fromkeys(RESOURCES, 0))
    exported_total: dict[str, int] = field(default_factory=lambda: dict.fromkeys(RESOURCES, 0))


@dataclass
class GameOffer:
    offer_id: str
    proposer_id: str
    recipient_id: str
    give: dict[str, int]
    receive: dict[str, int]
    expires_tick: int
    status: int
    created_tick: int = 0
    created_version: int = 1
    transaction_id: str | None = None
    closed_tick: int | None = None
    settled_tick: int | None = None
    settled_version: int | None = None


@dataclass
class GameWorld:
    """A deterministic resource game with the Bazaar network contract."""

    scenario: Scenario
    credentials: dict[str, str]
    pb: Any = field(default_factory=protobuf_module)
    run_id: str = field(default_factory=lambda: f"demo-{secrets.token_hex(6)}")
    tick: int = 0
    world_version: int = 1
    duration_ticks: int = 30
    tick_duration_ms: int = 1_000
    phase: str = "READY"
    offers: list[GameOffer] = field(default_factory=list)
    connections: dict[str, ServerConnection] = field(default_factory=dict)
    snapshot_sequences: dict[str, int] = field(default_factory=dict)
    _offer_sequence: int = 0
    _transaction_sequence: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _ready_to_run: asyncio.Event = field(default_factory=asyncio.Event)
    finished: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.stations = {
            station.station_id: GameStation(
                station_id=station.station_id,
                token=self.credentials.get(station.station_id, "unknown"),
                inventory=dict(station.inventory),
                specialty=station.specialty,
                health=self.scenario.max_health,
            )
            for station in self.scenario.stations
        }
        self.duration_ticks = self.scenario.duration_ticks
        self.tick_duration_ms = self.scenario.tick_duration_ms


    def credential_payload(self) -> dict[str, list[dict[str, str]]]:
        return {"players": [{"station_id": item.station_id, "token": item.token} for item in self.stations.values()]}

    async def attach(self, station_id: str, socket: ServerConnection) -> None:
        async with self._lock:
            previous = self.connections.get(station_id)
            self.connections[station_id] = socket
            self.snapshot_sequences[station_id] = 0
        if previous is not None and previous is not socket:
            await previous.close(code=4001, reason="station reconnected")
        await self._send_state(station_id, socket)

    async def detach(self, station_id: str, socket: ServerConnection) -> None:
        async with self._lock:
            if self.connections.get(station_id) is socket:
                self.connections.pop(station_id, None)
                self.snapshot_sequences.pop(station_id, None)

    async def handle(self, station_id: str, message: Any) -> None:
        kind = message.WhichOneof("message")
        if kind == "ready":
            await self._ready(station_id, message.ready)
        elif kind == "sync":
            await self._send_state(station_id)
        elif kind == "offer":
            await self._offer(station_id, message.offer)
        elif kind == "accept":
            await self._accept(station_id, message.accept)
        elif kind == "withdraw":
            await self._withdraw(station_id, message.withdraw)
        else:
            await self._protocol_error(station_id, "CONTROL_CODE_BAD_MESSAGE")

    async def _ready(self, station_id: str, command: Any) -> None:
        if command.run_id != self.run_id:
            await self._protocol_error(station_id, "CONTROL_CODE_BAD_MESSAGE")
            return
        async with self._lock:
            self.stations[station_id].ready = bool(command.ready)
            starts_now = self.phase == "READY" and all(item.ready for item in self.stations.values())
            if starts_now:
                self.phase = "RUNNING"
                self.world_version += 1
                self._ready_to_run.set()
        response = self.pb.ServerMessage()
        response.readiness.type = self.pb.READINESS_TYPE_READINESS
        response.readiness.protocol_version = PROTOCOL_VERSION
        response.readiness.run_id = self.run_id
        response.readiness.ready = self.stations[station_id].ready
        response.readiness.snapshot_sequence = command.snapshot_sequence
        await self._send(station_id, response)
        if starts_now:
            LOG.info("All demo stations ready; starting run %s", self.run_id)
            await self.broadcast_state()

    async def _offer(self, station_id: str, command: Any) -> None:
        async with self._lock:
            station = self.stations[station_id]
            valid = self._can_trade(station_id, command) and command.body.recipient_id in self.stations
            give = self._bundle_from_proto(command.body.give)
            receive = self._bundle_from_proto(command.body.receive)
            valid = valid and any(give.values()) and all(station.inventory[name] >= amount for name, amount in give.items())
            if valid:
                self._offer_sequence += 1
                offer = GameOffer(
                    offer_id=f"demo-offer-{self._offer_sequence}", proposer_id=station_id,
                    recipient_id=command.body.recipient_id, give=give, receive=receive,
                    expires_tick=command.body.expires_tick, status=self.pb.OFFER_STATUS_OPEN,
                )
                self.offers.append(offer)
                self.world_version += 1
                offer.created_tick = self.tick
                offer.created_version = self.world_version
        if not valid:
            await self._result(station_id, command.request_id, False, "RESULT_CODE_INVALID_ARGUMENT")
            return
        await self._result(station_id, command.request_id, True, "RESULT_CODE_OK", object_id=offer.offer_id)
        await self.broadcast_state((offer.proposer_id, offer.recipient_id))

    async def _accept(self, station_id: str, command: Any) -> None:
        async with self._lock:
            offer = next((item for item in self.offers if item.offer_id == command.body.offer_id), None)
            valid = self._can_trade(station_id, command) and offer is not None
            valid = valid and offer.status == self.pb.OFFER_STATUS_OPEN and offer.recipient_id == station_id
            # Bazaar deadlines are exclusive: an offer expiring at this tick
            # cannot be accepted.
            valid = valid and offer.expires_tick > self.tick
            if valid:
                donor, recipient = self.stations[offer.proposer_id], self.stations[station_id]
                valid = all(donor.inventory[name] >= amount for name, amount in offer.give.items())
                valid = valid and all(recipient.inventory[name] >= amount for name, amount in offer.receive.items())
            if valid:
                for name in ("water", "food", "components"):
                    donor.inventory[name] -= offer.give[name]
                    recipient.inventory[name] += offer.give[name]
                    recipient.inventory[name] -= offer.receive[name]
                    donor.inventory[name] += offer.receive[name]
                    donor.exported_total[name] += offer.give[name]
                    donor.imported_total[name] += offer.receive[name]
                    recipient.imported_total[name] += offer.give[name]
                    recipient.exported_total[name] += offer.receive[name]
                self._transaction_sequence += 1
                offer.transaction_id = f"demo-transaction-{self._transaction_sequence}"
                offer.status = self.pb.OFFER_STATUS_ACCEPTED
                self.world_version += 1
                offer.closed_tick = self.tick
                offer.settled_tick = self.tick
                offer.settled_version = self.world_version
        if not valid:
            await self._result(station_id, command.request_id, False, "RESULT_CODE_NOT_OPEN")
            return
        await self._result(station_id, command.request_id, True, "RESULT_CODE_OK", object_id=offer.offer_id, transaction_id=offer.transaction_id)
        await self.broadcast_state((offer.proposer_id, offer.recipient_id))

    async def _withdraw(self, station_id: str, command: Any) -> None:
        # The demo has no advertisements; offer withdrawal is useful for bridge smoke tests.
        async with self._lock:
            offer = next((item for item in self.offers if item.offer_id == command.body.object_id), None)
            valid = self._can_trade(station_id, command) and offer is not None and offer.proposer_id == station_id
            valid = valid and offer.status == self.pb.OFFER_STATUS_OPEN
            if valid:
                offer.status = self.pb.OFFER_STATUS_WITHDRAWN
                self.world_version += 1
                offer.closed_tick = self.tick
        if not valid:
            await self._result(station_id, command.request_id, False, "RESULT_CODE_NOT_FOUND")
            return
        await self._result(station_id, command.request_id, True, "RESULT_CODE_OK", object_id=offer.offer_id)
        await self.broadcast_state((offer.proposer_id, offer.recipient_id))

    def _can_trade(self, station_id: str, command: Any) -> bool:
        station = self.stations[station_id]
        return (
            self.phase == "RUNNING" and command.protocol_version == PROTOCOL_VERSION
            and command.run_id == self.run_id and station.ready and not station.failed_once
        )

    @staticmethod
    def _bundle_from_proto(value: Any) -> dict[str, int]:
        return {name: int(getattr(value, name)) for name in ("water", "food", "components")}

    async def broadcast_state(self, recipients: tuple[str, ...] | None = None) -> None:
        for station_id, socket in tuple(self.connections.items()):
            if recipients is None or station_id in recipients:
                await self._send_state(station_id, socket)

    async def run_ticks(self) -> None:
        """Wait for all bridges, then advance the shared simulation clock."""
        await self._ready_to_run.wait()
        while self.phase == "RUNNING":
            await asyncio.sleep(self.tick_duration_ms / 1_000)
            if await self.advance_tick():
                await self.broadcast_state()

    async def advance_tick(self) -> bool:
        """Apply one production/upkeep cycle and expire stale offers."""
        async with self._lock:
            if self.phase != "RUNNING":
                return False
            self.tick += 1
            for offer in self.offers:
                if offer.status == self.pb.OFFER_STATUS_OPEN and offer.expires_tick <= self.tick:
                    offer.status = self.pb.OFFER_STATUS_EXPIRED
                    offer.closed_tick = self.tick
            for station in self.stations.values():
                self._advance_station(station)
            if self.tick >= self.duration_ticks:
                self.phase = "FINISHED"
                for offer in self.offers:
                    if offer.status == self.pb.OFFER_STATUS_OPEN:
                        offer.status = self.pb.OFFER_STATUS_RUN_ENDED
                        offer.closed_tick = self.tick
                self.finished.set()
            self.world_version += 1
            LOG.info(
                "Demo tick %d/%d | %s",
                self.tick,
                self.duration_ticks,
                "; ".join(self._station_status(station) for station in self.stations.values()),
            )
            return True

    @staticmethod
    def _station_status(station: GameStation) -> str:
        inventory = " ".join(f"{resource}={station.inventory[resource]}" for resource in RESOURCES)
        state = "FAILED" if station.failed_once else "active"
        return f"{station.station_id} health={station.health} {state} resources[{inventory}]"

    def _advance_station(self, station: GameStation) -> None:
        station.last_production = dict.fromkeys(RESOURCES, 0)
        station.last_unmet_upkeep = dict.fromkeys(RESOURCES, 0)
        if station.failed_once:
            return
        prod = next((s.production_per_tick for s in self.scenario.stations if s.station_id == station.station_id), 4)
        station.last_production[station.specialty] = prod
        station.inventory[station.specialty] += prod
        station.produced_total[station.specialty] += prod
        shortage = 0
        for resource, required in self.scenario.upkeep.items():
            paid = min(station.inventory[resource], required)
            station.inventory[resource] -= paid
            station.consumed_total[resource] += paid
            unmet = required - paid
            station.last_unmet_upkeep[resource] = unmet
            station.unmet_total[resource] += unmet
            shortage += unmet
        if shortage:
            station.shortage_ticks += 1
            station.current_shortage_streak += 1
            station.longest_shortage_streak = max(station.longest_shortage_streak, station.current_shortage_streak)
            station.health = max(0, station.health - shortage * self.scenario.shortage_damage_per_unit)
            if station.health == 0:
                station.failed_once = True
                station.first_failure_tick = self.tick
        else:
            station.fully_supplied_ticks += 1
            station.current_shortage_streak = 0
            station.health = min(self.scenario.max_health, station.health + 1)

    async def _send_state(self, station_id: str, socket: ServerConnection | None = None) -> None:
        message = self._state(station_id)
        await self._send(station_id, message, socket)

    def _state(self, station_id: str) -> Any:
        message = self.pb.ServerMessage()
        state = message.state
        state.type = self.pb.STATE_TYPE_STATE
        state.protocol_version = PROTOCOL_VERSION
        state.run_id = self.run_id
        state.snapshot_sequence = 1  # Per-connection sequence is assigned in _send.
        state.world_version = self.world_version
        state.tick = self.tick
        state.phase = getattr(self.pb, f"PHASE_{self.phase}")
        state.self_station_id = station_id
        self._copy_rules(state.rules)
        for item in self.stations.values():
            entry = state.directory.items.add()
            entry.station_id = item.station_id
            entry.display_name = f"Demo {item.station_id}"
        self._copy_station(self.stations[station_id], state.self)
        # These are required empty wrapper messages in the Bazaar proto2 schema.
        state.offers.SetInParent()
        state.advertisements.SetInParent()
        state.transactions.SetInParent()
        state.request_results.SetInParent()
        for item in self.offers:
            # Like Bazaar, each station receives its own offers and trades.
            # Sending the entire fleet history to every bridge grows quadratically.
            if station_id not in (item.proposer_id, item.recipient_id):
                continue
            target = state.offers.items.add()
            target.offer_id = item.offer_id
            target.proposer_id = item.proposer_id
            target.recipient_id = item.recipient_id
            self._copy_bundle(item.give, target.give)
            self._copy_bundle(item.receive, target.receive)
            target.created_tick = item.created_tick
            target.created_version = item.created_version
            target.expires_tick = item.expires_tick
            target.status = item.status
            if item.closed_tick is None:
                target.closed_tick.null = True
            else:
                target.closed_tick.value = item.closed_tick
            if item.transaction_id:
                target.transaction_id.value = item.transaction_id
            else:
                target.transaction_id.null = True
            if item.transaction_id:
                transaction = state.transactions.items.add()
                transaction.transaction_id = item.transaction_id
                transaction.offer_id = item.offer_id
                transaction.proposer_id = item.proposer_id
                transaction.recipient_id = item.recipient_id
                self._copy_bundle(item.give, transaction.give)
                self._copy_bundle(item.receive, transaction.receive)
                transaction.settled_tick = item.settled_tick or self.tick
                transaction.settled_version = item.settled_version or self.world_version
        if self.phase == "FINISHED":
            state.outcome.value.collective_success.value = not any(item.failed_once for item in self.stations.values())
            state.outcome.value.self_failed = self.stations[station_id].failed_once
            state.outcome.value.aborted = False
        else:
            state.outcome.null = True
        return message

    def _copy_rules(self, rules: Any) -> None:
        rules.rules_version = "demo-1"
        rules.duration_ticks = self.duration_ticks
        rules.tick_duration_ms = self.tick_duration_ms
        rules.resource_order.items.extend([self.pb.RESOURCE_WATER, self.pb.RESOURCE_FOOD, self.pb.RESOURCE_COMPONENTS])
        rules.max_health = self.scenario.max_health
        rules.shortage_damage_per_unit = self.scenario.shortage_damage_per_unit
        rules.recovery_per_fully_supplied_tick = 1
        rules.max_publication_ttl_ticks = 5
        rules.max_offer_ttl_ticks = 3
        rules.new_commands_per_station_per_tick = 1
        rules.max_request_records_per_station = 32
        rules.max_open_outgoing_offers = 1
        rules.max_command_bytes = 65_536

    def _copy_station(self, source: GameStation, target: Any) -> None:
        target.station_id = source.station_id
        self._copy_bundle(source.inventory, target.inventory)
        target.health = source.health
        target.failed_once = source.failed_once
        if source.first_failure_tick is None:
            target.first_failure_tick.null = True
        else:
            target.first_failure_tick.value = source.first_failure_tick
        self._copy_bundle(source.last_production, target.last_production)
        self._copy_bundle(source.last_unmet_upkeep, target.last_unmet_upkeep)
        target.fully_supplied_ticks = source.fully_supplied_ticks
        target.shortage_ticks = source.shortage_ticks
        target.current_shortage_streak = source.current_shortage_streak
        target.longest_shortage_streak = source.longest_shortage_streak
        for name in ("produced_total", "consumed_total", "unmet_total", "imported_total", "exported_total"):
            self._copy_bundle(getattr(source, name), getattr(target, name))
        self._copy_bundle(self.scenario.upkeep, target.upkeep_per_tick)
        target.specialty = getattr(self.pb, f"RESOURCE_{source.specialty.upper()}")

    @staticmethod
    def _copy_bundle(source: dict[str, int], target: Any) -> None:
        for name, amount in source.items():
            setattr(target, name, amount)

    async def _result(self, station_id: str, request_id: str, ok: bool, code: str, *, object_id: str | None = None, transaction_id: str | None = None) -> None:
        message = self.pb.ServerMessage()
        result = message.result
        result.type = self.pb.RESULT_TYPE_RESULT
        result.protocol_version = PROTOCOL_VERSION
        result.run_id = self.run_id
        result.request_id = request_id
        result.ok = ok
        result.code = getattr(self.pb, code)
        result.processed_tick = self.tick
        result.processed_version = self.world_version
        if object_id:
            result.object_id.value = object_id
        else:
            result.object_id.null = True
        if transaction_id:
            result.transaction_id.value = transaction_id
        else:
            result.transaction_id.null = True
        result.retry_after_tick.null = True
        await self._send(station_id, message)

    async def _protocol_error(self, station_id: str, code: str) -> None:
        message = self.pb.ServerMessage()
        error = message.protocol_error
        error.type = self.pb.PROTOCOL_ERROR_TYPE_PROTOCOL_ERROR
        error.protocol_version = PROTOCOL_VERSION
        error.run_id.value = self.run_id
        error.request_id.null = True
        error.code = getattr(self.pb, code)
        error.close_session = False
        await self._send(station_id, message)

    async def _send(self, station_id: str, message: Any, socket: ServerConnection | None = None) -> None:
        socket = socket or self.connections.get(station_id)
        if socket is None:
            return
        if message.WhichOneof("message") == "state":
            # Snapshot numbers are scoped to one connection, exactly as in the
            # supplied practice server.
            sequence = self.snapshot_sequences.get(station_id, 0) + 1
            self.snapshot_sequences[station_id] = sequence
            message.state.snapshot_sequence = sequence
        await socket.send(message.SerializeToString())

