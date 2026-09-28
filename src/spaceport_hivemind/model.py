"""Protocol-neutral facts and commands exchanged by the hive."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

RESOURCES = ("water", "food", "components")


@dataclass(frozen=True)
class Bundle:
    water: int = 0
    food: int = 0
    components: int = 0

    def __post_init__(self) -> None:
        if min(self.water, self.food, self.components) < 0:
            raise ValueError("Bundles cannot contain negative resources")

    def get(self, resource: str) -> int:
        return getattr(self, resource)

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "Bundle":
        return cls(**{resource: int(value.get(resource, 0)) for resource in RESOURCES})

    def as_dict(self) -> dict[str, int]:
        return {resource: self.get(resource) for resource in RESOURCES}


@dataclass(frozen=True)
class Rules:
    max_health: int
    shortage_damage_per_unit: int
    recovery_per_fully_supplied_tick: int
    max_offer_ttl_ticks: int
    new_commands_per_station_per_tick: int

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "Rules":
        return cls(
            max_health=int(value["max_health"]),
            shortage_damage_per_unit=int(value["shortage_damage_per_unit"]),
            recovery_per_fully_supplied_tick=int(value["recovery_per_fully_supplied_tick"]),
            max_offer_ttl_ticks=int(value["max_offer_ttl_ticks"]),
            new_commands_per_station_per_tick=int(value["new_commands_per_station_per_tick"]),
        )


@dataclass(frozen=True)
class Offer:
    offer_id: str
    proposer_id: str
    recipient_id: str
    give: Bundle
    receive: Bundle
    expires_tick: int
    status: str

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "Offer":
        return cls(
            offer_id=str(value["offer_id"]), proposer_id=str(value["proposer_id"]),
            recipient_id=str(value["recipient_id"]), give=Bundle.from_mapping(value["give"]),
            receive=Bundle.from_mapping(value["receive"]), expires_tick=int(value["expires_tick"]),
            status=str(value["status"]).removeprefix("OFFER_STATUS_"),
        )


@dataclass(frozen=True)
class Planet:
    station_id: str
    run_id: str
    tick: int
    snapshot_sequence: int
    phase: str
    inventory: Bundle
    production: Bundle
    upkeep: Bundle
    unmet_upkeep: Bundle
    health: int
    failed_once: bool
    rules: Rules
    offers: tuple[Offer, ...]

    @property
    def published_upkeep(self) -> Bundle:
        """The upkeep this station has published to the coordinator."""
        return self.upkeep

    @classmethod
    def from_observation(cls, value: dict[str, Any]) -> "Planet":
        observation = value["self"]
        offers = value.get("offers", [])
        # MessageToDict preserves the Bazaar list wrapper as
        # {"items": [...]}; accept a plain list as well for protocol-neutral
        # unit fixtures and alternate bridge implementations.
        if isinstance(offers, dict):
            offers = offers.get("items", [])
        return cls(
            station_id=str(value["station_id"]), run_id=str(value["run_id"]), tick=int(value["tick"]),
            snapshot_sequence=int(value["snapshot_sequence"]), phase=str(value["phase"]).removeprefix("PHASE_"),
            inventory=Bundle.from_mapping(observation["inventory"]),
            production=Bundle.from_mapping(observation["last_production"]),
            upkeep=Bundle.from_mapping(observation["upkeep_per_tick"]),
            unmet_upkeep=Bundle.from_mapping(observation["last_unmet_upkeep"]),
            health=int(observation["health"]), failed_once=bool(observation["failed_once"]),
            rules=Rules.from_mapping(value["rules"]),
            offers=tuple(Offer.from_mapping(item) for item in offers),
        )


@dataclass(frozen=True)
class Command:
    """A game action. It contains no local-policy hint for the bridge to interpret."""

    command_id: str
    station_id: str
    run_id: str
    action: dict[str, Any]

    def as_message(self) -> dict[str, Any]:
        return {
            "type": "command", "command_id": self.command_id, "station_id": self.station_id,
            "run_id": self.run_id, "action": self.action,
        }
