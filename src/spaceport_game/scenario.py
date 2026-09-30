from dataclasses import dataclass
import json
from pathlib import Path

RESOURCES = ("water", "food", "components")

@dataclass(frozen=True)
class StationSpec:
    station_id: str
    specialty: str
    inventory: dict[str, int]
    production_per_tick: int = 4

@dataclass(frozen=True)
class Scenario:
    stations: tuple[StationSpec, ...]
    duration_ticks: int = 30
    tick_duration_ms: int = 1000
    max_health: int = 100
    shortage_damage_per_unit: int = 4
    recovery_per_fully_supplied_tick: int = 1
    upkeep: dict[str, int] | None = None

    def __post_init__(self):
        upkeep = self.upkeep or {"water": 2, "food": 1, "components": 1}
        object.__setattr__(self, "upkeep", {r: int(upkeep.get(r, 0)) for r in RESOURCES})
        ids = [s.station_id for s in self.stations]
        if not 2 <= len(ids) <= 20 or len(set(ids)) != len(ids):
            raise ValueError("scenario must contain 2–20 distinct stations")
        if self.duration_ticks < 1 or self.tick_duration_ms < 1:
            raise ValueError("duration and tick duration must be positive")
        for station in self.stations:
            if station.specialty not in RESOURCES or station.production_per_tick < 1:
                raise ValueError("invalid station production")
            if set(station.inventory) - set(RESOURCES) or any(v < 0 for v in station.inventory.values()):
                raise ValueError("invalid station inventory")

    @classmethod
    def load(cls, path: Path) -> "Scenario":
        value = json.loads(path.read_text())
        stations = tuple(StationSpec(
            station_id=str(item["station_id"]), specialty=str(item["specialty"]),
            inventory={r: int(item.get("inventory", {}).get(r, 0)) for r in RESOURCES},
            production_per_tick=int(item.get("production_per_tick", 4)),
        ) for item in value["stations"])
        return cls(stations, int(value.get("duration_ticks", 30)), int(value.get("tick_duration_ms", 1000)),
                   int(value.get("max_health", 100)), int(value.get("shortage_damage_per_unit", 4)),
                   int(value.get("recovery_per_fully_supplied_tick", 1)), value.get("upkeep"))
