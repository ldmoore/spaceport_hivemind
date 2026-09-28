"""Run the complete deterministic Hivemind demo in one command."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from contextlib import suppress
from dataclasses import asdict, dataclass
from typing import Any

from websockets.asyncio.server import serve

from .client import HiveConfig, HivemindClient
from .coordinator import Coordinator, CoordinatorConfig
from .demo_server import DemoBazaar, DemoBazaarServer, DemoOffer
from .game import GameConfig
from .server import HivemindServer

LOG = logging.getLogger(__name__)
HOST = "127.0.0.1"
SHARED_KEY = "local-hive-key"


@dataclass(frozen=True)
class StationResult:
    health: int
    failed: bool
    inventory: dict[str, int]
    imported: dict[str, int]
    exported: dict[str, int]


@dataclass(frozen=True)
class DemoResult:
    success: bool
    ticks: int
    stations: dict[str, StationResult]
    offers: list[dict[str, Any]]


def _offer_result(offer: DemoOffer, pb: Any) -> dict[str, Any]:
    return {
        "id": offer.offer_id,
        "from": offer.proposer_id,
        "to": offer.recipient_id,
        "give": offer.give,
        "receive": offer.receive,
        "status": pb.OfferStatus.Name(offer.status).removeprefix("OFFER_STATUS_"),
        "settled_tick": offer.settled_tick,
    }


def result_from(world: DemoBazaar) -> DemoResult:
    return DemoResult(
        success=not any(station.failed_once for station in world.stations.values()),
        ticks=world.tick,
        stations={
            station_id: StationResult(
                health=station.health,
                failed=station.failed_once,
                inventory=dict(station.inventory),
                imported=dict(station.imported_total),
                exported=dict(station.exported_total),
            )
            for station_id, station in world.stations.items()
        },
        offers=[_offer_result(offer, world.pb) for offer in world.offers],
    )


async def _wait_until(predicate: Any, timeout_seconds: float) -> None:
    """Yield until an in-process service has observed an expected condition."""
    async with asyncio.timeout(timeout_seconds):
        while not predicate():
            await asyncio.sleep(0)


async def run_demo(
    station_ids: tuple[str, ...] = ("P01", "P02", "P03"),
    *,
    duration_ticks: int = 30,
    tick_duration_ms: int = 20,
    reserve_ticks: int = 4,
    timeout_seconds: float = 10,
) -> DemoResult:
    """Exercise the actual two-WebSocket bridge stack and return its outcome.

    Ephemeral ports and in-memory credentials isolate each run, so a developer
    can rerun the demo while changing the coordinator without stale processes
    or credential files affecting the next result.
    """
    world = DemoBazaar(station_ids, duration_ticks=duration_ticks, tick_duration_ms=tick_duration_ms)
    hive = HivemindServer(
        Coordinator(CoordinatorConfig(expected_station_count=len(station_ids), reserve_ticks=reserve_ticks)),
        SHARED_KEY,
    )
    bridges: list[asyncio.Task[None]] = []
    tick_task: asyncio.Task[None] | None = None
    try:
        async with serve(DemoBazaarServer(world).handler, HOST, 0, subprotocols=["bazaar.protobuf.v2"], max_size=2**20) as game_server:
            game_port = game_server.sockets[0].getsockname()[1]
            async with serve(hive.handler, HOST, 0, max_size=2**20) as hive_server:
                hive_port = hive_server.sockets[0].getsockname()[1]
                game_endpoint = f"ws://{HOST}:{game_port}/ws"
                hive_endpoint = f"ws://{HOST}:{hive_port}"
                tick_task = asyncio.create_task(world.run_ticks(), name="demo-ticks")
                for station_id in station_ids:
                    bridge = asyncio.create_task(
                        HivemindClient(
                            GameConfig(game_endpoint, station_id, world.stations[station_id].token),
                            HiveConfig(hive_endpoint, SHARED_KEY),
                        ).run(),
                        name=f"bridge-{station_id}",
                    )
                    bridges.append(bridge)
                    # Stabilize bridge registration before starting the next
                    # one. This preserves a fixed station order at the two
                    # protocol boundaries while retaining real WebSockets.
                    await _wait_until(
                        lambda station_id=station_id: (
                            station_id in hive.clients
                            and station_id in hive.coordinator.planets
                            and world.stations[station_id].ready
                        ),
                        timeout_seconds,
                    )
                await asyncio.wait_for(world.finished.wait(), timeout=timeout_seconds)
                # The final state has been broadcast. Let bridges forward it
                # before their sockets are closed during cleanup.
                await asyncio.sleep(0)
                return result_from(world)
    finally:
        for task in bridges:
            task.cancel()
        if tick_task is not None:
            tick_task.cancel()
            with suppress(asyncio.CancelledError):
                await tick_task
        if bridges:
            await asyncio.gather(*bridges, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run and summarize the full deterministic Hivemind demo")
    parser.add_argument("--stations", default="P01,P02,P03", help="Comma-separated station IDs")
    parser.add_argument("--duration-ticks", type=int, default=30)
    parser.add_argument("--tick-duration-ms", type=int, default=100)
    parser.add_argument("--reserve-ticks", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=10)
    parser.add_argument("--include-offers", action="store_true", help="Include the full offer history in JSON output")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    station_ids = tuple(item.strip() for item in args.stations.split(",") if item.strip())
    if len(station_ids) < 2 or len(set(station_ids)) != len(station_ids):
        parser.error("--stations must name at least two distinct stations")
    if args.duration_ticks < 1 or args.tick_duration_ms < 1 or args.timeout_seconds <= 0:
        parser.error("duration, tick duration, and timeout must be positive")
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")
    result = asyncio.run(
        run_demo(
            station_ids,
            duration_ticks=args.duration_ticks,
            tick_duration_ms=args.tick_duration_ms,
            reserve_ticks=args.reserve_ticks,
            timeout_seconds=args.timeout_seconds,
        )
    )
    output = asdict(result)
    if not args.include_offers:
        offers = output["offers"]
        output["offers"] = {
            "accepted": sum(offer["status"] == "ACCEPTED" for offer in offers),
            "expired": sum(offer["status"] == "EXPIRED" for offer in offers),
            "open": sum(offer["status"] == "OPEN" for offer in offers),
            "total": len(offers),
        }
    print(json.dumps(output, indent=2, sort_keys=True))
    if not result.success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
