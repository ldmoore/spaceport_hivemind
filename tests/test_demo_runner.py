from __future__ import annotations

from spaceport_hivemind.demo import run_demo


async def test_demo_runner_exercises_every_bridge_and_returns_a_final_state() -> None:
    result = await run_demo(duration_ticks=1, tick_duration_ms=100, timeout_seconds=3)

    assert result.ticks == 1
    assert set(result.stations) == {"P01", "P02", "P03"}
    assert all(not station.failed for station in result.stations.values())
    # P01 begins with spare water, so the actual coordinator/bridge/server
    # path should create at least one trade before the first tick completes.
    assert result.offers
