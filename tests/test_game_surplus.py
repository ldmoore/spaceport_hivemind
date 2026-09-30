import asyncio
from unittest.mock import AsyncMock
import pytest
from spaceport_game.server import DemoBazaar, RESOURCES, UPKEEP

@pytest.mark.parametrize('count', [2, 9, 18])
@pytest.mark.parametrize('surplus', [-50, -10, 0, 25, 100])
def test_resource_budget_and_production_schedule(count, surplus):
    world = DemoBazaar(tuple(f'P{i:02}' for i in range(count)), duration_ticks=120)
    world.participants = set(world.stations)
    world.surplus_percent = surplus
    world.randomize_resources()
    for resource in RESOURCES:
        required = count * UPKEEP[resource] * 120
        available = round(required * (100 + surplus) / 100)
        initial = sum(p.inventory[resource] for p in world.stations.values())
        production = sum(p.production_budget[resource] for p in world.stations.values())
        assert initial + production == available
        assert world.resource_budget[resource]['balance'] == available - required
        for planet in world.stations.values():
            budget = planet.production_budget[resource]
            assert sum(budget*t//120-budget*(t-1)//120 for t in range(1,121)) == budget
            assert planet.inventory[resource] >= UPKEEP[resource]

def test_start_uses_only_joined_planets_and_locks_budget():
    async def check():
        world = DemoBazaar(tuple(f'P{i:02}' for i in range(18)), duration_ticks=120)
        world.broadcast_state = AsyncMock()
        for station in list(world.stations)[:9]:
            world.connections[station] = object()
            world.stations[station].ready = True
        await world.start(25)
        assert len(world.participants) == 9
        assert world.resource_budget['water']['required'] == 9*2*120
        with pytest.raises(ValueError):
            await world.start(-50)
        assert world.surplus_percent == 25
        assert all(p.production_budget is None for s,p in world.stations.items() if s not in world.participants)
    asyncio.run(check())
