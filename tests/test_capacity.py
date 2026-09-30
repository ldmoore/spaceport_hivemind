import asyncio
import json
from contextlib import AsyncExitStack

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosedError

from spaceport_hivemind.coordinator import Coordinator, CoordinatorConfig
from spaceport_hivemind.server import HivemindServer
from spaceport_hivemind.demo import run_demo


@pytest.mark.parametrize('count', [18, 20])
async def test_full_bridge_stack_with_large_roster(count):
    hive = HivemindServer(Coordinator(CoordinatorConfig(expected_station_count=count)), 'test-key')
    result = await run_demo(tuple(f'P{i:02}' for i in range(1, count+1)), duration_ticks=3,
                            tick_duration_ms=200, timeout_seconds=10, hive=hive)
    assert len(result.stations) == count
    assert result.ticks == 3
    assert result.offers
    assert len(hive.coordinator.planets) == count
    assert not hive.clients


async def test_capacity_rejects_21st_but_allows_replacement_and_freed_slot():
    hive = HivemindServer(Coordinator(CoordinatorConfig(expected_station_count=0)), 'test-key')
    async with serve(hive.handler, '127.0.0.1', 0) as server, AsyncExitStack() as stack:
        endpoint = f'ws://127.0.0.1:{server.sockets[0].getsockname()[1]}'
        async def join(station):
            socket = await stack.enter_async_context(connect(endpoint))
            await socket.send(json.dumps(dict(type='hello',station_id=station,shared_key='test-key')))
            return socket
        clients=[]
        for i in range(20):
            socket=await join(f'P{i+1:02}')
            assert json.loads(await socket.recv())['type']=='welcome'
            clients.append(socket)
        extra = await join('P21')
        with pytest.raises(ConnectionClosedError) as error:
            await extra.recv()
        assert error.value.rcvd.code == 4005
        replacement = await join('P01')
        assert json.loads(await replacement.recv())['type']=='welcome'
        assert len(hive.clients)==20
        await clients[-1].close()
        async with asyncio.timeout(3):
            while len(hive.clients)==20: await asyncio.sleep(.01)
        newcomer=await join('P21')
        assert json.loads(await newcomer.recv())['type']=='welcome'
        assert len(hive.clients)==20


async def test_twenty_station_demo_finishes_full_run():
    result = await run_demo(tuple(f'P{i:02}' for i in range(1,21)), duration_ticks=30,
                            tick_duration_ms=100, timeout_seconds=20)
    assert result.ticks == 30
    assert len(result.stations) == 20
    assert any(o['status']=='ACCEPTED' for o in result.offers)
