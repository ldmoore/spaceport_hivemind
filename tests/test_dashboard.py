import asyncio
import json
from urllib.request import urlopen

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosedError

from spaceport_hivemind.coordinator import Coordinator, CoordinatorConfig
from spaceport_hivemind.demo_server import DemoBazaar
from spaceport_hivemind.client import HivemindClient
from spaceport_hivemind.model import Planet
from spaceport_hivemind.server import HivemindServer


def get(url):
    with urlopen(url, timeout=5) as response:
        return response.headers, response.read().decode()


async def test_http_and_authenticated_bridges_share_port():
    hive = HivemindServer(Coordinator(CoordinatorConfig(expected_station_count=3)), "test-secret")
    async with serve(hive.handler, "127.0.0.1", 0, process_request=hive.dashboard.http) as server:
        port = server.sockets[0].getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        headers, page = await asyncio.to_thread(get, base + "/")
        assert "Mission Control" in page
        assert headers["Content-Type"].startswith("text/html")
        assert "frame-ancestors 'none'" in headers["Content-Security-Policy"]
        for asset in ["dashboard.js", "dashboard.css", "healthz"]:
            assert (await asyncio.to_thread(get, base + "/" + asset))[1]
        async with connect(f"ws://127.0.0.1:{port}") as socket:
            await socket.send(json.dumps(dict(type="hello", station_id="P01", shared_key="test-secret")))
            assert json.loads(await socket.recv())["type"] == "welcome"
            world = DemoBazaar(("P01", "P02", "P03"))
            state = HivemindClient._to_dict(world._state("P01").state)
            state["station_id"] = "P01"
            await socket.send(json.dumps(dict(type="observation", station_id="P01", state=state)))
            async with asyncio.timeout(3):
                while "P01" not in hive.coordinator.planets:
                    await asyncio.sleep(.01)
            _, body = await asyncio.to_thread(get, base + "/api/state")
            value = json.loads(body)
            assert value["connected_stations"] == 1
            assert value["stations"][0]["online"] is True
            assert value["stations"][0]["inventory"]["water"] == 12
            assert "test-secret" not in body
            assert "shared_key" not in body
            assert all(s.token not in body for s in world.stations.values())
        async with asyncio.timeout(3):
            while hive.clients:
                await asyncio.sleep(.01)
        assert hive.dashboard.snapshot()["stations"][0]["online"] is False
        async with connect(f"ws://127.0.0.1:{port}/ws") as socket:
            await socket.send(json.dumps(dict(type="hello", station_id="bad", shared_key="wrong")))
            with pytest.raises(ConnectionClosedError):
                await socket.recv()
        assert "bad" not in hive.clients


def test_dashboard_ignores_stale_observations_and_bounds_events():
    hive = HivemindServer(Coordinator(CoordinatorConfig(expected_station_count=3)))
    world = DemoBazaar(("P01", "P02", "P03"))
    state = HivemindClient._to_dict(world._state("P01").state)
    state["station_id"] = "P01"
    planet = Planet.from_observation(state)
    hive.coordinator.ingest(planet)
    hive.dashboard.observed(planet)
    last_seen = hive.dashboard.seen["P01"]
    stale = Planet.from_observation(state)
    hive.coordinator.ingest(stale)
    hive.dashboard.observed(stale)
    assert hive.dashboard.seen["P01"] == last_seen
    for i in range(150):
        hive.dashboard.event("test", "P01", str(i))
    assert len(hive.dashboard.snapshot()["events"]) == 100
    assert hive.dashboard.snapshot()["events"][0]["detail"] == "149"


async def test_demo_runs_once_and_leaves_final_telemetry():
    hive = HivemindServer(Coordinator(CoordinatorConfig(expected_station_count=9)), "private-key")
    hive.clients["P01"] = object()
    assert not hive.dashboard.start_demo()["ok"]
    hive.clients.clear()
    assert hive.dashboard.start_demo(tick_duration_ms=100)["ok"]
    assert not hive.dashboard.start_demo()["ok"]
    assert hive.dashboard.snapshot()["demo"]["running"]
    await asyncio.wait_for(hive.dashboard.demo_task, 10)
    state = hive.dashboard.snapshot()
    assert state["demo"]["status"] == "finished"
    assert state["demo"]["success"] is True
    assert state["demo"]["can_start"] is True
    assert len(state["stations"]) == 3
    assert all(s["phase"] == "FINISHED" and s["tick"] == 30 for s in state["stations"])
    assert not hive.dashboard.demo_server.clients
    assert hive.coordinator.config.expected_station_count == 9
    assert "private-key" not in json.dumps(state)


async def test_demo_control_requires_same_origin_and_rejects_live_clients():
    from websockets.exceptions import InvalidStatus
    hive = HivemindServer(Coordinator(), "private-key")
    async with serve(hive.handler, "127.0.0.1", 0, process_request=hive.dashboard.http) as server:
        port = server.sockets[0].getsockname()[1]
        endpoint = f"ws://127.0.0.1:{port}"
        with pytest.raises(InvalidStatus):
            async with connect(endpoint + "/demo/control", origin="https://unrelated.example"):
                pass
        hive.clients["P01"] = object()
        async with connect(endpoint + "/demo/control", origin=f"http://127.0.0.1:{port}") as socket:
            await socket.send('{"action":"start"}')
            response = json.loads(await socket.recv())
            assert not response["ok"]
            assert "Disconnect" in response["message"]
        hive.clients.clear()
        hive.dashboard.start_demo(tick_duration_ms=100)
        async with connect(endpoint) as socket:
            await socket.send(json.dumps(dict(type="hello", station_id="P01", shared_key="private-key")))
            with pytest.raises(ConnectionClosedError) as error:
                await socket.recv()
            assert error.value.rcvd.code == 4004
        await hive.dashboard.demo_task


def test_demo_station_count_validation():
    hive = HivemindServer(Coordinator())
    for count in [1, 21, -1, 2.5, True, '18', None]:
        assert not hive.dashboard.start_demo(station_count=count)['ok']
        assert hive.dashboard.demo_task is None
