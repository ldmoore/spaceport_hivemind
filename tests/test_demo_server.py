import logging

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from spaceport_hivemind.demo_server import DemoBazaar, DemoBazaarServer
from spaceport_hivemind.game import SUBPROTOCOL
from spaceport_hivemind.model import Planet


class Socket:
    def __init__(self) -> None:
        self.frames: list[bytes] = []

    async def send(self, frame: bytes) -> None:
        self.frames.append(frame)


@pytest.mark.asyncio
async def test_demo_server_exposes_a_two_station_offer_and_settles_it() -> None:
    world = DemoBazaar(("P01", "P02"))
    world.phase = "RUNNING"
    p01, p02 = Socket(), Socket()
    world.connections = {"P01": p01, "P02": p02}

    offer = world.pb.ClientMessage()
    offer.offer.type = world.pb.OFFER_COMMAND_TYPE_OFFER
    offer.offer.protocol_version = "2.0"
    offer.offer.run_id = world.run_id
    offer.offer.request_id = "offer-water"
    offer.offer.body.recipient_id = "P02"
    offer.offer.body.give.water = 6
    offer.offer.body.expires_tick = 1
    world.stations["P01"].ready = True
    await world.handle("P01", offer)

    created = world.pb.ServerMessage()
    created.ParseFromString(p01.frames[0])
    offer_id = created.result.object_id.value
    assert created.result.ok
    assert offer_id == "demo-offer-1"

    accept = world.pb.ClientMessage()
    accept.accept.type = world.pb.ACCEPT_TYPE_ACCEPT
    accept.accept.protocol_version = "2.0"
    accept.accept.run_id = world.run_id
    accept.accept.request_id = "accept-water"
    accept.accept.body.offer_id = offer_id
    world.stations["P02"].ready = True
    await world.handle("P02", accept)

    settled = world.pb.ServerMessage()
    settled.ParseFromString(p02.frames[1])
    assert settled.result.ok
    assert world.stations["P01"].inventory["water"] == 6
    assert world.stations["P02"].inventory["water"] == 6


@pytest.mark.asyncio
async def test_demo_server_uses_the_bazaar_websocket_contract() -> None:
    world = DemoBazaar(("P01", "P02"))
    async with serve(DemoBazaarServer(world).handler, "127.0.0.1", 0, subprotocols=[SUBPROTOCOL]) as server:
        port = server.sockets[0].getsockname()[1]
        token = world.stations["P01"].token
        async with connect(
            f"ws://127.0.0.1:{port}/ws",
            additional_headers={"Authorization": f"Bearer {token}"},
            subprotocols=[SUBPROTOCOL],
        ) as socket:
            assert socket.subprotocol == SUBPROTOCOL
            first = world.pb.ServerMessage()
            first.ParseFromString(await socket.recv())
            assert first.WhichOneof("message") == "state"
            assert first.state.self_station_id == "P01"
            assert first.state.snapshot_sequence == 1

            ready = world.pb.ClientMessage()
            ready.ready.type = world.pb.READY_TYPE_READY
            ready.ready.protocol_version = "2.0"
            ready.ready.run_id = world.run_id
            ready.ready.ready = True
            ready.ready.snapshot_sequence = 1
            await socket.send(ready.SerializeToString())
            response = world.pb.ServerMessage()
            response.ParseFromString(await socket.recv())
            assert response.WhichOneof("message") == "readiness"
            assert response.readiness.ready


def test_coordinator_model_accepts_protobuf_json_list_wrappers() -> None:
    world = DemoBazaar(("P01", "P02"))
    state = world._state("P01").state
    # This mirrors the representation forwarded by HivemindClient.
    from google.protobuf.json_format import MessageToDict

    payload = MessageToDict(state, preserving_proto_field_name=True)
    payload["station_id"] = "P01"
    planet = Planet.from_observation(payload)

    assert planet.station_id == "P01"
    assert planet.offers == ()


@pytest.mark.asyncio
async def test_tick_produces_resources_charges_upkeep_and_can_finish_a_run() -> None:
    world = DemoBazaar(("P01", "P02"), duration_ticks=1)
    world.phase = "RUNNING"

    assert await world.advance_tick()

    p01, p02 = world.stations["P01"], world.stations["P02"]
    assert world.tick == 1
    assert world.phase == "FINISHED"
    assert p01.inventory == {"water": 14, "food": 3, "components": 3}
    assert p01.last_production == {"water": 4, "food": 0, "components": 0}
    assert p02.inventory["water"] == 0
    assert p02.last_unmet_upkeep["water"] == 2
    assert p02.health == 92


@pytest.mark.asyncio
async def test_tick_logs_health_and_resources_for_each_station(caplog: pytest.LogCaptureFixture) -> None:
    world = DemoBazaar(("P01", "P02"), duration_ticks=1)
    world.phase = "RUNNING"
    caplog.set_level(logging.INFO, logger="spaceport_hivemind.demo_server")

    assert await world.advance_tick()

    assert "Demo tick 1/1" in caplog.text
    assert "P01 health=100 active resources[water=14 food=3 components=3]" in caplog.text
    assert "P02 health=92 active resources[water=0 food=11 components=3]" in caplog.text
