import pytest

from spaceport_hivemind.game import BazaarConnection, GameConfig


class Socket:
    def __init__(self) -> None:
        self.frames: list[bytes] = []

    async def send(self, frame: bytes) -> None:
        self.frames.append(frame)


@pytest.mark.asyncio
async def test_bridge_serializes_the_hivemind_offer_without_local_policy() -> None:
    connection = BazaarConnection(GameConfig("ws://game", "P01", "token"))
    socket = Socket()
    connection.socket = socket

    await connection.send_action({
        "kind": "offer", "recipient_id": "P02",
        "give": {"water": 3, "food": 0, "components": 0},
        "receive": {"water": 0, "food": 0, "components": 0}, "expires_tick": 9,
    }, "run-1", "command-1")

    message = connection.pb.ClientMessage()
    message.ParseFromString(socket.frames[0])
    assert message.WhichOneof("message") == "offer"
    assert message.offer.run_id == "run-1"
    assert message.offer.body.recipient_id == "P02"
    assert message.offer.body.give.water == 3
    assert message.offer.body.receive.food == 0
