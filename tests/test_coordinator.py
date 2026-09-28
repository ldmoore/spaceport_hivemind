from spaceport_hivemind.coordinator import Coordinator, CoordinatorConfig
from spaceport_hivemind.model import Planet


def _planet(station: str, *, inventory: dict[str, int], production: dict[str, int] | None = None, upkeep: dict[str, int] | None = None, offers: list[dict] | None = None, tick: int = 7, snapshot_sequence: int | None = None) -> Planet:
    zeros = {"water": 0, "food": 0, "components": 0}
    return Planet.from_observation({
        "station_id": station, "run_id": "run-a", "tick": tick, "snapshot_sequence": snapshot_sequence if snapshot_sequence is not None else tick,
        "phase": "PHASE_RUNNING",
        "rules": {"max_health": 100, "shortage_damage_per_unit": 4, "recovery_per_fully_supplied_tick": 1, "max_offer_ttl_ticks": 3, "new_commands_per_station_per_tick": 1},
        "self": {"inventory": inventory, "last_production": production or zeros, "upkeep_per_tick": upkeep or zeros, "last_unmet_upkeep": zeros, "health": 100, "failed_once": False},
        "offers": offers or [],
    })


def test_hive_moves_only_a_donors_safe_surplus_to_an_at_risk_planet() -> None:
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=2))
    donor = _planet("P01", inventory={"water": 12, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    recipient = _planet("P02", inventory={"water": 0, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(donor) == []
    commands = hive.ingest(recipient)

    assert len(commands) == 1
    command = commands[0]
    assert command.station_id == "P01"
    assert command.action == {
        "kind": "offer", "recipient_id": "P02",
        "give": {"water": 4, "food": 0, "components": 0},
        "receive": {"water": 0, "food": 0, "components": 0}, "expires_tick": 10,
    }


def test_hive_uses_current_inventory_not_already_applied_last_production() -> None:
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=2))
    # P01's four water are exactly its two-tick reserve. The four water in
    # ``last_production`` were already added before this snapshot was sent,
    # so treating them as next tick's income would incorrectly donate two.
    donor = _planet(
        "P01", inventory={"water": 4, "food": 4, "components": 4},
        production={"water": 4, "food": 0, "components": 0},
        upkeep={"water": 2, "food": 1, "components": 1},
    )
    recipient = _planet("P02", inventory={"water": 3, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(donor) == []
    # P01 can safely give only one water (while retaining its next upkeep).
    # The already-applied production still must not be counted a second time.
    commands = hive.ingest(recipient)
    assert commands[0].action["give"] == {"water": 1, "food": 0, "components": 0}


def test_hive_can_spend_reserve_down_to_one_upkeep_for_an_urgent_peer() -> None:
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=3))
    donor = _planet("P01", inventory={"water": 4, "food": 2, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})
    recipient = _planet("P02", inventory={"water": 0, "food": 2, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})
    neutral = _planet("P03", inventory={"water": 2, "food": 2, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(donor) == []
    assert hive.ingest(recipient) == []
    commands = hive.ingest(neutral)

    # P01 retains one water upkeep payment. Its remaining reserve can prevent
    # P02's immediate shortage while preserving both next-upkeep guarantees.
    assert [(command.station_id, command.action["give"]) for command in commands] == [
        ("P01", {"water": 2, "food": 0, "components": 0}),
    ]


def test_hive_opens_a_safe_gift_while_awaiting_other_relay_resources() -> None:
    """An unrelated deficit must not prevent a station from forwarding its specialty."""
    hive = Coordinator(CoordinatorConfig(reserve_ticks=4, expected_station_count=2))
    donor = _planet("P01", inventory={"water": 4, "food": 0, "components": 0}, upkeep={"water": 2, "food": 1, "components": 1})
    recipient = _planet("P02", inventory={"water": 0, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(donor) == []
    commands = hive.ingest(recipient)

    assert ("P01", {"water": 2, "food": 0, "components": 0}, {"water": 0, "food": 0, "components": 0}) in [
        (command.station_id, command.action["give"], command.action["receive"])
        for command in commands
    ]


def test_hive_accepts_a_safe_gift_while_the_donor_awaits_a_relay() -> None:
    """Gift acceptance uses the pledged-resource floor, not bilateral safety."""
    hive = Coordinator(CoordinatorConfig(expected_station_count=2))
    offer = {
        "offer_id": "relay-water", "proposer_id": "P01", "recipient_id": "P02",
        "give": {"water": 2, "food": 0, "components": 0},
        "receive": {"water": 0, "food": 0, "components": 0},
        "expires_tick": 10, "status": "OFFER_STATUS_OPEN",
    }
    donor = _planet("P01", inventory={"water": 4, "food": 0, "components": 0}, upkeep={"water": 2, "food": 1, "components": 1}, offers=[offer])
    recipient = _planet("P02", inventory={"water": 0, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1}, offers=[offer])

    assert hive.ingest(donor) == []
    assert [(command.station_id, command.action) for command in hive.ingest(recipient)] == [
        ("P02", {"kind": "accept", "offer_id": "relay-water"}),
    ]


def test_hive_replenishes_a_peer_with_only_one_upkeep_payment_left() -> None:
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=2))
    donor = _planet("P01", inventory={"water": 4, "food": 2, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})
    recipient = _planet("P02", inventory={"water": 2, "food": 2, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(donor) == []
    commands = hive.ingest(recipient)

    assert [(command.station_id, command.action["give"]) for command in commands] == [
        ("P01", {"water": 2, "food": 0, "components": 0}),
    ]


def test_hive_prioritizes_a_next_tick_shortfall_over_a_reserve_deficit() -> None:
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=2))
    donor = _planet("P01", inventory={"water": 8, "food": 3, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})
    recipient = _planet("P02", inventory={"water": 2, "food": 0, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(donor) == []
    commands = hive.ingest(recipient)

    # The one-tick safe-aid floor lets P01 use two food, restoring P02's
    # two-tick target while P01 retains its own next upkeep payment.
    assert commands[0].action["give"] == {"water": 2, "food": 2, "components": 0}


def test_hive_does_not_empty_a_deficit_donors_delivery_buffer() -> None:
    """P03 must not forward the water it needs while P02 builds a reserve."""
    hive = Coordinator(CoordinatorConfig(reserve_ticks=4, expected_station_count=3))
    p01 = _planet("P01", inventory={"water": 2, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    p02 = _planet("P02", inventory={"water": 2, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    p03 = _planet("P03", inventory={"water": 6, "food": 4, "components": 8}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(p01) == []
    assert hive.ingest(p02) == []
    commands = hive.ingest(p03)

    # P03 is below its four-tick water reserve, so it can use its stock only
    # to fill P01's two-payment emergency buffer. The previous allocator sent
    # four water and left P03 with just one upkeep payment, which becomes zero
    # before a new offer can safely round-trip.
    assert [(command.station_id, command.action["recipient_id"], command.action["give"])
            for command in commands] == [
                ("P03", "P01", {"water": 2, "food": 0, "components": 0}),
            ]


def test_hive_does_not_duplicate_a_visible_incoming_aid_offer() -> None:
    """An in-flight offer must reduce the recipient's allocation deficit."""
    hive = Coordinator(CoordinatorConfig(reserve_ticks=4, expected_station_count=3))
    offer = {
        "offer_id": "water-aid", "proposer_id": "P01", "recipient_id": "P02",
        "give": {"water": 4, "food": 0, "components": 0},
        "receive": {"water": 0, "food": 0, "components": 0},
        "expires_tick": 10, "status": "OFFER_STATUS_OPEN",
    }
    p01 = _planet("P01", inventory={"water": 12, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1}, offers=[offer])
    p02 = _planet("P02", inventory={"water": 0, "food": 8, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1}, offers=[offer])
    p03 = _planet("P03", inventory={"water": 4, "food": 4, "components": 12}, upkeep={"water": 2, "food": 1, "components": 1}, offers=[offer])

    assert hive.ingest(p01) == []
    assert hive.ingest(p02) == []
    commands = hive.ingest(p03)

    assert [(command.station_id, command.action) for command in commands] == [
        ("P02", {"kind": "accept", "offer_id": "water-aid"}),
    ]


def test_hive_prefers_a_complementary_bilateral_bundle_to_aid() -> None:
    """P01's water surplus and food deficit complement P02's inverse delta."""
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=3))
    p01 = _planet("P01", inventory={"water": 8, "food": 1, "components": 1}, upkeep={"water": 2, "food": 1, "components": 1})
    p02 = _planet("P02", inventory={"water": 0, "food": 8, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})
    p03 = _planet("P03", inventory={"water": 0, "food": 2, "components": 8}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(p01) == []
    assert hive.ingest(p02) == []
    commands = hive.ingest(p03)

    # Paid matching is selected before any one-way relay aid.  A subsequent
    # safe relay is allowed so a third station can receive the input needed
    # to keep the resource cycle moving.
    assert (commands[0].station_id, commands[0].action["recipient_id"], commands[0].action["give"], commands[0].action["receive"]) == (
        "P01", "P02", {"water": 4, "food": 0, "components": 0}, {"water": 0, "food": 1, "components": 0},
    )


def test_hive_sizes_a_gift_from_the_recipient_known_upkeep_cost() -> None:
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=2))
    donor = _planet("P01", inventory={"water": 4, "food": 8, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    recipient = _planet("P02", inventory={"water": 4, "food": 0, "components": 4}, upkeep={"water": 2, "food": 3, "components": 1})

    assert hive.ingest(donor) == []
    commands = hive.ingest(recipient)

    # P02's two-tick food reserve is 2 × 3, not a generic two-unit gift.
    assert commands[0].action["give"] == {"water": 0, "food": 6, "components": 0}


def test_hive_accepts_a_visible_aid_offer_at_the_recipient() -> None:
    hive = Coordinator(CoordinatorConfig(expected_station_count=2))
    offer = {
        "offer_id": "aid-42", "proposer_id": "P01", "recipient_id": "P02",
        "give": {"water": 1, "food": 0, "components": 0}, "receive": {"water": 0, "food": 0, "components": 0},
        "expires_tick": 8, "status": "OFFER_STATUS_OPEN",
    }
    donor = _planet("P01", inventory={"water": 4, "food": 4, "components": 4}, offers=[offer])
    recipient = _planet("P02", inventory={"water": 4, "food": 4, "components": 4})

    assert hive.ingest(donor) == []
    commands = hive.ingest(recipient)

    assert [(command.station_id, command.action) for command in commands] == [("P02", {"kind": "accept", "offer_id": "aid-42"})]


def test_hive_accepts_a_safe_bilateral_offer_without_double_counting_its_give() -> None:
    hive = Coordinator(CoordinatorConfig(expected_station_count=2))
    offer = {
        "offer_id": "water-for-food", "proposer_id": "P01", "recipient_id": "P02",
        "give": {"water": 4, "food": 0, "components": 0},
        "receive": {"water": 0, "food": 1, "components": 0},
        "expires_tick": 8, "status": "OFFER_STATUS_OPEN",
    }
    # The exchange leaves P01 with exactly one water upkeep payment. Counting
    # its already-open offer a second time would incorrectly reject it.
    donor = _planet("P01", inventory={"water": 6, "food": 1, "components": 1}, upkeep={"water": 2, "food": 1, "components": 1}, offers=[offer])
    recipient = _planet("P02", inventory={"water": 0, "food": 3, "components": 1}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(donor) == []
    assert [(command.station_id, command.action) for command in hive.ingest(recipient)] == [
        ("P02", {"kind": "accept", "offer_id": "water-for-food"}),
    ]


def test_duplicate_snapshot_does_not_repeat_a_command() -> None:
    hive = Coordinator(CoordinatorConfig(expected_station_count=2))
    donor = _planet("P01", inventory={"water": 12, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    recipient = _planet("P02", inventory={"water": 0, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    hive.ingest(donor)
    assert hive.ingest(recipient)
    assert hive.ingest(recipient) == []


def test_new_snapshot_before_offer_is_visible_does_not_crash_or_repeat_command() -> None:
    hive = Coordinator(CoordinatorConfig(expected_station_count=2))
    donor = _planet("P01", inventory={"water": 12, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    recipient = _planet("P02", inventory={"water": 0, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(donor) == []
    assert hive.ingest(recipient)

    # A request result or other update can produce a new snapshot at the
    # same tick before P01's offer appears in the public offer list.
    refreshed_recipient = _planet(
        "P02", inventory={"water": 0, "food": 4, "components": 4},
        upkeep={"water": 2, "food": 1, "components": 1}, snapshot_sequence=8,
    )
    assert hive.ingest(refreshed_recipient) == []


def test_new_snapshot_before_a_bilateral_offer_is_visible_does_not_issue_its_inverse() -> None:
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=2))
    p01 = _planet("P01", inventory={"water": 8, "food": 1, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})
    p02 = _planet("P02", inventory={"water": 0, "food": 8, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(p01) == []
    assert hive.ingest(p02)

    refreshed_p02 = _planet(
        "P02", inventory={"water": 0, "food": 8, "components": 2},
        upkeep={"water": 2, "food": 1, "components": 1}, snapshot_sequence=8,
    )
    assert hive.ingest(refreshed_p02) == []


def test_hive_accounts_for_an_open_offer_before_using_remaining_surplus() -> None:
    hive = Coordinator(CoordinatorConfig(expected_station_count=2))
    existing = {
        "offer_id": "already-promised", "proposer_id": "P01", "recipient_id": "P03",
        "give": {"water": 6, "food": 0, "components": 0}, "receive": {"water": 0, "food": 0, "components": 0},
        "expires_tick": 8, "status": "OFFER_STATUS_OPEN",
    }
    donor = _planet("P01", inventory={"water": 12, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1}, offers=[existing])
    recipient = _planet("P02", inventory={"water": 0, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    hive.ingest(donor)

    # P01's visible six-water commitment is deducted first. The remaining
    # four water can be allocated while retaining its two-water upkeep floor.
    commands = hive.ingest(recipient)
    assert [(command.station_id, command.action["give"]) for command in commands] == [
        ("P01", {"water": 4, "food": 0, "components": 0}),
    ]


def test_hive_aids_only_surplus_left_after_bilateral_matching() -> None:
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=3))
    p01 = _planet("P01", inventory={"water": 8, "food": 1, "components": 1}, upkeep={"water": 2, "food": 1, "components": 1})
    p02 = _planet("P02", inventory={"water": 0, "food": 8, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})
    # P03 can cover its own next upkeep and has components left after its
    # reserve. Its aid is selected after P01/P02's paid exchange.
    p03 = _planet("P03", inventory={"water": 2, "food": 2, "components": 8}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(p01) == []
    assert hive.ingest(p02) == []
    commands = hive.ingest(p03)

    assert [(command.station_id, command.action["recipient_id"], command.action["give"], command.action["receive"])
            for command in commands] == [
                ("P01", "P02", {"water": 4, "food": 0, "components": 0}, {"water": 0, "food": 1, "components": 0}),
                ("P03", "P01", {"water": 0, "food": 0, "components": 1}, {"water": 0, "food": 0, "components": 0}),
            ]


def test_hive_can_spend_a_matching_reserve_down_to_next_upkeep() -> None:
    hive = Coordinator(CoordinatorConfig(reserve_ticks=2, expected_station_count=2))
    # P01 has only one water above its two-tick target, but can safely spend
    # three while keeping its two-water next-upkeep floor. That fully resolves
    # P02's immediate water shortfall and receives its matching food surplus.
    p01 = _planet("P01", inventory={"water": 5, "food": 1, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})
    p02 = _planet("P02", inventory={"water": 0, "food": 8, "components": 2}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(p01) == []
    commands = hive.ingest(p02)
    assert [(command.action["give"], command.action["receive"]) for command in commands] == [
        ({"water": 3, "food": 0, "components": 0}, {"water": 0, "food": 1, "components": 0}),
    ]


def test_hive_waits_for_the_configured_station_quorum_before_allocating() -> None:
    hive = Coordinator(CoordinatorConfig(expected_station_count=3))
    donor = _planet("P01", inventory={"water": 12, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    recipient = _planet("P02", inventory={"water": 0, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})
    neutral = _planet("P03", inventory={"water": 4, "food": 4, "components": 4}, upkeep={"water": 2, "food": 1, "components": 1})

    assert hive.ingest(donor) == []
    assert hive.ingest(recipient) == []
    commands = hive.ingest(neutral)

    assert ("P01", "offer") in [(command.station_id, command.action["kind"]) for command in commands]


def test_hive_does_not_accept_an_offer_at_its_exclusive_expiry_tick() -> None:
    hive = Coordinator(CoordinatorConfig(expected_station_count=2))
    offer = {
        "offer_id": "expired-aid", "proposer_id": "P01", "recipient_id": "P02",
        "give": {"water": 1, "food": 0, "components": 0}, "receive": {"water": 0, "food": 0, "components": 0},
        "expires_tick": 7, "status": "OFFER_STATUS_OPEN",
    }
    donor = _planet("P01", inventory={"water": 4, "food": 4, "components": 4}, offers=[offer])
    recipient = _planet("P02", inventory={"water": 4, "food": 4, "components": 4})

    assert hive.ingest(donor) == []
    assert hive.ingest(recipient) == []
