"""Central allocation policy. Only this module makes trading decisions."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import count

from .model import Bundle, Command, Offer, Planet, RESOURCES

MAX_STATIONS = 20


@dataclass(frozen=True)
class CoordinatorConfig:
    """Conservative inventory buffer and coordination quorum."""

    # A Bazaar transfer requires an offer snapshot and a later accept. In the
    # three-specialty demo, a resource may also need to traverse a second
    # station before its producer can safely send again. Four ticks provides
    # that command-latency buffer; callers may tune it for a different run.
    reserve_ticks: int = 4
    expected_station_count: int = 9
    # ``None`` deliberately means the full TTL permitted by the active run.
    # A shorter positive value can still be used for a test or a known-fast run.
    offer_ttl_ticks: int | None = None

    def __post_init__(self) -> None:
        if self.reserve_ticks < 0:
            raise ValueError("reserve_ticks must not be negative")
        if not 0 <= self.expected_station_count <= MAX_STATIONS:
            raise ValueError("expected_station_count must be 0 (automatic) or between 1 and 20")
        if self.offer_ttl_ticks is not None and self.offer_ttl_ticks < 1:
            raise ValueError("offer_ttl_ticks must be positive or None")


class Coordinator:
    """Match reserve deficits before donating unmatched safe surplus.

    Every offer is created by one station and accepted by the other on a later
    authoritative snapshot.  Planning therefore treats every visible outgoing
    offer, as well as each proposed reciprocal payment, as a commitment before
    it reuses inventory for another candidate.
    """

    def __init__(self, config: CoordinatorConfig | None = None) -> None:
        self.config = config or CoordinatorConfig()
        self.planets: dict[str, Planet] = {}
        self.active_station_ids: set[str] | None = None
        self._sequence = count(1)
        self._issued: set[tuple[str, str, int]] = set()
        self._pending_bilateral_recipients: set[tuple[str, int]] = set()

    def ingest(self, planet: Planet) -> list[Command]:
        """Record an authoritative snapshot and return newly safe commands."""
        previous = self.planets.get(planet.station_id)
        if previous and previous.run_id == planet.run_id and planet.snapshot_sequence <= previous.snapshot_sequence:
            return []
        if previous and previous.run_id != planet.run_id:
            self.planets.clear()
            self._issued.clear()
            self._pending_bilateral_recipients.clear()
        self.planets[planet.station_id] = planet
        return self._plan(planet.run_id)

    def _plan(self, run_id: str) -> list[Command]:
        # Fixed mode waits for its quorum; automatic mode uses the connected
        # roster. Retained snapshots of offline stations aren't spendable.
        registered = sorted(
            (item for item in self.planets.values() if item.run_id == run_id and item.snapshot_sequence > 0
             and (self.active_station_ids is None or item.station_id in self.active_station_ids)),
            key=lambda item: item.station_id,
        )
        if len(registered) < self.config.expected_station_count:
            return []
        if self.config.expected_station_count == 0 and self.active_station_ids is not None and len(registered) < len(self.active_station_ids):
            return []
        planets = sorted(
            (
                item for item in registered
                if item.run_id == run_id and item.phase == "RUNNING" and not item.failed_once
            ),
            key=lambda item: item.station_id,
        )
        if not planets:
            return []
        # Every command issued in one pass belongs to the same game tick.
        tick = min(item.tick for item in planets)
        current = [item for item in planets if item.tick == tick]
        # Never allocate against a mixed-tick view: waiting one snapshot is
        # safer than treating a stale donor's inventory as spendable.
        if len(current) != len(planets):
            return self._accept_visible_offers(current, run_id)

        commands = self._accept_visible_offers(current, run_id)
        busy_stations = {command.station_id for command in commands} | {
            planet.station_id
            for planet in current
            if self._offer_was_issued_this_tick(planet.station_id, tick)
        } | {
            station_id
            for station_id, issued_tick in self._pending_bilateral_recipients
            if issued_tick == tick
        }
        commitments = {
            planet.station_id: self._open_outgoing_commitments(planet)
            for planet in current
        }
        incoming = {
            planet.station_id: self._open_incoming_projection(planet)
            for planet in current
        }
        # ``projected`` is the desired final inventory for this allocation
        # round. It includes visible open offers and then each planned
        # settlement. ``commitments`` remains pessimistic for spendability: a
        # future receipt is never used to fund another outgoing offer.
        projected = {
            planet.station_id: {
                resource: (
                    planet.inventory.get(resource)
                    - commitments[planet.station_id][resource]
                    + incoming[planet.station_id][resource]
                )
                for resource in RESOURCES
            }
            for planet in current
        }

        # A bilateral exchange is preferable to a gift whenever both sides
        # have a reserve deficit that the other side's reserve surplus solves.
        # Recompute after each selection so a resource cannot be allocated
        # twice through different offers.
        while candidate := self._best_bilateral_candidate(current, projected, commitments, busy_stations):
            sender, recipient, give, receive = candidate
            commands.append(self._offer(sender, recipient, give, receive))
            self._apply_settlement_projection(projected, sender, recipient, give, receive)
            self._add_commitment(commitments[sender.station_id], give)
            self._add_commitment(commitments[recipient.station_id], receive)
            busy_stations.add(sender.station_id)
            # The recipient's reciprocal payment is already committed by this
            # exchange. It cannot safely create a distinct outgoing offer
            # until the trade is visible (or the next tick begins).
            busy_stations.add(recipient.station_id)

        # Only after reciprocal opportunities have been exhausted may a
        # station's remaining safe liquidity become aid. A reserve is a goal,
        # not a reason to let a peer miss the next upkeep: aid may spend down
        # to one known upkeep payment, but never below it. A planned payment
        # is already in ``commitments``, so aid cannot over-promise it.
        while candidate := self._best_aid_candidate(current, projected, commitments, busy_stations):
            donor, recipient, give = candidate
            commands.append(self._offer(donor, recipient, give, Bundle()))
            self._apply_settlement_projection(projected, donor, recipient, give, Bundle())
            self._add_commitment(commitments[donor.station_id], give)
            busy_stations.add(donor.station_id)
        return commands

    def _accept_visible_offers(self, planets: list[Planet], run_id: str) -> list[Command]:
        commands: list[Command] = []
        seen: set[str] = set()
        busy_recipients: set[str] = set()
        by_station = {planet.station_id: planet for planet in planets}
        commitments = {
            planet.station_id: self._open_outgoing_commitments(planet)
            for planet in planets
        }
        for planet in planets:
            for offer in planet.offers:
                if offer.offer_id in seen or offer.status != "OPEN":
                    continue
                seen.add(offer.offer_id)
                recipient = by_station.get(offer.recipient_id)
                # Bazaar deadlines are exclusive: an offer expiring at the
                # current tick is already unusable.
                if (
                    recipient is None
                    or recipient.station_id in busy_recipients
                    or offer.proposer_id not in by_station
                    or offer.expires_tick <= recipient.tick
                    # ``commitments`` already contains this open offer's
                    # give bundle. Remove it here because the settlement
                    # calculation subtracts ``give`` itself; otherwise a
                    # paid offer is charged twice and can never be accepted
                    # when it correctly spends down to a reserve floor.
                    or not self._offer_is_safe_to_accept(
                        by_station[offer.proposer_id], recipient, offer,
                        self._commitments_without_offer(commitments, offer.proposer_id, offer.give),
                    )
                ):
                    continue
                marker = (recipient.station_id, f"accept:{offer.offer_id}", 0)
                if marker in self._issued:
                    continue
                self._issued.add(marker)
                commands.append(self._command(recipient.station_id, run_id, {"kind": "accept", "offer_id": offer.offer_id}))
                # The published command budget applies to accepts as well as
                # offers. Other safe gifts stay open for the next update.
                busy_recipients.add(recipient.station_id)
        return commands

    def _offer_is_safe_to_accept(
        self,
        sender: Planet,
        recipient: Planet,
        offer: Offer,
        commitments: dict[str, dict[str, int]],
    ) -> bool:
        """Apply bilateral safety only when the recipient must pay.

        A zero-price aid offer is a one-way relay. Its sender's pledged
        resources are protected by the same local floor used when issuing the
        offer; requiring every unrelated resource to be present would prevent
        the matching inbound relay from ever arriving.
        """
        if not any(offer.receive.as_dict().values()):
            return self._gift_keeps_donor_safe(sender, offer.give, commitments)
        return self._settlement_keeps_both_safe(sender, recipient, offer.give, offer.receive, commitments)

    def _offer(self, donor: Planet, recipient: Planet, give: Bundle, receive: Bundle) -> Command:
        marker = self._offer_marker(donor, recipient, give, receive)
        if marker in self._issued:
            raise RuntimeError("Coordinator attempted duplicate donor command")
        self._issued.add(marker)
        if any(receive.as_dict().values()):
            self._pending_bilateral_recipients.add((recipient.station_id, donor.tick))
        ttl = donor.rules.max_offer_ttl_ticks
        if self.config.offer_ttl_ticks is not None:
            ttl = min(ttl, self.config.offer_ttl_ticks)
        return self._command(donor.station_id, donor.run_id, {
            "kind": "offer", "recipient_id": recipient.station_id, "give": give.as_dict(),
            "receive": receive.as_dict(), "expires_tick": donor.tick + ttl,
        })

    @staticmethod
    def _offer_marker(donor: Planet, recipient: Planet, give: Bundle, receive: Bundle) -> tuple[str, str, int]:
        return donor.station_id, f"{recipient.station_id}:{give.as_dict()}:{receive.as_dict()}", donor.tick

    def _offer_was_issued(self, donor: Planet, recipient: Planet, give: Bundle, receive: Bundle) -> bool:
        return self._offer_marker(donor, recipient, give, receive) in self._issued

    def _bilateral_offer_was_issued(self, sender: Planet, recipient: Planet, give: Bundle, receive: Bundle) -> bool:
        """Treat the inverse representation as the same pending exchange."""
        return (
            self._offer_was_issued(sender, recipient, give, receive)
            or self._offer_was_issued(recipient, sender, receive, give)
        )

    def _offer_was_issued_this_tick(self, station_id: str, tick: int) -> bool:
        return any(marker[0] == station_id and marker[2] == tick for marker in self._issued)

    def _command(self, station_id: str, run_id: str, action: dict[str, object]) -> Command:
        return Command(f"hive-{next(self._sequence)}", station_id, run_id, action)

    def _desired_delta(self, planet: Planet, resource: str, projected: dict[str, int]) -> int:
        """Positive means reserve deficit; negative means reserve surplus.

        Inventory is a post-tick balance, so production must not be added a
        second time.  The only public survival forecast is published upkeep.
        """
        target = self.config.reserve_ticks * planet.published_upkeep.get(resource)
        return target - projected[resource]

    def _survival_delta(self, planet: Planet, resource: str, projected: dict[str, int]) -> int:
        """Return the shortfall over the offer/accept delivery window.

        ``reserve_ticks`` is deliberately a longer-term objective.  It must
        not make a station that is itself below that objective forward its
        final stock merely because another station has an even larger reserve
        deficit.  Bazaar transfers require an offer and a later acceptance,
        so two upkeep payments are the minimum useful buffer while an aid
        offer is in flight.
        """
        transfer_window = min(2, self.config.reserve_ticks)
        target = transfer_window * planet.published_upkeep.get(resource)
        return max(0, target - projected[resource])

    def _best_bilateral_candidate(
        self,
        planets: list[Planet],
        projected: dict[str, dict[str, int]],
        commitments: dict[str, dict[str, int]],
        busy_stations: set[str],
    ) -> tuple[Planet, Planet, Bundle, Bundle] | None:
        candidates: list[tuple[tuple[int, int, str, str], Planet, Planet, Bundle, Bundle]] = []
        for sender in planets:
            if sender.station_id in busy_stations:
                continue
            for recipient in planets:
                if recipient.station_id == sender.station_id:
                    continue
                give = self._complementary_bundle(sender, recipient, projected, commitments, sender_gives=True)
                receive = self._complementary_bundle(sender, recipient, projected, commitments, sender_gives=False)
                if not any(give.as_dict().values()) or not any(receive.as_dict().values()):
                    continue
                if self._bilateral_offer_was_issued(sender, recipient, give, receive):
                    continue
                if not self._settlement_keeps_both_safe(sender, recipient, give, receive, commitments):
                    continue
                score = sum(give.as_dict().values()) + sum(receive.as_dict().values())
                candidates.append(((-score, -self._risk(sender) - self._risk(recipient), sender.station_id, recipient.station_id), sender, recipient, give, receive))
        if not candidates:
            return None
        _, sender, recipient, give, receive = min(candidates, key=lambda item: item[0])
        return sender, recipient, give, receive

    def _best_aid_candidate(
        self,
        planets: list[Planet],
        projected: dict[str, dict[str, int]],
        commitments: dict[str, dict[str, int]],
        busy_stations: set[str],
    ) -> tuple[Planet, Planet, Bundle] | None:
        candidates: list[tuple[tuple[int, int, int, str, str], Planet, Planet, Bundle]] = []
        for donor in planets:
            if donor.station_id in busy_stations:
                continue
            for recipient in planets:
                if recipient.station_id == donor.station_id:
                    continue
                quantities: dict[str, int] = {}
                emergency = 0
                for resource in RESOURCES:
                    safe_stock = self._safe_aid_surplus(donor, resource, commitments)
                    recipient_reserve_deficit = max(
                        0,
                        self._desired_delta(recipient, resource, projected[recipient.station_id]),
                    )
                    recipient_survival_deficit = self._survival_delta(
                        recipient, resource, projected[recipient.station_id],
                    )
                    donor_reserve_surplus = max(
                        0,
                        -self._desired_delta(donor, resource, projected[donor.station_id]),
                    )

                    donor_survival_deficit = self._survival_delta(
                        donor, resource, projected[donor.station_id],
                    )
                    if recipient_survival_deficit > donor_survival_deficit:
                        # Drawing into the donor's reserve is emergency aid,
                        # not reserve rebalancing.  Fill only the recipient's
                        # two-payment delivery buffer; otherwise a component-
                        # rich planet can immediately forward the water it
                        # needs for its own next offer/accept cycle.
                        quantities[resource] = min(safe_stock, recipient_survival_deficit)
                    elif donor_reserve_surplus:
                        # A genuine reserve surplus may restore the peer's
                        # longer-term buffer, but never exceeds the amount
                        # that remains surplus after visible commitments.
                        quantities[resource] = min(
                            safe_stock, donor_reserve_surplus, recipient_reserve_deficit,
                        )
                    else:
                        quantities[resource] = 0
                    emergency += recipient_survival_deficit
                give = Bundle(**quantities)
                if not any(give.as_dict().values()):
                    continue
                if self._offer_was_issued(donor, recipient, give, Bundle()):
                    continue
                # ``give`` was built entirely from ``_safe_aid_surplus``, so
                # this donor retains the next upkeep payment for every
                # resource it pledges.  Do not reject it merely because the
                # donor is awaiting a different resource from a peer: that
                # would deadlock a safe three-station relay where each member
                # gives its specialty and receives its missing input.
                # Settlement-time safety remains strict for paid exchanges,
                # whose recipient must fund a reciprocal payment.
                if not self._gift_keeps_donor_safe(donor, give, commitments):
                    continue
                amount = sum(give.as_dict().values())
                # During a scarce-resource run, resolve the shortest delivery
                # buffer first.  Ranking only by bundle size repeatedly fills
                # one station's long reserve while a peer reaches zero.
                candidates.append(((-emergency, -amount, -self._risk(recipient), donor.station_id, recipient.station_id), donor, recipient, give))
        if not candidates:
            return None
        _, donor, recipient, give = min(candidates, key=lambda item: item[0])
        return donor, recipient, give

    @staticmethod
    def _gift_keeps_donor_safe(
        donor: Planet,
        give: Bundle,
        commitments: dict[str, dict[str, int]],
    ) -> bool:
        """A gift may depend on a separate, coordinated inbound gift.

        Unlike a bilateral settlement, no resource leaves the recipient. The
        only inventory that must be locally available is the donor's gift,
        and it must leave the donor with its next published upkeep payment.
        """
        return all(
            give.get(resource) == 0
            or donor.inventory.get(resource) - commitments[donor.station_id][resource] - give.get(resource)
            >= donor.published_upkeep.get(resource)
            for resource in RESOURCES
        )

    def _complementary_bundle(
        self,
        sender: Planet,
        recipient: Planet,
        projected: dict[str, dict[str, int]],
        commitments: dict[str, dict[str, int]],
        *,
        sender_gives: bool,
    ) -> Bundle:
        quantities: dict[str, int] = {}
        for resource in RESOURCES:
            sender_delta = self._desired_delta(sender, resource, projected[sender.station_id])
            recipient_delta = self._desired_delta(recipient, resource, projected[recipient.station_id])
            owner = sender if sender_gives else recipient
            surplus, deficit = (sender_delta, recipient_delta) if sender_gives else (recipient_delta, sender_delta)
            # A resource must still be a genuine reserve surplus at the
            # matching stage. Once it is, however, a paid trade may spend
            # that reserve down to one upkeep payment only as far as the
            # counterparty's delivery-window shortfall.  Filling a longer
            # reserve target in one offer strands the producer for the next
            # offer/accept round while the recipient sits on stock it cannot
            # consume before then.  The safety check below proves both
            # post-settlement upkeep floors.
            quantities[resource] = min(
                self._safe_aid_surplus(owner, resource, commitments) if surplus < 0 else 0,
                max(0, deficit),
                self._survival_delta(
                    recipient if sender_gives else sender,
                    resource,
                    projected[(recipient if sender_gives else sender).station_id],
                ),
            )
        return Bundle(**quantities)

    @staticmethod
    def _safe_aid_surplus(planet: Planet, resource: str, commitments: dict[str, dict[str, int]]) -> int:
        """Stock a station can give while retaining its next upkeep payment."""
        return max(
            0,
            planet.inventory.get(resource)
            - commitments[planet.station_id][resource]
            - planet.published_upkeep.get(resource),
        )

    @staticmethod
    def _add_commitment(commitment: dict[str, int], bundle: Bundle) -> None:
        for resource in RESOURCES:
            commitment[resource] += bundle.get(resource)

    @staticmethod
    def _commitments_without_offer(
        commitments: dict[str, dict[str, int]],
        proposer_id: str,
        give: Bundle,
    ) -> dict[str, dict[str, int]]:
        """Copy commitments with the offer currently being settled removed."""
        adjusted = {station_id: dict(amounts) for station_id, amounts in commitments.items()}
        for resource in RESOURCES:
            adjusted[proposer_id][resource] = max(0, adjusted[proposer_id][resource] - give.get(resource))
        return adjusted

    @staticmethod
    def _apply_settlement_projection(
        projected: dict[str, dict[str, int]],
        sender: Planet,
        recipient: Planet,
        give: Bundle,
        receive: Bundle,
    ) -> None:
        for resource in RESOURCES:
            amount_given, amount_received = give.get(resource), receive.get(resource)
            projected[sender.station_id][resource] += amount_received - amount_given
            projected[recipient.station_id][resource] += amount_given - amount_received

    @staticmethod
    def _open_outgoing_commitments(planet: Planet) -> dict[str, int]:
        return {
            resource: sum(
                offer.give.get(resource)
                for offer in planet.offers
                if offer.proposer_id == planet.station_id and offer.status == "OPEN"
            )
            for resource in RESOURCES
        }

    @staticmethod
    def _open_incoming_projection(planet: Planet) -> dict[str, int]:
        """Net resources from offers this station can accept on its next command.

        This is used only to avoid allocating the same shortage twice while
        the first offer is already visible.  It deliberately does not relax
        ``_safe_aid_surplus`` or the settlement safety check, so an unaccepted
        receipt can never bankroll a new outgoing promise.
        """
        return {
            resource: sum(
                offer.give.get(resource) - offer.receive.get(resource)
                for offer in planet.offers
                if offer.recipient_id == planet.station_id and offer.status == "OPEN"
            )
            for resource in RESOURCES
        }

    def _settlement_keeps_both_safe(
        self,
        sender: Planet,
        recipient: Planet,
        give: Bundle,
        receive: Bundle,
        commitments: dict[str, dict[str, int]],
    ) -> bool:
        """Check the pessimistic post-settlement balance of both parties."""
        for resource in RESOURCES:
            sender_post = sender.inventory.get(resource) - commitments[sender.station_id][resource] - give.get(resource) + receive.get(resource)
            recipient_post = recipient.inventory.get(resource) - commitments[recipient.station_id][resource] - receive.get(resource) + give.get(resource)
            if sender_post < sender.published_upkeep.get(resource) or recipient_post < recipient.published_upkeep.get(resource):
                return False
        return True

    @staticmethod
    def _risk(planet: Planet) -> int:
        immediate_shortage = sum(max(0, planet.upkeep.get(r) - planet.inventory.get(r)) for r in RESOURCES)
        one_tick_left = sum(
            planet.upkeep.get(r) > 0 and planet.inventory.get(r) == planet.upkeep.get(r)
            for r in RESOURCES
        )
        return (
            immediate_shortage * planet.rules.shortage_damage_per_unit * 1_000
            + one_tick_left * planet.rules.shortage_damage_per_unit * 100
            + max(0, planet.rules.max_health - planet.health)
        )
