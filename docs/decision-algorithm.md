# How Hivemind Calculates Decisions

`Coordinator` is the only component that decides what to trade.  The Bazaar
clients are intentionally policy-free: they report authoritative game state to
the coordinator and execute commands that it returns.

The policy first matches complementary reserve deficits into bilateral offers,
then gives any unmatched safe surplus as zero-price aid. It does not assign
prices beyond the exact resource quantities that restore those reserves.

## Inputs and state

Each bridge sends a `Planet` observation containing its station ID, run ID,
tick, snapshot sequence, phase, inventory, most recent production, per-tick
upkeep, health, failure flag, public rules, and visible offers.  Hivemind keeps
the newest observation for each station.

An observation is ignored when it belongs to the same run and its snapshot
sequence is not newer than the stored one.  When the run ID changes, Hivemind
discards every stored observation and every record of commands it issued; a
new run starts from a clean slate.

Hivemind waits until it has observations from `expected_station_count` stations
(default: `9`) before it plans. The server exposes this as `--station-count`,
so smaller practice runs can set their actual bridge count. Only stations that
satisfy all of the following are considered for planning:

- belong to the run currently being planned;
- are in the `RUNNING` phase; and
- have not failed once.

## Tick alignment

Hivemind will create new offers only from a coherent, same-tick view of all
eligible stations.  It finds the earliest tick among their stored snapshots.
If any eligible station is ahead of that tick, the view is mixed and new aid
offers are deferred until the remaining snapshots catch up.  This prevents an
old donor inventory from being treated as available after newer activity.

Even during a mixed-tick view, Hivemind can accept already-visible safe offers
among the stations at the earliest tick; accepting one does not require a new
inventory allocation.

## Reserve math

For each resource independently, the algorithm calculates:

```text
target = reserve_ticks × published_upkeep
delta  = target - inventory
```

`reserve_ticks` defaults to `4`, so the target is four future upkeep ticks. A
positive delta is a deficit and a negative delta is surplus. Before calculating
that delta, Hivemind deducts every open outgoing offer's `give` bundle. Those
offers can settle independently, so their stock is already committed.
Inventory is already the authoritative post-tick balance: the reported
`last_production` was included before upkeep was charged, and is not a promise
about the next tick. Upkeep is published from the first snapshot, so it is the
reliable input for calculating desired inventory deltas.

The default intentionally covers more than a single offer/accept round. In a
specialist cycle, a resource can need to wait for another station's command
slot before its producer can safely send again. Use `hivemind-server
--reserve-ticks N` to tune this for a different tick duration or roster.

For example, a water inventory of 12 and upkeep of 2 has a reserve target of
4, so the station can safely offer 8 water. With inventory 0 and the same
upkeep, the station needs 4 water to reach that reserve.

The target decides what is desirable; next-upkeep coverage is the hard safety
floor. Every candidate is rejected unless both post-settlement inventories,
after open-offer commitments, still cover every resource in each planet's next
published upkeep. A future receipt is never used to fund another outgoing
offer.

## Matching trades and aid

Candidates are ordered by the amount of reserve deficit resolved, then risk,
with station ID as a deterministic tie-breaker. Risk is:

```text
immediate_shortage = Σ max(0, upkeep - inventory)
risk = immediate_shortage × shortage_damage_per_unit × 1,000
       + max(0, max_health - health)
```

The sum is over all three resources.  Thus a station expected to miss upkeep
is prioritized ahead of ordinary health recovery, and a less healthy station
wins ties where the immediate shortage is the same.

First, Hivemind makes a bilateral candidate when A has surplus of resources B
needs and B has surplus of resources A needs. One offer can contain a bundle in
each direction, with every amount capped by the two complementary deltas:

```text
give[A→B, resource]    = min(A surplus, B deficit)
receive[A←B, resource] = min(B surplus, A deficit)
```

After no valid bilateral candidate remains, Hivemind allocates one-way aid.
A donor with genuine reserve surplus can fill a recipient's reserve deficit.
When aid would draw into the donor's reserve, it is capped at the recipient's
two-payment offer/accept delivery buffer and is allowed only when that
recipient has the larger survival shortfall. This prevents a station from
immediately forwarding the water it needs for its own in-flight transfer while
another station accumulates a longer reserve. Visible incoming offers also
reduce the recipient's allocation deficit, though an unaccepted receipt is
never treated as spendable stock for another outgoing offer.
Every selected offer consumes its sender's one command for that planning pass;
an acceptance also consumes that station's command. Recipients remain eligible
to receive multiple offers because accepting them occurs on later snapshots.

A station is never selected as its own donor. Open outgoing offers are not a
blanket exclusion: their `give` bundle is reserved first, so a station can use
only stock that remains safe after every visible promise.

## Offer and acceptance behavior

New transfers are expressed as Bazaar offers with these properties:

- the sender creates the offer;
- bilateral `give` and `receive` are the complementary bundles; aid has an
  all-zero `receive` bundle; and
- expiration is `current_tick + max_offer_ttl_ticks` by default. A configured
  positive `offer_ttl_ticks` can lower that ceiling for a known-fast run.

The default uses the full lifetime permitted by the game rules, because Bazaar
still requires an offer to be visible before its recipient can accept it. Once
the game exposes the open offer ID in a later snapshot, the recipient
automatically receives an `accept` command. Hivemind accepts only offers that are
open, unexpired, visible to a currently considered recipient, made by a
currently considered proposer, and safe for both parties after open-offer
commitments. This includes bilateral offers it created on the prior snapshot.

## Duplicate protection

Hivemind assigns each command a monotonic `hive-<n>` ID.  It also records
issued work so a repeated snapshot does not recreate it:

- an offer is keyed by sender, recipient, full give bundle, full receive
  bundle, and tick;
- an acceptance is keyed by recipient and offer ID.

Those records live for the run and are cleared only when a new run is seen.
This makes replayed snapshots harmless while preserving the two-step
offer-then-accept protocol required by Bazaar.

## What the baseline does not optimize

This is a survival-reserve heuristic, not a market optimizer.  It does not use
specialties, advertisements, transaction history, unmet-upkeep history,
resource prices, or future production forecasts beyond the latest production
value. Those are natural extension points if the policy later needs to optimize
for a broader objective than keeping every active station safely supplied.

## Implementation reference

The behavior described here is implemented in
`src/spaceport_hivemind/coordinator.py`, principally in `Coordinator._plan`,
`_desired_delta`, `_safe_aid_surplus`, `_risk`, and
`_accept_visible_offers`.
