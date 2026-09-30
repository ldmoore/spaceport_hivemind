# Spaceport Hivemind

**Joining as a team?** Follow [Getting started](GETTING_STARTED.md) to install the client, configure your private credentials, and connect to the hosted engine or the teacher's server and Hivemind.

The Hivemind Server decides trades; each `hivemind-client` only forwards game
state and executes its commands.

## Start

Clone the repository and open it in a Dev Container. The setup installs the
project and generates its protocol bindings. Without a Dev Container, run:

```sh
python -m pip install -e '.[dev]'
make generate
```

If an existing Dev Container reports that `.venv/bin/pip` or a project command
cannot execute, the workspace has a virtual environment created on another
operating system. Recreate that disposable environment, then install:

```sh
rm -rf .venv
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
make PYTHON=.venv/bin/python generate
```

The container puts `.venv/bin` on `PATH`; if you are working outside it,
either activate the environment with `source .venv/bin/activate` or invoke
commands through `.venv/bin/`.

Start one Hivemind Server:

```sh
hivemind-server --host 127.0.0.1 --port 8765 --shared-key local-hive-key \
  --station-count 9
```

## Run the multi-station demo

The supplied Bazaar practice server only accepts P01, so it cannot test a
coordinated multi-bridge run. Start this repository's compatible demo server
instead; it writes a fresh credential for each station and begins with P01
holding spare water and P02 needing it. The game waits until every configured
station reports ready, then advances one tick per second: each planet produces
its specialty, pays upkeep, takes damage for shortages, expires old offers,
and finishes after 30 ticks.

```sh
hivemind-demo-server --credential-file ./demo-credentials.json \
  --stations P01,P02,P03 --duration-ticks 30 --tick-duration-ms 1000
```

In two more terminals, start the Hivemind service and one bridge per station:

```sh
hivemind-server --host 127.0.0.1 --port 8765 --shared-key local-hive-key \
  --station-count 3

hivemind-client --credentials ./demo-credentials.json --station P01 \
  --endpoint ws://127.0.0.1:3001/ws \
  --hivemind-endpoint ws://127.0.0.1:8765 --hivemind-key local-hive-key

hivemind-client --credentials ./demo-credentials.json --station P02 \
  --endpoint ws://127.0.0.1:3001/ws \
  --hivemind-endpoint ws://127.0.0.1:8765 --hivemind-key local-hive-key

hivemind-client --credentials ./demo-credentials.json --station P03 \
  --endpoint ws://127.0.0.1:3001/ws \
  --hivemind-endpoint ws://127.0.0.1:8765 --hivemind-key local-hive-key
```

The expected path starts with a zero-price water offer from P01 followed by
P02's automatic acceptance, then continues with production, upkeep, and further aid
decisions. Use `--duration-ticks` and `--tick-duration-ms` to make a run
longer or faster. Every configured station needs a bridge before the game
starts.

For repeatable coordinator iteration, the single-command runner starts the
demo game, Hivemind service, and every bridge on isolated ephemeral ports. It
prints the final station inventories, offer history, and an overall success
value; a failed run exits nonzero:

```sh
make demo
# or: hivemind-demo --duration-ticks 30 --tick-duration-ms 100
```

Pass `--include-offers` when you need the complete trade trace, or `--verbose`
to watch individual connections and commands.

This is the preferred local regression loop. The commands below remain useful
when you want to observe the individual processes interactively.

## Provide game credentials

Give every bridge the credentials JSON created by the Bazaar game server. The
file must contain the station ID and its token, for example:

```json
{
  "players": [
    {"station_id": "P01", "token": "station-token"}
  ]
}
```

Save it as `credentials.json` (or use any path) and start one bridge for each
station. All bridges use the same Hivemind endpoint and shared key:

```sh
hivemind-client \
  --credentials ./credentials.json --station P01 \
  --endpoint ws://127.0.0.1:3001/ws \
  --hivemind-endpoint ws://127.0.0.1:8765 --hivemind-key local-hive-key
```

Repeat the client command with `--station P02`, `P03`, and so on. Do not put a
game token in the Hivemind Server command; tokens stay in the clients.
