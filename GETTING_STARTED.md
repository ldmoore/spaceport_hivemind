# Getting started: connect your planet to Hivemind

Run one client on your computer. It connects to the game engine and Hivemind at the same time:

```text
Game engine → your client → Hivemind
Game engine ← your client ← Hivemind trade commands
```


## 1. Get your connection details

Ask the gamemaster for:

- Your station ID, such as `P01` or `P09`.
- Your private planet token for that game server.
- The shared Hivemind key.
- The game server's WebSocket endpoint.

Our hosted game endpoint is:

```text
wss://bazaar-game-kamalv.apps.cloudapps.unc.edu/ws
```

For the teacher's game, use the teacher's endpoint and teacher-issued planet token instead. Tokens from our engine will not authenticate with the teacher's server.

## 2. Open in a Dev Container (recommended)

Install **Docker Desktop** (or Docker Engine on Linux), **VS Code**, and the VS Code **Dev Containers** extension. Start Docker.

1. Clone this repository using Git or VS Code's **Git: Clone** command.
2. Open the repository's root folder in VS Code.
3. Open the Command Palette and select **Dev Containers: Reopen in Container**.
4. Wait for the container build and setup command to finish. The first build requires internet access and can take several minutes.
5. Open a **new terminal inside VS Code** after setup finishes.

The container automatically:

- Installs Python and the required system tools.
- Creates a virtual environment and installs the client and development dependencies.
- Generates the Protobuf bindings.
- Makes `hivemind-client` available in new terminals without manual activation.
- Creates a private `my-credentials.json` from the template if it does not already exist.
- Configures VS Code to use the container's Python environment.

**Your only setup task inside the container is to replace the three placeholders in `my-credentials.json`.** Continue at step 3, skipping the copy command because the file already exists. Existing credentials are preserved on rebuild.

After changing container configuration, select **Dev Containers: Rebuild Container**. To rerun dependency setup manually inside the container, use `bash .devcontainer/setup.sh`.

### Alternative: install without a Dev Container

Install Git and Python **3.11 or newer**. Clone the latest published version of this repository using its URL from the repository's Code menu:

```bash
git clone <REPOSITORY_URL>
cd spaceport_hivemind
```

If you chose a different clone directory, change into that directory instead.

### Linux, macOS, or WSL

Run these commands in the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python -m grpc_tools.protoc -I proto --python_out=src/spaceport_hivemind/generated proto/bazaar.proto
```

If your Dev Container already created `.venv`, activate it and run the install and generation commands above.

### Windows PowerShell

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e '.[dev]'
.\.venv\Scripts\python.exe -m grpc_tools.protoc -I proto --python_out=src/spaceport_hivemind/generated proto/bazaar.proto
```

Use Python 3.11 or newer. The PowerShell commands use the virtual environment directly, so activation is unnecessary.

## 3. Create your private credentials file

The Dev Container creates this file automatically. For a manual installation, copy the included template:

```bash
cp credentials.example.json my-credentials.json
```

In PowerShell:

```powershell
Copy-Item credentials.example.json my-credentials.json
```

Open `my-credentials.json` in your editor. Replace **all three placeholders**:

```json
{
  "hivemind_key": "PASTE_SHARED_HIVEMIND_KEY_HERE",
  "players": [
    {
      "station_id": "YOUR_STATION_ID_HERE",
      "token": "PASTE_YOUR_PLANET_TOKEN_HERE"
    }
  ]
}
```

For example, a team assigned `P09` replaces `YOUR_STATION_ID_HERE` with `P09`. Its token must be the token issued specifically for P09.

Keep the double quotes and valid JSON formatting. `hivemind_key` belongs at the top level, outside `players`. No environment variable or `.env` file is required.

`my-credentials.json` is ignored by Git. Keep it private; do not edit the example file to contain real credentials or share another team's token.

## 4. Connect to both services

Stop any other client using your assigned station, including a standalone Spaceport_Bazaar client. A second connection can replace the first.

Run this from the repository root, replacing `P01` with the station ID in your credentials file:

```bash
hivemind-client --credentials ./my-credentials.json --station P01 --endpoint wss://bazaar-game-kamalv.apps.cloudapps.unc.edu/ws --hivemind-endpoint wss://hivemind-kamalv.apps.cloudapps.unc.edu
```

On Windows PowerShell, use:

```powershell
.\.venv\Scripts\hivemind-client.exe --credentials ./my-credentials.json --station P01 --endpoint wss://bazaar-game-kamalv.apps.cloudapps.unc.edu/ws --hivemind-endpoint wss://hivemind-kamalv.apps.cloudapps.unc.edu
```

To use the teacher's server, replace only the game `--endpoint` URL and the planet credentials with those supplied by the teacher. Keep the hosted `--hivemind-endpoint` above. The teacher's server must be online and use the compatible Bazaar Protobuf protocol.

Successful startup includes:

```text
INFO P01 connected to Bazaar
INFO P01 connected to Hivemind
```

The client reads the shared key from `my-credentials.json`. The optional `--hivemind-key` argument overrides that value if supplied.

## 5. Wait for the gamemaster to start

Keep the terminal running. The client sends readiness and forwards observations automatically. After the game starts, Hivemind can send commands that your client executes automatically; this is not a suggestions-only mode.

For our engine:

- [Engine dashboard](https://bazaar-game-kamalv.apps.cloudapps.unc.edu/)
- [Hivemind dashboard](https://hivemind-kamalv.apps.cloudapps.unc.edu/)

The gamemaster selects surplus and tick speed, then presses **Start simulation**. Our engine starts with the currently connected, ready planets, with a minimum of two. Participants are fixed for that run; participating teams can reconnect. The planet display slider only changes visibility, not participation.

On the teacher's server, the teacher controls when and how the game starts.

Press `Ctrl+C` to disconnect. Disconnecting during a run stops Hivemind's ability to execute trades through your client.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| `hivemind-client: command not found` | Activate `.venv` and repeat the install command, or use `.venv/bin/hivemind-client` directly. On Windows use the executable path above. |
| Missing generated bindings | Run the `grpc_tools.protoc` command from step 2. |
| No token for your station | Make `--station` match the JSON `station_id` exactly and replace the placeholder. |
| Game authentication rejected | Check that the planet token belongs to that station on that specific game server. |
| Hivemind closes with `unauthorized` | Check the top-level `hivemind_key`. Use a client version that includes JSON key support. |
| Connection refused, timeout, or DNS error | Confirm the endpoint, server availability, and any required school network/VPN access. |
| Connected but ticks do not advance | Wait for the gamemaster. Our engine requires an explicit Start action and at least two ready planets. |
| Planet is not participating in the current run | Join before the next run starts; unused slots cannot join a run already in progress. |
| Session replaced or superseded | Ensure only one client is using your station credentials. |

Our hosted engine's planet tokens persist across restarts and deployments. Its simulation state is in memory, so a restart resets the run. The teacher's token and restart behavior may differ.

## Updating your client

After new changes are published to the repository, stop the client, then run:

```bash
git pull
python -m pip install -e '.[dev]'
python -m grpc_tools.protoc -I proto --python_out=src/spaceport_hivemind/generated proto/bazaar.proto
```

Use the activated virtual environment, or the explicit Windows Python path from step 2. Then reconnect with the same private credentials file. Changes made only in someone else's local workspace are not included in a fresh clone until published.
