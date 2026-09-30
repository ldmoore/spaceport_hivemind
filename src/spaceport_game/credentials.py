import json
import secrets
from pathlib import Path

def load_or_create(path: Path, station_ids: tuple[str, ...]) -> dict[str, str]:
    if path.exists():
        value = json.loads(path.read_text())
        result = {}
        for player in value["players"]:
            station, token = player["station_id"], player["token"]
            if not isinstance(station, str) or not isinstance(token, str) or not token.strip():
                raise ValueError("Invalid station credential")
            if station in result or token in result.values():
                raise ValueError("Duplicate station or token in credential file")
            result[station] = token
        if not all(station in result for station in station_ids):
            raise ValueError("Credential file is missing configured stations")
        return result
    result = {station: secrets.token_urlsafe(24) for station in station_ids}
    path.write_text(json.dumps({"players": [{"station_id": s, "token": t} for s, t in result.items()]}, indent=2) + "\n")
    return result
