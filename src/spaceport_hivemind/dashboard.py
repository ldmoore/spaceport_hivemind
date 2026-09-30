"""Live telemetry and a bounded demo controller on the bridge's port."""

from collections import deque
from dataclasses import asdict
from importlib.resources import files
from time import time
from urllib.parse import urlsplit
import json
import asyncio
import logging
import secrets
from websockets.exceptions import ConnectionClosed
from .coordinator import MAX_STATIONS


class Dashboard:
    def __init__(self, server):
        self.server = server
        self.events = deque(maxlen=100)
        self.seen = {}
        self.demo_server = None
        self.demo_task = None
        self.demo_status = "idle"
        self.demo_success = None
        self.assets = {
            "/": ("index.html", "text/html; charset=utf-8"),
            "/dashboard.js": ("dashboard.js", "text/javascript; charset=utf-8"),
            "/dashboard.css": ("dashboard.css", "text/css; charset=utf-8"),
        }

    def event(self, kind, station_id, detail):
        self.events.appendleft(dict(time=time(), kind=kind, station_id=station_id, detail=detail))

    def observed(self, planet):
        # Coordinator rejects out-of-order snapshots; don't report them as fresh.
        if self.server.coordinator.planets.get(planet.station_id) is planet:
            self.seen[planet.station_id] = time()

    @property
    def demo_running(self):
        return self.demo_task is not None and not self.demo_task.done()

    def start_demo(self, *, station_count=3, tick_duration_ms=1000):
        # No await between the admission check and task creation: concurrent
        # clicks cannot start multiple games or replace connected bridges.
        if type(station_count) is not int or not 2 <= station_count <= MAX_STATIONS:
            return dict(ok=False, message="Choose between 2 and 20 demo stations.")
        if self.demo_running:
            return dict(ok=False, message="A demo is already running.")
        if self.server.clients:
            return dict(ok=False, message="Disconnect station clients before starting a demo.")
        from .server import HivemindServer
        from .coordinator import Coordinator, CoordinatorConfig
        self.demo_server = HivemindServer(Coordinator(CoordinatorConfig(expected_station_count=station_count)), secrets.token_urlsafe(32))
        self.demo_status = "running"
        self.demo_success = None
        self.demo_task = asyncio.create_task(self._run_demo(station_count, tick_duration_ms))
        return dict(ok=True, message=f"Demo started. {station_count} stations will run for 30 ticks.")

    async def _run_demo(self, station_count, tick_duration_ms):
        from .demo import run_demo
        try:
            result = await asyncio.wait_for(run_demo(
                station_ids=tuple(f"P{i:02}" for i in range(1, station_count + 1)),
                duration_ticks=30, tick_duration_ms=tick_duration_ms,
                timeout_seconds=40, hive=self.demo_server,
            ), timeout=50)
            self.demo_success = result.success
            self.demo_status = "finished"
        except asyncio.CancelledError:
            self.demo_status = "cancelled"
            raise
        except Exception:
            logging.getLogger(__name__).exception("Dashboard demo failed")
            self.demo_status = "failed"

    async def control(self, socket):
        # The browser sends one explicit start request, never credentials.
        try:
            raw = await asyncio.wait_for(socket.recv(), 5)
            try:
                command = json.loads(raw)
            except (ValueError, TypeError):
                command = None
            if not isinstance(command, dict) or command.get("action") != "start":
                response = dict(ok=False, message="Unknown demo action.")
            else:
                response = self.start_demo(station_count=command.get("station_count", 3))
            await socket.send(json.dumps(response))
        except (TimeoutError, OSError, ConnectionClosed):
            pass
        finally:
            await socket.close()

    def snapshot(self):
        if self.demo_server is not None:
            value = self.demo_server.dashboard.snapshot()
        else:
            value = self._telemetry()
        value["demo"] = dict(status=self.demo_status, running=self.demo_running,
                             can_start=not self.demo_running and not self.server.clients,
                             success=self.demo_success, showing_demo=self.demo_server is not None)
        return value

    def _telemetry(self):
        server = self.server
        stations = []
        offers = {}
        planets = sorted(server.coordinator.planets.values(), key=lambda p: (p.tick, self.seen.get(p.station_id, 0)))
        for planet in planets:
            data = asdict(planet)
            data.pop("offers")
            data.update(online=planet.station_id in server.clients, last_seen=self.seen.get(planet.station_id))
            stations.append(data)
            for offer in planet.offers:
                offers[(planet.run_id, offer.offer_id)] = dict(asdict(offer), run_id=planet.run_id)
        known = {p.station_id for p in planets}
        return dict(
            generated_at=time(), expected_stations=server.coordinator.config.expected_station_count,
            max_stations=MAX_STATIONS, automatic=server.coordinator.config.expected_station_count == 0,
            connected_stations=len(server.clients),
            pending_stations=sorted(set(server.clients) - known),
            stations=sorted(stations, key=lambda p: p["station_id"]),
            offers=list(offers.values())[-100:], events=list(self.events),
        )

    def http(self, connection, request):
        path = urlsplit(request.path).path
        if request.headers.get("Upgrade", "").lower() == "websocket":
            if path == "/demo/control":
                origin = urlsplit(request.headers.get("Origin", ""))
                if origin.scheme not in ("http", "https") or origin.netloc != request.headers.get("Host"):
                    return connection.respond(403, "Same-origin demo requests only\n")
                return None
            if path in ("/", "/ws"):
                return None
            return connection.respond(404, "Not found\n")
        content_type = "text/plain; charset=utf-8"
        if path == "/api/state":
            body = json.dumps(self.snapshot(), separators=(",", ":"))
        elif path == "/healthz":
            body = "ok\n"
        elif path in self.assets:
            name, content_type = self.assets[path]
            body = files("spaceport_hivemind").joinpath("static", name).read_text()
        else:
            return connection.respond(404, "Not found\n")
        if path == "/api/state":
            content_type = "application/json"
        response = connection.respond(200, body)
        del response.headers["Content-Type"]
        response.headers["Content-Type"] = content_type
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'"
        return response
