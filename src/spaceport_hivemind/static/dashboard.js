'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const resources = ['water', 'food', 'components'];
const bundle = b => resources.filter(r => b[r]).map(r => `${b[r]} ${r}`).join(', ') || 'Nothing';
let latest;
let demoStarting = false;
function renderOffers() {
  const offers = [...latest.offers].reverse().filter(o => $('filter').value === 'ALL' || o.status === $('filter').value);
  $('offers').innerHTML = offers.length ? offers.map(o => `<tr><td>${esc(o.proposer_id)} → ${esc(o.recipient_id)}<small>${esc(o.offer_id)}</small></td><td>${esc(bundle(o.give))}<small>For ${esc(bundle(o.receive))}</small></td><td class="status">${esc(o.status)}</td></tr>`).join('') : '<tr><td colspan="3" class="muted">No offers to show.</td></tr>';
}
function render(data) {
  latest = data;
  const stations = data.stations;
  const demo = data.demo;
  $('run-demo').disabled = demoStarting || !demo.can_start;
  $('demo-stations').disabled = demoStarting || !demo.can_start;
  $('run-demo').textContent = demo.running ? '◌ Demo running…' : '▶ Run demo';
  $('demo-help').textContent = demo.running ? 'Watch the stations below' : !demo.can_start ? 'Disconnect station clients to run a demo' : '2–20 stations · 30 seconds';
  $('online').textContent = `${data.connected_stations} / ${data.automatic ? data.max_stations : data.expected_stations}`;
  $('waiting').textContent = data.automatic ? 'Automatic roster · 20-station capacity' : data.connected_stations >= data.expected_stations ? 'All expected bridges connected' : `${Math.max(0, data.expected_stations - data.connected_stations)} more needed`;
  $('tick').textContent = stations.length ? Math.max(...stations.map(s => s.tick)) : '—';
  $('phase').textContent = stations.length ? [...new Set(stations.map(s => s.phase))].join(' / ') : 'No active game';
  $('risk').textContent = stations.filter(s => s.failed_once || s.health < s.rules.max_health / 2 || resources.some(r => s.unmet_upkeep[r] > 0)).length;
  $('accepted').textContent = data.offers.filter(o => o.status === 'ACCEPTED').length;
  $('updated').textContent = `Updated ${new Date(data.generated_at * 1000).toLocaleTimeString()}`;
  $('notice').className = 'notice';
  $('notice').textContent = !stations.length ? 'Waiting for station telemetry. Connect your bridges to see the fleet here.' : !data.connected_stations ? 'All bridges are offline. Showing the last reported game state.' : stations.every(s => s.phase === 'FINISHED') ? 'Game finished. Final station states and recent trade history are shown below.' : `Receiving station telemetry. ${data.pending_stations.length ? 'Awaiting first snapshots from ' + data.pending_stations.join(', ') + '.' : 'Each station reports its own latest tick.'}`;
  if (demo.running) $('notice').textContent = `Demo running: ${data.expected_stations} stations are trading through Hivemind. The run finishes after 30 ticks.`;
  else if (demo.showing_demo && demo.status === 'finished') $('notice').textContent = demo.success ? 'Demo complete. All stations survived. Run again to start a fresh game.' : 'Demo complete. Some stations did not survive. Final results are shown below.';
  else if (demo.showing_demo && demo.status === 'failed') $('notice').textContent = 'The demo could not finish. You can try again.';
  if (stations.length) $('stations').innerHTML = stations.map(s => {
    const danger = s.failed_once || s.health < s.rules.max_health / 2 || resources.some(r => s.unmet_upkeep[r] > 0);
    const age = s.last_seen ? Math.max(0, Math.floor(data.generated_at - s.last_seen)) : null;
    return `<article class="station ${!s.online ? 'offline' : ''} ${danger ? 'danger' : ''}"><div class="station-head"><h3>${esc(s.station_id)}</h3><span class="pill">${s.online ? '● ONLINE' : '○ OFFLINE'}</span></div><p class="station-meta">${esc(s.run_id)} · Tick ${s.tick}</p><div class="health-label"><span>${s.failed_once ? 'Station failed' : 'Station health'}</span><span>${s.health} / ${s.rules.max_health}</span></div><progress max="${s.rules.max_health}" value="${s.health}" aria-label="${esc(s.station_id)} health"></progress><div class="resources">${resources.map(r => `<div class="${s.unmet_upkeep[r] ? 'short' : ''}"><small>${r[0].toUpperCase()+r.slice(1)}</small><b>${s.inventory[r]}</b><small>Upkeep ${s.upkeep[r]} / tick</small>${s.unmet_upkeep[r] ? `<small>Short ${s.unmet_upkeep[r]}</small>` : ''}</div>`).join('')}</div><p class="station-bottom">${esc(s.phase)} · Last snapshot ${age === null ? 'unknown' : age + 's ago'}</p></article>`;
  }).join('');
  renderOffers();
  $('events').innerHTML = data.events.length ? data.events.map(e => `<div class="event"><time>${esc(new Date(e.time * 1000).toLocaleTimeString([], {hour12:false}))}</time><div>${esc(e.station_id)} <span class="pill">${esc(e.kind.toUpperCase())}</span><small>${esc(e.detail)}</small></div></div>`).join('') : '<p class="muted">Connection and trade activity will appear here.</p>';
}
$('run-demo').addEventListener('click', async () => {
  if (!$('demo-stations').reportValidity()) return;
  const stationCount = Number($('demo-stations').value);
  demoStarting = true;
  $('run-demo').disabled = true;
  $('demo-message').textContent = 'Starting demo…';
  try {
    const result = await new Promise((resolve, reject) => {
      const socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/demo/control`);
      let settled = false;
      const finish = (error, value) => {
        if (settled) return;
        settled = true; clearTimeout(timer); socket.close();
        error ? reject(error) : resolve(value);
      };
      const timer = setTimeout(() => finish(new Error('Request timed out. Check the demo status before trying again.')), 8000);
      socket.onopen = () => socket.send(JSON.stringify({action:'start', station_count:stationCount}));
      socket.onmessage = event => {
        try { finish(null, JSON.parse(event.data)); } catch (_) { finish(new Error('Unexpected server response.')); }
      };
      socket.onerror = () => finish(new Error('Could not reach the demo service. Please try again.'));
      socket.onclose = () => finish(new Error('Demo connection closed. Please try again.'));
    });
    $('demo-message').textContent = result.message;
  } catch (error) {
    $('demo-message').textContent = error.message;
  } finally {
    demoStarting = false;
  }
});
$('filter').addEventListener('change', () => { if (latest) renderOffers(); });
async function poll() {
  try {
    const response = await fetch('/api/state', {cache:'no-store', signal:AbortSignal.timeout(5000)});
    if (!response.ok) throw new Error('Server unavailable');
    render(await response.json());
    $('connection').textContent = '● LIVE CONNECTION'; $('connection').className = 'connection';
  } catch (_) {
    $('connection').textContent = '○ RECONNECTING'; $('connection').className = 'connection error';
    $('notice').className = 'notice warning';
    $('notice').textContent = 'Connection interrupted. Displayed data may be out of date. Retrying automatically…';
  } finally { setTimeout(poll, 1000); }
}
poll();
