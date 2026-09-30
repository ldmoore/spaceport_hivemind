const $ = selector => document.querySelector(selector);
const slider = $('#planet-count');
let state = null;
let busy = false;
function node(tag, className, text) {
  const el = document.createElement(tag);
  el.className = className;
  if (text !== undefined) el.textContent = text;
  return el;
}
function render() {
  if (!state) return;
  const stations = state.stations || [];
  slider.max = Math.max(1, stations.length);
  const count = Math.min(Number(slider.value), stations.length);
  $('#count-value').textContent = count;
  $('#phase').textContent = state.phase;
  $('#surplus').disabled = busy || state.phase !== 'READY';
  if (state.phase !== 'READY') $('#surplus').value = state.surplus_percent;
  $('#budget-summary').textContent = Object.entries(state.resource_budget || {}).map(([r, b]) => `${r}: ${b.available} available / ${b.required} required (${b.balance >= 0 ? '+' : ''}${b.balance})`).join(' · ');
  if (!$('#tick-speed').matches(':focus') && !$('#tick-speed').dataset.edited) $('#tick-speed').value = state.tick_duration_ms / 1000;
  $('#tick').textContent = `${state.tick} / ${state.duration_ticks}`;
  $('#online').textContent = `${stations.filter(s => s.online).length} / ${stations.length}`;
  $('#start').disabled = busy || state.phase !== 'READY';
  $('#view-note').textContent = `Showing ${count} of ${stations.length} planets`;
  // Enlarge the orbit with the population so cards never pile up or get clipped.
  const size = Math.max(800, count * 67);
  const radius = (size - 190) / 2;
  $('#stations').style.width = `${size}px`;
  $('#stations').style.height = `${size}px`;
  const fragment = document.createDocumentFragment();
  stations.slice(0, count).forEach((station, index) => {
    const angle = index / count * Math.PI * 2 - Math.PI / 2;
    const specialty = ['water', 'food', 'components'].includes(station.specialty) ? station.specialty : 'water';
    const health = Math.max(0, Math.min(100, Number(station.health) || 0));
    const card = node('article', `planet ${specialty}${station.failed ? ' failed' : ''}`);
    card.style.left = `${size / 2 + Math.cos(angle) * radius}px`;
    card.style.top = `${size / 2 + Math.sin(angle) * radius}px`;
    const sphere = node('div', 'sphere');
    sphere.setAttribute('aria-hidden', 'true');
    card.append(sphere, node('h3', '', station.station_id));
    card.append(node('span', `connection ${station.online ? 'connected' : ''}`, station.online ? 'Online' : 'Offline'));
    card.append(node('div', 'planet-meta', `Tick ${state.tick} · Health ${health}%`));
    const meter = node('div', 'health-meter');
    meter.setAttribute('role', 'meter');
    meter.setAttribute('aria-label', `${station.station_id} health`);
    meter.setAttribute('aria-valuenow', health);
    meter.setAttribute('aria-valuemin', '0');
    meter.setAttribute('aria-valuemax', '100');
    const fill = node('span', ''); fill.style.width = `${health}%`; meter.append(fill); card.append(meter);
    const inventory = station.inventory || {};
    card.append(node('div', 'resources', `W ${inventory.water ?? 0}  ·  F ${inventory.food ?? 0}  ·  C ${inventory.components ?? 0}`));
    if (station.production_per_tick && Object.keys(station.production_per_tick).length) {
      card.append(node('div', 'resources', 'Produces ' + Object.entries(station.production_per_tick).filter(([,v]) => v > 0).map(([r,v]) => `${v.toFixed(2)} ${r}/tick`).join(', ')));
    }
    fragment.append(card);
  });
  $('#planets').replaceChildren(fragment);
}
async function refresh() {
  try {
    const response = await fetch('/api/state', {cache: 'no-store'});
    if (!response.ok) throw new Error('State unavailable');
    state = await response.json(); render(); $('#notice').textContent = '';
  } catch {
    $('#notice').textContent = 'Connection interrupted. Retrying…'; $('#start').disabled = true;
  }
}
slider.addEventListener('input', render);
$('#start').addEventListener('click', async () => {
  busy = true; render();
  try {
    await new Promise((resolve, reject) => {
      const socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/control/start`, 'bazaar.protobuf.v2');
      let replied = false;
      const timer = setTimeout(() => { socket.close(); reject(new Error('Start request timed out')); }, 8000);
      socket.onopen = () => socket.send(JSON.stringify({surplus_percent: Number($('#surplus').value)}));
      socket.onmessage = event => {
        replied = true; clearTimeout(timer);
        const result = JSON.parse(event.data); socket.close();
        if (result.error) reject(new Error(result.error)); else resolve();
      };
      socket.onclose = () => { clearTimeout(timer); if (!replied) reject(new Error('Unable to connect to engine')); };
    });
    await refresh();
  } catch (error) { $('#notice').textContent = error.message; }
  finally { busy = false; render(); }
});
refresh(); setInterval(refresh, 2000);

$('#tick-speed').addEventListener('input', () => { $('#tick-speed').dataset.edited = 'true'; });
$('#apply-speed').addEventListener('click', () => {
  const input = $('#tick-speed');
  if (!input.reportValidity() || !input.value) return;
  const button = $('#apply-speed'); button.disabled = true;
  const socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/control/tick-speed`, 'bazaar.protobuf.v2');
  let replied = false;
  const timer = setTimeout(() => socket.close(), 8000);
  socket.onopen = () => socket.send(JSON.stringify({tick_duration_ms: Math.round(Number(input.value) * 1000)}));
  socket.onmessage = event => {
    const result = JSON.parse(event.data); replied = true;
    $('#speed-status').textContent = result.error || `Applied: ${result.tick_duration_ms / 1000}s per tick`;
    if (!result.error) { state.tick_duration_ms = result.tick_duration_ms; delete input.dataset.edited; }
    socket.close();
  };
  socket.onclose = () => { clearTimeout(timer); button.disabled = false; if (!replied) $('#speed-status').textContent = 'Unable to apply speed. Try again.'; };
});

$('#surplus').addEventListener('change', () => {
  const value = Number($('#surplus').value);
  $('#budget-note').textContent = value < 0 ? 'Total supply is below upkeep. Reserves and health may delay deaths; survival is not guaranteed.' : value > 0 ? 'Extra resources provide a buffer. Planets still need timely trades to survive.' : 'Starting stock + production equals full-run upkeep for each resource. Trade timing still matters.';
});
