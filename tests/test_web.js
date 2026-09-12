'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../web/index.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const GiB = 2 ** 30;
const NOW = 1_800_000_000_000;
const decode = text => String(text).replace(/&(?:amp|lt|gt|quot|#39);/g,
  entity => ({'&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&#39;': "'"})[entity]);

// Minimal browser boundary only: the dashboard's real script handles fetching,
// events, rendering, storage and animation; no production functions are mocked.
class Element {
  constructor(id = '') {
    this.id = id;
    this.style = {};
    this.dataset = {};
    this.className = '';
    this.markup = '';
    this.text = '';
    this.children = [];
    this.clientWidth = 800;
    this.clientHeight = 300;
    this.classList = {
      add: name => { this.className = [...new Set([...this.className.split(' '), name])].join(' ').trim(); },
      remove: name => { this.className = this.className.split(' ').filter(n => n !== name).join(' '); },
      contains: name => this.className.split(' ').includes(name),
    };
  }
  set textContent(value) { this.text = String(value); this.markup = ''; this.children = []; }
  get textContent() { return this.text || decode(this.markup.replace(/<[^>]*>/g, '')); }
  set innerHTML(value) {
    this.markup = String(value); this.text = ''; this.children = [];
    for (const match of this.markup.matchAll(/<button\b([^>]*)>([\s\S]*?)<\/button>/g)) {
      const child = new Element();
      child.className = match[1].match(/class="([^"]*)"/)?.[1] || '';
      child.dataset.id = decode(match[1].match(/data-id="([^"]*)"/)?.[1] || '');
      child.innerHTML = match[2];
      this.children.push(child);
    }
  }
  get innerHTML() { return this.markup || this.text; }
  querySelectorAll(selector) { return selector === '.spark-tab' ? this.children : []; }
  click() { assert.equal(typeof this.onclick, 'function', `click handler for ${this.id || this.dataset.id}`); this.onclick(); }
}
function state(id, extra = {}) {
  return {
    spark_id: id, spark_label: id, fetched_at: NOW / 1000, last_success_at: NOW / 1000,
    mem: {total: 128 * GiB, available: 100 * GiB, free: 90 * GiB},
    disk: {total: 4e12, used: 1e12}, gpu: {utilization_gpu: 0, temperature_gpu: 40, power_draw_w: 12},
    ollama_ps: {models: []}, ollama_tags: {models: []}, hf_models: [], docker: [],
    vllm_models: {data: []}, llamacpp_models: {data: []}, sglang_models: {data: []},
    ...extra,
  };
}
function payload(states, sparks = states.map(s => ({id: s.spark_id, label: s.spark_label || s.spark_id, mode: 'remote'}))) {
  return {sparks, states, units: 'GiB', headroom_gib: 2};
}
async function dashboard(initial, {search = '', storage = {}} = {}) {
  const elements = new Map([...html.matchAll(/\bid="([^"]+)"/g)].map(m => [m[1], new Element(m[1])]));
  const draws = [];
  const ctx = new Proxy({}, {get(target, prop) { return target[prop] ?? ((...args) => draws.push([prop, ...args])); }});
  elements.get('c').getContext = () => ctx;
  const frames = [];
  const timers = new Map();
  let timerId = 0;
  let requestCount = 0;
  const events = new Map();
  let now = NOW;
  const values = new Map(Object.entries(storage));
  const queue = [initial];
  const location = {search, protocol: 'http:', origin: 'http://dashboard.test', href: `http://dashboard.test/${search}`};
  const sandbox = {
    console, URL, URLSearchParams, AbortSignal,
    Date: class extends Date { constructor(...args) { super(...(args.length ? args : [now])); } static now() { return now; } },
    document: {getElementById: id => { assert.ok(elements.has(id), `known element ${id}`); return elements.get(id); }, addEventListener(name, fn) { events.set(name, fn); }},
    window: {addEventListener(name, fn) { events.set(name, fn); }, devicePixelRatio: 1}, location,
    history: {replaceState(_state, _title, url) { const parsed = new URL(url, location.href); location.href = parsed.href; location.search = parsed.search; }},
    localStorage: {getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, String(value))},
    fetch: async () => {
      requestCount++;
      assert.ok(queue.length, 'a response was supplied');
      const next = await queue.shift();
      if (next instanceof Error) throw next;
      return {ok: true, status: 200, json: async () => next};
    },
    setTimeout(fn, ms) { const id = ++timerId; timers.set(id, {fn, ms}); return id; },
    clearTimeout(id) { timers.delete(id); },
    requestAnimationFrame(fn) { frames.push(fn); },
  };
  const context = vm.createContext(sandbox);
  vm.runInContext(script, context, {filename: 'web/index.html'});
  const settle = () => new Promise(resolve => setImmediate(resolve));
  await settle();
  return {
    el: id => elements.get(id), values, location, draws,
    text: id => elements.get(id).textContent,
    tab(id) { return elements.get('spark-tabs').children.find(btn => btn.dataset.id === id); },
    select(id) { const tab = this.tab(id); assert.ok(tab, `tab ${id}`); tab.click(); },
    async poll(data) { queue.push(data); elements.get('refresh-btn').click(); await settle(); },
    startPoll(data) { queue.push(data); elements.get('refresh-btn').click(); },
    event(name) { events.get(name)(); },
    requests: () => requestCount,
    refreshTimers: () => [...timers.values()].filter(t => t.ms === 10000),
    settle,
    advance(ms) { now += ms; },
    tick(ms) { now += ms; for (const [id, timer] of [...timers]) if (timer.ms <= ms) { timers.delete(id); timer.fn(); } },
    frame() { draws.length = 0; const batch = frames.splice(0); batch.forEach(fn => fn()); return draws; },
    inspect(expression) { return vm.runInContext(expression, context); },
  };
}

test('metadata interpolated into idle and fit notes is escaped', async () => {
  const data = {...payload([state('spark-88de')]), units: 'GiB<img src=x onerror=alert(1)>', headroom_gib: '<svg onload=alert(1)>'};
  const app = await dashboard(data);
  for (const id of ['list-hot', 'room-note', 'spark-tabs']) assert.doesNotMatch(app.el(id).innerHTML, /<img|<svg/, id);
  await app.poll({...data, states: [state('spark-88de', {ollama_tags: {models: [{name: 'huge', size: 200 * GiB}]}})]});
  for (const id of ['list-fit', 'room-note']) assert.doesNotMatch(app.el(id).innerHTML, /<img|<svg/, id);
});

test('cached API reads never reset the selected sample age', async () => {
  const data = payload([
    state('spark-2bee', {last_success_at: NOW / 1000 - 20}),
    state('spark-88de', {last_success_at: undefined, fetched_at: NOW / 1000 - 90}),
  ]);
  const app = await dashboard(data);
  assert.equal(app.text('last-updated'), 'last success 20s ago');
  app.advance(10000);
  await app.poll(data);
  assert.equal(app.text('last-updated'), 'last success 30s ago');
  app.select('spark-88de');
  assert.equal(app.text('last-updated'), 'last success 1m 40s ago');
  await app.poll(payload([data.states[0], {...data.states[1], stale: true, error: 'timeout'}]));
  assert.equal(app.text('last-updated'), 'last success unknown', 'failed fetch time must not substitute for missing success time');
});

test('initial collector failure without a sample is unavailable, not stale last-success data', async () => {
  const app = await dashboard(payload([{
    spark_id: 'spark-88de', error: 'SSH failed', stale: true, last_success_at: null, pending: false,
  }]));
  assert.equal(app.text('active-model-badge'), 'Data unavailable');
  assert.equal(app.text('room-note'), 'Data unavailable: SSH failed');
  assert.equal(app.text('status-text'), 'error · spark-88de');
  assert.equal(app.el('status-dot').className, 'status-dot error');
  assert.match(app.tab('spark-88de').textContent, /error.*memory unavailable/s);
  assert.equal(app.text('last-updated'), 'awaiting first sample');
  assert.equal(app.el('sample-notice').style.display, 'none');
  for (const id of ['status-text', 'spark-tabs', 'last-updated', 'sample-notice']) {
    assert.doesNotMatch(app.text(id), /stale|last success/i, id);
  }
  app.tick(1000);
  assert.equal(app.text('last-updated'), 'awaiting first sample');
  assert.equal(app.text('sample-notice'), '');
});

test('API failure marks retained samples stale even when switching nodes or units', async () => {
  const data = payload([state('spark-2bee'), state('spark-88de', {last_success_at: undefined, fetched_at: NOW / 1000 - 30})]);
  const app = await dashboard(data);
  await app.poll(new Error('network down'));
  app.select('spark-88de');
  assert.match(app.text('status-text'), /stale · spark-88de/i);
  assert.match(app.text('sample-notice'), /STALE.*last success 30s ago.*server/s);
  assert.equal(app.text('m-ram'), '28.0G');
  app.el('units-btn').click();
  assert.match(app.text('status-text'), /stale/i);
  assert.doesNotMatch(app.tab('spark-88de').textContent, /online/);
  await app.poll(data);
  assert.equal(app.text('status-text'), 'online · spark-88de');
  assert.equal(app.el('sample-notice').style.display, 'none');
});

test('switching sampled nodes clears previous-node animation before repainting', async () => {
  const a = state('spark-2bee');
  const b = state('spark-88de', {stale: true, error: 'timeout', mem: {total: 128 * GiB, available: 120 * GiB}});
  const app = await dashboard(payload([a, b]));
  await app.poll(payload([{...a, ollama_ps: {models: [{name: 'ONLY-ON-2BEE', size: GiB}]}}, b]));
  assert.ok(app.inspect('particles.length') > 0);
  app.draws.length = 0;
  app.select('spark-88de');
  assert.equal(app.inspect('particles.length'), 0);
  assert.ok(app.draws.some(call => call[0] === 'clearRect'));
  assert.equal(app.inspect('BLOCKS.some(b=>b.kind === "model")'), false);
  assert.equal(app.text('m-ram'), '8.0G');
});

test('focus, clicks and queued timers share one in-flight refresh and one next timer', async () => {
  const data = payload([state('spark-2bee')]);
  const app = await dashboard(data);
  const scheduled = app.refreshTimers()[0].fn;
  let release;
  app.startPoll(new Promise(resolve => { release = resolve; }));
  app.el('refresh-btn').click();
  app.event('focus');
  app.event('online');
  app.event('visibilitychange');
  scheduled();
  assert.equal(app.requests(), 2, 'initial request plus one shared request');
  release(data);
  await app.settle();
  assert.equal(app.refreshTimers().length, 1);
  await app.poll(data);
  assert.equal(app.requests(), 3, 'subsequent refresh still works');
  assert.equal(app.refreshTimers().length, 1);
});

test('tab metadata and unavailable-state errors render as inert text', async () => {
  const id = 'spark-88de" data-evil="yes';
  const label = '<img src=x onerror=alert(1)> & "Idle"';
  const mode = '<svg onload=alert(1)>';
  const error = '<img src=x onerror=alert(2)> & "SSH failed"';
  const app = await dashboard(payload([{spark_id: id, error}], [{id, label, mode}]));
  const markup = app.el('spark-tabs').innerHTML;
  assert.doesNotMatch(markup, /<img|<svg|data-id="spark-88de" data-evil=/);
  assert.ok(app.tab(id), 'escaped identifier round-trips through dataset');
  assert.match(app.tab(id).textContent, /<img src=x onerror=alert\(1\)> & "Idle"/);
  app.select(id);
  assert.equal(app.text('product-badge'), label);
  for (const target of ['list-hot', 'list-fit', 'list-ollama', 'list-hf', 'list-engines']) {
    assert.doesNotMatch(app.el(target).innerHTML, /<img/);
    assert.match(app.text(target), /<img src=x onerror=alert\(2\)> & "SSH failed"/);
  }
  assert.equal(new URLSearchParams(app.location.search).get('spark'), id);
});

test('probe errors and their recovery cannot fabricate load events for any engine', async () => {
  for (const eng of ['ollama', 'vllm', 'llamacpp', 'sglang']) {
    const loaded = name => state('spark-2bee', eng === 'ollama'
      ? {ollama_ps: {models: [{name, size: GiB}]}}
      : {[`${eng}_models`]: {data: [{id: name, _size_gib: 1}]}});
    const app = await dashboard(payload([loaded('before')]));
    const failed = state('spark-2bee', eng === 'ollama'
      ? {ollama_ps: {models: [], error: 'timeout'}}
      : {[eng]: [{models: {error: 'timeout'}}], [`${eng}_models`]: {data: []}});
    await app.poll(payload([failed]));
    assert.equal(app.inspect('(swapLogById[activeId] || []).length'), 0, `${eng} unknown is not unloaded`);
    await app.poll(payload([loaded('after')]));
    assert.equal(app.inspect('(swapLogById[activeId] || []).length'), 0, `${eng} recovery resets baseline`);
    await app.poll(payload([loaded('changed')]));
    assert.equal(app.inspect('swapLogById[activeId].length'), 2, `${eng} healthy tracking resumes`);
  }
});

test('unknown model probes are disclosed instead of proving no models are loaded', async () => {
  for (const probes of [
    {ollama_ps: {error: 'not installed', models: []}, vllm: [{models: {error: 'connection refused'}}]},
    {ollama_ps: undefined, vllm_models: undefined, llamacpp_models: undefined, sglang_models: undefined},
  ]) {
    const app = await dashboard(payload([state('spark-88de', {...probes, ollama_tags: {error: 'unavailable'}})]));
    assert.equal(app.text('active-model-badge'), 'No models detected');
    assert.match(app.text('active-model-sub'), /detection incomplete.*probes unavailable/i);
    assert.match(app.text('list-hot'), /detection incomplete/i);
    assert.match(app.text('list-engines'), /probes unavailable/i);
    assert.match(app.text('list-ollama'), /catalog unavailable/i);
    assert.doesNotMatch(app.text('active-model-sub'), /no models loaded|empty/i);
  }
});

test('an empty catalog is distinct from models that do not fit or are already loaded', async () => {
  const app = await dashboard(payload([state('spark-88de')]));
  assert.match(app.text('list-fit'), /No catalogued models found/i);
  assert.match(app.text('room-note'), /No catalogued models found/i);
  assert.doesNotMatch(app.text('list-fit'), /no.*fits|too big/i);
  await app.poll(payload([state('spark-88de', {ollama_tags: {models: [{name: 'huge', size: 200 * GiB}]}})]));
  assert.match(app.text('list-fit'), /No catalogued model clearly fits/i);
  assert.doesNotMatch(app.text('list-fit'), /No catalogued models found/i);
  await app.poll(payload([state('spark-88de', {
    ollama_tags: {models: [{name: 'small', size: GiB}]}, ollama_ps: {models: [{name: 'small', size: GiB}]},
  })]));
  assert.match(app.text('list-fit'), /All catalogued models already detected/i);
});

test('idle memory is measured system/other, never empty RAM or GPU', async () => {
  for (const used of [6.4, 3]) {
    const app = await dashboard(payload([state('spark-88de', {mem: {total: 128 * GiB, available: (128 - used) * GiB}})]));
    assert.equal(app.text('active-model-badge'), 'No models detected');
    assert.match(app.text('list-hot'), /No models detected/);
    assert.match(app.text('room-note'), new RegExp(`system/other ${used.toFixed(1)} GiB`));
    assert.equal(app.text('m-ram'), `${used.toFixed(1)}G`);
    assert.ok(app.inspect('BLOCKS.some(b=>b.kind === "system")'), 'measured memory remains on canvas');
    const visible = ['list-hot', 'room-note', 'active-model-badge', 'active-model-sub'].map(id => app.text(id)).join(' ');
    assert.doesNotMatch(visible, /GPU(?:\/RAM)? empty|memory empty|no models loaded|OS only/i);
    const canvasText = app.frame().filter(call => call[0] === 'fillText').map(call => call[1]).join(' ');
    assert.doesNotMatch(canvasText, /memory empty/i);
  }
});

test('zero available memory is not replaced by free memory anywhere', async () => {
  const app = await dashboard(payload([state('spark-88de', {mem: {total: 128 * GiB, available: 0, free: 90 * GiB}})]));
  assert.match(app.tab('spark-88de').textContent, /· 0\.0 GiB available/);
  assert.equal(app.text('vram-display'), '0.0G free');
  assert.equal(app.text('m-ram'), '128.0G');
  assert.equal(app.text('mem-pct'), '100.0%');
  assert.equal(app.el('mem-bar').style.width, '100%');
  assert.equal(app.inspect('occupancy.freeGiB'), 0);
  assert.equal(app.inspect('BLOCKS.filter(b=>b.kind === "free").length'), 0);
  await app.poll(payload([state('spark-88de', {mem: {total: 128 * GiB, available: null, free: 90 * GiB}})]));
  assert.equal(app.text('vram-display'), '90.0G free');
});

test('load events require consecutive healthy samples, including after gaps and recovery', async () => {
  const loaded = name => state('spark-2bee', {ollama_ps: {models: [{name, size: GiB}]}});
  const app = await dashboard(payload([loaded('one')]));
  assert.equal(app.inspect('(swapLogById[activeId] || []).length'), 0, 'first observation is a baseline, not a load event');
  await app.poll(payload([loaded('two')]));
  assert.equal(app.inspect('swapLogById[activeId].length'), 2, 'healthy transition reports load and unload');
  for (const gap of [
    {spark_id: 'spark-2bee', error: 'SSH failed'},
    {...loaded('untrusted'), stale: true, error: 'timeout'},
    {spark_id: 'spark-2bee', pending: true},
    null,
    new Error('network offline'),
  ]) {
    const before = app.inspect('swapLogById[activeId].length');
    await app.poll(gap instanceof Error ? gap : payload(gap ? [gap] : [], [{id: 'spark-2bee'}]));
    assert.equal(app.inspect('swapLogById[activeId].length'), before, 'gap cannot unload');
    await app.poll(payload([loaded('recovered')]));
    assert.equal(app.inspect('swapLogById[activeId].length'), before, 'recovery only resets baseline');
    await app.poll(payload([loaded('two')]));
    assert.equal(app.inspect('swapLogById[activeId].length'), before + 2, 'healthy tracking resumes');
  }
});

test('stale nodes retain only their own last data with an aging prominent warning', async () => {
  const a = state('spark-2bee', {ollama_ps: {models: [{name: 'ONLY-ON-2BEE', size: 20 * GiB}]}});
  const b = state('spark-88de', {
    mem: {total: 120 * GiB, available: 80 * GiB},
    vllm_models: {data: [{id: 'ONLY-ON-88DE', _size_gib: 30}]},
    stale: true, error: 'SSH timed out', last_success_at: NOW / 1000 - 90,
  });
  const app = await dashboard(payload([a, b]));
  app.select('spark-88de');
  assert.equal(app.text('m-ram'), '40.0G');
  assert.match(app.text('active-model-badge'), /ONLY-ON-88DE/);
  assert.doesNotMatch(app.text('list-hot'), /ONLY-ON-2BEE/);
  assert.match(app.text('status-text'), /stale · spark-88de/i);
  assert.match(app.text('last-updated'), /last success 1m 30s ago/i);
  assert.match(app.text('sample-notice'), /STALE.*last success 1m 30s ago.*SSH timed out/s);
  assert.notEqual(app.el('sample-notice').style.display, 'none');
  assert.doesNotMatch(app.text('list-engines'), /LIVE/);
  assert.match(app.tab('spark-88de').textContent, /stale.*last success 1m 30s ago/s);
  app.tick(1000);
  assert.match(app.text('last-updated'), /1m 31s ago/);
  assert.doesNotMatch(app.text('last-updated'), /updated/);
  assert.deepEqual(Array.from(app.inspect('occupancy.models.map(m=>m.name)')), ['ONLY-ON-88DE']);
  app.select('spark-2bee');
  assert.equal(app.el('sample-notice').style.display, 'none');
  assert.match(app.text('active-model-badge'), /ONLY-ON-2BEE/);
});

test('unavailable nodes clear previous node values, inventory and canvas immediately', async () => {
  for (const unavailable of [{pending: true}, {error: 'SSH failed'}, null]) {
    const a = state('spark-2bee', {ollama_ps: {models: [{name: 'ONLY-ON-2BEE', size: 20 * GiB}]}});
    const b = unavailable && {spark_id: 'spark-88de', ...unavailable};
    const data = payload(b ? [a, b] : [a], [{id: a.spark_id}, {id: 'spark-88de'}]);
    const app = await dashboard(data);
    assert.match(app.text('active-model-badge'), /ONLY-ON-2BEE/);
    app.draws.length = 0;
    app.select('spark-88de');
    for (const id of ['m-ram', 'm-disk', 'm-gpu', 'm-gpu-sub', 'm-count', 'vram-display', 'mem-used-label', 'mem-total-label', 'mem-pct', 'disk-used-label', 'disk-total-label', 'disk-pct', 'ollama-count', 'hf-count']) {
      assert.equal(app.text(id), '—', `${JSON.stringify(unavailable)} clears ${id}`);
    }
    assert.equal(app.el('mem-bar').style.width, '0%');
    assert.equal(app.el('disk-bar').style.width, '0%');
    assert.match(app.text('active-model-badge'), /pending|unavailable/i);
    for (const id of ['active-model-sub', 'room-note', 'list-hot', 'list-fit', 'swap-log']) assert.doesNotMatch(app.text(id), /ONLY-ON-2BEE/);
    assert.equal(app.inspect('occupancy.models.length'), 0);
    assert.equal(app.inspect('particles.length'), 0);
    assert.ok(app.draws.some(call => call[0] === 'clearRect'), 'canvas cleared synchronously');
    assert.equal(app.frame().filter(call => call[0] === 'arcTo').length, 0, 'no capacity blocks painted for missing memory');
    await app.poll(data);
    assert.ok(app.tab('spark-88de').classList.contains('active'), 'missing state does not switch away from selected metadata');
  }
});

test('spark deep links take precedence and tab selection persists across reloads', async () => {
  const data = payload([state('spark-2bee'), {spark_id: 'spark-88de', pending: true}]);
  const app = await dashboard(data, {search: '?spark=spark-88de&api=http%3A%2F%2Fdashboard.test', storage: {dgx_smd_spark: 'spark-2bee'}});
  assert.ok(app.tab('spark-88de').classList.contains('active'));
  assert.equal(app.values.get('dgx_smd_spark'), 'spark-88de');
  app.select('spark-2bee');
  assert.equal(app.values.get('dgx_smd_spark'), 'spark-2bee');
  assert.equal(new URLSearchParams(app.location.search).get('spark'), 'spark-2bee');
  assert.equal(new URLSearchParams(app.location.search).get('api'), 'http://dashboard.test');
  const restored = await dashboard(data, {storage: {dgx_smd_spark: 'spark-88de'}});
  assert.ok(restored.tab('spark-88de').classList.contains('active'));
  const invalid = await dashboard(data, {search: '?spark=missing', storage: {dgx_smd_spark: 'spark-88de'}});
  assert.ok(invalid.tab('spark-88de').classList.contains('active'));
  const fallback = await dashboard(data, {search: '?spark=missing', storage: {dgx_smd_spark: 'removed'}});
  assert.ok(fallback.tab('spark-2bee').classList.contains('active'));
});

test('selected node owns the status and product header immediately', async () => {
  const a = state('spark-2bee');
  const b = {spark_id: 'spark-88de', pending: true};
  const meta = [{id: a.spark_id, label: 'Main', mode: 'remote'}, {id: b.spark_id, label: 'Idle', mode: 'remote'}];
  const app = await dashboard(payload([a, b], meta));
  app.select('spark-88de');
  assert.match(app.text('status-text'), /pending · Idle/i);
  assert.equal(app.text('product-badge'), 'Idle');
  assert.doesNotMatch(app.el('status-dot').className, /online/);
  await app.poll(payload([a, {...b, pending: false, error: 'SSH failed'}], meta));
  assert.match(app.text('status-text'), /error · Idle/i);
  app.select('spark-2bee');
  assert.equal(app.text('status-text'), 'online · Main');
  assert.equal(app.text('product-badge'), 'Main');
  assert.match(app.el('status-dot').className, /online/);
});

test('unit toggle rerenders every tab immediately and persists', async () => {
  const data = payload([state('spark-2bee'), state('spark-88de')]);
  const app = await dashboard(data);
  app.el('units-btn').click();
  for (const id of ['spark-2bee', 'spark-88de']) {
    assert.match(app.tab(id).textContent, /107\.4 GB available \/ 137\.4 GB total/);
  }
  assert.equal(app.values.get('dgx_smd_units'), 'GB');
  const restored = await dashboard(data, {storage: Object.fromEntries(app.values)});
  assert.match(restored.tab('spark-88de').textContent, /107\.4 GB available/);
});

test('tabs count detected models from all four engines', async () => {
  const app = await dashboard(payload([state('spark-2bee', {
    ollama_ps: {models: [{name: 'ollama-model', size: GiB}]},
    vllm_models: {data: [{id: 'vllm-model', _size_gib: 1}]},
    llamacpp_models: {data: [{id: 'llama-model', _size_gib: 1}]},
    sglang_models: {data: [{id: 'sglang-model', _size_gib: 1}]},
  })]));
  assert.match(app.tab('spark-2bee').textContent, /4 (?:hot|detected)/);
});

test('tabs label available and total memory with node health', async () => {
  const app = await dashboard(payload([state('spark-2bee'), {spark_id: 'spark-88de', pending: true}]));
  assert.match(app.tab('spark-2bee').textContent, /100\.0 GiB available \/ 128\.0 GiB total/);
  assert.match(app.tab('spark-2bee').textContent, /online/i);
  assert.match(app.tab('spark-88de').textContent, /pending/i);
});
