// Concurrency-bench orchestrator: seeds via the bench writer, runs reader
// workers through baseline and write phases, and measures
// reactive-delivery latency against commit notices.
const params = new URLSearchParams(location.search);
const READERS = Number(params.get('readers') || 2);
const SEED_ROWS = Number(params.get('seed') || 100000);
const MODE = params.get('mode') || 'fence';
// payload=N adds a live subscription returning N rows per reader, so the
// run measures WIRE cost (hash + transfer + parse of a real grid payload)
// and not just the scheduler with toy results.
const PAYLOAD = Number(params.get('payload') || 0);

const logEl = document.getElementById('log');
window.__progress = [];
window.__bench = null;
window.__error = null;
function log(msg) {
  window.__progress.push(`${new Date().toISOString().slice(11, 19)} ${msg}`);
  logEl.textContent = window.__progress.slice(-30).join('\n');
}
log(`concurrency bench: readers=${READERS} seed=${SEED_ROWS} mode=${MODE}`);

const results = { readers: READERS, seed_rows: SEED_ROWS, mode: MODE, payload_rows: PAYLOAD };
const writer = new Worker('./dist/writer.js', { type: 'module' });
const readers = [];
for (let i = 0; i < READERS; i++) readers.push(new Worker('./dist/reader.js', { type: 'module' }));

// Reactive-delivery latency: notice wall time per seq vs sub-results wall.
const noticeWall = new Map();
const deliveryLatencies = [];
const payloadLatencies = [];
const payloadBytes = [];
const bc = new BroadcastChannel('ldb:commits:bench.db');
bc.onmessage = (e) => noticeWall.set(e.data.seq, e.data.wall);

function waitMsg(w, type) {
  return new Promise((res, rej) => {
    const h = (e) => {
      if (e.data.type === type) {
        w.removeEventListener('message', h);
        res(e.data);
      } else if (e.data.type === 'error') rej(new Error(e.data.error));
    };
    w.addEventListener('message', h);
  });
}
for (const w of [writer, ...readers]) {
  w.addEventListener('message', (e) => {
    if (e.data.type === 'error') {
      window.__error = e.data.error;
      log('WORKER-ERROR ' + String(e.data.error).slice(0, 200));
    }
    if (e.data.type === 'progress') log(e.data.msg);
    else if (e.data.type === 'sub-results') {
      for (const item of e.data.items) {
        const wall = noticeWall.get(item.commitSeq);
        const isPayload = String(item.key).startsWith('payload:');
        if (wall) {
          deliveryLatencies.push(e.data.wall - wall);
          if (isPayload) payloadLatencies.push(e.data.wall - wall);
        }
        if (isPayload && item.bytes) payloadBytes.push(item.bytes);
      }
    }
  });
  w.addEventListener('error', (e) => {
    window.__error = String(e.message || 'worker error');
    log('WORKER ERROR ' + window.__error);
  });
}

async function run() {
  writer.postMessage({ cmd: 'init', seedRows: SEED_ROWS });
  const seeded = await waitMsg(writer, 'seeded');
  results.seed_ms = seeded.seed_ms;
  log(`seeded in ${seeded.seed_ms}ms`);

  for (const [i, r] of readers.entries())
    r.postMessage({ cmd: 'start', id: i, mode: MODE, payloadRows: PAYLOAD });
  await Promise.all(readers.map((r) => waitMsg(r, 'ready')));

  for (const r of readers) r.postMessage({ cmd: 'phase', name: 'baseline' });
  await new Promise((s) => setTimeout(s, 6000));

  for (const r of readers) r.postMessage({ cmd: 'phase', name: 'write' });
  writer.postMessage({ cmd: 'write', batchRows: 1000, intervalMs: 150, durationMs: 14000 });
  const wrote = await waitMsg(writer, 'wrote');
  results.writer = wrote.stats;

  for (const r of readers) r.postMessage({ cmd: 'stop' });
  const readerStats = await Promise.all(readers.map((r) => waitMsg(r, 'stats')));
  results.reader_stats = readerStats.map((m) => m.stats);

  writer.postMessage({ cmd: 'finalize' });
  const fin = await waitMsg(writer, 'finalized');
  results.integrity = fin.integrity;
  results.final_count = fin.count;

  const agg = (phase, field) =>
    results.reader_stats.reduce((a, s) => a + (s[phase]?.[field] || 0), 0);
  // Freshness-check iterations are 10% of the loop and excluded from
  // samples; scale qps so freshness checks do not distort throughput.
  results.baseline_agg_qps = Math.round(agg('baseline', 'n') / 0.9 / 6);
  results.write_agg_qps = Math.round(agg('write', 'n') / 0.9 / 14);
  results.staleness = results.reader_stats.reduce(
    (a, s) => ({
      checks: a.checks + s.staleness.checks,
      violations: a.violations + s.staleness.violations,
      maxShortfall: Math.max(a.maxShortfall, s.staleness.maxShortfall),
    }),
    { checks: 0, violations: 0, maxShortfall: 0 }
  );
  results.dv_lag = results.reader_stats.map((s) => s.dv_lag).filter(Boolean);
  if (payloadBytes.length) {
    const pl = [...payloadLatencies].sort((a, b) => a - b);
    results.payload = {
      deliveries: payloadBytes.length,
      bytes_each: Math.round(payloadBytes.reduce((a, b) => a + b, 0) / payloadBytes.length),
      total_mb: +(payloadBytes.reduce((a, b) => a + b, 0) / 1048576).toFixed(1),
      latency_p50_ms: pl.length ? pl[pl.length >> 1] : null,
      latency_p95_ms: pl.length ? pl[Math.floor(pl.length * 0.95)] : null,
      latency_max_ms: pl.length ? pl[pl.length - 1] : null,
    };
  }
  const dl = [...deliveryLatencies].sort((a, b) => a - b);
  results.reactive_delivery = dl.length
    ? {
        n: dl.length,
        p50_ms: dl[dl.length >> 1],
        p95_ms: dl[Math.floor(dl.length * 0.95)],
        max_ms: dl[dl.length - 1],
      }
    : null;

  window.__bench = results;
  log('DONE');
}
run().catch((e) => {
  window.__error = String((e && e.stack) || e);
  log('ERROR ' + window.__error);
});
