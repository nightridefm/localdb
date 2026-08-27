// Simulates the read-service <-> registry contract after the hash-aware
// change: the "engine" hashes rows and materializes text only when the
// caller's prevHashOf disagrees, exactly as engine.execRows now does.
import assert from 'node:assert/strict';

import { hashBytes } from '../src/protocol.ts';
import { QueryRegistry } from '../src/worker/registry.ts';

const enc = new TextEncoder();
const reg = new QueryRegistry();

// --- full (non-delta) path: recordRun with engine-computed hash ---
{
  const e = reg.subscribe('kq', { sql: 'SELECT 1' }, new Set(['t']));
  const payload = enc.encode('[{"a":1}]');
  const h1 = hashBytes(payload);
  // engine: prevHash null on first run -> bytes shipped
  let out = reg.recordRun(e, h1, payload.slice(), 1, 100);
  assert.ok(out.bytes, 'first run ships');
  assert.equal(out.version, 1);
  // unchanged re-run: engine suppressed the copy (hash === lastHash)
  out = reg.recordRun(e, h1, null, 2, 200);
  assert.equal(out.bytes, undefined, 'unchanged run ships nothing');
  assert.equal(out.version, 1);
  assert.equal(e.dirty, false);
  // raced reset: subscribe() zeroed lastHash after the engine already
  // suppressed against the OLD hash -> must flag dirty, ship nothing
  e.lastHash = 0;
  out = reg.recordRun(e, h1, null, 3, 300);
  assert.equal(out.bytes, undefined);
  assert.equal(e.dirty, true, 'raced reset re-marks dirty');
  // follow-up run (prevHash now 0 -> engine materializes)
  out = reg.recordRun(e, h1, payload.slice(), 3, 400);
  assert.ok(out.bytes, 'post-reset run ships full result');
  assert.equal(out.version, 2);
  console.log('recordRun: ship/suppress/raced-reset ok');
}

// --- delta path ---
type Row = [string, number, string | null];
// "engine": rows materialized only when prev disagrees
function engineRows(data: Array<[string, string]>, prev: Map<string, number> | null): Row[] {
  return data.map(([k, text]) => {
    const h = hashBytes(enc.encode(text));
    const changed = !prev || prev.get(k) !== h;
    return [k, h, changed ? text : null];
  });
}
const dec = new TextDecoder();
type Doc = { k: string[] | null; c: Record<string, unknown> };
const parse = (b: Uint8Array): Doc => JSON.parse(dec.decode(b)) as Doc;

{
  const e = reg.subscribe('kd', { sql: 'SELECT 2', delta: true }, new Set(['t']));
  const d1: Array<[string, string]> = [
    ['a', '{"id":"a","v":1}'],
    ['b', '{"id":"b","v":2}'],
    ['c', '{"id":"c","v":3}'],
  ];
  // tick 1: initial - prev captured before run, all rows ship
  let prev = e.rowHashes;
  let out = reg.recordDeltaRun(e, engineRows(d1, prev), prev, 1, 100);
  assert.ok(out.bytes);
  let doc = parse(out.bytes!);
  assert.deepEqual(doc.k, ['a', 'b', 'c']);
  assert.equal(Object.keys(doc.c).length, 3, 'initial ships all rows');

  // tick 2: unchanged - nothing ships
  prev = e.rowHashes;
  out = reg.recordDeltaRun(e, engineRows(d1, prev), prev, 2, 200);
  assert.equal(out.bytes, undefined, 'unchanged delta ships nothing');

  // tick 3: one row changes in place - only that row, k:null
  const d2: Array<[string, string]> = [
    ['a', '{"id":"a","v":1}'],
    ['b', '{"id":"b","v":99}'],
    ['c', '{"id":"c","v":3}'],
  ];
  prev = e.rowHashes;
  out = reg.recordDeltaRun(e, engineRows(d2, prev), prev, 3, 300);
  assert.ok(out.bytes);
  doc = parse(out.bytes!);
  assert.equal(doc.k, null, 'same order -> k:null');
  assert.deepEqual(Object.keys(doc.c), ['b'], 'only changed row shipped');
  assert.deepEqual(doc.c['b'], { id: 'b', v: 99 });

  // tick 4: pure removal - k list ships, no changed rows
  const d3 = d2.filter(([k]) => k !== 'a');
  prev = e.rowHashes;
  out = reg.recordDeltaRun(e, engineRows(d3, prev), prev, 4, 400);
  assert.ok(out.bytes, 'removal ships');
  doc = parse(out.bytes!);
  assert.deepEqual(doc.k, ['b', 'c']);
  assert.equal(Object.keys(doc.c).length, 0);

  // tick 5: raced reset - rows computed against OLD map, then subscribe()
  // swapped in a fresh one before record
  const stale = e.rowHashes;
  const rows = engineRows(d3, stale);
  e.rowHashes = new Map();
  e.orderHash = 0;
  out = reg.recordDeltaRun(e, rows, stale, 5, 500);
  assert.equal(out.bytes, undefined, 'raced reset ships nothing');
  assert.equal(e.dirty, true, 'raced reset re-marks dirty');
  // follow-up run against the fresh empty map ships everything
  prev = e.rowHashes;
  out = reg.recordDeltaRun(e, engineRows(d3, prev), prev, 5, 600);
  assert.ok(out.bytes);
  doc = parse(out.bytes!);
  assert.deepEqual(doc.k, ['b', 'c']);
  assert.equal(Object.keys(doc.c).length, 2, 'post-reset ships full rows');
  console.log('recordDeltaRun: initial/unchanged/patch/removal/raced-reset ok');
}
console.log('REGISTRY DELTA LIFECYCLE PASSED');
