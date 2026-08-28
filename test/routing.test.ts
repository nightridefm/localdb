// Wide-lane routing: which reader a query is sent to.
//
// This is correctness, not cache locality. A cross-database query sent to
// a reader that did not ATTACH the file fails with "no such table", so the
// lane arithmetic has to be exact - and it is the one part of the attach
// work that browser smoke can only catch by luck.
//
// LocalDb takes its workers by injection, so the REAL client runs here
// against fakes: no browser, no OPFS, no sqlite. Everything asserted below
// is the shipped routing path, not a reimplementation of it.

import assert from 'node:assert/strict';

import type { FromReader, ToReader } from '../src/protocol.ts';
import type { LocalDbConfig, ModelMeta } from '../src/types.ts';

import { LocalDb } from '../src/client/LocalDb.ts';

class FakeWorker {
  onmessage: ((e: MessageEvent<FromReader>) => void) | null = null;
  onerror: ((e: ErrorEvent) => void) | null = null;
  sent: ToReader[] = [];
  postMessage(m: ToReader): void {
    this.sent.push(m);
  }
  terminate(): void {}
  deliver(m: FromReader): void {
    this.onmessage?.({ data: m } as MessageEvent<FromReader>);
  }
}
const asWorker = (w: FakeWorker) => w as unknown as Worker;

const model = (over: Partial<ModelMeta> = {}): ModelMeta => ({
  table: 't',
  primaryKey: ['id'],
  columns: ['id'],
  dateColumns: [],
  boolColumns: [],
  jsonColumns: [],
  listColumns: [],
  ddl: '',
  ...over,
});

type Harness = { db: LocalDb; writer: FakeWorker; readers: FakeWorker[] };

/** Build a client and drive it to topology-ready. `boot:false` leaves it queueing. */
function harness(
  cfg: Partial<LocalDbConfig> = {},
  connMode: 'multi' | 'baton' | 'single' = 'multi',
  boot = true
): Harness {
  const writer = new FakeWorker();
  const readers: FakeWorker[] = [];
  const db = new LocalDb(
    {
      dbName: 'project.db',
      schemaSql: '',
      schemaVersion: 1,
      shapes: [],
      readers: 4,
      ...cfg,
    },
    {
      writer: asWorker(writer),
      spawnReader: () => {
        const w = new FakeWorker();
        readers.push(w);
        return asWorker(w);
      },
    }
  );
  const h = { db, writer, readers };
  if (boot) bootReady(h, connMode);
  return h;
}

function bootReady(h: Harness, connMode: 'multi' | 'baton' | 'single'): void {
  h.writer.deliver({ t: 'ready', wiped: false, connMode } as unknown as FromReader);
  if (connMode !== 'single') for (const r of h.readers) r.deliver({ t: 'r-ready' });
}

const ATTACH = [{ alias: 'tenant', dbName: 'tenant.db' }];
const tick = () => new Promise((r) => setTimeout(r, 0));

/** Reader indexes that received a subscribe for each of `sqls`. */
function subIndexes(h: Harness, sqls: string[]): number[] {
  return sqls.map((sql) => {
    const i = h.readers.findIndex((r) =>
      r.sent.some((m) => m.t === 'subscribe' && m.spec.sql === sql)
    );
    assert.notEqual(i, -1, `no reader received: ${sql}`);
    return i;
  });
}

// Cross-db queries whose FIRST from/join table is LOCAL, so routingTable()
// hashes on a different table for each one. If the lane were not applied
// these would scatter across the pool - which the control case below
// asserts they actually do.
const LOCALS = ['link', 'item', 'note', 'part', 'cell', 'edge'];
const crossSql = (t: string) => `SELECT x FROM ${t} l JOIN tenant.upload u ON u.id = l.upload_id`;
const plainSql = (t: string) => `SELECT x FROM ${t} l JOIN upload u ON u.id = l.upload_id`;

// ── r-init: only the wide readers are told to attach ────────────────────
{
  for (const [attachReaders, readerCount, wantWide] of [
    [undefined, 4, 1], // default
    [2, 4, 2],
    [9, 2, 2], // clamped to the pool size
    [0, 3, 1], // floored at 1
  ] as const) {
    const h = harness({
      readers: readerCount,
      attach: ATTACH,
      ...(attachReaders === undefined ? {} : { attachReaders }),
    });
    const withAttach = h.readers.map((r) =>
      r.sent.some((m) => m.t === 'r-init' && m.attach !== undefined)
    );
    assert.deepEqual(
      withAttach,
      Array.from({ length: readerCount }, (_, i) => i < wantWide),
      `attachReaders=${attachReaders} readers=${readerCount}`
    );
    h.db.destroy();
  }
}
console.log('r-init: attach reaches exactly the first K readers ok');

// ── r-init carries the MERGED models, writer keeps its own ──────────────
{
  const h = harness({
    attach: [{ alias: 'tenant', dbName: 'tenant.db', models: { Upload: model() } }],
    models: { Link: model() },
  });
  const init = h.readers[0]!.sent.find((m) => m.t === 'r-init')!;
  assert.deepEqual(Object.keys(init.models!).sort(), ['Link', 'tenant:Upload']);
  const winit = h.writer.sent.find((m) => (m as { t: string }).t === 'init') as unknown as {
    config: { models: Record<string, ModelMeta> };
  };
  assert.deepEqual(Object.keys(winit.config.models), ['Link'], 'writer gets only its own models');
  h.db.destroy();
}
console.log('r-init: merged models to readers, own models to writer ok');

// ── subscriptions: cross-db confined to the lane ────────────────────────
{
  const h = harness({ readers: 4, attach: ATTACH, attachReaders: 1 });
  const sqls = LOCALS.map(crossSql);
  for (const sql of sqls) h.db.subscribe({ sql }, () => {});
  assert.deepEqual(
    subIndexes(h, sqls),
    sqls.map(() => 0),
    'every cross-db subscription must land on the single wide reader'
  );
  h.db.destroy();
}
{
  // CONTROL: the same corpus without the alias scatters over the pool, so
  // the assertion above is really the lane and not an artefact of hashing.
  const h = harness({ readers: 4, attach: ATTACH, attachReaders: 1 });
  const sqls = LOCALS.map(plainSql);
  for (const sql of sqls) h.db.subscribe({ sql }, () => {});
  const idx = subIndexes(h, sqls);
  assert.ok(new Set(idx).size > 1, `single-db queries should spread, got ${JSON.stringify(idx)}`);
  assert.ok(
    idx.every((i) => i >= 0 && i < 4),
    'and stay inside the pool'
  );
  h.db.destroy();
}
{
  const h = harness({ readers: 4, attach: ATTACH, attachReaders: 2 });
  const sqls = LOCALS.map(crossSql);
  for (const sql of sqls) h.db.subscribe({ sql }, () => {});
  const idx = subIndexes(h, sqls);
  assert.ok(
    idx.every((i) => i < 2),
    `attachReaders=2 confines to readers 0-1, got ${JSON.stringify(idx)}`
  );
  h.db.destroy();
}
console.log('subscribe: cross-db confined to the wide lane (with scatter control) ok');

// ── unsubscribe must reach the reader that holds the entry ──────────────
{
  const h = harness({ readers: 4, attach: ATTACH, attachReaders: 1 });
  const sql = crossSql('link');
  const off = h.db.subscribe({ sql }, () => {});
  const subOn = subIndexes(h, [sql])[0]!;
  off();
  const unsubOn = h.readers.findIndex((r) => r.sent.some((m) => m.t === 'unsubscribe'));
  assert.equal(unsubOn, subOn, 'unsubscribe must reach the subscribing reader (registry leak)');
  h.db.destroy();
}
console.log('unsubscribe: routed to the same reader ok');

// ── queued-before-ready subscriptions route the same way ────────────────
{
  const h = harness({ readers: 4, attach: ATTACH, attachReaders: 1 }, 'multi', false);
  const sqls = LOCALS.map(crossSql);
  for (const sql of sqls) h.db.subscribe({ sql }, () => {});
  assert.ok(
    h.readers.every((r) => !r.sent.some((m) => m.t === 'subscribe')),
    'nothing dispatched before topology is ready'
  );
  bootReady(h, 'multi');
  assert.deepEqual(
    subIndexes(h, sqls),
    sqls.map(() => 0),
    'outbox replay honours the lane too'
  );
  h.db.destroy();
}
console.log('outbox replay: lane honoured ok');

// ── one-shot batches: mixed batch splits by lane ────────────────────────
{
  const h = harness({ readers: 4, attach: ATTACH, attachReaders: 1 });
  const cross = LOCALS.slice(0, 3).map(crossSql);
  const plain = LOCALS.slice(3).map(plainSql);
  for (const sql of [...cross, ...plain]) void h.db.query(sql).catch(() => {});
  await tick();
  const batched = h.readers.map((r) =>
    r.sent.filter((m) => m.t === 'query').flatMap((m) => m.queries.map((q) => q.sql))
  );
  for (let i = 1; i < batched.length; i++) {
    assert.ok(
      batched[i]!.every((s) => !cross.includes(s)),
      `reader ${i} is not wide and must get no cross-db one-shot`
    );
  }
  assert.deepEqual([...batched[0]!].filter((s) => cross.includes(s)).sort(), [...cross].sort());
  assert.ok(
    batched.slice(1).some((b) => b.length > 0),
    'single-db members still spread across the pool'
  );
  h.db.destroy();
}
console.log('one-shot: mixed batch split by lane ok');

// ── analyze goes somewhere that can prepare cross-db SQL ────────────────
{
  const h = harness({ readers: 4, attach: ATTACH, attachReaders: 1 });
  void h.db.analyze().catch(() => {});
  void h.db.analyze().catch(() => {});
  const gotAnalyze = h.readers.map((r) => r.sent.filter((m) => m.t === 'analyze').length);
  assert.deepEqual(gotAnalyze, [2, 0, 0, 0], 'analyze must not round-robin onto narrow readers');
  h.db.destroy();
}
console.log('analyze: pinned to a wide reader ok');

// ── no attach configured: dispatch is exactly as before ─────────────────
{
  const h = harness({ readers: 4 });
  const sqls = LOCALS.slice(0, 4).map(plainSql);
  for (const sql of sqls) void h.db.query(sql).catch(() => {});
  await tick();
  const counts = h.readers.map((r) =>
    r.sent.filter((m) => m.t === 'query').reduce((n, m) => n + m.queries.length, 0)
  );
  assert.deepEqual(counts, [1, 1, 1, 1], '4 one-shots split one per reader, as before attach');
  // analyze still round-robins when nothing is attached.
  void h.db.analyze().catch(() => {});
  void h.db.analyze().catch(() => {});
  const an = h.readers.map((r) => r.sent.filter((m) => m.t === 'analyze').length);
  assert.equal(
    an.filter((n) => n > 0).length,
    2,
    'without attach, analyze round-robins over the pool'
  );
  h.db.destroy();
}
console.log('no-attach: dispatch unchanged ok');

// ── single mode: cross-db fails closed, single-db keeps working ─────────
{
  const h = harness({ readers: 4, attach: ATTACH }, 'single');
  assert.equal(h.db.getConnMode(), 'single');
  let crossErr: string | undefined;
  h.db.subscribe({ sql: crossSql('link') }, (r) => {
    crossErr = r.error;
  });
  h.db.subscribe({ sql: plainSql('link') }, () => {});
  const crossQ: Promise<string> = h.db.query(crossSql('item')).then(
    () => 'RESOLVED - a cross-db one-shot must reject in single mode',
    (e: Error) => e.message
  );
  await tick();
  assert.match(crossErr ?? '', /'single' connection mode/, 'cross-db subscription errors');
  assert.match(await crossQ, /'single' connection mode/, 'cross-db one-shot rejects');
  const toWriter = h.writer.sent.filter((m) => (m as { t: string }).t === 'subscribe');
  assert.equal(toWriter.length, 1, 'the single-db subscription still reaches the writer');
  assert.equal(
    (toWriter[0] as { spec: { sql: string } }).spec.sql,
    plainSql('link'),
    'and it is the single-db one'
  );
  h.db.destroy();
}
console.log('single mode: cross-db fails closed, single-db unaffected ok');

console.log('routing.test.ts: all assertions passed');
