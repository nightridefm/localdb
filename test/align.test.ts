// alignCommits: every live query runs on one reader, so a tick reads them
// all in one snapshot and they are delivered in one render. The REAL
// client runs against fake workers (as in routing.test.ts); the effect on
// screen is measured by localsync's browser smoke and collab soak.

import assert from 'node:assert/strict';

import type { FromReader, ToReader } from '../src/protocol.ts';
import type { LocalDbConfig } from '../src/types.ts';

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

function harness(cfg: Partial<LocalDbConfig>) {
  const writer = new FakeWorker();
  const readers: FakeWorker[] = [];
  const db = new LocalDb(
    { dbName: 'a.db', schemaSql: '', schemaVersion: 1, shapes: [], readers: 4, ...cfg },
    {
      writer: asWorker(writer),
      spawnReader: () => {
        const w = new FakeWorker();
        readers.push(w);
        return asWorker(w);
      },
    }
  );
  writer.deliver({ t: 'ready', wiped: false, connMode: 'multi' } as unknown as FromReader);
  for (const r of readers) r.deliver({ t: 'r-ready' });
  return { db, writer, readers };
}

const TABLES = ['task', 'comment', 'project', 'member', 'tag', 'note', 'part', 'edge'];
const subscribedOn = (readers: FakeWorker[]) =>
  readers.map((r) => r.sent.filter((m) => m.t === 'subscribe').length);

// ── on: every live query on reader 0, fence reads ───────────────────────
{
  const h = harness({ alignCommits: true, readMode: 'seqlock' });
  for (const t of TABLES) h.db.subscribe({ sql: `SELECT * FROM ${t}` }, () => {});
  const counts = subscribedOn(h.readers);
  assert.deepEqual(counts, [TABLES.length, 0, 0, 0], `subscriptions spread: ${counts}`);
  for (const r of h.readers) {
    const init = r.sent.find((m) => m.t === 'r-init') as { readMode?: string } | undefined;
    assert.equal(init?.readMode, 'fence', 'aligned readers must read under the fence');
  }
  console.log('alignCommits: all live queries on one reader, fence reads: ok');
}

// ── off: spread by table affinity, as before ────────────────────────────
{
  const h = harness({ readMode: 'seqlock' });
  for (const t of TABLES) h.db.subscribe({ sql: `SELECT * FROM ${t}` }, () => {});
  const used = subscribedOn(h.readers).filter((n) => n > 0).length;
  assert.ok(used > 1, 'control: without alignCommits subscriptions should spread');
  const init = h.readers[0]!.sent.find((m) => m.t === 'r-init') as { readMode?: string };
  assert.equal(init.readMode, 'seqlock');
  console.log('off: spread over the pool, configured read mode: ok');
}

console.log('align.test.ts: all assertions passed');
