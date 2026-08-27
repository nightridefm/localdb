// End-to-end smoke: boots LocalDb against the mock Electric endpoint served
// by harness/server.js, subscribes three queries, and records every event so
// the headless driver can assert on window.__events.

import { LocalDb } from '../src/index.ts';

type Ev = [string, ...unknown[]];
const events: Ev[] = [];
const w = globalThis as unknown as {
  __events: Ev[];
  __db: LocalDb;
  __error: string | null;
};
w.__events = events;
w.__error = null;

const SCHEMA = `
	CREATE TABLE IF NOT EXISTS upload (
		id TEXT PRIMARY KEY,
		title TEXT NOT NULL,
		plays INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_upload_plays ON upload(plays);
`;

const db = new LocalDb(
  {
    dbName: 'smoke.db',
    schemaSql: SCHEMA,
    schemaVersion: 1,
    shapes: [{ table: 'upload', primaryKey: ['id'], url: '/shape/upload' }],
    readers: 2,
  },
  {
    writer: new Worker('/smoke/dist/writer.worker.js', { type: 'module' }),
    spawnReader: () => new Worker('/smoke/dist/reader.worker.js', { type: 'module' }),
  }
);
w.__db = db;

db.onSyncStatus((s) => events.push(['status', s.state]));

db.subscribe<{ id: string; title: string; plays: number }>(
  { sql: 'SELECT id, title, plays FROM upload ORDER BY plays DESC, id LIMIT 5' },
  (r) => events.push(['top5', r.version, r.rows.length, r.rows[0]?.plays ?? null])
);

db.subscribe<{ n: number }>({ sql: 'SELECT count(*) AS n FROM upload' }, (r) =>
  events.push(['count', r.version, r.rows[0]?.n])
);

// Constant query: dirtied by every commit (no extractable deps) but its
// result never changes - the hash dedupe must deliver it exactly once.
db.subscribe<{ one: number }>({ sql: 'SELECT 1 AS one' }, (r) => events.push(['const', r.version]));

// One-shot (loader-path) query once sync is live.
const un = db.onSyncStatus((s) => {
  if (s.state === 'live') {
    un();
    void db
      .query<{ n: number }>('SELECT count(*) AS n FROM upload')
      .then((rows) => events.push(['oneshot', rows[0]?.n]))
      .catch((e) => events.push(['oneshot-error', String(e)]));
  }
});
