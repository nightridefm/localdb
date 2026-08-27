// Pattern validation page: priority lanes, visibility throttling, index
// advisor, and row-level delta shipping - all against the mock shape
// server. readers:1 pins every subscription to one reader so within-tick
// ordering (priority lanes) is observable via delivery order.

import { LocalDb } from '../src/index.ts';

type Ev = [string, ...unknown[]];
const events: Ev[] = [];
const w = globalThis as unknown as { __events: Ev[]; __db: LocalDb; __error: string | null };
w.__events = events;
w.__error = null;

const SCHEMA = `
	CREATE TABLE IF NOT EXISTS upload (
		id TEXT PRIMARY KEY,
		title TEXT NOT NULL,
		plays INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_up_plays ON upload(plays);
	CREATE INDEX IF NOT EXISTS idx_up_title ON upload(title);
`;

const pauseSync = new URLSearchParams(location.search).get('pausesync') === '1';

const db = new LocalDb(
  {
    dbName: pauseSync ? 'patterns-ps.db' : 'patterns.db',
    pauseSyncWhenHidden: pauseSync,
    schemaSql: SCHEMA,
    schemaVersion: 1,
    shapes: [{ table: 'upload', primaryKey: ['id'], url: '/shape/upload' }],
    readers: 1,
    advise: true,
    fts: [{ table: 'upload', columns: ['title'] }],
  },
  {
    writer: new Worker('/smoke/dist/writer.worker.js', { type: 'module' }),
    spawnReader: () => new Worker('/smoke/dist/reader.worker.js', { type: 'module' }),
  }
);
w.__db = db;

db.onSyncStatus((s) => events.push(['status', s.state, Date.now()]));
db.onAdvice((a) => events.push(['advice', a.notes.join(' / ')]));

// Priority: one visible + two background queries. Within a tick the
// visible one must be DELIVERED first (batch order = execution order).
db.subscribe<{ n: number }>(
  { sql: 'SELECT count(*) AS n FROM upload', priority: 'background' },
  (r) => events.push(['bg-count', r.version, r.rows[0]?.n, Date.now()])
);
db.subscribe<{ s: number }>(
  { sql: 'SELECT sum(plays) AS s FROM upload', priority: 'background' },
  (r) => events.push(['bg-sum', r.version, r.rows[0]?.s, Date.now()])
);
db.subscribe<{ id: string }>(
  { sql: 'SELECT id, title, plays FROM upload ORDER BY plays DESC LIMIT 3' },
  (r) => events.push(['vis-top3', r.version, Date.now()])
);

// Advisor bait: WHERE on an unindexed column - must produce a SCAN note.
db.subscribe<{ id: string }>({ sql: "SELECT id FROM upload WHERE title = 'nope' LIMIT 1" }, (r) =>
  events.push(['bait', r.version])
);

// FTS5 trigram mirror: indexed substring search over titles. Invalidated
// by commits to `upload` via the fts->base dep mapping; maintained by
// triggers after hydration.
db.subscribe<{ n: number }>(
  { sql: "SELECT count(*) AS n FROM upload_fts WHERE title MATCH 'title'" },
  (r) => events.push(['fts', r.version, r.rows[0]?.n, Date.now()])
);

// Delta mode: full ordered list keyed by id. Events record payload size so
// the bench can compare full-result vs delta-patch bytes.
db.subscribe<{ id: string; title: string; plays: number }>(
  { sql: 'SELECT id, title, plays FROM upload ORDER BY id', delta: true, deltaKey: 'id' },
  (r) => events.push(['delta-list', r.version, r.rows.length, r.byteLength, Date.now()])
);

// Full mode twin of the same list - the byteLength comparison baseline.
db.subscribe<{ id: string }>(
  { sql: 'SELECT id, title, plays FROM upload ORDER BY id, title' },
  (r) => events.push(['full-list', r.version, r.rows.length, r.byteLength, Date.now()])
);
