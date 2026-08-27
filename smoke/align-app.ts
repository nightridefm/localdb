// Cross-shape LSN alignment validation. Two shapes (upload, link) share a
// global LSN at the mock server; POST /shape/pair writes one row to EACH
// at the same LSN (a cross-table transaction). The upload stream is
// artificially delayed 400ms per response (_delay param), so WITHOUT
// alignment the link row commits ~400ms before its upload row and the
// orphan probe observes it. WITH alignment (?align=1) the link batch is
// held until the upload head reaches its LSN - orphans must never appear.

import { LocalDb } from '../src/index.ts';

type Ev = [string, ...unknown[]];
const events: Ev[] = [];
const w = globalThis as unknown as { __events: Ev[]; __db: LocalDb; __error: string | null };
w.__events = events;
w.__error = null;

const align = new URLSearchParams(location.search).get('align') === '1';

const SCHEMA = `
	CREATE TABLE IF NOT EXISTS upload (
		id TEXT PRIMARY KEY,
		title TEXT NOT NULL,
		plays INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS link (
		id TEXT PRIMARY KEY,
		upload_id TEXT NOT NULL,
		position INTEGER NOT NULL
	);
`;

const db = new LocalDb(
  {
    dbName: align ? 'align-on.db' : 'align-off.db',
    schemaSql: SCHEMA,
    schemaVersion: 1,
    shapes: [
      {
        table: 'upload',
        primaryKey: ['id'],
        url: '/shape/upload',
        params: { _delay: '400' },
      },
      { table: 'link', primaryKey: ['id'], url: '/shape/link' },
    ],
    readers: 1,
    alignShapes: align ? { maxSkewMs: 2000 } : false,
    // Tight tick so the orphan window (if any) cannot be missed.
    writeCoalesceMs: 10,
  },
  {
    writer: new Worker('/smoke/dist/writer.worker.js', { type: 'module' }),
    spawnReader: () => new Worker('/smoke/dist/reader.worker.js', { type: 'module' }),
  }
);
w.__db = db;

db.onSyncStatus((s) => events.push(['status', s.state, Date.now()]));

// The skew probe: link rows whose upload row is missing locally.
db.subscribe<{ orphans: number }>(
  {
    sql: `SELECT count(*) AS orphans FROM link l LEFT JOIN upload u ON u.id = l.upload_id WHERE u.id IS NULL`,
    minIntervalMs: 0,
  },
  (r) => events.push(['orphans', r.version, r.rows[0]?.orphans, Date.now()])
);

db.subscribe<{ n: number }>({ sql: 'SELECT count(*) AS n FROM link' }, (r) =>
  events.push(['links', r.version, r.rows[0]?.n, Date.now()])
);
