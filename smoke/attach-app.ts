// Cross-database ATTACH end-to-end. Two LocalDb instances over the mock
// Electric endpoint, deliberately partitioned the way the feature is meant
// to be used:
//
//   tenant.db  <- shape 'upload'   (shared reference data)
//   project.db <- shape 'link'     (per-project data), ATTACHes tenant.db
//
// The mock server's POST /shape/pair writes one upload row and one link
// row at the SAME global LSN - a cross-table transaction that here becomes
// a cross-FILE one, which is exactly the case alignShapes cannot cover.
//
// The PROJECT db is constructed FIRST, on purpose: its readers attach a
// tenant file that does not exist yet. That is the create:true path in
// openEngine, and if it regresses the whole page fails to boot.

import { LocalDb } from '../src/index.ts';

type Ev = [string, ...unknown[]];
const events: Ev[] = [];
const w = globalThis as unknown as {
  __events: Ev[];
  __project: LocalDb;
  __tenant: LocalDb;
  __error: string | null;
};
w.__events = events;
w.__error = null;

const params = new URLSearchParams(location.search);
const mode = params.get('mode') as 'multi' | 'baton' | 'single' | null;
// ?skew=400 delays the tenant stream's responses, widening the window in
// which a /shape/pair transaction is visible in one file but not the other.
const skew = Number(params.get('skew') || 0);
const readMode = (params.get('read') as 'fence' | 'seqlock' | null) ?? undefined;

const TENANT_DB = `attach-tenant-${mode ?? 'auto'}.db`;
const PROJECT_DB = `attach-project-${mode ?? 'auto'}.db`;

const workers = () => ({
  writer: new Worker('/smoke/dist/writer.worker.js', { type: 'module' }),
  spawnReader: () => new Worker('/smoke/dist/reader.worker.js', { type: 'module' }),
});

// ── project.db (attaches tenant) ────────────────────────────────────────
const project = new LocalDb(
  {
    dbName: PROJECT_DB,
    schemaSql: `CREATE TABLE IF NOT EXISTS link (
			id TEXT PRIMARY KEY, upload_id TEXT NOT NULL, position INTEGER NOT NULL
		);`,
    schemaVersion: 1,
    shapes: [{ table: 'link', primaryKey: ['id'], url: '/shape/link' }],
    attach: [{ alias: 'tenant', dbName: TENANT_DB }],
    readers: 2,
    attachReaders: 1,
    ...(readMode ? { readMode } : {}),
    ...(mode ? { forceConnMode: mode } : {}),
  },
  workers()
);
w.__project = project;

// ── tenant.db (the attached one; an ordinary LocalDb) ───────────────────
const tenant = new LocalDb(
  {
    dbName: TENANT_DB,
    schemaSql: `CREATE TABLE IF NOT EXISTS upload (
			id TEXT PRIMARY KEY, title TEXT NOT NULL, plays INTEGER NOT NULL
		);`,
    schemaVersion: 1,
    shapes: [
      {
        table: 'upload',
        primaryKey: ['id'],
        url: '/shape/upload',
        ...(skew ? { params: { _delay: String(skew) } } : {}),
      },
    ],
    readers: 1,
    ...(mode ? { forceConnMode: mode } : {}),
  },
  workers()
);
w.__tenant = tenant;

void project.ready().then(
  () => events.push(['mode', project.getConnMode()]),
  (e) => events.push(['ready-error', String(e)])
);
// Carry the failure detail: a shape stream erroring here is far more
// likely to be an environment problem (connection limits, port) than a
// library one, and 'error' with no reason is undebuggable from CI logs.
const statusEv = (which: string) => (s: { state: string; error?: string; httpStatus?: number }) =>
  events.push([`${which}-status`, s.state, s.error ?? null, s.httpStatus ?? null]);
tenant.onSyncStatus(statusEv('tenant'));
project.onSyncStatus(statusEv('project'));

// The core question: does a commit on the ATTACHED db re-run a query that
// reads it? This subscription touches ONLY tenant tables, so nothing on
// project.db's own commit channel can refresh it.
project.subscribe<{ n: number }>({ sql: 'SELECT count(*) AS n FROM tenant.upload' }, (r) =>
  events.push(['tenant-only', r.version, r.rows[0]?.n ?? null, r.error ?? null])
);

// The cross-file join itself.
project.subscribe<{ n: number }>(
  {
    sql: `SELECT count(*) AS n FROM link l JOIN tenant.upload u ON u.id = l.upload_id`,
  },
  (r) => events.push(['xjoin', r.version, r.rows[0]?.n ?? null, r.error ?? null])
);

// Skew probe: link rows whose upload row is not visible locally. Within
// one file alignShapes drives this to zero; across two files nothing
// coordinates the writers, so this measures the gap rather than asserting
// it away.
project.subscribe<{ orphans: number }>(
  {
    sql: `SELECT count(*) AS orphans FROM link l
			LEFT JOIN tenant.upload u ON u.id = l.upload_id WHERE u.id IS NULL`,
    minIntervalMs: 0,
  },
  (r) => events.push(['orphans', r.version, r.rows[0]?.orphans ?? null, Date.now()])
);

// Regression guard: an ordinary single-db query on the SAME LocalDb must
// behave exactly as before, and it must be routed to the whole pool.
project.subscribe<{ n: number }>({ sql: 'SELECT count(*) AS n FROM link' }, (r) =>
  events.push(['local', r.version, r.rows[0]?.n ?? null])
);

// The attached db serving its own readers normally, unaffected.
tenant.subscribe<{ n: number }>({ sql: 'SELECT count(*) AS n FROM upload' }, (r) =>
  events.push(['tenant-own', r.version, r.rows[0]?.n ?? null])
);

// One-shot (loader path) across the attach - a different dispatch path
// than subscriptions, and the one that has to split a mixed batch.
const un = project.onSyncStatus((s) => {
  if (s.state !== 'live') return;
  un();
  void Promise.all([
    project.query<{ n: number }>('SELECT count(*) AS n FROM tenant.upload'),
    project.query<{ n: number }>('SELECT count(*) AS n FROM link'),
  ])
    .then(([x, l]) => events.push(['oneshot', x[0]?.n ?? null, l[0]?.n ?? null]))
    .catch((e) => events.push(['oneshot-error', String(e)]));
});
