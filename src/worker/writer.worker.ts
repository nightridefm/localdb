// Writer worker entry. Sole owner of the write path: runs one shapeStream
// per configured shape, buffers messages per stream, and commits a fenced
// transaction per up-to-date boundary (or when a buffer crosses maxBatch
// during initial hydration). After every commit it broadcasts a
// CommitNotice.
//
// Connection-mode resolution happens HERE (workers can probe OPFS): the
// ready message carries it so the client knows whether to spawn reader
// workers ('multi') or route reads to this worker's embedded ReadService
// ('single', where SAHPool means one connection per file).

import type { FromReader, FromWriter, ToReader, ToWriter, WriterInit } from '../protocol.ts';
import type { ModelMeta, SyncStatus } from '../types.ts';

import {
  commitChannel,
  fenceName,
  statusChannel,
  visChannel,
  visibleLockName,
  writerLockName,
} from '../protocol.ts';
import { makeApplier, type TableApplier } from '../sync/apply.ts';
import { shapeStream, type ShapeCursor, type ShapeMessage } from '../sync/electric.ts';
import {
  syncTransport,
  type SqlValue,
  type SyncTransport,
  type TransportDb,
} from '../sync/transport.ts';
import { openEngine, resolveConnMode, type Engine, type Sqlite3Stmt } from './engine.ts';
import { withWriteFence } from './fence.ts';
import { ReadService } from './read-service.ts';
import { opfsAvailable } from './vfs.ts';

// Electric's global_last_seen_lsn is an integer string in every observed
// build, but the aligner must not die on a format change (e.g. pg's
// 'X/YYYYYY' text form) - unparseable LSNs align as 0 (commit immediately).
function parseLsn(v: string | null | undefined): bigint {
  if (!v) return 0n;
  try {
    return BigInt(v);
  } catch {
    return 0n;
  }
}

const post = (m: FromWriter | FromReader, transfer?: Transferable[]) =>
  (postMessage as (m: unknown, t?: Transferable[]) => void)(m, transfer);

let engine: Engine | null = null;
let commits: BroadcastChannel | null = null;
let readService: ReadService | null = null; // single-connection mode only
// Ingest-coercion metadata per TABLE (shapes reference tables, models are
// keyed by model name). From WriterInit.models.
const metaByTable = new Map<string, ModelMeta>();

/**
 * Exclusive fence + (baton) handle window around a db operation. In
 * multi/single, acquire/release are no-ops and this is exactly
 * withWriteFence - so every writer-side db touch goes through here and
 * the baton invariant (no db op without a held handle) holds by
 * construction.
 */
async function withWriteDb<T>(fn: () => T): Promise<T> {
  return withWriteFence(fence, async () => {
    await engine!.acquire();
    try {
      return fn();
    } finally {
      engine!.release();
    }
  });
}
let fence = '';
// Shape-cursor rows in _ldb_shape are keyed `<table>@<schemaVersion>` so a
// leader only ever trusts cursors written under ITS config. Without the
// version in the key, a stale old-version leader can re-persist its live
// cursors AFTER a new-version tab's wipe; the takeover then resumes from
// live offsets without re-snapshotting, and every table stays empty while
// status reads "live".
let schemaVer = 0;
// Always BOUND, never interpolated - offsets/handles come from response
// headers and must not touch SQL text.
function shapeCursorKey(table: string): string {
  return `${table}@${schemaVer}`;
}
// Cursor persistence, prepared once per worker (it runs on every commit).
let cursorUpsertStmt: Sqlite3Stmt | null = null;
function persistCursor(table: string, cursor: ShapeCursor): void {
  if (!cursorUpsertStmt) {
    cursorUpsertStmt = engine!.db.prepare(
      `INSERT INTO _ldb_shape VALUES (?,?,?)
			 ON CONFLICT(table_name) DO UPDATE SET offset_v=excluded.offset_v, handle=excluded.handle`
    );
  }
  const s = cursorUpsertStmt;
  try {
    s.bind([shapeCursorKey(table), cursor.offset, cursor.handle]).stepReset();
  } catch (err) {
    // A failed bind/step keeps bindings attached and the next bind() dies
    // with SQLITE_MISUSE - reset, or drop the cached statement entirely.
    try {
      s.reset();
    } catch {
      cursorUpsertStmt = null;
      try {
        s.finalize();
      } catch {
        // already finalized
      }
    }
    throw err;
  }
}
function runBound(sql: string, values: (string | number | null)[]): void {
  const s = engine!.db.prepare(sql);
  try {
    s.bind(values).stepReset();
  } finally {
    s.finalize();
  }
}
let seq = 0;
let abort = new AbortController();
let coalesceMs = 50;
// Leading-edge write throttle reference: wall time of the last committed
// transaction (any shape - the flush the window amortizes is per-file).
let lastCommitAt = -Infinity;

// ── Cross-shape LSN alignment (LocalDbConfig.alignShapes) ──────────────────
// A live batch for one table is held until every OTHER live shape's head
// has reached its LSN (or maxSkewMs elapses - a quiet shape's long-poll
// cannot advance its head, so alignment is bounded, not absolute). Eligible
// batches from ALL shapes commit together in one fenced transaction with a
// merged notice, so joins across actively-syncing tables never observe
// skew. Hydration and the first up-to-date commit stay unaligned.
let alignEnabled = false;
let alignMaxSkewMs = 2000;
type AlignedBatch = {
  table: string;
  messages: ShapeMessage[];
  cursor: ShapeCursor;
  lsn: bigint;
  enqueuedAt: number;
  applier: TableApplier;
  /** The owning shape's failure hook: restarts it from its stored cursor. */
  fail: (err: unknown) => void;
};
const alignHeads = new Map<string, bigint>();
const alignLive = new Set<string>();
const alignQueue: AlignedBatch[] = [];
let alignTimer: ReturnType<typeof setTimeout> | null = null;

function alignMinOtherHead(table: string): bigint | null {
  let min: bigint | null = null;
  for (const t of alignLive) {
    if (t === table) continue;
    const h = alignHeads.get(t) ?? -1n;
    if (min === null || h < min) min = h;
  }
  return min; // null = only live shape, nothing to align with
}

function alignFeed(b: AlignedBatch): void {
  alignHeads.set(b.table, b.lsn);
  alignLive.add(b.table);
  alignQueue.push(b);
  void alignDrain().catch((err) => post({ t: 'error', error: String(err) }));
}

async function alignDrain(): Promise<void> {
  const now = performance.now();
  const eligible: AlignedBatch[] = [];
  let nextDeadline = Infinity;
  for (const b of alignQueue) {
    const other = alignMinOtherHead(b.table);
    if (other === null || b.lsn <= other || now - b.enqueuedAt >= alignMaxSkewMs) {
      eligible.push(b);
    } else {
      nextDeadline = Math.min(nextDeadline, b.enqueuedAt + alignMaxSkewMs);
    }
  }
  if (eligible.length) {
    // Rebuild once rather than splice-per-item (indexOf + splice is
    // quadratic in the queue length, and a stalled slow shape can make the
    // queue long).
    const drop = new Set(eligible);
    for (let i = alignQueue.length - 1; i >= 0; i--) {
      if (drop.has(alignQueue[i]!)) alignQueue.splice(i, 1);
    }
    eligible.sort((a, b) => (a.lsn < b.lsn ? -1 : a.lsn > b.lsn ? 1 : 0));
    const e = engine!;
    const tables = new Set<string>();
    let rows = 0;
    try {
      await withWriteDb(() => {
        e.db.exec('BEGIN');
        try {
          for (const b of eligible) {
            rows += b.applier.apply(b.messages);
            tables.add(b.table);
            persistCursor(b.table, b.cursor);
          }
          e.db.exec('COMMIT');
          e.flush();
        } catch (err) {
          e.db.exec('ROLLBACK');
          throw err;
        }
      });
    } catch (err) {
      // The rolled-back batches are already out of the queue. Restart each
      // affected shape from its stored cursor, which re-fetches all of it.
      for (const b of eligible) b.fail(err);
      throw err;
    }
    broadcastCommit([...tables], rows);
    await rebuildIndexesIfPending([...tables]);
  }
  if (alignQueue.length && alignTimer === null && nextDeadline < Infinity) {
    alignTimer = setTimeout(
      () => {
        alignTimer = null;
        void alignDrain().catch((err) => post({ t: 'error', error: String(err) }));
      },
      Math.max(10, nextDeadline - performance.now())
    );
  }
}

// Initial hydration: don't wait for up-to-date to commit - a 1M-row first
// sync should land in bounded chunks (bench: ~100k rows/s apply).
const MAX_BATCH_ROWS = 50_000;

// ── Deferred indexes ────────────────────────────────────────────────────────
// Hydrating THROUGH secondary indexes is ~5x slower (bench: 104k -> ~20k
// rows/s at 8 indexes). Whenever a shape needs a full hydration - fresh
// boot (no stored cursor) or must-refetch - its table's index DDL is
// captured from sqlite_schema, the indexes are dropped, and they are
// rebuilt (+ ANALYZE) right after the shape's first up-to-date commit.
// Schema-version wipes need no handling: DROP TABLE takes indexes with it,
// and the recreated ones are captured here like any fresh boot. Indexes
// stay declared in schemaSql; no extra config.
const pendingIndexRebuild = new Map<string, Array<{ name: string; sql: string }>>();

function captureAndDropIndexes(table: string): void {
  const e = engine!;
  const stmt = e.db.prepare(
    `SELECT name, sql FROM sqlite_schema WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL`
  );
  const found: Array<{ name: string; sql: string }> = [];
  try {
    stmt.bind([table]);
    while (stmt.step()) found.push({ name: String(stmt.get(0)), sql: String(stmt.get(1)) });
  } finally {
    stmt.finalize();
  }
  if (found.length) {
    for (const idx of found) e.db.exec(`DROP INDEX IF EXISTS "${idx.name.replaceAll('"', '""')}"`);
    pendingIndexRebuild.set(table, found);
  }
  // The FTS mirror + triggers would slow bulk hydration the same way.
  ftsDrop(table);
}

// ── FTS5 trigram mirrors (LocalDbConfig.fts) ────────────────────────────────
// `<table>_fts` external-content FTS5 + three sync triggers. Deferred like
// secondary indexes: dropped for full hydrations (trigger-maintained bulk
// inserts are slow), recreated + rebuilt after the first up-to-date.
const ftsSpecs = new Map<string, string[]>(); // table -> columns
const pendingFtsRebuild = new Set<string>();

function ftsExists(table: string): boolean {
  return !!engine!.db.selectValue(
    `SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '${table.replaceAll("'", "''")}_fts'`
  );
}

// Cached (and fenced) wrapper: rebuildIndexesIfPending runs its no-work
// check on every up-to-date commit, and in baton mode a bare db read
// outside a fence window has no handle. One db probe per table per boot;
// ftsEnsure/ftsDrop keep the cache truthful.
const ftsExistsCache = new Map<string, boolean>();
async function ftsExistsCached(table: string): Promise<boolean> {
  const hit = ftsExistsCache.get(table);
  if (hit !== undefined) return hit;
  const v = await withWriteDb(() => ftsExists(table));
  ftsExistsCache.set(table, v);
  return v;
}

function ftsDrop(table: string): void {
  ftsExistsCache.set(table, false);
  const e = engine!;
  const t = table.replaceAll('"', '""');
  for (const trig of ['ai', 'ad', 'au']) e.db.exec(`DROP TRIGGER IF EXISTS "${t}_fts_${trig}"`);
  e.db.exec(`DROP TABLE IF EXISTS "${t}_fts"`);
  if (ftsSpecs.has(table)) pendingFtsRebuild.add(table);
}

function ftsEnsure(table: string): void {
  const cols = ftsSpecs.get(table);
  if (!cols) return;
  const e = engine!;
  const t = table.replaceAll('"', '""');
  const colList = cols.map((c) => `"${c.replaceAll('"', '""')}"`).join(', ');
  const newVals = cols.map((c) => `new."${c.replaceAll('"', '""')}"`).join(', ');
  const oldVals = cols.map((c) => `old."${c.replaceAll('"', '""')}"`).join(', ');
  const existed = !!e.db.selectValue(
    `SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '${t}_fts'`
  );
  e.db.exec(`
		CREATE VIRTUAL TABLE IF NOT EXISTS "${t}_fts" USING fts5(${colList}, content="${t}", content_rowid='rowid', tokenize='trigram');
		CREATE TRIGGER IF NOT EXISTS "${t}_fts_ai" AFTER INSERT ON "${t}" BEGIN
			INSERT INTO "${t}_fts"(rowid, ${colList}) VALUES (new.rowid, ${newVals});
		END;
		CREATE TRIGGER IF NOT EXISTS "${t}_fts_ad" AFTER DELETE ON "${t}" BEGIN
			INSERT INTO "${t}_fts"("${t}_fts", rowid, ${colList}) VALUES ('delete', old.rowid, ${oldVals});
		END;
		CREATE TRIGGER IF NOT EXISTS "${t}_fts_au" AFTER UPDATE ON "${t}" BEGIN
			INSERT INTO "${t}_fts"("${t}_fts", rowid, ${colList}) VALUES ('delete', old.rowid, ${oldVals});
			INSERT INTO "${t}_fts"(rowid, ${colList}) VALUES (new.rowid, ${newVals});
		END;
	`);
  if (!existed || pendingFtsRebuild.has(table)) {
    e.db.exec(`INSERT INTO "${t}_fts"("${t}_fts") VALUES ('rebuild')`);
    pendingFtsRebuild.delete(table);
  }
  ftsExistsCache.set(table, true);
}

async function rebuildIndexesIfPending(tables: string[]): Promise<void> {
  const e = engine!;
  for (const table of tables) {
    const saved = pendingIndexRebuild.get(table) ?? [];
    if (
      !saved.length &&
      !pendingFtsRebuild.has(table) &&
      !(ftsSpecs.has(table) && !(await ftsExistsCached(table)))
    ) {
      continue;
    }
    pendingIndexRebuild.delete(table);
    await withWriteDb(() => {
      e.db.exec('BEGIN');
      try {
        for (const idx of saved) e.db.exec(idx.sql);
        ftsEnsure(table);
        e.db.exec(`ANALYZE "${table.replaceAll('"', '""')}"`);
        e.db.exec('COMMIT');
        // Refresh planner stats opportunistically now that the table has
        // its final shape (no-op when nothing changed).
        e.db.exec('PRAGMA optimize');
        e.flush();
      } catch (err) {
        e.db.exec('ROLLBACK');
        throw err;
      }
    });
    post({
      t: 'status',
      status: {
        state: 'initial-sync',
        table,
        rows: -1, // sentinel: index rebuild phase
      },
    });
  }
}

function broadcastCommit(tables: string[], rows: number): void {
  seq++;
  lastCommitAt = performance.now();
  const notice = { seq, tables, rows };
  commits!.postMessage(notice);
  // BroadcastChannel does NOT loop back to the posting context - notify
  // the embedded read service (single mode) directly.
  readService?.onCommit(notice);
  post({ t: 'commit', notice });
}

// ── Multi-tab: exactly ONE sync engine per dbName ───────────────────────────
// Every tab spawns this worker; leadership is a never-released exclusive Web
// Lock. Non-leaders idle as hot standbys: if the leader tab closes, the lock
// transfers and the new leader resumes from the crash-consistent cursors in
// _ldb_shape. The read path needs NO election: the OPFS file is shared
// storage and commit notices ride BroadcastChannel, both cross-tab already.
// SyncStatus is re-broadcast on a status channel so every tab's client sees
// the leader's state.
let isLeader = false;
let statusBc: BroadcastChannel | null = null;

// ── pauseSyncWhenHidden: cross-tab visibility census ────────────────────────
// Every tab's writer worker holds a SHARED Web Lock while its page is
// visible and releases it when hidden; the leader pauses the Electric
// streams only when NO tab holds the lock. Web Locks give the census
// crash-safety for free: a closed or crashed tab's lock releases itself.
let pauseWhenHidden = false;
let visLockRelease: (() => void) | null = null;
let visBc: BroadcastChannel | null = null;
let syncRunning = false;
let syncConfig: WriterInit | null = null;
// Manual pause (LocalDb.pauseSync). Per-tab: it stops THIS tab's sync
// engine and overrides the visibility census until resumeSync. A standby
// just records the flag and honours it if promoted.
let manualPause = false;

function setVisibleLock(visible: boolean, dbName: string): void {
  if (visible && !visLockRelease) {
    void navigator.locks.request(
      visibleLockName(dbName),
      { mode: 'shared' },
      () => new Promise<void>((res) => (visLockRelease = res))
    );
  } else if (!visible && visLockRelease) {
    visLockRelease();
    visLockRelease = null;
  }
}

async function anyTabVisible(dbName: string): Promise<boolean> {
  const q = await navigator.locks.query();
  return (q.held ?? []).some((l) => l.name === visibleLockName(dbName));
}

// ── Pluggable transport (LocalDbConfig.sync) ────────────────────────────────
// Replaces the shape streams when configured. Writes go through the same
// write fence and commit broadcast as Electric batches.
let transport: SyncTransport | null = null;

function transportDb(e: Engine): TransportDb {
  // Statements cached by SQL text: the transport re-runs the same few.
  const stmts = new Map<string, Sqlite3Stmt>();
  const stmt = (sql: string) => {
    let s = stmts.get(sql);
    if (!s) {
      s = e.db.prepare(sql);
      stmts.set(sql, s);
    }
    return s;
  };
  return {
    exec(sql: string, bind?: SqlValue[]) {
      if (!bind?.length) {
        e.db.exec(sql);
        return;
      }
      const s = stmt(sql);
      try {
        s.bind(bind).stepReset();
      } catch (err) {
        s.reset();
        throw err;
      }
    },
    all(sql: string, bind?: SqlValue[]) {
      const s = stmt(sql);
      const out: SqlValue[][] = [];
      try {
        if (bind?.length) s.bind(bind);
        while (s.step()) {
          const row: SqlValue[] = [];
          for (let i = 0; i < s.columnCount; i++) row.push(s.get(i) as SqlValue);
          out.push(row);
        }
      } finally {
        s.reset();
      }
      return out;
    },
  };
}

function startTransport(name: string, options: unknown): boolean {
  const factory = syncTransport(name);
  if (!factory) {
    post({ t: 'error', error: `sync transport '${name}' is not registered in this writer worker` });
    return false;
  }
  const db = transportDb(engine!);
  transport = factory(
    {
      run: (fn) => withWriteDb(() => fn(db)),
      committed: (tables, rows) => broadcastCommit(tables, rows),
      status: postStatus,
      log: (message) => post({ t: 'error', error: message }),
    },
    options
  );
  transport.start();
  return true;
}

function startSync(): void {
  if (syncRunning || !syncConfig) return;
  syncRunning = true;
  abort = new AbortController();
  postStatus({ state: 'connecting' });
  if (syncConfig.sync) {
    if (!startTransport(syncConfig.sync.transport, syncConfig.sync.options)) syncRunning = false;
    return;
  }
  for (const shape of syncConfig.shapes) void runShapeWithRetry(shape);
}

function stopSync(): void {
  if (!syncRunning) return;
  syncRunning = false;
  abort.abort();
  transport?.stop();
  transport = null;
  postStatus({ state: 'paused' });
}

async function evaluatePause(): Promise<void> {
  if (!isLeader || !pauseWhenHidden || !syncConfig || manualPause) return;
  // Give the visibility lock transfer a beat to settle before querying.
  await new Promise((r) => setTimeout(r, 50));
  const visible = await anyTabVisible(syncConfig.dbName);
  if (visible) startSync();
  else stopSync();
}

function postStatus(status: SyncStatus): void {
  post({ t: 'status', status });
  if (isLeader) statusBc?.postMessage(status);
}

async function init(config: WriterInit) {
  schemaVer = config.schemaVersion;
  coalesceMs = config.writeCoalesceMs ?? 50;
  alignEnabled = !!config.alignShapes && config.shapes.length > 1;
  if (typeof config.alignShapes === 'object' && config.alignShapes.maxSkewMs != null) {
    alignMaxSkewMs = config.alignShapes.maxSkewMs;
  }
  for (const f of config.fts ?? []) ftsSpecs.set(f.table, f.columns);
  for (const m of Object.values(config.models ?? {})) metaByTable.set(m.table, m);
  pauseWhenHidden = config.pauseSyncWhenHidden ?? false;
  syncConfig = config;
  // Fail LOUD, not with an infinite spinner: no OPFS (unsupported browser,
  // Safari Private Browsing) or a storage-init failure is TERMINAL - post
  // the status and stop; the app surfaces it to the user.
  if (!(await opfsAvailable())) {
    post({
      t: 'status',
      status: {
        state: 'unsupported',
        reason: 'no-opfs',
        error: 'OPFS is unavailable (unsupported browser, or Safari Private Browsing)',
      },
    });
    return;
  }
  const connMode = config.forceConnMode ?? (await resolveConnMode());
  console.info(
    `[localdb] ${config.dbName}: connMode=${connMode}${config.forceConnMode ? ' (forced)' : ''}, schemaVersion=${config.schemaVersion}`
  );
  fence = fenceName(config.dbName);
  try {
    if (connMode === 'baton') {
      // The DB open reads the header through the handle - take the write
      // fence for the open itself, then release the handle until the next
      // write window.
      await withWriteFence(fence, async () => {
        engine = await openEngine({
          dbName: config.dbName,
          role: 'writer',
          connMode,
          models: config.models,
        });
        engine.release();
      });
    } else {
      engine = await openEngine({
        dbName: config.dbName,
        role: 'writer',
        connMode,
        models: config.models,
      });
    }
  } catch (err) {
    post({
      t: 'status',
      status: {
        state: 'unsupported',
        reason: 'storage-failed',
        error:
          String((err as Error)?.message ?? err) +
          (connMode === 'single'
            ? ' (single-connection mode: the local DB may be held open by another tab)'
            : ''),
      },
    });
    return;
  }
  commits = new BroadcastChannel(commitChannel(config.dbName));
  statusBc = new BroadcastChannel(statusChannel(config.dbName));
  statusBc.onmessage = (e) => {
    // Standby tabs relay the leader's status to their own client.
    if (!isLeader) post({ t: 'status', status: e.data });
  };
  visBc = new BroadcastChannel(visChannel(config.dbName));
  visBc.onmessage = () => void evaluatePause();
  // Until told otherwise, assume this tab is visible (the client pushes the
  // real initial state right after boot).
  setVisibleLock(true, config.dbName);
  if (connMode === 'single') {
    readService = new ReadService(engine!, fence, post, {
      readMode: config.readMode,
      advise: config.advise,
      maxUpdateHz: config.maxUpdateHz,
    });
  }

  // Schema init is idempotent and fenced, so any tab can run it - readers
  // (which boot in parallel across tabs) always find the tables.
  let wiped = false;
  await withWriteDb(() => {
    engine!.db.exec(`
			CREATE TABLE IF NOT EXISTS _ldb_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
			CREATE TABLE IF NOT EXISTS _ldb_shape (
				table_name TEXT PRIMARY KEY, offset_v TEXT NOT NULL, handle TEXT
			);
		`);
    const stored = engine!.db.selectValue(`SELECT value FROM _ldb_meta WHERE key='schema_version'`);
    if (stored !== undefined && stored !== null && Number(stored) !== config.schemaVersion) {
      // During a deploy rollout, tabs on DIFFERENT app versions wipe on
      // every boot until the old tabs reload - loud so a wipe loop in the
      // console reads as "stale tab open", not data loss.
      console.warn(
        `[localdb] ${config.dbName}: schema version ${stored} != ${config.schemaVersion} - wiping local cache (a tab on another app version will wipe it back until reloaded)`
      );
      engine!.dropUserTables();
      engine!.db.exec(`DELETE FROM _ldb_shape;`);
      wiped = true;
    }
    engine!.db.exec(config.schemaSql);
    engine!.db.exec(
      `INSERT INTO _ldb_meta VALUES ('schema_version','${config.schemaVersion}')
			 ON CONFLICT(key) DO UPDATE SET value=excluded.value`
    );
    engine!.flush();
  });
  post({ t: 'ready', wiped, connMode });
  postStatus({ state: 'connecting' });

  // Leadership: granted immediately in a single tab; otherwise this tab is
  // a standby until the current leader's tab closes. The callback promise
  // never resolves, so the lock is held for the worker's lifetime.
  let leaseLogged = false;
  void navigator.locks.request(writerLockName(config.dbName), { mode: 'exclusive' }, async () => {
    isLeader = true;
    leaseLogged = true;
    console.info(
      `[localdb] ${config.dbName}: sync leader - THIS tab runs the sync (Electric shapes or a registered transport)`
    );
    if (manualPause || (pauseWhenHidden && !(await anyTabVisible(config.dbName)))) {
      postStatus({ state: 'paused' });
    } else {
      startSync();
    }
    return new Promise<never>(() => {});
  });
  // If the lease wasn't granted promptly, another tab is leading - say so,
  // loudly enough that "why is/isn't this tab making shape requests" is
  // answerable from the console alone (the lock callback above still fires
  // on failover, logging the promotion).
  setTimeout(() => {
    if (!leaseLogged) {
      console.info(
        `[localdb] ${config.dbName}: standby - another tab holds the sync lease; this tab reads the shared DB only`
      );
    }
  }, 300);
}

// A failed batch (constraint violation, transient engine error) must never
// silently kill a shape's stream - the real-Electric smoke found exactly
// that failure mode. The transaction rolled back, so the stored cursor
// still points at the last good commit; restarting runShape resumes from
// there and re-applies idempotently.
//
// The loop is bound to the signal of the startSync that launched it, NOT
// the module-level `abort`: a pause + resume replaces that controller, and
// a loop sleeping out a retry would otherwise see the NEW, unaborted one
// and start a second stream for the shape next to the one resume started.
async function runShapeWithRetry(shape: WriterInit['shapes'][number]): Promise<void> {
  const signal = abort.signal;
  for (;;) {
    try {
      await runShape(shape, signal);
      return; // stream ended (abort)
    } catch (err) {
      post({ t: 'error', error: `shape ${shape.table}: ${String((err as Error)?.stack || err)}` });
      if (signal.aborted) return;
      post({ t: 'status', status: { state: 'error', error: String(err), retryInMs: 5000 } });
      await new Promise((r) => setTimeout(r, 5000));
      if (signal.aborted) return;
    }
  }
}

async function runShape(shape: WriterInit['shapes'][number], signal: AbortSignal) {
  const e = engine!;
  // A commit that fails off the main loop (coalesce timer, aligner) cannot
  // throw into it. It records the error and aborts this shape's stream
  // instead; the loop then rethrows, and runShapeWithRetry resumes from
  // the stored cursor. Until then every commit refuses to run: one that
  // succeeded after the failure would persist a cursor past the
  // rolled-back rows and skip them for good.
  const shapeAbort = new AbortController();
  const onAbort = () => shapeAbort.abort();
  signal.addEventListener('abort', onAbort, { once: true });
  // Cast, not annotation: assigned only inside closures, so a plain
  // `= null` would narrow it to null for the whole function body.
  let failure = null as { err: unknown } | null;
  const fail = (err: unknown) => {
    failure ??= { err };
    shapeAbort.abort();
    // Queued aligned batches are uncommitted state from before the
    // restart point: committing one later would persist a cursor past the
    // rolled-back rows, or replay stale rows over the restarted stream's.
    for (let i = alignQueue.length - 1; i >= 0; i--) {
      if (alignQueue[i]!.table === shape.table) alignQueue.splice(i, 1);
    }
  };
  const applier = makeApplier(e, shape.table, shape.primaryKey, metaByTable.get(shape.table));
  const storedCursor = await withWriteDb(() =>
    e.db.selectValue(
      `SELECT json_object('offset', offset_v, 'handle', handle) FROM _ldb_shape WHERE table_name=?`,
      [shapeCursorKey(shape.table)]
    )
  );
  const cursor: ShapeCursor | null = storedCursor
    ? (JSON.parse(String(storedCursor)) as { offset: string; handle: string | null })
    : null;
  // Full hydration ahead (no resume cursor): start from an EMPTY table
  // (a stale leader on an older config may have written rows after our
  // wipe - the snapshot is ground truth and would never delete them),
  // and hydrate WITHOUT secondary indexes, rebuilding after the first
  // up-to-date.
  if (!cursor) {
    await withWriteDb(() => {
      e.db.exec('BEGIN');
      applier.truncate();
      e.db.exec('COMMIT');
      e.flush();
      captureAndDropIndexes(shape.table);
    });
  }

  let pending: Parameters<typeof applier.apply>[0] = [];
  let pendingRows = 0;
  let liveOnce = false;
  // Write-side coalescing (see LocalDbConfig.writeCoalesceMs): once live, a
  // chatty stream must not turn every long-poll response into a
  // commit+flush cycle - readers only consume at the scheduler tick, so
  // committing faster than that is pure flush/fence waste. Buffer and
  // commit at most once per window; the trailing timer always carries the
  // NEWEST cursor.
  let coalesceTimer: ReturnType<typeof setTimeout> | null = null;
  let coalesceCursor: ShapeCursor | null = null;

  const commit = async (upToDate: boolean, latest: ShapeCursor) => {
    if (coalesceTimer) {
      clearTimeout(coalesceTimer);
      coalesceTimer = null;
    }
    coalesceCursor = null;
    const batch = pending;
    pending = [];
    pendingRows = 0;
    let rows = 0;
    await withWriteDb(() => {
      if (failure) throw failure.err;
      e.db.exec('BEGIN');
      try {
        rows = applier.apply(batch);
        // Cursor persisted in the SAME txn as the data - a crash can
        // never leave the cursor ahead of (or behind) the rows.
        persistCursor(shape.table, latest);
        e.db.exec('COMMIT');
        // Make the commit visible to reader workers' handles BEFORE
        // the notice goes out - see vfs.flushPreopened.
        e.flush();
      } catch (err) {
        e.db.exec('ROLLBACK');
        throw err;
      }
    }).catch((err: unknown) => {
      fail(err);
      throw err;
    });
    broadcastCommit([shape.table], rows);
    if (upToDate) await rebuildIndexesIfPending([shape.table]);
    if (upToDate && !liveOnce) {
      liveOnce = true;
      postStatus({ state: 'live' });
    }
  };

  try {
    for await (const batch of shapeStream({
      url: shape.url,
      params: shape.params,
      cursor,
      signal: shapeAbort.signal,
      onRetry: (err, delayMs, httpStatus) =>
        post({
          t: 'status',
          status: { state: 'error', error: String(err), retryInMs: delayMs, httpStatus },
        }),
    })) {
      if (batch.mustRefetch) {
        // A pending coalesced commit would persist a pre-refetch cursor over
        // the reset - cancel it along with the buffered messages.
        if (coalesceTimer) {
          clearTimeout(coalesceTimer);
          coalesceTimer = null;
        }
        coalesceCursor = null;
        // Aligned batches for this table are pre-refetch state too.
        for (let i = alignQueue.length - 1; i >= 0; i--) {
          if (alignQueue[i]!.table === shape.table) alignQueue.splice(i, 1);
        }
        await withWriteDb(() => {
          e.db.exec('BEGIN');
          applier.truncate();
          runBound(`DELETE FROM _ldb_shape WHERE table_name=?`, [shapeCursorKey(shape.table)]);
          e.db.exec('COMMIT');
          e.flush();
          // The refetch re-hydrates the whole shape - do it index-free.
          captureAndDropIndexes(shape.table);
        });
        broadcastCommit([shape.table], 0);
        pending = [];
        pendingRows = 0;
        continue;
      }
      for (const m of batch.messages) {
        // Change messages AND headers-only move-out events reach the applier;
        // pure control messages (up-to-date / snapshot-end) do not.
        if (m.headers.operation || m.headers.event === 'move-out') pending.push(m);
      }
      pendingRows = pending.length;
      if (pendingRows >= MAX_BATCH_ROWS) {
        // Hydration chunk - commit immediately, never coalesce (latency to
        // first paint beats flush amortization here).
        post({
          t: 'status',
          status: { state: 'initial-sync', table: shape.table, rows: pendingRows },
        });
        await commit(false, batch.cursor);
      } else if (batch.upToDate) {
        if (!liveOnce || (coalesceMs <= 0 && !alignEnabled)) {
          // First up-to-date (initial snapshot) - commit now for fast boot.
          await commit(true, batch.cursor);
          if (alignEnabled) {
            // Seed the aligner so other shapes' heads have a reference.
            alignHeads.set(shape.table, parseLsn(batch.upToDateLsn));
            alignLive.add(shape.table);
          }
        } else if (alignEnabled) {
          // Live steady state, aligned: hand the batch to the cross-shape
          // committer (it owns pacing; per-shape coalescing is bypassed).
          const msgs = pending;
          pending = [];
          pendingRows = 0;
          alignFeed({
            table: shape.table,
            messages: msgs,
            cursor: batch.cursor,
            lsn: parseLsn(batch.upToDateLsn),
            enqueuedAt: performance.now(),
            applier,
            fail,
          });
        } else {
          // Live steady state - leading-edge throttle. If the last commit
          // (any shape) is older than the window, commit NOW: an isolated
          // mutation pays zero coalescing latency. Only churn inside the
          // window buffers, and then only for the REMAINDER of the window;
          // a pending trailing timer just absorbs the batch.
          coalesceCursor = batch.cursor;
          if (!coalesceTimer) {
            const sinceLast = performance.now() - lastCommitAt;
            if (sinceLast >= coalesceMs) {
              const cur = coalesceCursor;
              coalesceCursor = null;
              await commit(true, cur);
            } else {
              coalesceTimer = setTimeout(
                () => {
                  coalesceTimer = null;
                  const cur = coalesceCursor;
                  coalesceCursor = null;
                  // commit() has already routed the error to fail(); still
                  // log it, since a timer firing after the stream ended (a
                  // pause) has no loop left to rethrow it.
                  if (cur) void commit(true, cur).catch(onShapeError);
                },
                Math.max(1, coalesceMs - sinceLast)
              );
            }
          }
        }
      }
    }
  } catch (err) {
    // Aborting mid-read surfaces as an AbortError; the recorded failure is
    // the real cause.
    if (failure) throw failure.err;
    throw err;
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
  if (failure) {
    if (coalesceTimer) clearTimeout(coalesceTimer);
    throw failure.err;
  }
}

function onShapeError(err: unknown): void {
  post({ t: 'error', error: String((err as Error)?.stack || err) });
}

onmessage = (ev: MessageEvent<ToWriter | Exclude<ToReader, { t: 'r-init' }>>) => {
  const m = ev.data;
  const fail = (err: unknown) => post({ t: 'error', error: String((err as Error)?.stack || err) });
  if (m.t === 'init') {
    void init(m.config).catch(fail);
  } else if (m.t === 'pause') {
    manualPause = true;
    // Leader: stop the streams now (stopSync posts 'paused'; a no-op if
    // the census already paused them). Standby: the flag alone suffices -
    // it is honoured on promotion.
    if (isLeader) stopSync();
  } else if (m.t === 'resume') {
    manualPause = false;
    if (isLeader) {
      // Hand the decision back to the census when it is active; otherwise
      // restart directly. startSync resumes from the stored cursors.
      if (pauseWhenHidden) void evaluatePause();
      else startSync();
    }
  } else if (m.t === 'sync-command') {
    transport?.command?.(m.payload);
  } else if (m.t === 'subscribe') {
    // single mode: reads are hosted here (SAHPool allows one connection).
    if (readService) void readService.subscribe(m.key, m.spec).catch(fail);
    else
      post({
        t: 'r-error',
        key: m.key,
        error: 'no read service on this worker (multi mode routes reads to reader workers)',
      });
  } else if (m.t === 'visibility') {
    readService?.setHidden(m.hidden);
    if (syncConfig) {
      setVisibleLock(!m.hidden, syncConfig.dbName);
      visBc?.postMessage(null); // poke every leader candidate to re-census
      void evaluatePause();
    }
  } else if (m.t === 'unsubscribe') {
    readService?.unsubscribe(m.key);
  } else if (m.t === 'analyze') {
    if (readService) void readService.analyze(m.id, m.queries).catch(fail);
    else post({ t: 'r-analyze', id: m.id, report: [] });
  } else if (m.t === 'query') {
    if (readService) void readService.oneShot(m.queries).catch(fail);
    else {
      post({
        t: 'r-query-results',
        results: m.queries.map((q) => ({ id: q.id, error: 'no read service on this worker' })),
      });
    }
  }
};

// Apps register alternative sync transports from their writer worker entry
// (see sync/transport.ts).
export { registerSyncTransport } from '../sync/transport.ts';
