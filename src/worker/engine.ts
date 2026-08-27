// SQLite engine bootstrap for a worker (writer or reader).
//
// Connection modes (capability-detected at boot, see resolveConnMode):
//   'multi'  - primary. OPFS access-handle modes exist (Chrome/Edge 121+,
//              Safari ~17.4): the "opfs-modes" VFS gives every worker its
//              OWN handle on the same file - near-linear read scaling,
//              unimpeded writer, multi-tab.
//   'baton'  - Safari/Firefox: 'read-only' handle mode exists but not
//              readwrite-unsafe. Readers share read-only handles; the
//              writer opens an exclusive readwrite handle only inside its
//              write fence. Reader pool + multi-tab still work; every db
//              operation runs inside a fence-scoped acquire()..release().
//   'single' - fallback. SAHPool, ONE connection per file: the writer
//              worker hosts the read service too; no reader workers, no
//              second tab.
//
// The DB is a disposable sync cache: synchronous=OFF, journal in memory.
// Corruption or schema-version mismatch -> drop user tables, resync.

import type { ModelMeta } from '../types.ts';

import { hashBytes } from '../protocol.ts';
import { registerSimilarityUdf } from '../similarity.ts';
import {
  OPFS_MODES_VFS,
  acquirePreopened,
  closePreopened,
  detectHandleModes,
  detectReadOnlyMode,
  flushPreopened,
  installOpfsModesVfs,
  preopenFile,
  releasePreopened,
} from './vfs.ts';

// The sqlite-wasm JS API surface we touch. Kept intentionally narrow and
// loose - the upstream .d.ts is not strict-friendly.
export type Sqlite3Db = {
  exec: (sql: string) => void;
  prepare: (sql: string) => Sqlite3Stmt;
  selectValue: (sql: string, bind?: unknown[]) => unknown;
  selectValues: (sql: string, bind?: unknown[]) => unknown[];
  createFunction: (name: string, opts: object) => unknown;
  close: () => void;
  pointer: number;
};
export type Sqlite3Stmt = {
  bind: (values: unknown[]) => Sqlite3Stmt;
  step: () => boolean;
  stepReset: () => Sqlite3Stmt;
  reset: () => Sqlite3Stmt;
  get: (i: number) => unknown;
  finalize: () => void;
  columnCount: number;
  /** Raw sqlite3_stmt* - needed for the zero-transcode byte read path. */
  pointer: number;
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Sqlite3Module = any;

let sqlite3: Sqlite3Module = null;

/**
 * Result-column names that are JSON-typed (JSON columns and scalar lists,
 * both stored as JSON TEXT) or boolean-typed in ANY model. Used by
 * embedExpr so direct selects (`SELECT t.*`, `SELECT a.links`) deliver
 * real JSON values and booleans, not escaped strings and 1/0.
 *
 * The mapping is name-global on purpose: it must work for arbitrary
 * result columns, where per-table attribution is impossible. That is only
 * sound while no column name is bool/json-typed in one model and
 * differently typed in another - validateModels() (src/sql.ts) enforces
 * exactly that, and the json_valid guard degrades any residue to raw
 * text instead of erroring the row.
 */
export type EmbedHints = { json: Set<string>; bool: Set<string> };

export function embedHints(models?: Record<string, ModelMeta>): EmbedHints {
  const hints: EmbedHints = { json: new Set(), bool: new Set() };
  for (const m of Object.values(models ?? {})) {
    for (const c of m.jsonColumns) hints.json.add(c);
    for (const c of m.listColumns) hints.json.add(c);
    for (const c of m.boolColumns) hints.bool.add(c);
  }
  return hints;
}

const NO_HINTS: EmbedHints = { json: new Set(), bool: new Set() };

/**
 * json_object entry for one result column. `<x>__json` embeds strictly
 * (authoring convention - a non-JSON value should error loudly); declared
 * JSON/list column names embed through a json_valid guard; declared
 * boolean column names deliver as real JSON true/false (storage is 1/0
 * for SQL predicates, but consumers - and React `{x && <jsx>}` gates,
 * which literally render a 0 - expect booleans). Exported for tests.
 */
export function embedExpr(n: string, hints: EmbedHints = NO_HINTS): string {
  const isJson = n.endsWith('__json');
  const key = (isJson ? n.slice(0, -6) : n).replaceAll("'", "''");
  const ref = `"${n.replaceAll('"', '""')}"`;
  if (isJson) return `'${key}', json(${ref})`;
  if (hints.json.has(n)) {
    return `'${key}', CASE WHEN json_valid(${ref}) THEN json(${ref}) ELSE ${ref} END`;
  }
  if (hints.bool.has(n)) {
    return `'${key}', CASE WHEN ${ref} IS NULL THEN NULL WHEN ${ref} THEN json('true') ELSE json('false') END`;
  }
  return `'${key}', ${ref}`;
}

/**
 * Advisor classifier for one EXPLAIN QUERY PLAN detail line: does it
 * describe a genuinely UNINDEXED scan of a user table? Exported for tests.
 *
 * Exemptions, each a real advisory that turned out to be noise:
 * - `SCAN x USING (COVERING) INDEX ...` - an index-ordered scan, the
 *   INTENDED plan for ORDER BY + LIMIT pagination (tracks grid over
 *   idx_track_alive).
 * - `SCAN <vtab> VIRTUAL TABLE INDEX <n>:<idxStr>` with a non-empty
 *   idxStr - a CONSTRAINED virtual-table probe. EQP never says "USING
 *   INDEX" for virtual tables, so every FTS5 trigram LIKE probe
 *   (idxStr `L<col>`) and MATCH (`m`) otherwise reads as a table scan
 *   and fires on every search subscription. An UNconstrained vtab scan
 *   (empty idxStr) still flags.
 * - sqlite_* / _ldb_* internals.
 */
export function isUnindexedScan(detail: string): boolean {
  return (
    detail.startsWith('SCAN') &&
    !detail.includes('USING INDEX') &&
    !detail.includes('USING COVERING INDEX') &&
    !/VIRTUAL TABLE INDEX \d+:./.test(detail) &&
    !detail.includes('sqlite_') &&
    !detail.includes('_ldb_')
  );
}

async function ensureModule(): Promise<Sqlite3Module> {
  if (!sqlite3) {
    const { default: init } = await import('@sqlite.org/sqlite-wasm');
    // Upstream .d.ts types the initializer as zero-arg; it accepts an
    // Emscripten module config at runtime.
    sqlite3 = await (init as unknown as (opts?: object) => Promise<Sqlite3Module>)({
      print: () => {},
      printErr: () => {},
    });
  }
  return sqlite3;
}

// 'multi'  - readwrite-unsafe handles held persistently (Chromium).
// 'baton'  - read-only reader handles + an exclusive readwrite writer
//            handle, acquired/released around fence windows (Safari 16.4+,
//            Firefox: OPFS with 'read-only' mode but no readwrite-unsafe).
// 'single' - SAHPool, one connection, reads hosted on the writer worker.
export type ConnMode = 'multi' | 'baton' | 'single';

export type Engine = {
  db: Sqlite3Db;
  connMode: ConnMode;
  /** Flush the underlying OPFS handle so other workers see this commit NOW. */
  flush: () => void;
  /** Run `sql` (a SELECT) and return the whole result as one JSON text: `[{...},...]`. */
  execJson: (sql: string, params?: ReadonlyArray<unknown>) => string;
  /**
   * Same as execJson but returns the result as UTF-8 BYTES copied straight
   * out of the wasm heap - no JS string is ever materialized. This is the
   * hot read path: the result crosses to the main thread as bytes anyway,
   * so building a UTF-16 string first meant transcoding the whole result
   * twice (wasm UTF-8 -> JS UTF-16 -> UTF-8) and holding a 2x-sized
   * intermediate. Hashing runs on the bytes too.
   */
  execJsonBytes: (sql: string, params?: ReadonlyArray<unknown>) => Uint8Array;
  /**
   * Subscription read path: hash the result's UTF-8 bytes IN the wasm heap
   * and copy them out only when the hash differs from `prevHash`. The
   * common re-run outcome is "dirty but unchanged" (a commit touched a dep
   * table without changing this result), and that path is now
   * zero-allocation: no copy, no string, nothing to GC.
   */
  execJsonHashed: (
    sql: string,
    params: ReadonlyArray<unknown> | undefined,
    prevHash: number | null
  ) => { hash: number; bytes: Uint8Array | null };
  /**
   * Delta read path: `[key, rowHash, rowJson | null][]` per row. `keyCol`
   * must be a column of the result set. Row JSON is hashed in the wasm
   * heap; the text is materialized ONLY when `prevHashOf(key)` disagrees
   * (or is absent), so unchanged rows - the overwhelming majority on a
   * typical tick - never pay the UTF-8 -> UTF-16 decode.
   */
  execRows: (
    sql: string,
    keyCol: string,
    params?: ReadonlyArray<unknown>,
    prevHashOf?: (key: string) => number | undefined
  ) => Array<[string, number, string | null]>;
  /**
   * Cross-connection change counter (PRAGMA data_version). Bumps when
   * ANOTHER connection commits - the seqlock read validation primitive.
   */
  dataVersion: () => number;
  /** EXPLAIN QUERY PLAN lines that full-scan a user table (index advisor). */
  explainScans: (sql: string) => string[];
  /** Full EXPLAIN QUERY PLAN detail lines (analyze tooling). */
  explainPlan: (sql: string) => string[];
  /**
   * Tear down and re-create the CONNECTION (not the OPFS handles). Needed
   * by seqlock reads: a torn page walk can leave the pager's cache poisoned
   * (observed: every later statement, even pragma reads, returns
   * SQLITE_CORRUPT). Reopen is ~1-3ms; all statement caches are flushed.
   */
  reopen: () => void;
  /** Tables read by `sql`, extracted via sqlite3_set_authorizer at prepare time. */
  tablesRead: (sql: string) => Set<string>;
  /** Bounded row estimate: exact up to `cap`, cap+1 beyond. Advisor triage. */
  approxRows: (table: string, cap?: number) => number;
  /** Drop every non-_ldb_ table (schema-version wipe). Caller re-applies DDL. */
  dropUserTables: () => void;
  /** True in baton mode: every db operation needs acquire()..release(). */
  baton: boolean;
  /**
   * Baton mode: open this connection's OPFS access handle. MUST be called
   * under the matching fence (shared for readers, exclusive for the
   * writer) - the fence provides the mutual exclusion that makes the
   * platform's mode-matching rule satisfiable. No-op in other modes.
   */
  acquire: () => Promise<void>;
  /** Baton mode: flush (writer) + close the handle. No-op otherwise. */
  release: () => void;
  close: () => void;
};

/**
 * Start fetching + compiling the sqlite-wasm module without opening a
 * database. Reader workers call this at load time so the wasm work
 * overlaps the writer's OPFS probe, engine open and schema init instead of
 * starting only once r-init arrives. Idempotent (ensureModule caches), and
 * failures are ignored here - the real open reports them properly.
 */
export function warmModule(): void {
  void ensureModule().catch(() => {});
}

/**
 * Capability probe ladder: dual readwrite-unsafe -> 'multi'; else dual
 * read-only (Safari/Firefox handle-mode support) -> 'baton'; else SAHPool
 * -> 'single'.
 */
export async function resolveConnMode(): Promise<ConnMode> {
  if (await detectHandleModes()) return 'multi';
  if (await detectReadOnlyMode()) return 'baton';
  return 'single';
}

export async function openEngine(opts: {
  dbName: string;
  /** 'writer' opens read-write and may create; 'reader' opens read-only. */
  role: 'writer' | 'reader';
  /** From resolveConnMode(). Readers exist only in 'multi'. */
  connMode: ConnMode;
  /** See LocalDbConfig.models - drives typed JSON/boolean delivery. */
  models?: Record<string, ModelMeta>;
}): Promise<Engine> {
  const s3 = await ensureModule();
  const hints = embedHints(opts.models);
  const path = `/${opts.dbName}`;
  let db: Sqlite3Db;

  const PRAGMAS =
    opts.role === 'writer'
      ? `PRAGMA journal_mode=MEMORY; PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-65536;`
      : `PRAGMA journal_mode=MEMORY; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-32768; PRAGMA query_only=1;`;

  if (opts.connMode === 'multi') {
    // Both roles hold readwrite-unsafe handles: the platform refuses to
    // mix read-only with a live writer (lock modes must match). Readers
    // enforce read-only-ness at the SQLite layer (open 'r' + query_only)
    // and the VFS never sees a write from them.
    await preopenFile(path, {
      mode: 'readwrite-unsafe',
      create: opts.role === 'writer',
    });
    installOpfsModesVfs(s3);
    db = new s3.oo1.DB(path, opts.role === 'writer' ? 'c' : 'r', OPFS_MODES_VFS);
  } else if (opts.connMode === 'baton') {
    // Readers hold 'read-only' handles (N concurrent allowed); the writer
    // opens the single exclusive 'readwrite' handle only inside its write
    // fence. Handles are acquired/released around fence windows - the
    // CALLER of openEngine must hold the matching fence, since the DB
    // open below reads the header through the handle.
    await preopenFile(path, {
      mode: opts.role === 'writer' ? 'readwrite' : 'read-only',
      create: opts.role === 'writer',
      baton: true,
    });
    installOpfsModesVfs(s3);
    db = new s3.oo1.DB(path, opts.role === 'writer' ? 'c' : 'r', OPFS_MODES_VFS);
  } else {
    if (opts.role === 'reader') {
      // SAHPool handles are exclusive - a second worker cannot open the
      // file at all. The client never spawns readers in single mode.
      throw new Error('single-connection mode has no separate reader connections');
    }
    const poolUtil = await s3.installOpfsSAHPoolVfs({
      name: `ldb-${opts.dbName}`,
      initialCapacity: 6,
    });
    db = new poolUtil.OpfsSAHPoolDb(path);
  }

  db.exec(PRAGMAS);
  // The trigram UDFs (similarity, word_similarity, search_similarity) must
  // exist on EVERY connection, reader and writer alike, so any query can
  // use pg_trgm-style fuzzy predicates.
  registerSimilarityUdf(db);

  const capi = s3.capi;
  const wasm = s3.wasm;

  // Dependency extraction: install an authorizer, prepare the statement
  // (which triggers SQLITE_READ callbacks for every table/column the query
  // touches), then remove it. Cached per SQL text.
  const depsCache = new Map<string, Set<string>>();
  function tablesRead(sql: string): Set<string> {
    const cached = depsCache.get(sql);
    if (cached) return cached;
    const tables = new Set<string>();
    let pAuth = 0;
    try {
      // All-args-as-pointers signature: the C authorizer is
      // int xAuth(void*, int, const char*, const char*, const char*, const char*)
      // and installFunction passes char* through as raw wasm pointers -
      // decode explicitly (a plain 's' letter yields pointer numbers).
      pAuth = wasm.installFunction('i(pipppp)', (_pArg: number, op: number, zTab: number) => {
        if (op === capi.SQLITE_READ && zTab) tables.add(wasm.cstrToJs(zTab));
        return capi.SQLITE_OK;
      });
      capi.sqlite3_set_authorizer(db.pointer, pAuth, 0);
      const stmt = db.prepare(sql);
      stmt.finalize();
    } catch {
      // Fallback: unknown deps - callers must treat as "depends on
      // everything" so correctness never hinges on the authorizer.
      tables.clear();
    } finally {
      try {
        capi.sqlite3_set_authorizer(db.pointer, 0, 0);
      } catch {
        // authorizer already gone
      }
      if (pAuth) wasm.uninstallFunction(pAuth);
    }
    // FTS mirrors: a query on `<t>_fts` (or its shadow tables) must be
    // invalidated by commits to `<t>` - map the dep back to the base table.
    const ftsBases: string[] = [];
    for (const t of tables) {
      const m = /^(.+)_fts(_[a-z]+)?$/.exec(t);
      if (m) ftsBases.push(m[1]!);
    }
    for (const b of ftsBases) tables.add(b);
    depsCache.set(sql, tables);
    return tables;
  }

  // The JSON read path: wrap any SELECT so the full result crosses the
  // WASM boundary as ONE string. The wrapped SQL (which needs the result
  // column list) is derived once per SQL text and cached.
  const wrappedCache = new Map<string, string>();
  function wrapJson(sql: string): string {
    const cached = wrappedCache.get(sql);
    if (cached) return cached;
    const probe = db.prepare(sql);
    const names: string[] = [];
    for (let i = 0; i < probe.columnCount; i++) {
      names.push((probe as unknown as { getColumnName: (i: number) => string }).getColumnName(i));
    }
    probe.finalize();
    // Nesting convention: a column named `<x>__json` carries JSON TEXT
    // (e.g. a rowJson() subselect) and is embedded as a real JSON value
    // under `<x>` instead of an escaped string. Columns whose NAME matches
    // a declared JSON/list column embed the same way - so `SELECT t.*`
    // delivers parsed values - but through a json_valid guard, since a
    // name collision with a plain text column (or pre-coercion residue)
    // must degrade to raw text, not error.
    const cols = names.map((n) => embedExpr(n, hints)).join(', ');
    const wrapped = `SELECT COALESCE(json_group_array(json(__row)), '[]') FROM (
			SELECT json_object(${cols}) AS __row FROM (${sql})
		)`;
    wrappedCache.set(sql, wrapped);
    return wrapped;
  }

  // Prepared-statement cache for the read path. Registered queries re-run
  // on every relevant commit; preparing the (wrapped) statement each time
  // was the dominant fixed cost per re-run. LRU by Map insertion order,
  // capped so ad-hoc one-shot SQL can't grow it unboundedly.
  const STMT_CACHE_MAX = 256;
  const stmtCache = new Map<string, Sqlite3Stmt>();
  function evictStmt(sql: string, stmt: Sqlite3Stmt): void {
    stmtCache.delete(sql);
    try {
      stmt.finalize();
    } catch {
      // already finalized
    }
  }

  function cachedJsonStmt(sql: string): Sqlite3Stmt {
    const hit = stmtCache.get(sql);
    if (hit) {
      // Refresh LRU position.
      stmtCache.delete(sql);
      stmtCache.set(sql, hit);
      return hit;
    }
    const stmt = db.prepare(wrapJson(sql));
    if (stmtCache.size >= STMT_CACHE_MAX) {
      const oldest = stmtCache.entries().next().value as [string, Sqlite3Stmt];
      evictStmt(oldest[0], oldest[1]);
    }
    stmtCache.set(sql, stmt);
    return stmt;
  }

  function execJson(sql: string, params?: ReadonlyArray<unknown>): string {
    const stmt = cachedJsonStmt(sql);
    try {
      if (params?.length) stmt.bind(params as unknown[]);
      stmt.step();
      return String(stmt.get(0) ?? '[]');
    } finally {
      try {
        stmt.reset();
      } catch {
        // Statement wedged (e.g. schema changed underneath it) - evict so
        // the next run re-prepares instead of dying on SQLITE_MISUSE.
        evictStmt(sql, stmt);
      }
    }
  }

  // Zero-transcode read path. sqlite3_column_blob gives a pointer to the
  // result's UTF-8 bytes inside the wasm heap; copying that range straight
  // into a Uint8Array skips BOTH the UTF-8 -> UTF-16 decode that building a
  // JS string costs and the UTF-8 re-encode the postMessage needs. The
  // pointer is only valid until the next step/reset, so the copy happens
  // immediately. column_blob before column_bytes is the documented-safe
  // order (no type conversion happens for a TEXT value read as a blob).
  const EMPTY_RESULT = new TextEncoder().encode('[]');
  const EMPTY_ROW = new Uint8Array(0);
  function execJsonBytes(sql: string, params?: ReadonlyArray<unknown>): Uint8Array {
    const stmt = cachedJsonStmt(sql);
    try {
      if (params?.length) stmt.bind(params as unknown[]);
      stmt.step();
      const ptr = capi.sqlite3_column_blob(stmt.pointer, 0) as number;
      if (!ptr) return EMPTY_RESULT.slice();
      const n = capi.sqlite3_column_bytes(stmt.pointer, 0) as number;
      return wasm.heap8u().slice(ptr, ptr + n);
    } finally {
      try {
        stmt.reset();
      } catch {
        evictStmt(sql, stmt);
      }
    }
  }

  // Hash-aware variant of execJsonBytes for the subscription tick: the
  // hash runs over the bytes IN PLACE (a heap subarray, no copy), and the
  // copy out of the heap happens only on a hash change. NOTE heap8u() is
  // re-fetched after step() - statement execution can grow wasm memory,
  // which detaches previously captured views.
  function execJsonHashed(
    sql: string,
    params: ReadonlyArray<unknown> | undefined,
    prevHash: number | null
  ): { hash: number; bytes: Uint8Array | null } {
    const stmt = cachedJsonStmt(sql);
    try {
      if (params?.length) stmt.bind(params as unknown[]);
      stmt.step();
      const ptr = capi.sqlite3_column_blob(stmt.pointer, 0) as number;
      const n = ptr ? (capi.sqlite3_column_bytes(stmt.pointer, 0) as number) : 0;
      const heap = ptr ? wasm.heap8u() : EMPTY_RESULT;
      const view = ptr ? (heap.subarray(ptr, ptr + n) as Uint8Array) : EMPTY_RESULT;
      const hash = hashBytes(view);
      if (prevHash !== null && hash === prevHash) return { hash, bytes: null };
      return { hash, bytes: view.slice() };
    } finally {
      try {
        stmt.reset();
      } catch {
        evictStmt(sql, stmt);
      }
    }
  }

  function clearStmtCache(): void {
    for (const [sql, stmt] of stmtCache) evictStmt(sql, stmt);
  }

  // Delta read path: same statement-cache discipline as execJson, but the
  // wrapper emits (key, rowJson) pairs so the caller can diff per row.
  const rowDecoder = new TextDecoder();
  function execRows(
    sql: string,
    keyCol: string,
    params?: ReadonlyArray<unknown>,
    prevHashOf?: (key: string) => number | undefined
  ): Array<[string, number, string | null]> {
    const cacheKey = `rows\x01${keyCol}\x01${sql}`;
    let stmt = stmtCache.get(cacheKey);
    if (stmt) {
      stmtCache.delete(cacheKey);
      stmtCache.set(cacheKey, stmt);
    } else {
      const probe = db.prepare(sql);
      const names: string[] = [];
      for (let i = 0; i < probe.columnCount; i++) {
        names.push((probe as unknown as { getColumnName: (i: number) => string }).getColumnName(i));
      }
      probe.finalize();
      if (!names.includes(keyCol)) throw new Error(`delta key '${keyCol}' not in result columns`);
      const cols = names.map((n) => embedExpr(n, hints)).join(', ');
      stmt = db.prepare(
        `SELECT CAST("${keyCol.replaceAll('"', '""')}" AS TEXT), json_object(${cols}) FROM (${sql})`
      );
      if (stmtCache.size >= STMT_CACHE_MAX) {
        const oldest = stmtCache.entries().next().value as [string, Sqlite3Stmt];
        evictStmt(oldest[0], oldest[1]);
      }
      stmtCache.set(cacheKey, stmt);
    }
    const out: Array<[string, number, string | null]> = [];
    try {
      if (params?.length) stmt.bind(params as unknown[]);
      while (stmt.step()) {
        const key = String(stmt.get(0));
        // json_object() output is TEXT; column_blob reads its UTF-8 bytes
        // without conversion. Hash in place, decode to a JS string only
        // when the caller doesn't already hold this content.
        const ptr = capi.sqlite3_column_blob(stmt.pointer, 1) as number;
        const n = ptr ? (capi.sqlite3_column_bytes(stmt.pointer, 1) as number) : 0;
        const view = ptr ? (wasm.heap8u().subarray(ptr, ptr + n) as Uint8Array) : EMPTY_ROW;
        const hash = hashBytes(view);
        const changed = !prevHashOf || prevHashOf(key) !== hash;
        out.push([key, hash, changed ? rowDecoder.decode(view) : null]);
      }
      return out;
    } finally {
      try {
        stmt.reset();
      } catch {
        evictStmt(cacheKey, stmt);
      }
    }
  }

  // Seqlock primitive: cheap prepared read of the connection's view of the
  // file change counter. Bumps only when another connection committed.
  let dvStmt: Sqlite3Stmt | null = null;
  function dataVersion(): number {
    if (!dvStmt) dvStmt = db.prepare('SELECT data_version FROM pragma_data_version()');
    try {
      dvStmt.step();
      return Number(dvStmt.get(0));
    } finally {
      try {
        dvStmt.reset();
      } catch {
        try {
          dvStmt.finalize();
        } catch {
          // already gone
        }
        dvStmt = null;
      }
    }
  }

  // Index advisor: EXPLAIN QUERY PLAN detail lines that start with 'SCAN'
  // against a non-system table (a SCAN of a tiny lookup table is fine -
  // that judgement is the developer's; we just surface it).
  function explainScans(sql: string): string[] {
    const notes: string[] = [];
    try {
      const stmt = db.prepare(`EXPLAIN QUERY PLAN ${sql}`);
      try {
        while (stmt.step()) {
          const detail = String(stmt.get(3) ?? '');
          if (isUnindexedScan(detail)) notes.push(detail);
        }
      } finally {
        stmt.finalize();
      }
    } catch {
      // advisory only - never fail a subscription over it
    }
    return notes;
  }

  /**
   * Exact row count up to `cap`, cap+1 beyond - scans at most cap+1 rows,
   * so it is safe to call per new subscription regardless of table size.
   */
  function approxRows(table: string, cap = 200): number {
    try {
      return Number(
        db.selectValue(
          `SELECT count(*) FROM (SELECT 1 FROM "${table.replaceAll('"', '""')}" LIMIT ${cap + 1})`
        )
      );
    } catch {
      return cap + 1; // unknown -> treat as big so advisories still surface
    }
  }

  function explainPlan(sql: string): string[] {
    const lines: string[] = [];
    try {
      const stmt = db.prepare(`EXPLAIN QUERY PLAN ${sql}`);
      try {
        while (stmt.step()) lines.push(String(stmt.get(3) ?? ''));
      } finally {
        stmt.finalize();
      }
    } catch (err) {
      lines.push(`explain failed: ${String(err)}`);
    }
    return lines;
  }

  function reopen(): void {
    clearStmtCache();
    if (dvStmt) {
      try {
        dvStmt.finalize();
      } catch {
        // already gone
      }
      dvStmt = null;
    }
    depsCache.clear();
    wrappedCache.clear();
    try {
      db.close();
    } catch {
      // connection already unusable - that is why we are here
    }
    if (opts.connMode === 'multi' || opts.connMode === 'baton') {
      // baton: caller holds the fence + an acquired handle (reopen only
      // happens inside read/write windows).
      db = new s3.oo1.DB(path, opts.role === 'writer' ? 'c' : 'r', OPFS_MODES_VFS);
    } else {
      throw new Error('reopen unsupported in single-connection mode (shared connection)');
    }
    db.exec(PRAGMAS);
    registerSimilarityUdf(db);
  }

  function dropUserTables(): void {
    const names = db.selectValues(
      `SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE '\\_ldb\\_%' ESCAPE '\\' AND name NOT LIKE 'sqlite_%'`
    );
    for (const n of names) db.exec(`DROP TABLE IF EXISTS "${String(n).replaceAll('"', '""')}"`);
    depsCache.clear();
    wrappedCache.clear();
    clearStmtCache();
  }

  return {
    get db() {
      return db;
    },
    connMode: opts.connMode,
    flush: opts.connMode !== 'single' ? () => flushPreopened(path) : () => {},
    baton: opts.connMode === 'baton',
    acquire: opts.connMode === 'baton' ? () => acquirePreopened(path) : async () => {},
    release: opts.connMode === 'baton' ? () => releasePreopened(path) : () => {},
    execJson,
    execJsonBytes,
    execJsonHashed,
    execRows,
    dataVersion,
    explainScans,
    explainPlan,
    tablesRead,
    approxRows,
    dropUserTables,
    reopen,
    close: () => {
      clearStmtCache();
      db.close();
      if (opts.connMode !== 'single') closePreopened(path);
    },
  };
}
