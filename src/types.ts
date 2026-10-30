// Public types.
//
// Architecture:
//   - One writer worker: applies Electric shape streams into an OPFS-backed
//     sqlite-wasm DB in fenced transactions, broadcasts commit notices.
//   - One+ reader workers: own connections (multi-connection VFS: own OPFS access
//     handles), execute registered queries, hash results, post only changes.
//   - Main thread: thin client. Subscribes queries, receives UTF-8 JSON
//     bytes, parses only when a result actually changed.

/**
 * Column metadata for one synced table, normally emitted by
 * `scripts/generate-models.mjs` from a Prisma schema. The library uses it
 * for two things:
 *   - ingest coercion (sync/apply.ts): Electric's wire format is
 *     text-typed, so boolean, JSON, and scalar-list columns need
 *     column-type-aware conversion at write time;
 *   - typed delivery (worker/engine.ts): boolean columns come back as real
 *     JSON true/false and JSON/list columns as parsed values, instead of
 *     the stored 1/0 and JSON text.
 */
export type ModelMeta = {
  /** Local (and Postgres) table name. */
  table: string;
  primaryKey: readonly string[];
  columns: readonly string[];
  /** DateTime columns (stored as ISO-8601 TEXT). */
  dateColumns: readonly string[];
  /** Boolean columns (stored as INTEGER 1/0, delivered as true/false). */
  boolColumns: readonly string[];
  /** JSON columns (stored as JSON TEXT, delivered as parsed values). */
  jsonColumns: readonly string[];
  /** Scalar-list columns (stored as JSON TEXT, delivered as arrays). */
  listColumns: readonly string[];
  /** CREATE TABLE statement for the local cache. */
  ddl: string;
};

/** A registered reactive query. SQL is raw SQLite dialect - that is the point. */
export type QuerySpec = {
  sql: string;
  params?: ReadonlyArray<string | number | null>;
  /**
   * Re-run policy: minimum milliseconds between re-runs of THIS query when
   * its dependencies keep changing (trailing-edge debounce). The global
   * scheduler tick (LocalDbConfig.maxUpdateHz) still applies. Default 0
   * (query re-runs at most once per scheduler tick).
   */
  minIntervalMs?: number;
  /**
   * Keep the registry entry (and its last result) warm for this long after
   * the last subscriber unsubscribes, so remounts get an instant snapshot.
   * Default 30_000.
   */
  gcTimeMs?: number;
  /**
   * Scheduling lane. 'visible' queries run before 'background' ones within
   * a tick, so heavy off-screen aggregates can never delay on-screen
   * results. Default 'visible'.
   */
  priority?: 'visible' | 'background';
  /**
   * Row-level delta shipping. Instead of one JSON string for the whole
   * result, the worker hashes each row (keyed by `deltaKey`) and ships only
   * changed rows + the key order; the main thread patches its row cache in
   * place. Wins when results are large and changes are small (grids).
   * Requires `deltaKey` to be a column of the result. Default off.
   */
  delta?: boolean;
  /** Key column for delta mode. Default 'id'. */
  deltaKey?: string;
};

/** Snapshot of a query result delivered to subscribers. */
export type QueryResult<Row = unknown> = {
  /** Parsed rows. Lazily decoded from the worker's UTF-8 JSON on first access. */
  rows: Row[];
  /** Monotonic per-query version - bumps only when the result content changed. */
  version: number;
  /** DB commit sequence this result was computed at (0 = initial). */
  commitSeq: number;
  /** Transferred payload size for this delivery (diagnostics/benching). */
  byteLength: number;
  /**
   * Set when the query's last run failed (SQL error, torn topology). The
   * rows are the last good result (empty if there never was one); the
   * field clears on the next successful delivery.
   */
  error?: string;
};

export type Unsubscribe = () => void;

/** Electric shape subscription definition. */
export type ShapeDef = {
  /** Local table the shape lands in. Also the invalidation key. */
  table: string;
  /** Primary key column(s) of the local table. */
  primaryKey: string[];
  /** Base URL of the Electric shape endpoint (e.g. `/shape/track`). */
  url: string;
  /** Extra query params forwarded to Electric (where clauses, columns...). */
  params?: Record<string, string>;
};

/**
 * One read-only attached database (see LocalDbConfig.attach).
 */
export type AttachDef = {
  /**
   * SQL schema alias the file is attached under - queries address it as
   * `alias.table`. Must be a bare identifier, and not 'main' or 'temp'.
   */
  alias: string;
  /** The attached database's OPFS file name (its own LocalDbConfig.dbName). */
  dbName: string;
  /**
   * Model metadata for the ATTACHED db's tables, so its boolean and JSON
   * columns are delivered typed rather than as raw 1/0 and JSON text.
   * Merged into this db's delivery hints under alias-prefixed keys, then
   * validated together - see validateModels for the naming rule that
   * merge has to satisfy.
   */
  models?: Record<string, ModelMeta>;
};

export type LocalDbConfig = {
  /** OPFS database file name, e.g. 'app.db'. */
  dbName: string;
  /** DDL to apply on a fresh database (CREATE TABLE / CREATE INDEX ...). */
  schemaSql: string;
  /**
   * Bump to invalidate the local cache wholesale: on mismatch with the
   * stored version the file is wiped and every shape re-syncs. The local
   * DB is a disposable cache - this must always be safe.
   */
  schemaVersion: number;
  shapes: ShapeDef[];
  /**
   * Use a registered sync transport instead of Electric shape streams (see
   * sync/transport.ts). `options` must be structured-cloneable: it is sent
   * to the writer worker, where the transport registered under `transport`
   * receives it. `shapes` is ignored (may be empty) when this is set.
   */
  sync?: { transport: string; options?: unknown };
  /**
   * Model metadata keyed by model name (see ModelMeta). Required for any
   * synced table with boolean, JSON, or scalar-list columns - without it
   * those values are stored and delivered as the raw wire text. Plain
   * data; it is forwarded to the workers verbatim.
   */
  models?: Record<string, ModelMeta>;
  /**
   * Read-only ATTACH of other localdb files, so a query can join across
   * partitioned databases - e.g. a per-project db joined to a shared
   * tenant db. Queries then reference `alias.table`.
   *
   * READERS ONLY. The writer never opens an attached file, so sync, the
   * write fence and commit broadcasting are completely unchanged; an
   * attached db is written by its OWN LocalDb instance (usually in the
   * same page), and this side only reads it.
   *
   * Invalidation follows automatically: the reader subscribes to each
   * attached db's commit channel and re-runs any query whose dependency
   * set includes one of its tables.
   *
   * Two constraints worth knowing before you reach for it:
   *   - 'single' connection mode CANNOT attach - SAHPool access handles
   *     are exclusive per file, so the second file cannot be opened at
   *     all. Boot fails loudly there rather than silently degrading.
   *   - Cross-FILE joins are NOT LSN-aligned. `alignShapes` coordinates
   *     shapes within one writer; nothing coordinates two writers. A
   *     logical change spanning both files lands as two independent
   *     commits, so a joined query briefly observes the intermediate
   *     state before converging. Same-file joins are unaffected.
   */
  attach?: AttachDef[];
  /**
   * How many reader-pool members open the attached files ("wide" readers).
   * Default 1. Cross-db queries are routed to these; everything else uses
   * the whole pool as before.
   *
   * The default is 1 because each attach costs every wide reader a second
   * persistent OPFS access handle (multi mode) and a second shared fence
   * hold per read window - and that fence is the ATTACHED db's, so N
   * project tabs contend on one tenant writer. Raise it only if cross-db
   * queries are a large share of read traffic and the bench says the
   * single wide reader is the bottleneck. Clamped to [1, pool size].
   */
  attachReaders?: number;
  /** Global ceiling for reactive re-run delivery. Default 10 (Hz). */
  maxUpdateHz?: number;
  /**
   * Write-side coalescing window (ms). Live shape messages are buffered
   * and committed at most once per window - one fenced transaction, ONE
   * flush, one merged commit notice - instead of once per long-poll
   * response. Anything faster than the reader tick is wasted flushes and
   * fence contention; this caps it. Adds at most one window of latency
   * after the first live commit (hydration chunks and the first
   * up-to-date commit bypass the window for fast first paint).
   * Default 50 (half the 100ms reader tick).
   */
  writeCoalesceMs?: number;
  /**
   * Reader pool size (multi-connection mode only). Default: derived from
   * navigator.hardwareConcurrency, clamped to [1, 4].
   */
  readers?: number;
  /**
   * Read consistency mode.
   * 'fence' (default): readers take a shared Web Lock per tick; writer
   *   commits under the exclusive lock. Proven; readers can briefly wait
   *   on a commit and vice versa.
   * 'seqlock': readers take NO lock. Each query samples
   *   `PRAGMA data_version` before and after execution; a change (or an
   *   engine error from a torn page walk) discards the result and retries,
   *   falling back to the fence after 4 attempts. The writer never waits
   *   for readers at all.
   */
  readMode?: 'fence' | 'seqlock';
  /**
   * Testing/debug override: force a connection tier instead of probing
   * ('baton' lets Chromium exercise the Safari/Firefox path). Baton and
   * single ignore readMode='seqlock' - handle acquisition is
   * lock-mediated, so fence mode is structurally required.
   */
  forceConnMode?: 'multi' | 'baton' | 'single';
  /**
   * Cross-shape LSN alignment. When several shapes sync, a live batch for
   * one table is held until every other live shape has reported the same
   * global LSN (or `maxSkewMs` elapses - a quiet shape's long-poll can't
   * advance its head, so alignment is bounded, not absolute). Eligible
   * batches commit together in ONE transaction with a merged commit
   * notice, so cross-table joins never observe skew between active
   * streams. Initial hydration commits unaligned. Default off.
   */
  alignShapes?: boolean | { maxSkewMs?: number };
  /**
   * Live queries never render from different commits. Without it, live
   * queries are spread over the reader pool by table, each reader re-runs
   * its own on its own tick, and one render can show one query at a new
   * commit and another at an older one (a task beside a stale comment
   * count) - measured in localsync's collab soak at 93% of render turns
   * under two people's steady edits. With it, every live query runs on
   * one reader: each tick reads them all in ONE snapshot (fence reads) and
   * posts them in one message, delivered in one render. One-shot queries
   * still use the whole pool. Costs live-query parallelism, so turn it on
   * when screens combine several live queries that must agree. Default off.
   */
  alignCommits?: boolean;
  /**
   * Dev-mode index advisor: on first registration of each query, run
   * EXPLAIN QUERY PLAN and surface full-table SCANs via onAdvice.
   * Default off.
   */
  advise?: boolean;
  /**
   * FTS5 trigram mirrors. For each entry the writer maintains
   * `<table>_fts` (external-content FTS5, tokenize=trigram) plus sync
   * triggers, giving indexed substring search (`LIKE '%x%'` on the fts
   * table) and fuzzy candidate retrieval (MATCH + rank). Like secondary
   * indexes, the mirror is dropped for full hydrations and rebuilt after
   * the first up-to-date. Queries against `<table>_fts` are invalidated
   * by commits to `<table>` automatically.
   */
  fts?: Array<{ table: string; columns: string[] }>;
  /**
   * Memory saver: tear the reader pool down after the tab has been hidden
   * this long, respawning it (and replaying live subscriptions) on unhide.
   * A hidden tab otherwise holds N workers, N page caches and N OPFS
   * handles indefinitely - which matters most on Safari, whose handle cap
   * is low. Cached snapshots keep serving while parked.
   *
   * Default 0 (DISABLED). The mechanism is verified, but only with a
   * couple of live subscriptions - unpark replays every live subscription
   * at once, and that has not been measured against an app syncing dozens
   * of shapes. Set 300_000 (5 min) to enable once it has. Ignored in
   * 'single' mode, where reads are hosted by the writer.
   */
  parkHiddenAfterMs?: number;
  /**
   * Battery saver: park the Electric streams entirely while EVERY tab of
   * this database is hidden, resuming from the stored cursors on unhide.
   * Tab visibility is tracked via a shared Web Lock census, so a hidden
   * leader keeps syncing as long as any other tab is visible, and crashed
   * tabs can never wedge the census. Default off (visibility throttling
   * of DELIVERY is always on regardless).
   */
  pauseSyncWhenHidden?: boolean;
};

/** Commit notice broadcast by the writer after every fenced transaction. */
export type CommitNotice = {
  seq: number;
  /** Local table names touched by the transaction. */
  tables: string[];
  /** Rows applied (diagnostic). */
  rows: number;
};

/** Sync progress surfaced to the app (initial hydration UI etc.). */
export type SyncStatus =
  | { state: 'connecting' }
  | { state: 'initial-sync'; table: string; rows: number }
  | { state: 'live' }
  | { state: 'paused' }
  /**
   * A shape stream failed and will be retried. `httpStatus` carries the
   * upstream response code when the failure was an HTTP error rather than a
   * transport/parse one, so the app can distinguish "network is flaky" from
   * "the server no longer authorises this shape".
   *
   * 401/403 usually means the session no longer authorizes a shape this
   * client is still subscribed to - a signal for the app to re-check its
   * own identity or permissions.
   */
  | { state: 'error'; error: string; retryInMs: number; httpStatus?: number }
  /**
   * TERMINAL: this environment cannot run the local DB (no retry will
   * follow). The app should tell the user plainly instead of spinning.
   *   'no-opfs'        - OPFS missing entirely (unsupported browser, or
   *                      Safari Private Browsing, where the API is
   *                      deliberately unavailable)
   *   'storage-failed' - OPFS present but storage init threw; in 'single'
   *                      mode the usual cause is the DB file being held
   *                      by another tab (SAHPool handles are exclusive)
   *   'worker-failed'  - the worker itself died before/at boot (wasm
   *                      load failure, CSP, OOM)
   */
  | {
      state: 'unsupported';
      reason: 'no-opfs' | 'storage-failed' | 'worker-failed';
      error?: string;
    };
