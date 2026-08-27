// Wire protocol between the main-thread client and the worker side.
// Results travel as transferable UTF-8 bytes: the whole result set is ONE
// JSON string built by SQLite (json_group_array(json_object(...)) wrapper),
// hashed in the worker, and only shipped when changed. The main thread pays
// exactly one JSON.parse per changed query per tick.
//
// Reader-side messages are 'r-' prefixed because in single-connection
// mode the writer worker hosts the read service on the same port - both
// families share one onmessage.

import type { CommitNotice, ModelMeta, QuerySpec, SyncStatus } from './types.ts';

/** Stable key for a (sql, params) pair. */
export type QueryKey = string;

// main -> reader worker ('multi' mode only; in 'single' these go to the writer port)
export type ToReaderInit = {
  t: 'r-init';
  dbName: string;
  connMode?: 'multi' | 'baton';
  readMode?: 'fence' | 'seqlock';
  advise?: boolean;
  /** See LocalDbConfig.maxUpdateHz (drives the scheduler tick interval). */
  maxUpdateHz?: number;
  /** See LocalDbConfig.models (drives typed result delivery). */
  models?: Record<string, ModelMeta>;
};
export type ToReader =
  | ToReaderInit
  | { t: 'subscribe'; key: QueryKey; spec: QuerySpec }
  | { t: 'unsubscribe'; key: QueryKey }
  | { t: 'analyze'; id: number; queries: Array<{ sql: string; params?: QuerySpec['params'] }> }
  /**
   * One-shot (loader) reads, BATCHED. The client coalesces every query
   * issued within a microtask into one message, and the read service runs
   * the whole batch inside ONE fence hold - in baton mode, one OPFS handle
   * open/close pair (~0.3ms) for the batch instead of per query.
   */
  | { t: 'query'; queries: Array<{ id: number; sql: string; params?: QuerySpec['params'] }> }
  | { t: 'visibility'; hidden: boolean };

// read service -> main. `results` buffers are transferred, not cloned.
export type AnalyzeEntry = {
  sql: string;
  /** Median of 3 timed runs (ms); -1 when the query errored. */
  ms: number;
  /** Result JSON size in bytes. */
  bytes: number;
  /** Full EXPLAIN QUERY PLAN detail lines. */
  plan: string[];
  /** Dependency tables with (bounded-at-1M) row counts. */
  deps: Array<{ table: string; rows: number }>;
  error?: string;
};

export type FromReader =
  | { t: 'r-ready' }
  | {
      t: 'r-results';
      batch: Array<{
        key: QueryKey;
        version: number;
        commitSeq: number;
        bytes: Uint8Array;
        /** Payload is a row-delta document, not a plain row array. */
        delta?: boolean;
      }>;
    }
  | { t: 'r-advice'; key: QueryKey; sql: string; notes: string[] }
  | { t: 'r-analyze'; id: number; report: AnalyzeEntry[] }
  | {
      t: 'r-query-results';
      results: Array<{ id: number; bytes?: Uint8Array; error?: string }>;
    }
  | { t: 'r-error'; key?: QueryKey; error: string };

// main -> writer worker (visibility goes to the writer in EVERY mode - it
// drives the pause census; readers get it separately for tick throttling)
export type ToWriter =
  | { t: 'init'; config: WriterInit }
  | { t: 'visibility'; hidden: boolean }
  /**
   * Manual sync pause/resume (LocalDb.pauseSync/resumeSync). Pause stops
   * the Electric streams (data + cursors stay put); resume restarts them
   * from the stored cursors. Per-tab: a standby writer records the flag
   * and honours it if later promoted to leader.
   */
  | { t: 'pause' }
  | { t: 'resume' };

export type WriterInit = {
  dbName: string;
  schemaSql: string;
  schemaVersion: number;
  /** See LocalDbConfig.writeCoalesceMs. */
  writeCoalesceMs?: number;
  /** See LocalDbConfig.alignShapes. */
  alignShapes?: boolean | { maxSkewMs?: number };
  /** See LocalDbConfig.readMode (writer hosts reads in single mode). */
  readMode?: 'fence' | 'seqlock';
  /** See LocalDbConfig.advise. */
  advise?: boolean;
  /** See LocalDbConfig.maxUpdateHz (writer hosts reads in single mode). */
  maxUpdateHz?: number;
  /** See LocalDbConfig.fts. */
  fts?: Array<{ table: string; columns: string[] }>;
  /** See LocalDbConfig.pauseSyncWhenHidden. */
  pauseSyncWhenHidden?: boolean;
  /** See LocalDbConfig.models (drives ingest coercion + typed delivery). */
  models?: Record<string, ModelMeta>;
  /** Testing override: skip capability detection and force a tier. */
  forceConnMode?: 'multi' | 'baton' | 'single';
  shapes: Array<{
    table: string;
    primaryKey: string[];
    url: string;
    params?: Record<string, string>;
  }>;
};

// writer worker -> main
export type FromWriter =
  | { t: 'ready'; wiped: boolean; connMode: 'multi' | 'baton' | 'single' }
  | { t: 'status'; status: SyncStatus }
  | { t: 'commit'; notice: CommitNotice }
  | { t: 'error'; error: string };

/** BroadcastChannel name carrying CommitNotice from writer to readers. */
export const commitChannel = (dbName: string) => `ldb:commits:${dbName}`;

/** Web Locks fence name - readers shared, writer exclusive. */
export const fenceName = (dbName: string) => `ldb:fence:${dbName}`;

/** Cross-tab writer leadership lock - exactly one sync engine per dbName. */
export const writerLockName = (dbName: string) => `ldb:writer:${dbName}`;

/** Shared lock each tab holds WHILE VISIBLE - the leader's pause census. */
export const visibleLockName = (dbName: string) => `ldb:visible:${dbName}`;

/** BroadcastChannel poked on any tab's visibility change (census recheck). */
export const visChannel = (dbName: string) => `ldb:vis:${dbName}`;

/** BroadcastChannel relaying SyncStatus from the leader tab to all tabs. */
export const statusChannel = (dbName: string) => `ldb:status:${dbName}`;

/** djb2-xor - fast, stable, good enough for query keys and result hashes. */
export function hashString(s: string): number {
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h) ^ s.charCodeAt(i);
  return h >>> 0;
}

/**
 * Same hash over UTF-8 bytes - the result read path never materializes a
 * JS string, so change detection runs directly on the bytes. Values are
 * only ever compared against previous hashes of the SAME query, so the
 * fact that this and hashString disagree for non-ASCII input is
 * irrelevant (and no query mixes the two paths).
 *
 * Mixes 4 bytes per round (word composed manually, so the value is
 * independent of the buffer's alignment - the same result bytes at a
 * different wasm heap offset MUST hash identically). Hashes are held
 * in-memory only, so the word-wise form needs no compatibility with the
 * old per-byte one.
 */
export function hashBytes(b: Uint8Array): number {
  let h = 5381;
  const n = b.length;
  let i = 0;
  for (const n4 = n - 3; i < n4; i += 4) {
    const w = b[i]! | (b[i + 1]! << 8) | (b[i + 2]! << 16) | (b[i + 3]! << 24);
    h = ((h << 5) + h) ^ w;
  }
  for (; i < n; i++) h = ((h << 5) + h) ^ b[i]!;
  return h >>> 0;
}

export function queryKey(
  sql: string,
  params?: QuerySpec['params'],
  opts?: { delta?: boolean; deltaKey?: string }
): QueryKey {
  // delta changes the PAYLOAD FORMAT, so it must split the key - the same
  // SQL subscribed plain and in delta mode are different subscriptions.
  // Scheduling options (priority, minIntervalMs) deliberately do not split.
  const d = opts?.delta ? `:d${opts.deltaKey ?? 'id'}` : '';
  // Length is mixed in alongside the 32-bit hash: a hash collision between
  // two DIFFERENT queries would silently merge their subscriptions and
  // serve one's rows to the other's consumers - wrong data with no error.
  // Two same-length collisions are far less likely than a bare hash clash,
  // and the key stays short.
  const p = params?.length ? JSON.stringify(params) : '';
  return `${hashString(sql).toString(36)}${sql.length.toString(36)}:${p ? hashString(p).toString(36) + p.length.toString(36) : '0'}${d}`;
}

/**
 * Cheap main-thread routing hint: the first table named by a FROM/JOIN.
 * Used ONLY to pick a reader worker - correctness never depends on it (any
 * reader can execute any query), so a miss just costs cache locality.
 *
 * Why not the authorizer's real dep set: that lives in the worker, and
 * routing has to happen before the query is ever sent to one. Grouping
 * same-table queries onto the same reader keeps that reader's page cache
 * hot for those pages instead of warming N copies across the pool; hashing
 * the TABLE (not the query key) still spreads distinct tables over the
 * pool. FTS mirrors collapse to their base table so `track` and
 * `track_fts` queries share a reader.
 */
const FROM_JOIN = /\b(?:from|join)\s+"?([a-z_][a-z0-9_]*)"?/i;
export function routingTable(sql: string): string | null {
  const m = FROM_JOIN.exec(sql);
  if (!m) return null;
  return m[1]!.replace(/_fts(_[a-z]+)?$/, '');
}
