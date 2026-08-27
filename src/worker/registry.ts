// Query registry + debounced scheduler. Lives in the READER worker.
//
// Data flow: writer commits -> CommitNotice {tables} on the commit channel
// -> mark dependent queries dirty -> scheduler tick (global <= maxUpdateHz)
// re-runs due queries -> hash result text -> only changed results are
// shipped, batched into ONE postMessage per tick.
//
// Zero-subscriber entries are never executed; they linger (with their last
// result hash) for gcTimeMs after the last unsubscribe so a remount gets an
// instant "unchanged" verdict, then drop out entirely.

import type { QueryKey } from '../protocol.ts';
import type { QuerySpec } from '../types.ts';

import { hashString } from '../protocol.ts';

const encoder = new TextEncoder();

export type RegistryEntry = {
  key: QueryKey;
  spec: QuerySpec;
  /** Tables this query reads. Empty set = unknown = depends on everything. */
  deps: Set<string>;
  subscribers: number;
  dirty: boolean;
  lastRunAt: number;
  lastHash: number;
  version: number;
  /** Set when subscribers hit 0; entry is dropped once now > gcAt. */
  gcAt: number | null;
  /** Delta mode: per-row content hashes from the last run, keyed by deltaKey. */
  rowHashes: Map<string, number> | null;
  /** Delta mode: hash of the last key order. */
  orderHash: number;
};

export type RunOutcome = {
  key: QueryKey;
  version: number;
  commitSeq: number;
  /** UTF-8 result bytes; undefined when unchanged (not shipped). */
  bytes?: Uint8Array;
};

const DEFAULT_GC_MS = 30_000;

export class QueryRegistry {
  private entries = new Map<QueryKey, RegistryEntry>();
  /** table -> keys reading it. Rebuilt incrementally on (un)subscribe. */
  private byTable = new Map<string, Set<QueryKey>>();
  /** Entries with unknown deps - dirtied by EVERY commit. */
  private unknownDeps = new Set<QueryKey>();

  subscribe(key: QueryKey, spec: QuerySpec, deps: Set<string>): RegistryEntry {
    let e = this.entries.get(key);
    if (e) {
      e.subscribers++;
      e.gcAt = null;
      return e;
    }
    e = {
      key,
      spec,
      deps,
      subscribers: 1,
      dirty: true, // first run happens on the next tick (or immediately via runNow)
      lastRunAt: 0,
      lastHash: 0,
      version: 0,
      gcAt: null,
      rowHashes: spec.delta ? new Map() : null,
      orderHash: 0,
    };
    this.entries.set(key, e);
    if (deps.size === 0) this.unknownDeps.add(key);
    for (const t of deps) {
      let s = this.byTable.get(t);
      if (!s) this.byTable.set(t, (s = new Set()));
      s.add(key);
    }
    return e;
  }

  unsubscribe(key: QueryKey, now: number): void {
    const e = this.entries.get(key);
    if (!e) return;
    e.subscribers = Math.max(0, e.subscribers - 1);
    if (e.subscribers === 0) e.gcAt = now + (e.spec.gcTimeMs ?? DEFAULT_GC_MS);
  }

  get(key: QueryKey): RegistryEntry | undefined {
    return this.entries.get(key);
  }

  /** Mark queries dirty for a commit touching `tables`. Returns dirtied count. */
  markDirty(tables: string[]): number {
    let n = 0;
    for (const t of tables) {
      const keys = this.byTable.get(t);
      if (!keys) continue;
      for (const k of keys) {
        const e = this.entries.get(k);
        if (e && !e.dirty) {
          e.dirty = true;
          n++;
        }
      }
    }
    for (const k of this.unknownDeps) {
      const e = this.entries.get(k);
      if (e && !e.dirty) {
        e.dirty = true;
        n++;
      }
    }
    return n;
  }

  /**
   * Dirty entries with subscribers whose per-query interval has elapsed.
   * 'visible' priority lane first, so heavy background aggregates can
   * never delay on-screen results within a tick.
   */
  due(now: number): RegistryEntry[] {
    // Two-bucket partition instead of collect-then-sort: the lane is a
    // binary flag, so a comparison sort is strictly more work than
    // appending to one of two lists. runTickInner calls this twice per
    // tick (once to run, once to decide whether to reschedule).
    const visible: RegistryEntry[] = [];
    let background: RegistryEntry[] | null = null;
    for (const e of this.entries.values()) {
      if (!e.dirty || e.subscribers === 0) continue;
      if (now - e.lastRunAt < (e.spec.minIntervalMs ?? 0)) continue;
      if (e.spec.priority === 'background') (background ??= []).push(e);
      else visible.push(e);
    }
    if (background) visible.push(...background);
    return visible;
  }

  /** Drop zero-subscriber entries past their gc deadline. */
  gc(now: number): void {
    for (const e of this.entries.values()) {
      if (e.gcAt !== null && now > e.gcAt) {
        this.entries.delete(e.key);
        this.unknownDeps.delete(e.key);
        for (const t of e.deps) this.byTable.get(t)?.delete(e.key);
      }
    }
  }

  /**
   * Record a run. The hash was computed by the engine over the result
   * bytes IN the wasm heap (execJsonHashed), and `json` is null exactly
   * when that hash matched the previous run - the unchanged path costs no
   * copy, no string, no allocation at all.
   */
  recordRun(
    e: RegistryEntry,
    hash: number,
    json: Uint8Array | null,
    commitSeq: number,
    now: number
  ): RunOutcome {
    e.dirty = false;
    e.lastRunAt = now;
    if (hash === e.lastHash && e.version > 0) {
      return { key: e.key, version: e.version, commitSeq };
    }
    if (json === null) {
      // The engine suppressed the copy against the hash captured at run
      // time, but lastHash has moved since - a subscribe() reset raced
      // this tick. Re-run against the fresh state instead of shipping
      // nothing (the resubscriber needs a full result).
      e.dirty = true;
      return { key: e.key, version: e.version, commitSeq };
    }
    e.lastHash = hash;
    e.version++;
    return {
      key: e.key,
      version: e.version,
      commitSeq,
      bytes: json,
    };
  }

  /**
   * Delta-mode run: diff per-row hashes against the previous run and build
   * a compact patch document `{"k":[keys in order],"c":{key:row,...}}`.
   * The main thread reconstructs rows as k.map(cache) and prunes cache
   * keys absent from k. Ships nothing when rows AND order are unchanged.
   * The payload is assembled by string concatenation - row JSON is never
   * parsed on the worker.
   */
  recordDeltaRun(
    e: RegistryEntry,
    rows: Array<[string, number, string | null]>,
    prevAtRun: Map<string, number> | null,
    commitSeq: number,
    now: number
  ): RunOutcome & { delta?: boolean } {
    e.dirty = false;
    e.lastRunAt = now;
    if ((e.rowHashes ?? null) !== prevAtRun) {
      // A subscribe() reset replaced the hash map between the query run
      // and this record - the rows' null texts were decided against the
      // OLD map, so a patch built now would be incoherent (missing texts
      // for rows the resubscriber needs). Re-run against the fresh state.
      e.dirty = true;
      return { key: e.key, version: e.version, commitSeq };
    }
    const prev = prevAtRun ?? new Map<string, number>();
    const next = new Map<string, number>();
    const changed: string[] = [];
    let orderAcc = 5381;
    // Row hashes come from the engine (hashed over the wasm heap bytes);
    // `text` is non-null exactly for the rows whose hash differs from
    // `prev` - the engine was handed prev.get as its prevHashOf, so the
    // two views of "changed" can never disagree.
    for (const [key, h, text] of rows) {
      next.set(key, h);
      orderAcc = ((orderAcc << 5) + orderAcc) ^ hashString(key);
      if (prev.get(key) !== h) changed.push(`${JSON.stringify(key)}:${text!}`);
    }
    orderAcc = orderAcc >>> 0;
    const orderChanged = orderAcc !== e.orderHash;
    e.rowHashes = next;
    // Nothing shipped only when content, order AND row count all held:
    // a pure removal leaves every surviving row's hash intact, so the
    // size comparison is what catches it.
    if (changed.length === 0 && !orderChanged && prev.size === next.size && e.version > 0) {
      return { key: e.key, version: e.version, commitSeq };
    }
    const sameOrder = !orderChanged && prev.size === next.size;
    e.orderHash = orderAcc;
    e.version++;
    // Unchanged key order (the common case for in-place row updates): ship
    // "k":null and let the client reuse its cached order - the key list
    // otherwise dominates the payload for large lists.
    const keysJson = sameOrder && e.version > 1 ? 'null' : JSON.stringify(rows.map(([k]) => k));
    const payload = `{"k":${keysJson},"c":{${changed.join(',')}}}`;
    return {
      key: e.key,
      version: e.version,
      commitSeq,
      bytes: encoder.encode(payload),
      delta: true,
    };
  }
}
