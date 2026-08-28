// The read side as a reusable service: query registry, dirty marking,
// throttled tick loop, batched changed-only result delivery.
//
// The tick is a leading-edge THROTTLE, not a debounce: a commit landing
// after an idle period runs immediately (isolated updates pay ~0 scheduling
// latency); only sustained churn is paced to the tick interval. Bench: this
// took reactive delivery p50 from ~102ms (old trailing-edge tick) to the
// low single digits for sparse commits, with burst behavior unchanged.
//
// Hosted by: reader.worker.ts ('multi' mode: own connection, own worker)
// and writer.worker.ts ('single' fallback: same worker + same connection
// as the sync engine, because SAHPool allows one connection per file).
//
// Read consistency modes:
//   fence   - one shared Web Lock hold per tick; the whole batch shares a
//             committed snapshot (writer waits at most one tick's SQL).
//   seqlock - NO locks. Each query samples PRAGMA data_version before and
//             after; a bump (or an engine error from a torn page walk)
//             invalidates and retries, falling back to the fence after 4
//             attempts. The writer never waits for readers. Queries within
//             a tick may observe different (individually consistent)
//             snapshots.

import type { AnalyzeEntry, FromReader, ToReaderInit } from '../protocol.ts';
import type { CommitNotice, QuerySpec } from '../types.ts';
import type { Engine } from './engine.ts';

import { withReadFences } from './fence.ts';
import { QueryRegistry } from './registry.ts';

type Post = (m: FromReader, transfer?: Transferable[]) => void;

export type ReadMode = 'fence' | 'seqlock';

const DEFAULT_MAX_UPDATE_HZ = 10;
const TICK_HIDDEN_MS = 1000;
const SEQLOCK_MAX_ATTEMPTS = 4;

export class ReadService {
  private registry = new QueryRegistry();
  private commitSeq = 0;
  private tickTimer: ReturnType<typeof setTimeout> | null = null;
  /** Visible-tab tick interval, from LocalDbConfig.maxUpdateHz. */
  private tickVisibleMs: number;
  /** Hidden tabs tick at 1Hz (never faster than the visible rate). */
  private tickHiddenMs: number;
  private tickIntervalMs: number;
  // Re-entrancy guard: runTick captures its `due` list up front, so a
  // subscribe/commit landing mid-tick (fence mode awaits) would otherwise
  // be missed by the in-flight pass AND run concurrently by a second one.
  // Overlap instead coalesces into exactly one follow-up pass.
  private ticking = false;
  private tickAgain = false;
  /** End of the last tick that ran queries - the throttle reference point. */
  private lastRunEndAt = -Infinity;

  private mode: ReadMode;

  /**
   * Every fence this service reads under: its own db's, plus one per
   * attached db. Sorted so the acquisition order is globally consistent.
   */
  private fences: readonly string[];

  constructor(
    private engine: Engine,
    fence: string | readonly string[],
    private post: Post,
    private opts: { readMode?: ReadMode; advise?: boolean; maxUpdateHz?: number } = {}
  ) {
    this.fences = (typeof fence === 'string' ? [fence] : [...fence]).sort();
    // Baton mode forces fence reads: the OPFS handle only exists inside a
    // fence window, so lock-free seqlock reads are structurally impossible.
    this.mode = engine.baton ? 'fence' : (opts.readMode ?? 'fence');
    const hz = opts.maxUpdateHz && opts.maxUpdateHz > 0 ? opts.maxUpdateHz : DEFAULT_MAX_UPDATE_HZ;
    this.tickVisibleMs = Math.max(1, Math.round(1000 / hz));
    this.tickHiddenMs = Math.max(TICK_HIDDEN_MS, this.tickVisibleMs);
    this.tickIntervalMs = this.tickVisibleMs;
  }

  /** Shared-fence + (baton) handle window around a batch of db reads. */
  private async withReadDb<T>(fn: () => T): Promise<T> {
    return withReadFences(this.fences, async () => {
      // acquire() is all-or-nothing (it rolls back its own partial holds
      // on failure), so it stays OUTSIDE the try: pairing a failed acquire
      // with a release would decrement a hold belonging to a concurrent
      // window and close the handle underneath it.
      await this.engine.acquire();
      try {
        return fn();
      } finally {
        this.engine.release();
      }
    });
  }

  /**
   * A commit landed on some database this service reads. `origin` is the
   * ATTACH alias the notice arrived for, or undefined for this db's own
   * commits; the notice carries LOCAL table names, so an attached db's are
   * qualified here to match the dep keys the authorizer produced.
   *
   * commitSeq deliberately tracks THIS db's sequence only. Each writer has
   * its own counter starting at 0, so folding an attached db's seq in
   * would make QueryResult.commitSeq jump around non-monotonically. A
   * tenant-only commit therefore leaves commitSeq where it was, which is
   * the honest answer - the number means "main's commit sequence".
   */
  onCommit(notice: CommitNotice, origin?: string): void {
    if (origin === undefined) this.commitSeq = notice.seq;
    const tables =
      origin === undefined ? notice.tables : notice.tables.map((t) => `${origin}.${t}`);
    if (this.registry.markDirty(tables) > 0) this.scheduleTick();
  }

  /**
   * Visibility throttling: hidden pages tick at 1Hz instead of 10Hz. Sync
   * keeps running at full speed - only DELIVERY slows - and the first tick
   * after becoming visible fires immediately if anything is dirty.
   */
  setHidden(hidden: boolean): void {
    this.tickIntervalMs = hidden ? this.tickHiddenMs : this.tickVisibleMs;
    if (!hidden && this.tickTimer) {
      clearTimeout(this.tickTimer);
      this.tickTimer = null;
      void this.runTick();
    }
  }

  async subscribe(key: string, spec: QuerySpec): Promise<void> {
    const existing = this.registry.get(key);
    const deps = existing
      ? existing.deps
      : this.engine.baton
        ? await this.withReadDb(() => this.engine.tablesRead(spec.sql))
        : this.engine.tablesRead(spec.sql);
    const e = this.registry.subscribe(key, spec, deps);
    if (existing) {
      // (Re)attaching to a warm entry: the subscriber has NO client-side
      // snapshot (a new client sub always starts empty), so the unchanged-
      // hash suppression in recordRun would starve it forever if the data
      // hasn't moved since the last ship. Reset the ship state so the next
      // run delivers a full result. This is the remount-within-gcTimeMs
      // path (route revisit inside 30s) - without the reset the grid sits
      // on "loading" until the next real commit touches a dep table.
      e.dirty = true;
      e.lastHash = 0;
      if (e.rowHashes) {
        e.rowHashes = new Map();
        e.orderHash = 0;
      }
    }
    if (this.opts.advise && !existing) {
      // Row-count-aware advisories: a SCAN over a tiny table is the
      // planner being RIGHT (probing an index costs more than reading a
      // handful of rows), so when every dep table holds <=50 rows the
      // advisory is suppressed; otherwise each note carries the dep
      // sizes for instant triage. Counts are bounded probes (<=201 rows
      // scanned). Cold-boot caveat: pre-hydration tables count as 0, so
      // advisories effectively fire on hydrated data - warm boots and
      // route navigation, which is when the numbers mean something.
      const evaluate = () => {
        const notes = this.engine.explainScans(spec.sql);
        if (!notes.length) return null;
        const sizes = [...deps].map((t) => [t, this.engine.approxRows(t)] as const);
        if (sizes.length && sizes.every(([, n]) => n <= 50)) return null;
        const label = sizes.map(([t, n]) => `${t}${n > 200 ? '>200' : `≈${n}`}`).join(', ');
        return notes.map((n) => `${n} [${label} rows]`);
      };
      const notes = this.engine.baton ? await this.withReadDb(evaluate) : evaluate();
      if (notes) this.post({ t: 'r-advice', key, sql: spec.sql, notes });
    }
    // First result immediately - never wait a tick for mount.
    await this.runTick();
  }

  unsubscribe(key: string): void {
    this.registry.unsubscribe(key, performance.now());
  }

  /**
   * Debug/ops: time each query against the REAL local data, with its full
   * EXPLAIN plan and dependency-table sizes. Runs under the normal read
   * consistency mode (fence-safe in baton). Powers the prod console tool -
   * dev data cannot validate index choices, production rows can.
   */
  async analyze(
    id: number,
    queries: Array<{ sql: string; params?: QuerySpec['params'] }>
  ): Promise<void> {
    const report: AnalyzeEntry[] = [];
    for (const q of queries) {
      try {
        const deps = this.engine.baton
          ? await this.withReadDb(() => this.engine.tablesRead(q.sql))
          : this.engine.tablesRead(q.sql);
        const entry: AnalyzeEntry = { sql: q.sql, ms: -1, bytes: 0, plan: [], deps: [] };
        await this.withReadDb(() => {
          entry.plan = this.engine.explainPlan(q.sql);
          entry.deps = [...deps].map((t) => ({
            table: t,
            rows: this.engine.approxRows(t, 1_000_000),
          }));
        });
        const times: number[] = [];
        for (let i = 0; i < 3; i++) {
          const t0 = performance.now();
          const out = await this.readConsistent(() => this.engine.execJsonBytes(q.sql, q.params));
          times.push(performance.now() - t0);
          entry.bytes = out.byteLength;
        }
        times.sort((a, b) => a - b);
        entry.ms = +times[1]!.toFixed(2);
        report.push(entry);
      } catch (err) {
        report.push({ sql: q.sql, ms: -1, bytes: 0, plan: [], deps: [], error: String(err) });
      }
    }
    this.post({ t: 'r-analyze', id, report });
  }

  /**
   * One-shot (loader) reads. The client batches everything issued within a
   * microtask into ONE message, and the whole batch runs inside ONE
   * consistency window: one Web Lock acquisition, and in baton mode one
   * OPFS handle open/close pair, instead of per query. A route loader
   * firing 6 queries pays a single window. Batch members also share one
   * snapshot, which a sequence of independent reads never guaranteed.
   *
   * A failing query poisons only its own entry - the rest of the batch
   * still returns.
   */
  async oneShot(
    queries: Array<{ id: number; sql: string; params?: QuerySpec['params'] }>
  ): Promise<void> {
    type Ran = { id: number; bytes?: Uint8Array; error?: string };
    let results: Ran[];
    try {
      results = await this.readConsistent(() =>
        queries.map((q): Ran => {
          try {
            // Bytes straight from the wasm heap - no JS string, no encode.
            return { id: q.id, bytes: this.engine.execJsonBytes(q.sql, q.params) };
          } catch (err) {
            return { id: q.id, error: String(err) };
          }
        })
      );
    } catch (err) {
      // The window itself failed (fence/handle) - fail every member.
      results = queries.map((q) => ({ id: q.id, error: String(err) }));
    }
    this.post(
      { t: 'r-query-results', results },
      results.flatMap((r) => (r.bytes ? [r.bytes.buffer as ArrayBuffer] : []))
    );
  }

  /** Run `fn` under the configured consistency mode. */
  private async readConsistent<T>(fn: () => T): Promise<T> {
    if (this.mode !== 'seqlock') {
      return this.withReadDb(fn);
    }
    let poisoned = false;
    for (let attempt = 0; attempt < SEQLOCK_MAX_ATTEMPTS; attempt++) {
      try {
        const v1 = this.engine.dataVersion();
        const result = fn();
        if (this.engine.dataVersion() === v1) return result;
        // A commit landed mid-read: result may be torn - discard and retry.
      } catch {
        // Torn page walk mid-commit. SQLITE_CORRUPT poisons the pager cache
        // (observed: even pragma reads fail afterwards) - reopen the
        // connection before retrying. Cheap (~1-3ms), and rare.
        poisoned = true;
        try {
          this.engine.reopen();
          poisoned = false;
        } catch {
          // The unfenced reopen itself read a torn header - repair under
          // the fence below, where no commit can be in flight.
          break;
        }
      }
    }
    // Pathological contention - take the fence for guaranteed progress.
    return this.withReadDb(() => {
      if (poisoned) {
        try {
          this.engine.reopen();
        } catch {
          // No reopen on this connection mode - fn surfaces the error.
        }
      }
      return fn();
    });
  }

  private scheduleTick(): void {
    if (this.tickTimer) return;
    // Leading edge: if the last query-running tick is older than the
    // interval, run NOW - the interval is a rate ceiling, not a mandatory
    // delay. Only sustained churn waits out the remainder.
    const wait = this.tickIntervalMs - (performance.now() - this.lastRunEndAt);
    if (wait <= 0) {
      void this.runTick();
      return;
    }
    this.tickTimer = setTimeout(() => void this.runTick(), wait);
  }

  private async runTick(): Promise<void> {
    if (this.ticking) {
      this.tickAgain = true;
      return;
    }
    this.ticking = true;
    try {
      await this.runTickInner();
    } finally {
      this.ticking = false;
      if (this.tickAgain) {
        this.tickAgain = false;
        void this.runTick();
      }
    }
  }

  private async runTickInner(): Promise<void> {
    if (this.tickTimer) {
      clearTimeout(this.tickTimer);
      this.tickTimer = null;
    }
    const now = performance.now();
    const due = this.registry.due(now); // visible lane first
    this.registry.gc(now);
    if (due.length === 0) return;
    const seqAtRun = this.commitSeq;
    type Entry = (typeof due)[number];
    type Ran = {
      e: Entry;
      res?: { hash: number; bytes: Uint8Array | null };
      rows?: Array<[string, number, string | null]>;
      /** e.rowHashes captured at run time - recordDeltaRun detects a
       * mid-tick subscribe() reset by comparing identities. */
      prev?: Map<string, number> | null;
    };
    const ran: Ran[] = [];
    // Both paths hand the engine what this entry already holds (last full
    // hash / per-row hashes) so unchanged content is detected against the
    // wasm heap bytes and never copied or decoded.
    const runOne = (e: Entry): Ran => {
      if (e.spec.delta) {
        const prev = e.rowHashes;
        return {
          e,
          prev,
          rows: this.engine.execRows(
            e.spec.sql,
            e.spec.deltaKey ?? 'id',
            e.spec.params,
            prev ? (k) => prev.get(k) : undefined
          ),
        };
      }
      return {
        e,
        res: this.engine.execJsonHashed(
          e.spec.sql,
          e.spec.params,
          e.version > 0 ? e.lastHash : null
        ),
      };
    };

    if (this.mode === 'seqlock') {
      // Batch-level seqlock first: ONE data_version sample around the whole
      // tick (2 pragma reads total instead of 2 per query). If no commit
      // landed mid-tick - the overwhelmingly common case - every result is
      // valid AND the batch shared one snapshot. On a version bump or a
      // torn-walk error, fall back to per-query validation (which also
      // handles reopen-on-corrupt), re-running only what the fallback runs.
      let batchValid = false;
      try {
        const v1 = this.engine.dataVersion();
        const tmp: Ran[] = [];
        for (const e of due) tmp.push(runOne(e));
        if (this.engine.dataVersion() === v1) {
          ran.push(...tmp);
          batchValid = true;
        }
      } catch {
        // torn page walk - the per-query path below reopens and retries
      }
      if (!batchValid && ran.length === 0) {
        // Per-query seqlock: individually consistent, no shared snapshot.
        for (const e of due) {
          try {
            ran.push(await this.readConsistent(() => runOne(e)));
          } catch (err) {
            this.post({ t: 'r-error', key: e.key, error: String(err) });
            e.dirty = false;
          }
        }
      }
    } else {
      // One shared-fence hold for the whole tick: every query sees the same
      // committed snapshot, and ONLY SQL runs under the fence - hashing and
      // encoding happen after release so the writer is never delayed by
      // pure CPU work.
      await this.withReadDb(() => {
        for (const e of due) {
          try {
            ran.push(runOne(e));
          } catch (err) {
            this.post({ t: 'r-error', key: e.key, error: String(err) });
            e.dirty = false; // don't hot-loop a broken query
          }
        }
      });
    }

    const batch: Array<{
      key: string;
      version: number;
      commitSeq: number;
      bytes: Uint8Array;
      delta?: boolean;
    }> = [];
    for (const r of ran) {
      const out = r.rows
        ? this.registry.recordDeltaRun(r.e, r.rows, r.prev ?? null, seqAtRun, now)
        : this.registry.recordRun(r.e, r.res!.hash, r.res!.bytes, seqAtRun, now);
      if (out.bytes) {
        batch.push({
          key: out.key,
          version: out.version,
          commitSeq: out.commitSeq,
          bytes: out.bytes,
          ...('delta' in out && out.delta ? { delta: true } : {}),
        });
      }
    }
    if (batch.length) {
      this.post(
        { t: 'r-results', batch },
        batch.map((b) => b.bytes.buffer as ArrayBuffer)
      );
    }
    this.lastRunEndAt = performance.now();
    if (this.registry.due(performance.now()).length > 0) this.scheduleTick();
  }
}

export type { ToReaderInit };
