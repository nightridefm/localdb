// Main-thread client. Thin by design: subscription bookkeeping and byte
// decoding only - all SQL, scheduling, and diffing happen in the workers.
//
// Boot sequence: reader workers are spawned IMMEDIATELY (their wasm compile
// overlaps the writer's OPFS probe + engine open + schema init), while the
// writer probes capabilities and reports its connection mode. 'multi' /
// 'baton' -> the pool is initialized against the shared file. 'single' ->
// no reader connections can exist (SAHPool is exclusive), so the pool is
// torn down and read traffic routes to the writer's embedded ReadService.
// Messages sent before the topology is known are queued.
//
// Read routing:
//   subscriptions - pinned by TABLE affinity, so queries over one table
//                   share a reader (and its warm page cache) for their
//                   whole lifetime;
//   one-shots     - batched per microtask, sent to the least-loaded reader
//                   as ONE message that runs in ONE fence/handle window.

import type {
  AnalyzeEntry,
  FromReader,
  FromWriter,
  QueryKey,
  ToReader,
  ToWriter,
} from '../protocol.ts';
import type { LocalDbConfig, QueryResult, QuerySpec, SyncStatus, Unsubscribe } from '../types.ts';

import { hashString, queryKey, routingTable } from '../protocol.ts';
import { validateModels } from '../sql.ts';

type Listener = (result: QueryResult) => void;

type Subscription = {
  key: QueryKey;
  spec: QuerySpec;
  listeners: Set<Listener>;
  /** Last delivered snapshot - handed to new listeners synchronously. */
  snapshot: QueryResult | null;
  /** Delta mode: key -> parsed row cache patched by delta documents. */
  deltaCache: Map<string, unknown> | null;
  /** Delta mode: last key order ("k":null deliveries reuse it). */
  deltaOrder: string[] | null;
};

const decoder = new TextDecoder();

/**
 * Hidden this long -> tear the reader pool down (see parkHiddenAfterMs).
 * OFF by default: unpark replays every live subscription at once, and the
 * cost of that depends on the app's subscription count. Enable it
 * deliberately (`parkHiddenAfterMs: 300_000`) after measuring.
 */
const DEFAULT_PARK_HIDDEN_MS = 0;

export class LocalDb {
  private writer: Worker;
  private readers: Worker[] = [];
  /** Outstanding one-shot queries per reader - least-loaded routing. */
  private readerLoad: number[] = [];
  /** query id -> reader index, so completion can decrement readerLoad. */
  private queryReader = new Map<number, number>();
  private connMode: 'multi' | 'baton' | 'single' | null = null;
  private subs = new Map<QueryKey, Subscription>();
  /** Reader index a subscription is pinned to (table affinity). */
  private subReader = new Map<QueryKey, number>();
  private statusListeners = new Set<(s: SyncStatus) => void>();
  /** Last status seen - replayed to late onSyncStatus subscribers. */
  private lastStatus: SyncStatus | null = null;
  /** Set on a terminal 'unsupported' status - all reads fail fast from then on. */
  private terminalError: Error | null = null;
  private destroyed = false;
  private adviceListeners = new Set<(a: { key: QueryKey; sql: string; notes: string[] }) => void>();
  private visibilityHandler: (() => void) | null = null;
  private queryId = 0;
  private pendingQueries = new Map<
    number,
    { resolve: (rows: unknown[]) => void; reject: (e: Error) => void }
  >();
  private pendingAnalyze = new Map<
    number,
    { resolve: (report: AnalyzeEntry[]) => void; reject: (e: Error) => void }
  >();
  /**
   * Every distinct query this session (subscriptions + one-shots), with
   * the latest params - the corpus for analyze(). Capped; oldest evicted.
   */
  private recorded = new Map<string, { sql: string; params?: QuerySpec['params'] }>();
  private recordQuery(sql: string, params?: QuerySpec['params']): void {
    const key = sql + '\u0000' + JSON.stringify(params ?? []);
    this.recorded.delete(key);
    this.recorded.set(key, { sql, params });
    if (this.recorded.size > 300) {
      this.recorded.delete(this.recorded.keys().next().value as string);
    }
  }
  private outbox: ToReader[] = [];
  /** True once reads have somewhere to go (readers ready, or tier-1 writer). */
  private topologyReady = false;
  private rr = 0;
  private readyPromise: Promise<void>;
  private readyResolve!: () => void;
  private readyReject!: (e: Error) => void;
  private readersReady = 0;
  /**
   * Reader-pool supervision. `poolGeneration` invalidates handlers of
   * terminated workers (their queued events must not touch the fresh
   * pool); `poolFailures` counts consecutive respawns without a full
   * r-ready, capping the crash loop before declaring the environment
   * unsupported.
   */
  private poolGeneration = 0;
  private poolFailures = 0;

  // One-shot batching: queries issued within a microtask ship together.
  private oneShotQueue: Array<{ id: number; sql: string; params?: QuerySpec['params'] }> = [];
  private oneShotScheduled = false;

  // Delivery coalescing: snapshots update as messages land, but listeners
  // are notified once per turn so one commit spanning several readers
  // produces ONE render pass.
  private pendingDeliveries = new Set<Subscription>();
  private deliveryScheduled = false;

  // Hidden-tab parking.
  private parkTimer: ReturnType<typeof setTimeout> | null = null;
  private parked = false;

  constructor(
    public readonly config: LocalDbConfig,
    private workers: { writer: Worker; spawnReader: () => Worker }
  ) {
    // Name-global boolean/JSON delivery (engine.embedHints) requires
    // collision-free column naming across models - fail at boot, not with
    // silently mis-typed rows later.
    if (config.models) validateModels(config.models);
    this.writer = workers.writer;
    this.readyPromise = new Promise<void>((res, rej) => {
      this.readyResolve = res;
      this.readyReject = rej;
    });
    // The registered handler marks a terminal rejection as handled for
    // apps that never await ready(); callers who do still see it.
    this.readyPromise.catch(() => {});

    // A writer worker that dies before posting anything (wasm load
    // failure, CSP, OOM) would otherwise leave the app on an infinite
    // spinner - surface it as the terminal unsupported status.
    this.writer.onerror = (e: ErrorEvent) => {
      this.emitStatus({
        state: 'unsupported',
        reason: 'worker-failed',
        error: e.message || 'worker crashed',
      });
    };
    this.writer.onmessage = (e: MessageEvent<FromWriter | FromReader>) => {
      const m = e.data;
      if (m.t === 'ready') {
        this.connMode = m.connMode;
        if (m.connMode === 'multi' || m.connMode === 'baton') this.initReaders();
        else {
          // SAHPool: one connection per file, so the pre-spawned pool can
          // never open it. Tear it down and host reads on the writer.
          this.terminatePool();
          this.flushOutbox();
        }
      } else if (m.t === 'status') {
        this.emitStatus(m.status);
      } else if (m.t === 'error') {
        console.error('[localdb writer]', m.error);
      } else if (m.t !== 'commit') {
        // single mode: reader-family messages arrive on the writer port.
        // ('commit' notices are informational; hooks re-render off
        // r-results, not commits.)
        this.onReaderMessage(m);
      }
    };

    this.send(this.writer, {
      t: 'init',
      config: {
        dbName: config.dbName,
        pauseSyncWhenHidden: config.pauseSyncWhenHidden,
        forceConnMode: config.forceConnMode,
        schemaSql: config.schemaSql,
        schemaVersion: config.schemaVersion,
        writeCoalesceMs: config.writeCoalesceMs,
        alignShapes: config.alignShapes,
        readMode: config.readMode,
        advise: config.advise,
        maxUpdateHz: config.maxUpdateHz,
        fts: config.fts,
        shapes: config.shapes,
        models: config.models,
      },
    });

    // Spawn the pool NOW, before the writer has reported its mode: worker
    // startup + sqlite-wasm fetch/compile (~150ms) is pure latency that
    // overlaps the writer's probe + open + schema init for free. The
    // r-init handshake waits for the mode; the wasm work does not.
    this.spawnPool();

    // Visibility throttling: hidden tabs deliver at 1Hz (sync continues at
    // full speed). Wired here so every read host - reader pool or tier-1
    // writer - follows the page's visibility.
    if (typeof document !== 'undefined') {
      this.visibilityHandler = () => {
        const msg: ToReader & ToWriter = { t: 'visibility', hidden: document.hidden };
        // Writer always gets it (drives the cross-tab pause census);
        // readers get it for delivery throttling.
        this.writer.postMessage(msg);
        for (const r of this.readers) r.postMessage(msg);
        this.onVisibilityChange(document.hidden);
      };
      document.addEventListener('visibilitychange', this.visibilityHandler);
    }
  }

  private poolSize(): number {
    // Leave cores for the writer + main thread, cap at 4 (bench: near-linear
    // read scaling to 4 workers).
    const hw = typeof navigator !== 'undefined' ? (navigator.hardwareConcurrency ?? 4) : 4;
    return Math.max(1, this.config.readers ?? Math.max(1, Math.min(4, hw - 2)));
  }

  /** Create the worker objects (no r-init yet - the mode isn't known). */
  private spawnPool(): void {
    // Handlers are bound to THIS pool generation: a terminated worker's
    // queued events (late results, a crash racing a repool) must never
    // touch the replacement pool's bookkeeping.
    const gen = this.poolGeneration;
    const n = this.poolSize();
    for (let i = 0; i < n; i++) {
      const w = this.workers.spawnReader();
      this.readers.push(w);
      this.readerLoad.push(0);
      w.onmessage = (e: MessageEvent<FromReader>) => {
        if (gen !== this.poolGeneration) return;
        if (e.data.t === 'r-ready') this.onReaderReady();
        else this.onReaderMessage(e.data);
      };
      w.onerror = (e: ErrorEvent) => {
        this.onReaderFailure(gen, e.message || 'reader worker crashed');
      };
    }
  }

  /** Terminate every pool member and invalidate their handlers. */
  private terminatePool(): void {
    this.poolGeneration++;
    for (const r of this.readers) r.terminate();
    this.readers = [];
    this.readerLoad = [];
    this.subReader.clear();
    this.readersReady = 0;
  }

  /**
   * A reader worker died (boot failure or runtime crash). The pool is
   * rebuilt wholesale - the same path a park/unpark takes - and live
   * subscriptions are replayed onto the fresh workers. In-flight one-shots
   * died with their worker and are rejected. Repeated failures without an
   * intervening healthy pool mean the environment cannot run readers at
   * all: give up loudly instead of crash-looping.
   */
  private onReaderFailure(gen: number, error: string): void {
    if (this.destroyed || this.terminalError || gen !== this.poolGeneration) return;
    if (this.parked || this.connMode === 'single') return;
    console.error('[localdb] reader worker failed:', error);
    this.poolFailures++;
    if (this.poolFailures > 3) {
      this.terminatePool();
      this.emitStatus({
        state: 'unsupported',
        reason: 'worker-failed',
        error: `reader workers failed repeatedly: ${error}`,
      });
      return;
    }
    console.warn(`[localdb] respawning reader pool (attempt ${this.poolFailures}/3)`);
    this.topologyReady = false;
    this.terminatePool();
    // Only IN-FLIGHT one-shots are lost; queued ones re-route after respawn.
    for (const [id] of this.queryReader) {
      const p = this.pendingQueries.get(id);
      if (p) {
        this.pendingQueries.delete(id);
        p.reject(new Error('reader pool restarted'));
      }
    }
    this.queryReader.clear();
    // Analyze runs on a specific reader - any in flight died with it.
    for (const [, p] of this.pendingAnalyze) p.reject(new Error('reader pool restarted'));
    this.pendingAnalyze.clear();
    this.spawnPool();
    if (this.connMode === 'multi' || this.connMode === 'baton') this.initReaders();
    // Replay live subscriptions onto the new pool. Queued (un)subscribes
    // were aimed at the dead workers - drop them so registry subscriber
    // counts stay 1:1 with client-side subscriptions.
    this.outbox = this.outbox.filter((m) => m.t !== 'subscribe' && m.t !== 'unsubscribe');
    for (const [key, sub] of this.subs) {
      this.routeReaderMsg({ t: 'subscribe', key, spec: sub.spec });
    }
  }

  /** Hand the pool its connection mode and wait for every member. */
  private initReaders(): void {
    this.readersReady = 0;
    for (const w of this.readers) {
      w.postMessage({
        t: 'r-init',
        dbName: this.config.dbName,
        connMode: this.connMode === 'baton' ? 'baton' : 'multi',
        readMode: this.config.readMode,
        advise: this.config.advise,
        maxUpdateHz: this.config.maxUpdateHz,
        models: this.config.models,
      } satisfies ToReader);
    }
  }

  private onReaderReady(): void {
    if (++this.readersReady < this.readers.length) return;
    this.poolFailures = 0; // the pool proved healthy - reset the crash cap
    // Push the INITIAL state (visible or hidden) so the census is seeded.
    this.visibilityHandler?.();
    this.flushOutbox();
  }

  private flushOutbox(): void {
    this.topologyReady = true;
    const queued = this.outbox;
    this.outbox = [];
    for (const m of queued) this.routeReaderMsg(m);
    this.readyResolve();
  }

  // ── Hidden-tab parking ────────────────────────────────────────────────
  // A hidden tab still holds N reader workers, N page caches and N OPFS
  // handles. After parkHiddenAfterMs hidden the pool is terminated; the
  // client keeps serving its cached snapshots, and unhiding respawns the
  // pool and replays live subscriptions. Sync (writer) is untouched - that
  // is pauseSyncWhenHidden's job.
  private onVisibilityChange(hidden: boolean): void {
    if (this.connMode === 'single') return; // reads live on the writer
    const after = this.config.parkHiddenAfterMs ?? DEFAULT_PARK_HIDDEN_MS;
    if (this.parkTimer) {
      clearTimeout(this.parkTimer);
      this.parkTimer = null;
    }
    if (hidden) {
      if (after > 0) this.parkTimer = setTimeout(() => this.park(), after);
    } else if (this.parked) {
      this.unpark();
    }
  }

  private park(): void {
    this.parkTimer = null;
    if (this.parked || !this.readers.length) return;
    this.parked = true;
    // New read traffic queues in the outbox until the pool is back.
    this.topologyReady = false;
    this.terminatePool();
    // In-flight one-shots can never be answered by a terminated worker.
    for (const [, p] of this.pendingQueries) p.reject(new Error('reader pool parked'));
    this.pendingQueries.clear();
    this.queryReader.clear();
    for (const [, p] of this.pendingAnalyze) p.reject(new Error('reader pool parked'));
    this.pendingAnalyze.clear();
    console.info('[localdb] reader pool parked (tab hidden); will respawn on unhide');
  }

  private unpark(): void {
    this.parked = false;
    this.spawnPool();
    this.initReaders();
    // Live subscriptions must be re-registered: the new workers have empty
    // registries. Snapshots stay valid client-side until fresh results land.
    for (const [key, sub] of this.subs) {
      this.routeReaderMsg({ t: 'subscribe', key, spec: sub.spec });
    }
    console.info('[localdb] reader pool respawned (tab visible)');
  }

  /** Record + fan out a status; a terminal 'unsupported' fails all reads. */
  private emitStatus(status: SyncStatus): void {
    if (this.destroyed) return;
    this.lastStatus = status;
    for (const l of this.statusListeners) l(status);
    if (status.state === 'unsupported') this.onTerminal(status);
  }

  /**
   * Terminal environment failure (no OPFS, storage init failed, workers
   * keep dying): nothing will ever serve a read again. Settle everything
   * that is waiting - ready(), pending one-shots and analyzes - and hand
   * every live subscription an error result, so no caller is left on an
   * infinite spinner.
   */
  private onTerminal(status: SyncStatus & { state: 'unsupported' }): void {
    if (this.terminalError) return;
    this.terminalError = new Error(
      `localdb unsupported (${status.reason})${status.error ? `: ${status.error}` : ''}`
    );
    this.readyReject(this.terminalError);
    for (const [, p] of this.pendingQueries) p.reject(this.terminalError);
    this.pendingQueries.clear();
    this.queryReader.clear();
    for (const [, p] of this.pendingAnalyze) p.reject(this.terminalError);
    this.pendingAnalyze.clear();
    this.outbox = [];
    for (const sub of this.subs.values()) this.deliverError(sub, this.terminalError.message);
  }

  /**
   * Error delivery to a subscription: the previous good rows stay (a
   * transient failure must not blank the screen), `error` is set, and the
   * next successful result clears it by replacing the snapshot.
   */
  private deliverError(sub: Subscription, error: string): void {
    const prev = sub.snapshot;
    sub.snapshot = {
      rows: prev?.rows ?? [],
      version: prev?.version ?? 0,
      commitSeq: prev?.commitSeq ?? 0,
      byteLength: 0,
      error,
    };
    this.pendingDeliveries.add(sub);
    this.scheduleDelivery();
  }

  private onReaderMessage(m: FromReader): void {
    if (m.t === 'r-results') {
      for (const r of m.batch) {
        const sub = this.subs.get(r.key);
        if ((globalThis as { __ldbDebug?: boolean }).__ldbDebug) {
          console.debug(
            '[ldb r-results]',
            r.key,
            'v' + r.version,
            r.bytes?.byteLength ?? 0,
            'B',
            r.delta ? 'delta' : 'full',
            sub ? 'delivered' : 'DROPPED-no-sub'
          );
        }
        if (!sub) continue;
        let rows: unknown[];
        if (r.delta) {
          // Delta document: {"k":[keys in order],"c":{key:row,...}}.
          // Patch the row cache, prune keys gone from k, rebuild in order.
          const doc = JSON.parse(decoder.decode(r.bytes)) as {
            k: string[] | null;
            c: Record<string, unknown>;
          };
          const cache = (sub.deltaCache ??= new Map());
          for (const [k, row] of Object.entries(doc.c)) cache.set(k, row);
          const order = doc.k ?? sub.deltaOrder ?? [];
          if (doc.k) {
            sub.deltaOrder = doc.k;
            const keep = new Set(doc.k);
            for (const k of cache.keys()) if (!keep.has(k)) cache.delete(k);
          }
          rows = order.map((k) => cache.get(k));
        } else {
          rows = JSON.parse(decoder.decode(r.bytes)) as unknown[];
        }
        // Snapshot updates immediately (peek() must never lag a delivered
        // result); the LISTENER call is what gets coalesced.
        sub.snapshot = {
          rows,
          version: r.version,
          commitSeq: r.commitSeq,
          byteLength: r.bytes.byteLength,
        };
        this.pendingDeliveries.add(sub);
      }
      this.scheduleDelivery();
    } else if (m.t === 'r-analyze') {
      const p = this.pendingAnalyze.get(m.id);
      if (p) {
        this.pendingAnalyze.delete(m.id);
        p.resolve(m.report);
      }
    } else if (m.t === 'r-advice') {
      for (const l of this.adviceListeners) l({ key: m.key, sql: m.sql, notes: m.notes });
    } else if (m.t === 'r-query-results') {
      for (const res of m.results) {
        const ri = this.queryReader.get(res.id);
        if (ri !== undefined) {
          this.queryReader.delete(res.id);
          if (this.readerLoad[ri]! > 0) this.readerLoad[ri]!--;
        }
        const p = this.pendingQueries.get(res.id);
        if (!p) continue;
        this.pendingQueries.delete(res.id);
        if (res.error) p.reject(new Error(res.error));
        else p.resolve(JSON.parse(decoder.decode(res.bytes!)) as unknown[]);
      }
    } else if (m.t === 'r-error') {
      console.error('[localdb reader]', m.key ?? '', m.error);
      // A keyed error belongs to a subscription - deliver it so the app
      // can react (useLiveQuery surfaces it) instead of loading forever.
      if (m.key) {
        const sub = this.subs.get(m.key);
        if (sub) this.deliverError(sub, m.error);
      }
    }
  }

  /**
   * One commit can dirty queries pinned to DIFFERENT readers, arriving as
   * several postMessages. Notifying listeners per message means several
   * render passes for one logical change; batching to the end of the turn
   * restores "one commit, one render".
   *
   * setTimeout(0), not requestAnimationFrame: message tasks already queued
   * run before a freshly-scheduled timer, so sibling readers' results are
   * absorbed at ~zero added latency, whereas rAF would add up to a frame
   * to every update AND never fire in a hidden tab.
   */
  private scheduleDelivery(): void {
    if (this.deliveryScheduled) return;
    this.deliveryScheduled = true;
    setTimeout(() => {
      this.deliveryScheduled = false;
      const subs = [...this.pendingDeliveries];
      this.pendingDeliveries.clear();
      for (const sub of subs) {
        const snap = sub.snapshot;
        if (!snap) continue;
        for (const l of sub.listeners) l(snap);
      }
    }, 0);
  }

  /**
   * Route a reader-family message. Subscriptions pin to a pool member by
   * TABLE affinity (same table -> same reader -> one warm page cache
   * instead of N copies); one-shot batches go to the least-loaded reader.
   */
  private routeReaderMsg(m: ToReader): void {
    if (!this.topologyReady) {
      this.outbox.push(m);
      return;
    }
    if (this.connMode === 'single' || this.readers.length === 0) {
      this.writer.postMessage(m);
      return;
    }
    if (m.t === 'query') {
      // SPLIT the microtask batch across the pool rather than sending it
      // to one reader: a worker is single-threaded, so one big batch
      // serializes what the pool could run concurrently. Each reader still
      // gets ONE message (= one fence hold, one baton handle window) for
      // its share, so we keep the batching win AND the parallelism.
      // Measured: 8 concurrent one-shots 0.41ms round-robin (1 window
      // each) -> 0.54ms single batch -> 0.30ms split batches.
      const n = Math.min(this.readers.length, m.queries.length);
      const start = this.leastLoadedReader();
      for (let c = 0; c < n; c++) {
        const share = m.queries.filter((_, qi) => qi % n === c);
        if (!share.length) continue;
        const i = (start + c) % this.readers.length;
        this.readerLoad[i]! += share.length;
        for (const qq of share) this.queryReader.set(qq.id, i);
        this.readers[i]!.postMessage({ t: 'query', queries: share } satisfies ToReader);
      }
    } else if (m.t === 'analyze') {
      this.readers[this.rr++ % this.readers.length]!.postMessage(m);
    } else if (m.t === 'subscribe') {
      this.readers[this.readerFor(m.key, m.spec.sql)]!.postMessage(m);
    } else if (m.t === 'unsubscribe') {
      // Must reach the SAME worker that holds the registry entry.
      const i = this.subReader.get(m.key);
      this.readers[i ?? hashString(m.key) % this.readers.length]!.postMessage(m);
      this.subReader.delete(m.key);
    }
  }

  /** Sticky reader index for a subscription, by table affinity. */
  private readerFor(key: QueryKey, sql: string): number {
    let idx = this.subReader.get(key);
    if (idx === undefined) {
      const table = routingTable(sql);
      idx = (table ? hashString(table) : hashString(key)) % this.readers.length;
      this.subReader.set(key, idx);
    }
    return idx;
  }

  private leastLoadedReader(): number {
    let best = 0;
    for (let i = 1; i < this.readerLoad.length; i++) {
      if (this.readerLoad[i]! < this.readerLoad[best]!) best = i;
    }
    return best;
  }

  /** Resolves once sync + read topology are up (first paint can await this). */
  ready(): Promise<void> {
    return this.readyPromise;
  }

  /** Resolved connection tier ('multi' | 'baton' | 'single'); null before ready. */
  getConnMode(): 'multi' | 'baton' | 'single' | null {
    return this.connMode;
  }

  /**
   * Reactive subscription. The callback fires with the current snapshot as
   * soon as it exists, then on every CHANGED result (worker-side hash
   * dedupe), debounced to the scheduler tick. Identical (sql, params)
   * subscriptions share one registry entry and one execution.
   */
  subscribe<Row = unknown>(
    spec: QuerySpec,
    listener: (result: QueryResult<Row>) => void
  ): Unsubscribe {
    const key = queryKey(spec.sql, spec.params, spec);
    this.recordQuery(spec.sql, spec.params);
    let sub = this.subs.get(key);
    if (!sub) {
      sub = { key, spec, listeners: new Set(), snapshot: null, deltaCache: null, deltaOrder: null };
      this.subs.set(key, sub);
      if (this.terminalError) this.deliverError(sub, this.terminalError.message);
      else this.routeReaderMsg({ t: 'subscribe', key, spec });
    }
    sub.listeners.add(listener as Listener);
    if (sub.snapshot) (listener as Listener)(sub.snapshot);
    return () => {
      sub.listeners.delete(listener as Listener);
      if (sub.listeners.size === 0) {
        this.subs.delete(key);
        this.pendingDeliveries.delete(sub);
        this.routeReaderMsg({ t: 'unsubscribe', key });
      }
    };
  }

  /** Snapshot for a key if a live subscription already has one (hook fast path). */
  peek<Row = unknown>(spec: QuerySpec): QueryResult<Row> | null {
    return (
      (this.subs.get(queryKey(spec.sql, spec.params, spec))?.snapshot as QueryResult<Row>) ?? null
    );
  }

  /**
   * One-shot query (loader path). Same engine and fence as live queries.
   * Calls issued in the same microtask are batched into ONE worker message
   * and executed inside ONE fence/handle window - a route loader firing six
   * queries pays one acquisition, not six.
   */
  query<Row = unknown>(sql: string, params?: QuerySpec['params']): Promise<Row[]> {
    if (this.terminalError) return Promise.reject(this.terminalError);
    if (this.destroyed) return Promise.reject(new Error('LocalDb destroyed'));
    this.recordQuery(sql, params);
    const id = ++this.queryId;
    return new Promise<Row[]>((resolve, reject) => {
      this.pendingQueries.set(id, { resolve: resolve as (rows: unknown[]) => void, reject });
      this.oneShotQueue.push({ id, sql, params });
      if (!this.oneShotScheduled) {
        this.oneShotScheduled = true;
        queueMicrotask(() => {
          this.oneShotScheduled = false;
          const queries = this.oneShotQueue;
          this.oneShotQueue = [];
          if (queries.length) this.routeReaderMsg({ t: 'query', queries });
        });
      }
    });
  }

  /**
   * Debug/ops: run every query recorded this session against the LOCAL
   * data with timing + EXPLAIN plans + dependency sizes. Expose it on the
   * console and run it against PRODUCTION-sized data - dev-sized tables
   * cannot validate index choices.
   */
  analyze(): Promise<AnalyzeEntry[]> {
    if (this.terminalError) return Promise.reject(this.terminalError);
    if (this.destroyed) return Promise.reject(new Error('LocalDb destroyed'));
    const queries = [...this.recorded.values()];
    const id = ++this.queryId;
    return new Promise<AnalyzeEntry[]>((resolve, reject) => {
      this.pendingAnalyze.set(id, { resolve, reject });
      this.routeReaderMsg({ t: 'analyze', id, queries });
    });
  }

  /** Index-advisor findings (config.advise). Dev tooling, not control flow. */
  onAdvice(l: (a: { key: QueryKey; sql: string; notes: string[] }) => void): Unsubscribe {
    this.adviceListeners.add(l);
    return () => this.adviceListeners.delete(l);
  }

  onSyncStatus(l: (s: SyncStatus) => void): Unsubscribe {
    this.statusListeners.add(l);
    // Replay the current state so a listener attached after a status
    // landed (including a terminal 'unsupported') still sees it. Async so
    // the subscribe call returns before the first invocation.
    if (this.lastStatus) {
      const s = this.lastStatus;
      queueMicrotask(() => {
        if (this.statusListeners.has(l)) l(s);
      });
    }
    return () => this.statusListeners.delete(l);
  }

  /**
   * Pause Electric sync for THIS tab's writer (reads keep serving the
   * local data). If this tab is the sync leader, the shape streams stop;
   * a standby records the flag and honours it if promoted. Resume
   * restarts from the stored cursors. Overrides pauseSyncWhenHidden's
   * census until resumeSync().
   */
  pauseSync(): void {
    this.send(this.writer, { t: 'pause' });
  }

  resumeSync(): void {
    this.send(this.writer, { t: 'resume' });
  }

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    if (this.visibilityHandler)
      document.removeEventListener('visibilitychange', this.visibilityHandler);
    if (this.parkTimer) clearTimeout(this.parkTimer);
    this.writer.terminate();
    this.terminatePool();
    this.subs.clear();
    // Settle every outstanding promise - nothing can answer them now.
    const gone = new Error('LocalDb destroyed');
    for (const [, p] of this.pendingQueries) p.reject(gone);
    this.pendingQueries.clear();
    for (const [, p] of this.pendingAnalyze) p.reject(gone);
    this.pendingAnalyze.clear();
    this.queryReader.clear();
    this.outbox = [];
    this.pendingDeliveries.clear();
    this.statusListeners.clear();
    this.adviceListeners.clear();
  }

  private send(w: Worker, m: ToWriter): void {
    w.postMessage(m);
  }
}
