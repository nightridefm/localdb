// Concurrency-bench reader: the REAL library read path - src engine + the
// actual ReadService (registry, debounced ticks, hash dedupe). Three
// measurements per run:
//   1. Raw query loop under the shared fence (qps/latency).
//   2. FRESHNESS: every 10th iteration asserts count(*) >= the row total
//      announced by the last commit notice received BEFORE the fence was
//      requested. Any shortfall is a stale read.
//   3. Reactive pipeline: two live subscriptions through ReadService;
//      delivery latency is measured in main against notice wall times.

import type { FromReader } from '../src/protocol.ts';
import type { CommitNotice } from '../src/types.ts';

import { commitChannel, fenceName } from '../src/protocol.ts';
import { openEngine, type Engine } from '../src/worker/engine.ts';
import { withReadFence } from '../src/worker/fence.ts';
import { ReadService } from '../src/worker/read-service.ts';
// @ts-expect-error plain-JS bench module
import { median } from './gen.js';

const DB = 'bench.db';
let engine: Engine | null = null;
let svc: ReadService | null = null;
let commits: BroadcastChannel | null = null;
let id = -1;
let phase = 'idle';
let running = false;
let lastNotice: { seq: number; total: number } | null = null;

const post = (msg: string) => postMessage({ type: 'progress', msg: `[bench-reader${id}] ` + msg });

const QUERY = `
	SELECT json_group_array(json_object('id',u.id,'title',u.title,'artist',u.artist,'plays',u.plays,'created_at',u.created_at))
	FROM (
		SELECT u.* FROM link l JOIN upload u ON u.id = l.upload_id
		WHERE l.playlist_id = ? ORDER BY l.position LIMIT 100
	) u
`;

const samples: Record<string, number[]> = { baseline: [], write: [] };
const staleness = { checks: 0, violations: 0, maxShortfall: 0 };
let mode: 'fence' | 'seqlock' = 'fence';
let seqlockRetries = 0;
let seqlockReopens = 0;

// ── data_version lag instrument ─────────────────────────────────────────
// Is data_version a PROMPT signal? Every commit notice starts a stopwatch;
// the next observed change of this connection's data_version stops it. If
// the lag is ~0 the pragma is a sound basis for seqlock validation; if it
// runs into seconds, the reader's pager is serving a stale view and no
// amount of sampling discipline can save the protocol.
const dvLags: number[] = [];
let lastDv = -1;
let dvPendingSince = 0;
function sampleDv(): number {
  const v = engine!.dataVersion();
  if (v !== lastDv) {
    if (lastDv !== -1 && dvPendingSince) dvLags.push(performance.now() - dvPendingSince);
    lastDv = v;
    dvPendingSince = 0;
  }
  return v;
}

// Seqlock reads follow the library's ReadService discipline exactly:
// sample data_version, run, sample again, and discard on any bump
// (reopen on a torn-walk error); fall back to the fence after 4
// attempts or when the reopen itself fails (an unfenced reopen can read
// a torn header mid-commit). Every single read is validated.
async function readGuarded<T>(fn: () => T): Promise<T> {
  if (mode === 'fence') return withReadFence(fenceName(DB), fn);
  let poisoned = false;
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const v1 = sampleDv();
      const out = fn();
      if (sampleDv() === v1) return out;
      seqlockRetries++;
    } catch {
      seqlockRetries++;
      seqlockReopens++;
      try {
        engine!.reopen();
      } catch {
        poisoned = true;
        break; // reopen failed mid-commit - repair under the fence below
      }
    }
  }
  return withReadFence(fenceName(DB), () => {
    // Under the fence no commit is in flight, so a connection whose
    // unfenced reopen failed on a torn header can be reopened safely.
    if (poisoned) engine!.reopen();
    return fn();
  });
}

async function start(readerId: number, readMode: 'fence' | 'seqlock', payloadRows = 0) {
  id = readerId;
  mode = readMode;
  engine = await openEngine({ dbName: DB, role: 'reader', connMode: 'multi' });
  // The real reactive pipeline: forward subscription results (metadata
  // only) to main, which correlates commitSeq with notice wall times.
  svc = new ReadService(engine, fenceName(DB), (m: FromReader) => {
    if (m.t === 'r-results') {
      postMessage({
        type: 'sub-results',
        items: m.batch.map((b) => ({
          key: b.key,
          version: b.version,
          commitSeq: b.commitSeq,
          bytes: b.bytes.byteLength,
        })),
        wall: Date.now(),
      });
    }
  });
  await svc.subscribe(`count:${id}`, { sql: 'SELECT count(*) AS n FROM upload' });
  await svc.subscribe(`agg:${id}`, {
    sql: 'SELECT playlist_id, count(*) AS c FROM link GROUP BY playlist_id ORDER BY c DESC LIMIT 10',
  });
  // Wire test: a LARGE live result, re-delivered on every commit. The
  // tiny count/agg subscriptions above say nothing about payload cost -
  // this one puts hundreds of KB per commit through hash -> transfer ->
  // parse, which is what a real admin grid does.
  // ORDER BY rowid DESC so freshly inserted rows always enter the window:
  // an `ORDER BY id` window never moved (the writer's ids sort outside
  // it), so the query re-ran but its RESULT never changed and hash dedupe
  // correctly shipped nothing - which measured the scheduler, not the wire.
  if (payloadRows > 0) {
    await svc.subscribe(`payload:${id}`, {
      sql: `SELECT id, title, artist, plays, created_at FROM upload ORDER BY rowid DESC LIMIT ${payloadRows}`,
    });
  }
  commits = new BroadcastChannel(commitChannel(DB));
  commits.onmessage = (e: MessageEvent<CommitNotice & { total: number }>) => {
    lastNotice = { seq: e.data.seq, total: e.data.total };
    // Start the data_version stopwatch: the bytes are already flushed
    // and visible when the notice goes out, so any delay before this
    // connection's data_version moves is pure detection lag.
    if (!dvPendingSince) dvPendingSince = performance.now();
    svc!.onCommit(e.data);
  };
  postMessage({ type: 'ready' });

  running = true;
  const stmt = () => engine!.db.prepare(QUERY);
  let i = 0;
  while (running) {
    const ph = phase;
    if (ph === 'idle') {
      await new Promise((s) => setTimeout(s, 20));
      continue;
    }
    const expected = lastNotice; // captured BEFORE the read begins
    const tReq = performance.now();
    await readGuarded(() => {
      if (i % 10 === 9) {
        // Freshness assertion (excluded from latency samples).
        const n = Number(engine!.db.selectValue('SELECT count(*) FROM upload'));
        staleness.checks++;
        if (expected && n < expected.total) {
          staleness.violations++;
          staleness.maxShortfall = Math.max(staleness.maxShortfall, expected.total - n);
        }
      } else {
        const s = stmt();
        s.bind([1 + (i % 1000)]).step();
        s.get(0);
        s.finalize();
        samples[ph]?.push(performance.now() - tReq);
      }
    });
    i++;
    if (i % 25 === 0) await new Promise((s) => setTimeout(s, 0));
  }
  const mk = (ph: string) => {
    const arr = samples[ph]!;
    const sorted = [...arr].sort((a, b) => a - b);
    return {
      n: arr.length,
      p50_ms: arr.length ? +(median(arr) as number).toFixed(2) : null,
      p95_ms: arr.length ? +sorted[Math.floor(arr.length * 0.95)]!.toFixed(2) : null,
      max_ms: arr.length ? +Math.max(...arr).toFixed(1) : null,
    };
  };
  const dvSorted = [...dvLags].sort((a, b) => a - b);
  const stats = {
    baseline: mk('baseline'),
    write: mk('write'),
    staleness,
    mode,
    seqlockRetries,
    seqlockReopens,
    // How long after a commit notice this connection's data_version
    // actually moved. The seqlock protocol is only as sound as this.
    dv_lag: dvSorted.length
      ? {
          n: dvSorted.length,
          p50_ms: +dvSorted[dvSorted.length >> 1]!.toFixed(2),
          p95_ms: +dvSorted[Math.floor(dvSorted.length * 0.95)]!.toFixed(2),
          max_ms: +dvSorted[dvSorted.length - 1]!.toFixed(1),
        }
      : null,
  };
  post(
    `baseline p50 ${stats.baseline.p50_ms}ms n=${stats.baseline.n} | write p50 ${stats.write.p50_ms}ms n=${stats.write.n} | stale ${staleness.violations}/${staleness.checks}`
  );
  postMessage({ type: 'stats', stats });
  engine!.close();
}

onmessage = (
  e: MessageEvent<{ cmd: string; id?: number; name?: string; mode?: string; payloadRows?: number }>
) => {
  if (e.data.cmd === 'start')
    start(
      e.data.id!,
      (e.data.mode as 'fence' | 'seqlock') ?? 'fence',
      e.data.payloadRows ?? 0
    ).catch((err) => postMessage({ type: 'error', error: String((err as Error)?.stack || err) }));
  else if (e.data.cmd === 'phase') phase = e.data.name!;
  else if (e.data.cmd === 'stop') running = false;
};
