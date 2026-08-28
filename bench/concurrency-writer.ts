// Concurrency-bench writer: benchmarks the engine + fence + per-commit
// flush under concurrent readers.
//
// SCOPE - read this before quoting a number here as a write-path result.
// This file is a hand-rolled writer: it opens the engine directly and
// inserts through its own prepared statements. It does NOT use
// makeApplier and does NOT spawn writer.worker.ts, so NOTHING here
// exercises ingest coercion, the json_each bulk path, partial updates,
// deletes, tags/move-out, write coalescing, cursor persistence, deferred
// indexes or FTS maintenance. The schema below is synthetic and has no
// boolean, JSON or timestamp columns, so type fidelity is not covered
// either.
//
// This bench answers "how do reads and commits interfere under load".
// For the real write path use smoke/, which drives the actual writer
// worker.

import { commitChannel, fenceName } from '../src/protocol.ts';
import { openEngine, type Engine } from '../src/worker/engine.ts';
import { withWriteFence } from '../src/worker/fence.ts';
// @ts-expect-error plain-JS bench module
import { makeRowGen, makeLinkGen, median } from './gen.js';

const post = (msg: string) => postMessage({ type: 'progress', msg: '[bench-writer] ' + msg });

const DB = 'bench.db';
// attach=1 partitions the SAME data over two files: `upload` moves to its
// own db (with its own writer, fence and commit channel) and the readers
// ATTACH it. The join, row counts and write load are otherwise identical,
// so the A/B isolates the cost of the second file.
const TENANT_DB = 'bench-tenant.db';
let engine: Engine | null = null;
/** Owns `upload`. Same object as `engine` unless attach=1. */
let uploadEngine: Engine | null = null;
let commits: BroadcastChannel | null = null;
/** Channel `upload` commits go out on. Same as `commits` unless attach=1. */
let uploadCommits: BroadcastChannel | null = null;
let attached = false;
let uploadDbName = DB;
let seq = 0;
let totalRows = 0;
let seedRowsTotal = 0;

async function init(seedRows: number, attach = false) {
  attached = attach;
  uploadDbName = attach ? TENANT_DB : DB;
  const root = await navigator.storage.getDirectory();
  for (const f of [DB, TENANT_DB]) {
    try {
      await root.removeEntry(f);
    } catch {
      // fresh already
    }
  }
  engine = await openEngine({ dbName: DB, role: 'writer', connMode: 'multi' });
  commits = new BroadcastChannel(commitChannel(DB));
  if (attach) {
    uploadEngine = await openEngine({ dbName: TENANT_DB, role: 'writer', connMode: 'multi' });
    uploadCommits = new BroadcastChannel(commitChannel(TENANT_DB));
  } else {
    uploadEngine = engine;
    uploadCommits = commits;
  }
  const db = engine.db;
  const udb = uploadEngine.db;
  udb.exec(`
		CREATE TABLE upload (
			id TEXT PRIMARY KEY, title TEXT NOT NULL, artist TEXT NOT NULL,
			duration REAL NOT NULL, plays INTEGER NOT NULL, created_at TEXT NOT NULL
		);
	`);
  db.exec(`
		CREATE TABLE playlist (id INTEGER PRIMARY KEY, name TEXT NOT NULL, radio_id INTEGER NOT NULL);
		CREATE TABLE link (playlist_id INTEGER NOT NULL, upload_id TEXT NOT NULL, position INTEGER NOT NULL);
	`);
  const rowGen = makeRowGen(42);
  const t0 = performance.now();
  const seedUpload = () => {
    const stmt = udb.prepare('INSERT INTO upload VALUES (?,?,?,?,?,?)');
    for (let i = 0; i < seedRows; i++) {
      if (i % 25000 === 0) udb.exec('BEGIN');
      const r = rowGen(i);
      stmt.bind([r.id, r.title, r.artist, r.duration, r.plays, r.created_at]).stepReset();
      if (i % 25000 === 24999 || i === seedRows - 1) udb.exec('COMMIT');
    }
    stmt.finalize();
    uploadEngine!.flush();
  };
  const seedRest = () => {
    db.exec('BEGIN');
    const ps = db.prepare('INSERT INTO playlist VALUES (?,?,?)');
    for (let i = 1; i <= 1000; i++) ps.bind([i, 'playlist_' + i, i % 20]).stepReset();
    ps.finalize();
    const linkGen = makeLinkGen(7, seedRows);
    const ls = db.prepare('INSERT INTO link VALUES (?,?,?)');
    for (let i = 0; i < seedRows / 2; i++) {
      const l = linkGen(i);
      ls.bind([l.playlist_id, l.upload_id, l.position]).stepReset();
    }
    ls.finalize();
    db.exec('COMMIT');
    db.exec('CREATE INDEX idx_link_playlist ON link(playlist_id, upload_id); ANALYZE;');
    engine!.flush();
  };
  if (attach) {
    await withWriteFence(fenceName(TENANT_DB), seedUpload);
    await withWriteFence(fenceName(DB), seedRest);
  } else {
    // One hold for everything, exactly as before - the non-attached path
    // must stay byte-for-byte comparable with older runs.
    await withWriteFence(fenceName(DB), () => {
      seedUpload();
      seedRest();
    });
  }
  totalRows = seedRows;
  seedRowsTotal = seedRows;
  postMessage({ type: 'seeded', seed_ms: Math.round(performance.now() - t0) });
}

async function writeLoad(opts: { batchRows: number; intervalMs: number; durationMs: number }) {
  // The write load lands on whichever db owns `upload` - with attach=1
  // that is the ATTACHED file, so this measures a "tenant" writer
  // contending with readers that hold its fence.
  const e = uploadEngine!;
  const rowGen = makeRowGen(1337);
  const batchTimes: number[] = [];
  const flushTimes: number[] = [];
  const lockWaits: number[] = [];
  let batchNo = 0;
  const tEnd = performance.now() + opts.durationMs;
  while (performance.now() < tEnd) {
    const tReq = performance.now();
    await withWriteFence(fenceName(uploadDbName), () => {
      const tGot = performance.now();
      lockWaits.push(tGot - tReq);
      e.db.exec('BEGIN');
      const stmt = e.db.prepare('INSERT INTO upload VALUES (?,?,?,?,?,?)');
      for (let i = 0; i < opts.batchRows; i++) {
        const r = rowGen(seedRowsTotal + batchNo * opts.batchRows + i);
        r.id = (seedRowsTotal + batchNo * opts.batchRows + i).toString(16).padStart(32, '0');
        stmt.bind([r.id, r.title, r.artist, r.duration, r.plays, r.created_at]).stepReset();
      }
      stmt.finalize();
      e.db.exec('COMMIT');
      const tFlush = performance.now();
      e.flush();
      flushTimes.push(performance.now() - tFlush);
      batchTimes.push(performance.now() - tGot);
      totalRows += opts.batchRows;
      batchNo++;
    });
    seq++;
    uploadCommits!.postMessage({
      seq,
      tables: ['upload'],
      rows: opts.batchRows,
      total: totalRows,
      wall: Date.now(),
    });
    await new Promise((s) => setTimeout(s, opts.intervalMs));
  }
  const stats = {
    batches: batchTimes.length,
    rows: batchNo * opts.batchRows,
    batch_p50_ms: +(median(batchTimes) as number).toFixed(1),
    batch_max_ms: +Math.max(...batchTimes).toFixed(1),
    flush_p50_ms: +(median(flushTimes) as number).toFixed(2),
    flush_max_ms: +Math.max(...flushTimes).toFixed(1),
    writer_lock_wait_p50_ms: +(median(lockWaits) as number).toFixed(1),
    writer_lock_wait_max_ms: +Math.max(...lockWaits).toFixed(1),
  };
  post(
    `wrote ${stats.rows} rows, batch p50 ${stats.batch_p50_ms}ms (flush p50 ${stats.flush_p50_ms}ms)`
  );
  postMessage({ type: 'wrote', stats });
}

function finalize() {
  const integrity = uploadEngine!.db.selectValue('PRAGMA quick_check');
  const count = uploadEngine!.db.selectValue('SELECT count(*) FROM upload');
  if (attached) uploadEngine!.close();
  engine!.close();
  postMessage({ type: 'finalized', integrity, count });
}

onmessage = (
  e: MessageEvent<{
    cmd: string;
    seedRows?: number;
    attach?: boolean;
    batchRows?: number;
    intervalMs?: number;
    durationMs?: number;
  }>
) => {
  const run = async () => {
    if (e.data.cmd === 'init') await init(e.data.seedRows!, e.data.attach);
    else if (e.data.cmd === 'write')
      await writeLoad({
        batchRows: e.data.batchRows!,
        intervalMs: e.data.intervalMs!,
        durationMs: e.data.durationMs!,
      });
    else if (e.data.cmd === 'finalize') finalize();
  };
  run().catch((err) => postMessage({ type: 'error', error: String((err as Error)?.stack || err) }));
};
