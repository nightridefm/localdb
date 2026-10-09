// Seqlock read validation: which errors count as a torn read.
//
// A torn read (a commit landing mid-read) is retried after a connection
// reopen. A plain query error is not one: retrying cannot fix it, and each
// reopen flushes the statement and dependency caches of every query on the
// reader. The real ReadService runs here against a scripted engine; the
// error class it keys on is anchored to a real sqlite-wasm error below.
import assert from 'node:assert/strict';

import type { FromReader } from '../src/protocol.ts';
import type { Engine } from '../src/worker/engine.ts';

import { ReadService } from '../src/worker/read-service.ts';
import { initSqlite } from './init-sqlite.ts';

// The engine's errors are sqlite-wasm SQLite3Errors: bad SQL carries
// SQLITE_ERROR (1), which must classify as the query's own error.
const sqlite3 = await initSqlite();
const db = new sqlite3.oo1.DB(':memory:');
let realErr: unknown = null;
try {
  db.prepare('SELECT * FROM no_such_table');
} catch (err) {
  realErr = err;
}
assert.equal((realErr as { resultCode?: number }).resultCode, 1);
db.close();

const sqliteError = (resultCode: number) =>
  Object.assign(new Error(`sqlite rc ${resultCode}`), { resultCode });

/** Engine stub: execJsonHashed runs the next scripted step. */
function harness(steps: Array<() => void>) {
  let dv = 1;
  let reopens = 0;
  const posted: FromReader[] = [];
  const engine = {
    baton: false,
    tablesRead: () => new Set(['t']),
    dataVersion: () => dv,
    reopen: () => {
      reopens++;
    },
    execJsonHashed: () => {
      const step = steps.shift();
      step?.();
      return { hash: 7, bytes: new TextEncoder().encode('[{"a":1}]') };
    },
  } as unknown as Engine;
  const svc = new ReadService(engine, 'ldb:fence:test', (m) => posted.push(m), {
    readMode: 'seqlock',
  });
  return {
    svc,
    posted,
    reopens: () => reopens,
    bump: () => dv++,
  };
}

// Bad SQL: one r-error, zero reopens, no fence fallback (node has no
// navigator.locks, so reaching the fence would throw instead).
{
  const h = harness([
    () => {
      throw sqliteError(1);
    },
    () => {
      throw sqliteError(1);
    },
  ]);
  await h.svc.subscribe('k', { sql: 'SELECT * FROM t' });
  assert.equal(h.reopens(), 0, 'plain query error never reopens');
  assert.deepEqual(
    h.posted.map((m) => m.t),
    ['r-error']
  );
  console.log('plain error: surfaced once, no reopen ok');
}

// SQLITE_CORRUPT mid-read: reopen once, retry, deliver.
{
  const h = harness([
    () => {
      throw sqliteError(11);
    },
    () => {
      throw sqliteError(11);
    },
  ]);
  await h.svc.subscribe('k', { sql: 'SELECT * FROM t' });
  assert.equal(h.reopens(), 1, 'corrupt read reopens before the retry');
  assert.deepEqual(
    h.posted.map((m) => m.t),
    ['r-results']
  );
  console.log('torn read (corrupt): reopen + retry ok');
}

// A non-sqlite error while a commit landed mid-read is still torn.
{
  const h = harness([
    () => {
      throw new Error('batch');
    },
    () => {
      h.bump();
      throw new Error('garbage from a half-written page');
    },
  ]);
  await h.svc.subscribe('k', { sql: 'SELECT * FROM t' });
  assert.equal(h.reopens(), 1, 'error under a version bump counts as torn');
  assert.deepEqual(
    h.posted.map((m) => m.t),
    ['r-results']
  );
  console.log('torn read (version bump): reopen + retry ok');
}

console.log('seqlock-retry.test.ts: all assertions passed');
