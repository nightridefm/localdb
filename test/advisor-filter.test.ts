// The index advisor's scan classifier, pinned against REAL EXPLAIN QUERY
// PLAN output from sqlite-wasm - not hand-written fixtures - so a plan-
// format change in a SQLite upgrade fails here instead of silently
// re-noising (or muting) the advisor. The regression this guards: FTS5
// trigram LIKE probes report as `SCAN <t>_fts VIRTUAL TABLE INDEX 0:L0`
// (EQP never says "USING INDEX" for virtual tables) and were flagged as
// unindexed scans on every search subscription.
import assert from 'node:assert/strict';

import { initSqlite } from './init-sqlite.ts';

import { isUnindexedScan } from '../src/worker/engine.ts';

const sqlite3 = await initSqlite();
const db = new sqlite3.oo1.DB(':memory:');
db.exec(`
	CREATE TABLE track (id TEXT PRIMARY KEY, title TEXT, artist TEXT, album TEXT, created_at TEXT);
	CREATE INDEX idx_track_created ON track(created_at);
	CREATE VIRTUAL TABLE track_fts USING fts5(title, artist, album, content=track, content_rowid=rowid, tokenize='trigram');
`);

function plan(sql: string): string[] {
  const stmt = db.prepare(`EXPLAIN QUERY PLAN ${sql}`);
  const lines: string[] = [];
  try {
    while (stmt.step()) lines.push(String(stmt.get(3) ?? ''));
  } finally {
    stmt.finalize();
  }
  return lines;
}

// FTS5 trigram LIKE probes: constrained virtual-table access (idxStr
// L0/L1/L2), must NOT flag.
{
  const lines = plan(
    `SELECT rowid FROM track_fts WHERE title LIKE '%syn%' OR artist LIKE '%syn%' OR album LIKE '%syn%'`
  );
  const vtab = lines.filter((l) => l.includes('VIRTUAL TABLE'));
  assert.ok(vtab.length >= 1, `expected vtab plan lines, got: ${lines.join(' | ')}`);
  assert.ok(
    vtab.every((l) => /VIRTUAL TABLE INDEX \d+:./.test(l)),
    `expected constrained idxStr in: ${vtab.join(' | ')}`
  );
  for (const l of vtab) assert.equal(isUnindexedScan(l), false, `false positive: ${l}`);
}

// MATCH: also constrained, must not flag.
for (const l of plan(`SELECT rowid FROM track_fts WHERE track_fts MATCH 'syn'`)) {
  if (l.includes('VIRTUAL TABLE')) assert.equal(isUnindexedScan(l), false, `false positive: ${l}`);
}

// UNconstrained virtual-table scan (no WHERE): empty idxStr, MUST flag.
{
  const lines = plan(`SELECT rowid FROM track_fts`).filter((l) => l.includes('VIRTUAL TABLE'));
  assert.ok(lines.length >= 1, 'expected a bare vtab scan line');
  for (const l of lines) assert.equal(isUnindexedScan(l), true, `missed bare vtab scan: ${l}`);
}

// Bare table scan: MUST flag.
{
  const lines = plan(`SELECT * FROM track WHERE title = 'x'`);
  assert.ok(
    lines.some((l) => isUnindexedScan(l)),
    `expected a flagged scan in: ${lines.join(' | ')}`
  );
}

// Index-ordered scan (ORDER BY + LIMIT over an index): must NOT flag.
{
  const lines = plan(`SELECT * FROM track ORDER BY created_at DESC LIMIT 50`);
  for (const l of lines) assert.equal(isUnindexedScan(l), false, `false positive: ${l}`);
}

// Indexed point lookup: must not flag.
for (const l of plan(`SELECT * FROM track WHERE id = 'x'`)) {
  assert.equal(isUnindexedScan(l), false, `false positive: ${l}`);
}

db.close();
console.log('advisor-filter.test.ts: all assertions passed');
