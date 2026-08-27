// Inner half of bench/ingest-bench.mjs (bundled, run under node).
//
// A/B for the set-based hydration path: identical wire messages applied
// through the REAL applier, once forced down the per-row bind path and
// once through the json_each bulk path. The only difference is the run
// length threshold, so the delta is purely per-row binds vs one set-based
// statement per chunk.
import sqlite3InitModule from '@sqlite.org/sqlite-wasm';

import { ddlFor, models } from '../examples/models.ts';
import { makeApplier, normalizeValue } from '../src/sync/apply.ts';

const N = Number(process.env.INGEST_N || 200_000);
const results = {};

// Wire-realistic artist rows: Electric sends every value as text, and this
// model is the dense case (2 booleans + 1 json + 2 timestamptz).
function artistWire(i) {
  return {
    id: `01900000-0000-7000-8000-${String(i).padStart(12, '0')}`,
    name: `Artist ${i}`,
    slug: `artist-${i}`,
    bio: i % 3 === 0 ? null : `Bio text for artist number ${i}`,
    links: '{"site":"https://example.com","x":"@artist"}',
    is_verified: i % 2 === 0 ? 't' : 'f',
    has_avatar: i % 3 === 0 ? 't' : 'f',
    created_at: `2026-07-${String((i % 28) + 1).padStart(2, '0')} 12:41:13.736+00`,
    updated_at: `2026-08-${String((i % 28) + 1).padStart(2, '0')} 09:00:00+00`,
  };
}

const sqlite3 = await sqlite3InitModule({ print: () => {}, printErr: () => {} });
const db = new sqlite3.oo1.DB(':memory:');
db.exec(ddlFor('artist'));
const engine = { db };

const msgs = Array.from({ length: N }, (_, i) => ({
  headers: { operation: 'insert' },
  value: artistWire(i),
}));

// Both paths run against the SAME table (so the column-aware coercion is
// identical) into an empty table (so neither pays for the other's rows).
// The per-row path is what a run SHORTER than the bulk threshold takes:
// applying in slices of 8 reproduces it exactly through the same applier.
const cmp = `json_group_array(json_object('v',is_verified,'a',has_avatar,'c',created_at,'u',updated_at,'l',links,'b',bio))`;
const sample = () =>
  String(db.selectValue(`SELECT ${cmp} FROM (SELECT * FROM artist ORDER BY id LIMIT 500)`));

function ingest(sliced) {
  db.exec('DELETE FROM artist');
  const applier = makeApplier(engine, 'artist', ['id'], models.artist);
  db.exec('BEGIN');
  const t0 = performance.now();
  if (sliced) for (let i = 0; i < msgs.length; i += 8) applier.apply(msgs.slice(i, i + 8));
  else applier.apply(msgs);
  const ms = performance.now() - t0;
  db.exec('COMMIT');
  return { ms: +ms.toFixed(0), rows_per_s: Math.round(N / (ms / 1000)), stored: sample() };
}

const row = ingest(true);
const bulk = ingest(false);
results.hydration_ingest = {
  rows: N,
  row_path_rows_per_s: row.rows_per_s,
  row_path_ms: row.ms,
  bulk_path_rows_per_s: bulk.rows_per_s,
  bulk_path_ms: bulk.ms,
  speedup_pct: +(((bulk.rows_per_s - row.rows_per_s) / row.rows_per_s) * 100).toFixed(1),
};
console.log('hydration ingest ', JSON.stringify(results.hydration_ingest));

// Equality check: both paths must produce byte-identical stored rows.
results.paths_identical = row.stored === bulk.stored;
console.log('paths identical  ', results.paths_identical);
if (!results.paths_identical) {
  console.log('  bulk:', bulk.stored.slice(0, 220));
  console.log('  row :', row.stored.slice(0, 220));
}

// Timestamp normalizer: canonical '+00' wire (post-migration) with and
// without the string fast path.
{
  const vals = Array.from({ length: 300_000 }, (_, i) =>
    i % 2 ? `2026-07-${String((i % 28) + 1).padStart(2, '0')} 12:41:13.736+00` : `Artist ${i}`
  );
  const t0 = performance.now();
  let sink = 0;
  for (const v of vals) sink += normalizeValue(v).length;
  const ms = performance.now() - t0;
  results.normalizer = {
    values: vals.length,
    ms: +ms.toFixed(1),
    mvals_per_s: +(vals.length / (ms * 1000)).toFixed(2),
    sink,
  };
  console.log('normalizer       ', JSON.stringify(results.normalizer));
}

console.log('RESULTS ' + JSON.stringify(results));
