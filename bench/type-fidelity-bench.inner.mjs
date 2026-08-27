// Inner bench body - bundled and executed by type-fidelity-bench.mjs.
// Runs under TZ=America/New_York like the tests (realistic non-UTC client).
import sqlite3InitModule from '@sqlite.org/sqlite-wasm';

import { ddlFor, models, rowJson } from '../examples/models.ts';
import { makeApplier, makeColumnNormalizer, normalizeValue } from '../src/sync/apply.ts';
import { embedExpr, embedHints } from '../src/worker/engine.ts';

const hints = embedHints(models);

const results = {};
const p50 = (a) => a.slice().sort((x, y) => x - y)[Math.floor(a.length / 2)];

// ── wire-realistic artist rows (timestamptz text with a +00 offset) ─────
const N_ARTIST = 200_000;
function artistWire(i) {
  return {
    id: i.toString(16).padStart(32, '0'),
    slug: 'artist-' + i,
    name: 'Artist Name ' + i,
    bio: i % 3 ? 'A moderately sized biography string for artist ' + i : null,
    has_avatar: i % 2 ? 't' : 'f',
    links: i % 5 ? '[{"label":"web","url":"https://example.com/' + i + '"}]' : null,
    created_at: '2026-07-03 12:41:13.736+00',
    updated_at: '2026-07-03 12:41:13.736+00',
    is_verified: i % 7 ? 'false' : 'true',
  };
}

// 1 ── normalizer throughput ─────────────────────────────────────────────
{
  const cols = Object.keys(artistWire(0));
  const rows = Array.from({ length: 50_000 }, (_, i) => artistWire(i));
  const bench = (fn) => {
    const t0 = performance.now();
    let sink = 0;
    for (const r of rows) {
      for (const c of cols) {
        const v = fn(c, r[c]);
        if (v !== null) sink ^= typeof v === 'number' ? v : v.length;
      }
    }
    const ms = performance.now() - t0;
    return { ms, sink, mvals_per_s: +((rows.length * cols.length) / 1000 / ms).toFixed(2) };
  };
  const plain = bench((_c, v) => normalizeValue(v));
  const norm = makeColumnNormalizer(models.artist);
  const aware = bench(norm);
  results.normalizer = {
    values: rows.length * cols.length,
    plain_mvals_per_s: plain.mvals_per_s,
    column_aware_mvals_per_s: aware.mvals_per_s,
    aware_vs_plain_pct: +(
      ((aware.mvals_per_s - plain.mvals_per_s) / plain.mvals_per_s) *
      100
    ).toFixed(1),
  };
  console.log('1. normalizer      ', JSON.stringify(results.normalizer));
}

// ── db setup ────────────────────────────────────────────────────────────
const sqlite3 = await sqlite3InitModule({ print: () => {}, printErr: () => {} });
const db = new sqlite3.oo1.DB(':memory:');
db.exec(ddlFor('artist', 'track', 'playlist', 'playlist_track'));
// Clone of artist with NO model metadata -> applier falls back to plain
// normalizeValue (the pre-coercion ingest path) through identical machinery.
db.exec(models.artist.ddl.replaceAll('"artist"', '"artist_plain"'));
const engine = { db };

// 2 ── full applier ingest A/B ───────────────────────────────────────────
function ingest(table) {
  const applier = makeApplier(
    engine,
    table,
    ['id'],
    table === 'artist' ? models.artist : undefined
  );
  const msgs = Array.from({ length: N_ARTIST }, (_, i) => ({
    headers: { operation: 'insert' },
    value: artistWire(i),
  }));
  db.exec('BEGIN');
  const t0 = performance.now();
  applier.apply(msgs);
  const ms = performance.now() - t0;
  db.exec('COMMIT');
  return { ms: +ms.toFixed(0), rows_per_s: Math.round(N_ARTIST / (ms / 1000)) };
}
{
  const plain = ingest('artist_plain'); // no metadata: generic normalization only
  const coerced = ingest('artist'); // metadata: bool+json coercion active
  results.applier_ingest = {
    rows: N_ARTIST,
    plain_rows_per_s: plain.rows_per_s,
    coerced_rows_per_s: coerced.rows_per_s,
    coerced_vs_plain_pct: +(
      ((coerced.rows_per_s - plain.rows_per_s) / plain.rows_per_s) *
      100
    ).toFixed(1),
  };
  console.log('2. applier ingest  ', JSON.stringify(results.applier_ingest));
}

// 3 ── read delivery A/B ─────────────────────────────────────────────────
function wrapPlain(names, inner) {
  const cols = names.map((n) => `'${n}', "${n}"`).join(', ');
  return `SELECT COALESCE(json_group_array(json(__row)), '[]') FROM (SELECT json_object(${cols}) AS __row FROM (${inner}))`;
}
function wrapTyped(names, inner) {
  const cols = names.map((n) => embedExpr(n, hints)).join(', ');
  return `SELECT COALESCE(json_group_array(json(__row)), '[]') FROM (SELECT json_object(${cols}) AS __row FROM (${inner}))`;
}
function colNames(sql) {
  const probe = db.prepare(sql);
  const names = [];
  for (let i = 0; i < probe.columnCount; i++) names.push(probe.getColumnName(i));
  probe.finalize();
  return names;
}
function readBench(inner, iters) {
  const names = colNames(inner);
  const out = {};
  for (const [label, wrap] of [
    ['plain', wrapPlain(names, inner)],
    ['typed', wrapTyped(names, inner)],
  ]) {
    const times = [];
    let bytes = 0;
    for (let i = 0; i < iters; i++) {
      const t0 = performance.now();
      const text = String(db.selectValue(wrap));
      times.push(performance.now() - t0);
      bytes = text.length;
    }
    out[label + '_p50_ms'] = +p50(times).toFixed(2);
    out.bytes = bytes;
  }
  out.typed_vs_plain_pct = +(
    ((out.typed_p50_ms - out.plain_p50_ms) / out.plain_p50_ms) *
    100
  ).toFixed(1);
  return out;
}
{
  results.read_grid_500 = readBench('SELECT * FROM artist ORDER BY created_at, id LIMIT 500', 50);
  console.log('3a. read 500 rows  ', JSON.stringify(results.read_grid_500));
  results.read_bulk_50k = readBench('SELECT * FROM artist ORDER BY created_at, id LIMIT 50000', 7);
  console.log('3b. read 50k rows  ', JSON.stringify(results.read_bulk_50k));
}

// 4 ── rowJson aggregate A/B (members-style nested rows) ─────────────────
{
  const plainRow = models.artist.columns.map((c) => `'${c}', a."${c}"`).join(', ');
  const inner = (row) =>
    `SELECT COALESCE(json_group_array(json(x)), '[]') FROM (SELECT json_object(${row}) AS x FROM artist a ORDER BY a.created_at, a.id LIMIT 5000)`;
  const times = { plain: [], typed: [] };
  for (let i = 0; i < 30; i++) {
    let t0 = performance.now();
    db.selectValue(inner(plainRow));
    times.plain.push(performance.now() - t0);
    t0 = performance.now();
    db.selectValue(inner(rowJson('artist', 'a').replace(/^json_object\(|\)$/g, '')));
    times.typed.push(performance.now() - t0);
  }
  results.rowjson_5k = {
    plain_p50_ms: +p50(times.plain).toFixed(2),
    typed_p50_ms: +p50(times.typed).toFixed(2),
  };
  results.rowjson_5k.typed_vs_plain_pct = +(
    ((results.rowjson_5k.typed_p50_ms - results.rowjson_5k.plain_p50_ms) /
      results.rowjson_5k.plain_p50_ms) *
    100
  ).toFixed(1);
  console.log('4. rowJson 5k rows ', JSON.stringify(results.rowjson_5k));
}

console.log('\nRESULT_JSON ' + JSON.stringify(results));
db.close();
