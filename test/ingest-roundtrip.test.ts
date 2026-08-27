// End-to-end type-fidelity test: wire-realistic Electric messages (every
// value a string, exactly as the shape endpoints deliver them) -> applier
// -> SQLite storage -> the engine's json_object delivery wrapper ->
// JSON.parse, asserting the types a consumer actually receives. Runs
// under TZ=America/New_York (run.mjs) to catch local-time leakage.
//
// The failure classes this guards: booleans stored as text (SQL
// predicates silently match nothing), naive timestamps shifted by the
// client timezone, and JSON columns delivered as escaped strings instead
// of arrays/objects.
import assert from 'node:assert/strict';

import { ddlFor, models, rowJson } from '../examples/models.ts';
import { makeApplier } from '../src/sync/apply.ts';
import { embedExpr, embedHints } from '../src/worker/engine.ts';
import { initSqlite } from './init-sqlite.ts';

const sqlite3 = await initSqlite();
const db = new sqlite3.oo1.DB(':memory:');
db.exec(ddlFor('playlist', 'artist', 'track'));

const hints = embedHints(models);

// makeApplier only touches engine.db - a raw handle stands in for Engine.
const engine = { db } as unknown as Parameters<typeof makeApplier>[0];

// The engine's read wrapper, reproduced through the REAL embedExpr.
function queryRows(sql: string): Record<string, unknown>[] {
  const probe = db.prepare(sql);
  const names: string[] = [];
  for (let i = 0; i < probe.columnCount; i++) {
    names.push((probe as unknown as { getColumnName: (i: number) => string }).getColumnName(i));
  }
  probe.finalize();
  const wrapped = `SELECT COALESCE(json_group_array(json(__row)), '[]') FROM (
		SELECT json_object(${names.map((n) => embedExpr(n, hints)).join(', ')}) AS __row FROM (${sql})
	)`;
  return JSON.parse(String(db.selectValue(wrapped))) as Record<string, unknown>[];
}

function insert(model: keyof typeof models, value: Record<string, string | null>) {
  const meta = models[model];
  const applier = makeApplier(engine, meta.table, [...meta.primaryKey], meta);
  const n = applier.apply([
    { headers: { operation: 'insert' }, value } as Parameters<typeof applier.apply>[0][number],
  ]);
  assert.equal(n, 1, `${meta.table} insert applied`);
}

// ── booleans: snapshot ('true'/'false') AND live ('t'/'f') encodings ────
insert('playlist', {
  id: 'p1',
  name: 'Main',
  show_on_home: 't',
  is_active: 'true',
  weight: '3',
});
insert('playlist', {
  id: 'p2',
  name: 'Hidden',
  show_on_home: 'false',
  is_active: 'f',
  weight: '1',
});

assert.equal(db.selectValue(`SELECT typeof(show_on_home) FROM playlist WHERE id='p1'`), 'integer');
// THE regression class: a bare boolean predicate must match the flagged row.
assert.equal(db.selectValue(`SELECT count(*) FROM playlist WHERE show_on_home`), 1);
assert.equal(db.selectValue(`SELECT id FROM playlist WHERE show_on_home`), 'p1');

// Both boolean encodings landed as integers.
assert.equal(db.selectValue(`SELECT is_active FROM playlist WHERE id='p1'`), 1);
assert.equal(db.selectValue(`SELECT is_active FROM playlist WHERE id='p2'`), 0);

// ── JSON columns + boolean delivery ─────────────────────────────────────
insert('artist', {
  id: 'a1',
  name: 'Overboard',
  slug: 'overboard',
  is_verified: 'f',
  has_avatar: 'true',
  links: '[{"label":"web","url":"https://x.example"}]',
  created_at: '2026-07-11 08:16:53.9',
  updated_at: '2026-07-11 08:16:53.9',
});

// Direct select (`SELECT a.*` shape): booleans are REAL JSON booleans
// (React `{x && <jsx>}` gates literally render a 0, and strict ===
// comparisons expect true/false), JSON columns are real parsed values,
// timestamps ISO strings.
const [artist] = queryRows(`SELECT * FROM artist WHERE id='a1'`);
assert.equal(artist!.is_verified, false);
assert.equal(artist!.has_avatar, true);
assert.ok(Array.isArray(artist!.links), 'links delivered as a real array');
assert.equal((artist!.links as { label: string }[])[0]!.label, 'web');
assert.equal(artist!.created_at, '2026-07-11T08:16:53.900Z');

// rowJson nesting (`... AS artist__json`): same fidelity inside the
// embedded row.
const [wrapped] = queryRows(
  `SELECT ${rowJson('artist', 'a')} AS artist__json FROM artist a WHERE a.id='a1'`
);
const nested = wrapped!.artist as Record<string, unknown>;
assert.equal(nested.is_verified, false);
assert.equal(nested.has_avatar, true);
assert.ok(Array.isArray(nested.links), 'nested links is a real array');
// NULL booleans stay null, not false.
insert('artist', { id: 'a2', name: 'NoFlag', slug: 'noflag' });
const [nullArtist] = queryRows(`SELECT * FROM artist WHERE id='a2'`);
assert.equal(nullArtist!.is_verified, null);

// ── numeric strings + partial update with the other boolean encoding ────
insert('track', {
  id: 't1',
  title: 'Seed',
  duration: '180.5',
  lufs: '-14',
  track_number: '7',
  created_at: '2026-07-03 12:41:13.736',
});
// Column affinity turns Electric's numeric text into real numbers.
assert.equal(db.selectValue(`SELECT typeof(duration) FROM track WHERE id='t1'`), 'real');
assert.equal(db.selectValue(`SELECT typeof(track_number) FROM track WHERE id='t1'`), 'integer');
const [track] = queryRows(
  `SELECT duration, lufs, track_number, created_at FROM track WHERE id='t1'`
);
assert.equal(track!.duration, 180.5);
assert.equal(track!.lufs, -14);
assert.equal(track!.track_number, 7);
// Naive timestamp stored as exact UTC ISO - independent of process TZ.
assert.equal(track!.created_at, '2026-07-03T12:41:13.736Z');

// Partial update flips p1 off using the snapshot encoding.
const plApplier = makeApplier(engine, 'playlist', ['id'], models.playlist);
plApplier.apply([
  {
    headers: { operation: 'update' },
    value: { id: 'p1', show_on_home: 'false' },
  } as Parameters<typeof plApplier.apply>[0][number],
]);
assert.equal(db.selectValue(`SELECT count(*) FROM playlist WHERE show_on_home`), 0);

db.close();
console.log('ingest-roundtrip.test.ts: all assertions passed');
