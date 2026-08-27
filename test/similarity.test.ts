// The similarity() UDF family: pg_trgm-parity semantics for fuzzy search.
// Asserts both the pure functions and the SQL-registered UDFs (through a
// real sqlite-wasm connection), including the cases fuzzy search exists
// for: cross-field "Artist Title" queries, typos, and word reordering.
import assert from 'node:assert/strict';

import {
  registerSimilarityUdf,
  searchSimilarity,
  trigramSimilarity,
  wordSimilarity,
} from '../src/similarity.ts';
import { initSqlite } from './init-sqlite.ts';

// ── pure function ──────────────────────────────────────────────────────
// Values cross-checked against Postgres pg_trgm (SELECT similarity(...)).
assert.equal(trigramSimilarity('abc', 'abc'), 1, 'identical');
assert.equal(trigramSimilarity('', 'abc'), 0, 'empty side');
assert.equal(trigramSimilarity('!!!', 'abc'), 0, 'no alphanumeric words');
{
  // pg_trgm: similarity('word', 'two words') = 0.36363637
  const v = trigramSimilarity('word', 'two words');
  assert.ok(Math.abs(v - 0.36363637) < 1e-6, `pg_trgm parity: ${v}`);
}
// Case-insensitive, word-splitting on non-alphanumerics.
assert.equal(
  trigramSimilarity('Kavinsky - Nightcall', 'kavinsky nightcall'),
  1,
  'separator and case are invisible to trigram words'
);

// The three regressions the dropped arm caused, all must clear 0.3:
assert.ok(
  trigramSimilarity('Kavinsky - Nightcall', 'Kavinsky Nightcall') > 0.3,
  'cross-field query without " - "'
);
assert.ok(trigramSimilarity('Carpenter Brut - Turbo Killer', 'carpentr brut') > 0.3, 'typo');
assert.ok(trigramSimilarity('Kavinsky - Nightcall', 'nightcall kavinsky') > 0.3, 'word reordering');
// And a non-match stays below threshold.
assert.ok(trigramSimilarity('Kavinsky - Nightcall', 'aphex twin') < 0.3, 'unrelated stays out');

// ── wordSimilarity: the anti-dilution function search arms use ─────────
// The regression that motivated it: 'vsto' vs the full "artist - title"
// concat scores 0.15 whole-string (diluted below the 0.3 threshold) but
// 0.375 against the word 'vosto'.
assert.ok(
  trigramSimilarity('Vosto - Neon Dreams', 'vsto') < 0.3,
  'whole-string similarity IS diluted (documents why wordSimilarity exists)'
);
assert.ok(wordSimilarity('vsto', 'Vosto - Neon Dreams') > 0.3, 'typo vs one word of long text');
{
  // exact single-word extent -> 1.0 regardless of surrounding text
  const v = wordSimilarity('vosto', 'Vosto - Neon Dreams');
  assert.equal(v, 1, `exact word extent: ${v}`);
}
// Multi-word queries: trigram sets are unordered, so word reordering
// still matches via the 2-word window.
assert.equal(wordSimilarity('nightcall kavinsky', 'Kavinsky - Nightcall'), 1, 'reordered words');
assert.ok(wordSimilarity('carpentr brut', 'Carpenter Brut - Turbo Killer') > 0.3, 'typo pair');
assert.ok(wordSimilarity('aphex twin', 'Kavinsky - Nightcall') < 0.3, 'unrelated stays out');
assert.equal(wordSimilarity('', 'anything'), 0, 'empty query');
assert.equal(wordSimilarity('abc', '!!!'), 0, 'no words in text');

// ── searchSimilarity: AND over query words (the filter-arm scorer) ─────
// Single-word queries behave exactly like wordSimilarity.
assert.equal(
  searchSimilarity('vsto', 'Vosto - Neon Dreams'),
  wordSimilarity('vsto', 'Vosto - Neon Dreams'),
  'single-word query identical to wordSimilarity'
);
// Spacing variant: one query word vs two text words (2-word window).
assert.ok(searchSimilarity('nightcall', 'Night Call - Demo') > 0.3, 'word-spacing variant');
// The monotone-narrowing property word_similarity lacks: one matched
// word of a two-word query must NOT pass on its own.
assert.ok(
  wordSimilarity('vosto neon', 'Neon Nox - Last Stand') > 0.3,
  'best-extent scoring passes a single-word match (the old expansion bug)'
);
assert.ok(
  searchSimilarity('vosto neon', 'Neon Nox - Last Stand') < 0.3,
  'AND semantics reject it - both words must match'
);
assert.ok(searchSimilarity('vosto neon', 'Vosto - Neon Dreams') > 0.3, 'both words match');
// Monotone: adding query words never grows the match set.
{
  const catalogue = [
    'Vosto - Neon Dreams',
    'Kavinsky - Nightcall',
    'Neon Nox - Last Stand',
    'Signal Void - Neon City',
  ];
  const matches = (q: string) => catalogue.filter((c) => searchSimilarity(q, c) > 0.3);
  const m1 = matches('vosto');
  const m2 = matches('vosto neon');
  const m3 = matches('vosto neon dreams');
  assert.deepEqual(m1, ['Vosto - Neon Dreams']);
  assert.deepEqual(m2, ['Vosto - Neon Dreams'], 'adding a word did not expand');
  assert.deepEqual(m3, ['Vosto - Neon Dreams'], 'nor did a third');
  assert.deepEqual(matches('vosto remix'), [], 'noise word narrows to zero (AND semantics)');
}
assert.equal(searchSimilarity('', 'anything'), 0, 'empty query');
assert.equal(searchSimilarity('abc', '!!!'), 0, 'no words in text');

// ── SQL UDF through a real connection ──────────────────────────────────
const sqlite3 = await initSqlite();
const db = new sqlite3.oo1.DB(':memory:');
registerSimilarityUdf(db as unknown as Parameters<typeof registerSimilarityUdf>[0]);

db.exec(`CREATE TABLE track (id INTEGER PRIMARY KEY, artist TEXT, title TEXT)`);
const rows: Array<[string | null, string]> = [
  ['Kavinsky', 'Nightcall'],
  ['Carpenter Brut', 'Turbo Killer'],
  ['Aphex Twin', 'Xtal'],
  [null, 'Untitled Demo'],
  ['Vosto', 'Neon Dreams'],
  ['Neon Nox', 'Last Stand'],
];
const ins = db.prepare(`INSERT INTO track (artist, title) VALUES (?,?)`);
for (const [a, t] of rows) ins.bind([a, t]).stepReset();
ins.finalize();

// The app's FILTER-arm shape: search_similarity(query, row expr).
const search = (q: string): string[] =>
  db.selectValues(
    `SELECT title FROM track
     WHERE search_similarity(?, coalesce(artist,'') || ' - ' || coalesce(title,'')) > 0.3
     ORDER BY id`,
    [q]
  ) as string[];

assert.deepEqual(search('kavinsky nightcall'), ['Nightcall'], 'fuzzy cross-field via SQL');
assert.deepEqual(search('carpentr brut'), ['Turbo Killer'], 'typo via SQL');
assert.deepEqual(search('vsto'), ['Neon Dreams'], 'short typo vs long concat (the vsto case)');
assert.deepEqual(
  search('vosto neon'),
  ['Neon Dreams'],
  'multi-word narrows: the Neon Nox row must NOT ride in on one matched word'
);
assert.deepEqual(search('zzz qqq'), [], 'no false positives');
// Memo-staleness guard: both UDFs cache the query argument's trigram set
// across a scan - alternating queries must each get their own set, and
// repeating one must still be correct.
assert.deepEqual(search('aphex twin'), ['Xtal'], 'second distinct query after memo');
assert.deepEqual(search('kavinsky nightcall'), ['Nightcall'], 'repeat of first query');
{
  const plain = db.selectValues(
    `SELECT title FROM track WHERE similarity(title, ?) > 0.3 ORDER BY id`,
    ['nightcal']
  ) as string[];
  assert.deepEqual(plain, ['Nightcall'], 'plain similarity() still registered and correct');
}
// NULL propagation: pg_trgm returns NULL for a NULL argument; `> 0.3` on
// NULL is not true, so NULL-argument rows filter out without erroring.
assert.equal(
  db.selectValue(`SELECT similarity(NULL, 'x')`),
  null,
  'similarity NULL argument yields NULL, as in pg_trgm'
);
assert.equal(
  db.selectValue(`SELECT word_similarity('x', NULL)`),
  null,
  'word_similarity NULL argument yields NULL'
);
assert.equal(
  db.selectValue(`SELECT search_similarity(NULL, 'x')`),
  null,
  'search_similarity NULL argument yields NULL'
);

db.close();
console.log('similarity.test.ts: all assertions passed');
