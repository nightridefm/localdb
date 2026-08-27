// Move-in / move-out tag lifecycle, including COMPOSITE tags.
//
// Electric tags each row of a subquery shape with one component per
// condition in the WHERE clause, joined by '/'. Measured against a live
// Electric 1.7.9 instance:
//
//   nested subquery    -> tags ["b2863e12..."]              conditions [true]
//   compound (A AND B) -> tags ["980c0a5a.../980c0a5a..."]  conditions [true,true]
//
// A move-out message is headers-only and carries patterns [{value, pos}],
// where `pos` selects WHICH component of the tag the value refers to. The
// client must therefore match component-wise: matching the whole tag
// string against a single component would never hit, the tag would never
// be dropped, and the row would stay in the local cache forever - silent
// staleness, the one failure this layer must never produce.
//
// Composite tags only appear for COMPOUND subquery predicates (two or
// more subqueries joined by AND/OR); a NESTED subquery - one top-level
// condition, however deep - always produces a flat single-component tag.
import assert from 'node:assert/strict';

import { ddlFor, models } from '../examples/models.ts';
import { makeApplier } from '../src/sync/apply.ts';
import { initSqlite } from './init-sqlite.ts';

const sqlite3 = await initSqlite();
const db = new sqlite3.oo1.DB(':memory:');
db.exec(ddlFor('playlist'));
const engine = { db } as unknown as Parameters<typeof makeApplier>[0];
const applier = makeApplier(engine, 'playlist', ['id'], models.playlist);

const slugs = () =>
  (db.selectValues(`SELECT slug FROM playlist ORDER BY slug`) as string[]).join(',');

const insert = (id: string, slug: string, tags: string[]) =>
  applier.apply([
    {
      key: id,
      value: { id, slug, name: slug },
      headers: { operation: 'insert', is_move_in: true, tags },
    },
  ]);

const moveOut = (patterns: Array<{ value: string; pos: number }>) =>
  applier.apply([{ headers: { event: 'move-out', patterns } }]);

// ── flat tags (single or nested subquery) ───────────────────────────────
insert('r1', 'alpha', ['tagA']);
insert('r2', 'beta', ['tagB']);
assert.equal(slugs(), 'alpha,beta');

moveOut([{ value: 'tagA', pos: 0 }]);
assert.equal(slugs(), 'beta', 'flat tag move-out removes exactly its row');

// A row held by SEVERAL tags survives until the LAST one goes. This is
// what makes the same code correct for AND and OR shapes without the
// client knowing which it is.
insert('r3', 'gamma', ['t1']);
insert('r3', 'gamma', ['t2']); // same row, second tag
moveOut([{ value: 't1', pos: 0 }]);
assert.equal(slugs(), 'beta,gamma', 'row stays while another tag holds it');
moveOut([{ value: 't2', pos: 0 }]);
assert.equal(slugs(), 'beta', 'row leaves when its last tag is dropped');

// ── composite tags (compound subquery: A AND B) ─────────────────────────
// Component 0 belongs to the first condition, component 1 to the second.
insert('c1', 'comp-one', ['aaa/bbb']);
insert('c2', 'comp-two', ['aaa/ccc']);
insert('c3', 'comp-three', ['zzz/bbb']);
assert.equal(slugs(), 'beta,comp-one,comp-three,comp-two');

// Dropping component 1 = 'bbb' must remove ONLY the rows whose SECOND
// component is 'bbb' - not 'aaa/ccc', and not by matching 'bbb' anywhere.
moveOut([{ value: 'bbb', pos: 1 }]);
assert.equal(slugs(), 'beta,comp-two', 'pos-1 match removes exactly the right rows');

// Dropping component 0 = 'aaa' removes the survivor.
moveOut([{ value: 'aaa', pos: 0 }]);
assert.equal(slugs(), 'beta', 'pos-0 match works on composite tags');

// A component value must NOT match at the wrong position.
insert('d1', 'delta', ['xxx/yyy']);
moveOut([{ value: 'xxx', pos: 1 }]); // 'xxx' is at pos 0, not 1
assert.equal(slugs(), 'beta,delta', 'no cross-position false positive');
moveOut([{ value: 'yyy', pos: 0 }]); // 'yyy' is at pos 1, not 0
assert.equal(slugs(), 'beta,delta', 'still no cross-position false positive');
moveOut([{ value: 'yyy', pos: 1 }]);
assert.equal(slugs(), 'beta', 'correct position finally removes it');

// A whole composite string must not be matched as if it were flat.
insert('e1', 'epsilon', ['mmm/nnn']);
moveOut([{ value: 'mmm/nnn', pos: 0 }]);
assert.equal(slugs(), 'beta,epsilon', 'whole composite is not a pos-0 component');
moveOut([{ value: 'mmm', pos: 0 }]);
assert.equal(slugs(), 'beta', 'its actual pos-0 component does remove it');

// ── three conditions (A AND B AND C) ────────────────────────────────────
insert('f1', 'zeta', ['p/q/r']);
moveOut([{ value: 'q', pos: 1 }]);
assert.equal(slugs(), 'beta', 'middle component of a 3-part tag matches');

console.log('move-out-tags.test.ts: all assertions passed');
