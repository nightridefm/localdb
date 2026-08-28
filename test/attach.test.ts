// Cross-database ATTACH: the parts that decide whether a live query
// REFRESHES when an attached db commits, plus the config validation that
// guards the feature.
//
// All of it runs against a real sqlite-wasm connection with a real
// `ATTACH ':memory:'` - no OPFS, no browser. The VFS pre-open is the only
// piece that needs a browser; everything below is the logic that made the
// feature more than a syntax question.

import assert from 'node:assert/strict';

import { referencesAlias } from '../src/protocol.ts';
import { mergeAttachModels, validateAttach, validateModels } from '../src/sql.ts';
import { createDepsExtractor, quoteQualified } from '../src/worker/engine.ts';
import { initSqlite } from './init-sqlite.ts';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const s3: any = await initSqlite();
const db = new s3.oo1.DB(':memory:', 'c');
db.exec(`CREATE TABLE link (id TEXT PRIMARY KEY, upload_id TEXT, position INTEGER);`);
db.exec(`ATTACH ':memory:' AS tenant`);
db.exec(`CREATE TABLE tenant.upload (id TEXT PRIMARY KEY, title TEXT);`);

const deps = createDepsExtractor(() => db, s3.capi, s3.wasm);
const sorted = (s: Set<string>) => [...s].sort();

// ── dependency extraction across schemas ────────────────────────────────
{
  // The whole feature turns on this: without the schema qualifier a commit
  // on the attached db cannot be matched to the queries that read it.
  const d = deps.tablesRead(
    `SELECT l.id, u.title FROM link l LEFT JOIN tenant.upload u ON u.id = l.upload_id`
  );
  assert.deepEqual(sorted(d), ['link', 'tenant.upload']);
}
{
  // No regression for the ordinary case: main-db tables stay BARE, so
  // existing dep keys, commit notices and byTable entries are unchanged.
  assert.deepEqual(sorted(deps.tablesRead(`SELECT * FROM link WHERE position > 0`)), ['link']);
}
{
  // Quoted schema qualifiers reach the authorizer identically.
  const d = deps.tablesRead(`SELECT "u"."title" FROM "tenant"."upload" u`);
  assert.deepEqual(sorted(d), ['tenant.upload']);
}
console.log('deps: cross-schema qualification ok');

// ── FTS base-table mapping survives qualification ───────────────────────
{
  db.exec(`CREATE VIRTUAL TABLE tenant.upload_fts USING fts5(title);`);
  const d = deps.tablesRead(`SELECT rowid FROM tenant.upload_fts WHERE upload_fts MATCH 'x'`);
  // A commit to tenant.upload must invalidate a query over its FTS mirror.
  // The greedy `(.+)` in the base-table regex spans the dot, so the
  // mapping needed no schema awareness of its own.
  assert.ok(d.has('tenant.upload'), `expected tenant.upload in ${JSON.stringify(sorted(d))}`);
  assert.ok(
    !d.has('upload'),
    `bare 'upload' would collide with a main-db table of that name: ${JSON.stringify(sorted(d))}`
  );
}
console.log('deps: FTS mirror maps to the qualified base table ok');

// ── a failed prepare must not poison the cache ──────────────────────────
{
  // An attached db whose writer has not applied its DDL yet cannot resolve
  // `alias.table`. That used to be cached forever, leaving the query in
  // the registry's unknownDeps set (re-run on EVERY commit) for the life
  // of the connection.
  const sql = `SELECT l.id, s.note FROM link l JOIN tenant.standard_line s ON s.id = l.upload_id`;
  assert.equal(deps.tablesRead(sql).size, 0, 'unresolvable query reports unknown deps');
  db.exec(`CREATE TABLE tenant.standard_line (id TEXT PRIMARY KEY, note TEXT);`);
  assert.deepEqual(
    sorted(deps.tablesRead(sql)),
    ['link', 'tenant.standard_line'],
    'deps must be re-derived once the attached table exists'
  );
}
console.log('deps: failure fallback is not cached ok');

// ── per-schema data_version is readable ─────────────────────────────────
{
  // Seqlock validation samples one counter per schema. The table-valued
  // pragma_data_version() form takes no schema argument, so the attached
  // ones are prepared as plain PRAGMA statements - this asserts that form
  // actually prepares and returns a number.
  const main = Number(db.selectValue('SELECT data_version FROM pragma_data_version()'));
  const tenant = Number(db.selectValue('PRAGMA tenant.data_version'));
  assert.ok(Number.isFinite(main) && Number.isFinite(tenant), 'both counters read');
}
console.log('data_version: per-schema statement form ok');

// ── identifier quoting for qualified names ──────────────────────────────
{
  assert.equal(quoteQualified('upload'), '"upload"');
  assert.equal(quoteQualified('tenant.upload'), '"tenant"."upload"');
  assert.equal(quoteQualified('we"ird'), '"we""ird"');
  // The advisor's row probe interpolates this; quoting the whole thing as
  // one identifier addressed a table literally named `tenant.upload`, so
  // the probe threw and reported cap+1 - every cross-db subscription
  // looked huge.
  const n = Number(
    db.selectValue(
      `SELECT count(*) FROM (SELECT 1 FROM ${quoteQualified('tenant.upload')} LIMIT 5)`
    )
  );
  assert.equal(n, 0);
}
console.log('quoteQualified ok');

// ── routing helper (load-bearing: no false negatives allowed) ───────────
{
  const A = ['tenant'];
  assert.ok(referencesAlias('SELECT * FROM link l JOIN tenant.upload u ON 1', A));
  assert.ok(referencesAlias('SELECT * FROM "tenant"."upload"', A), 'quoted qualifier');
  assert.ok(referencesAlias('SELECT * FROM TENANT.upload', A), 'case-insensitive');
  assert.ok(!referencesAlias('SELECT * FROM link', A));
  assert.ok(!referencesAlias('SELECT * FROM tenant_local', A), 'prefix must not match');
  assert.ok(!referencesAlias('SELECT * FROM link', []), 'no aliases configured');
}
console.log('referencesAlias ok');

// ── config validation ───────────────────────────────────────────────────
{
  validateAttach([{ alias: 'tenant', dbName: 'tenant.db' }], 'project.db');
  const bad: Array<[string, unknown]> = [
    ['non-identifier alias', [{ alias: 'te-nant', dbName: 'x.db' }]],
    ['dotted alias', [{ alias: 'a.b', dbName: 'x.db' }]],
    ['quote injection', [{ alias: 'a"; DROP TABLE t; --', dbName: 'x.db' }]],
    ['reserved main', [{ alias: 'main', dbName: 'x.db' }]],
    ['reserved temp', [{ alias: 'TEMP', dbName: 'x.db' }]],
    ['empty dbName', [{ alias: 'ok', dbName: '' }]],
    [
      'duplicate alias',
      [
        { alias: 'a', dbName: 'x.db' },
        { alias: 'A', dbName: 'y.db' },
      ],
    ],
    [
      'duplicate file',
      [
        { alias: 'a', dbName: 'x.db' },
        { alias: 'b', dbName: 'x.db' },
      ],
    ],
  ];
  for (const [why, attach] of bad) {
    assert.throws(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      () => validateAttach(attach as any, 'project.db'),
      /localdb attach/,
      `should reject: ${why}`
    );
  }
  // Attaching your own file is a config mistake, not a no-op.
  assert.throws(
    () => validateAttach([{ alias: 'self', dbName: 'project.db' }], 'project.db'),
    /own file/
  );
}
console.log('validateAttach ok');

// ── merged models must satisfy the name-global typing rule ──────────────
{
  const meta = (over: Partial<Record<string, unknown>> = {}) => ({
    table: 't',
    primaryKey: ['id'],
    columns: ['id', 'meta'],
    dateColumns: [],
    boolColumns: [],
    jsonColumns: [],
    listColumns: [],
    ddl: '',
    ...over,
  });
  const own = { Link: meta({ table: 'link', jsonColumns: ['meta'] }) };
  const attached = { Upload: meta({ table: 'upload', jsonColumns: ['meta'] }) };

  // Compatible: same column, same typing on both sides.
  const okMerged = mergeAttachModels(own, [{ alias: 'tenant', dbName: 't.db', models: attached }])!;
  assert.deepEqual(Object.keys(okMerged).sort(), ['Link', 'tenant:Upload']);
  validateModels(okMerged);

  // Model NAMES may legitimately collide across databases - the alias
  // prefix is what keeps the merged map from losing one of them.
  const sameName = mergeAttachModels({ Row: meta({ table: 'link' }) }, [
    { alias: 'tenant', dbName: 't.db', models: { Row: meta({ table: 'upload' }) } },
  ])!;
  assert.equal(Object.keys(sameName).length, 2);

  // Incompatible: 'meta' is JSON in one db and plain text in the other.
  // embedHints is name-global, so this cannot be delivered correctly and
  // must fail at boot rather than mis-typing rows later.
  const clash = mergeAttachModels(own, [
    { alias: 'tenant', dbName: 't.db', models: { Upload: meta({ table: 'upload' }) } },
  ])!;
  assert.throws(() => validateModels(clash), /collides with a JSON\/list column/);

  // No attached models -> the owning map is passed straight through.
  assert.equal(mergeAttachModels(own, [{ alias: 'tenant', dbName: 't.db' }]), own);
}
console.log('mergeAttachModels + validateModels ok');

console.log('attach.test.ts: all assertions passed');
