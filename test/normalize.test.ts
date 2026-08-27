// Ingest-coercion unit tests. Runs under TZ=America/New_York (see run.mjs)
// so any local-time interpretation of naive timestamps shifts the result
// and fails the exact-string assertions.
import assert from 'node:assert/strict';

import { models } from '../examples/models.ts';
import { validateModels } from '../src/sql.ts';
import { makeColumnNormalizer, normalizeValue, pgArrayToJson } from '../src/sync/apply.ts';

// ── timestamps ──────────────────────────────────────────────────────────
// Naive Postgres text (Electric's format for `timestamp` columns) is UTC
// by Prisma convention. MUST NOT shift with the process timezone.
assert.equal(normalizeValue('2026-07-03 12:41:13.736'), '2026-07-03T12:41:13.736Z');
assert.equal(normalizeValue('2026-07-03T12:41:13.736'), '2026-07-03T12:41:13.736Z');
assert.equal(normalizeValue('2026-07-03 12:41:13'), '2026-07-03T12:41:13.000Z');
// Explicit offsets convert to UTC.
assert.equal(normalizeValue('2026-07-03 14:41:13.736+02'), '2026-07-03T12:41:13.736Z');
assert.equal(normalizeValue('2026-07-03 12:41:13.736+00'), '2026-07-03T12:41:13.736Z');
assert.equal(normalizeValue('2026-07-03T12:41:13.736Z'), '2026-07-03T12:41:13.736Z');
// Date-only values are NOT timestamps - pass through untouched.
assert.equal(normalizeValue('2026-08-01'), '2026-08-01');
// Timestamp-adjacent text must not be mangled.
assert.equal(normalizeValue('2026-07-03 12:41:13.736 remix'), '2026-07-03 12:41:13.736 remix');

// ── scalars ─────────────────────────────────────────────────────────────
assert.equal(normalizeValue(null), null);
assert.equal(normalizeValue(undefined), null);
assert.equal(normalizeValue(180.5), 180.5);
assert.equal(normalizeValue(true), 1);
assert.equal(normalizeValue(false), 0);
// Electric ships numbers as text; storage relies on SQLite column
// affinity, so the normalizer passes them through unchanged.
assert.equal(normalizeValue('180'), '180');
assert.equal(normalizeValue('-14'), '-14');

// ── pg array literals ───────────────────────────────────────────────────
assert.equal(pgArrayToJson('{1,2,3}'), '[1,2,3]');
assert.equal(pgArrayToJson('{0.5,-2.25}'), '[0.5,-2.25]');
assert.equal(pgArrayToJson('{}'), '[]');
assert.equal(pgArrayToJson('{NULL,t,f}'), '[null,true,false]');
assert.equal(pgArrayToJson('{"a","b c"}'), '["a","b c"]');
assert.equal(pgArrayToJson('{"quo\\"te","comma,inside"}'), '["quo\\"te","comma,inside"]');
assert.equal(pgArrayToJson('not an array'), null);
assert.equal(pgArrayToJson('{unterminated"'), null);

// ── column-aware coercion (generated model metadata) ────────────────────
const plNorm = makeColumnNormalizer(models.playlist);
// Every boolean encoding Electric uses: 't'/'f' (live), 'true'/'false'
// (snapshot), plus real booleans and numeric forms.
for (const v of ['t', 'true', '1', true, 1])
  assert.equal(plNorm('show_on_home', v), 1, `truthy ${v}`);
for (const v of ['f', 'false', '0', false, 0])
  assert.equal(plNorm('show_on_home', v), 0, `falsy ${v}`);
assert.equal(plNorm('show_on_home', null), null);
// Non-boolean columns on the same table keep normal handling.
assert.equal(plNorm('name', 'true'), 'true');
assert.equal(plNorm('created_at', '2026-07-03 12:41:13.736'), '2026-07-03T12:41:13.736Z');
// The timestamp heuristic is scoped to DECLARED dateColumns when metadata
// exists: a TEXT column whose value merely looks like a timestamp must
// arrive verbatim, not rewritten to ISO.
assert.equal(plNorm('name', '2026-07-03 14:41:13.736+02'), '2026-07-03 14:41:13.736+02');
assert.equal(plNorm('name', '2026-07-03 12:41:13'), '2026-07-03 12:41:13');

const artistNorm = makeColumnNormalizer(models.artist);
// JSON column: parsed values stringify, JSON text passes through.
assert.equal(artistNorm('links', [{ label: 'x', url: 'y' }]), '[{"label":"x","url":"y"}]');
assert.equal(artistNorm('links', '[{"label":"x"}]'), '[{"label":"x"}]');
assert.equal(artistNorm('is_verified', 'f'), 0);
// A boolean-looking value in a TEXT column stays text.
assert.equal(artistNorm('name', 'f'), 'f');

const trackNorm = makeColumnNormalizer(models.track);
// Scalar list: PG array literal converts to JSON array text.
assert.equal(trackNorm('beats', '{0.1,0.25,0.5}'), '[0.1,0.25,0.5]');
assert.equal(trackNorm('beats', [0.1, 0.25]), '[0.1,0.25]');

// Missing metadata degrades to generic normalization, never throws.
const bareNorm = makeColumnNormalizer();
assert.equal(bareNorm('anything', 't'), 't');
// Without metadata the timestamp heuristic applies to every text cell.
assert.equal(bareNorm('name', '2026-07-03 12:41:13'), '2026-07-03T12:41:13.000Z');

// ── name-collision guard (validateModels) ───────────────────────────────
// The engine's delivery layer maps result-column NAMES to bool/json
// conversions (embedExpr). That is only sound while no column name is
// bool/json-typed in one model and differently typed in another -
// validateModels enforces it at boot.
validateModels(models); // the example schema is collision-free
assert.throws(
  () =>
    validateModels({
      a: {
        table: 'a',
        primaryKey: ['id'],
        columns: ['id', 'flag'],
        dateColumns: [],
        boolColumns: ['flag'],
        jsonColumns: [],
        listColumns: [],
        ddl: '',
      },
      b: {
        table: 'b',
        primaryKey: ['id'],
        columns: ['id', 'flag'], // 'flag' is TEXT here - collision
        dateColumns: [],
        boolColumns: [],
        jsonColumns: [],
        listColumns: [],
        ddl: '',
      },
    }),
  /collides with a boolean column/
);

console.log('normalize.test.ts: all assertions passed');
