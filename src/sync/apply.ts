// Applies shape batches to the local table inside fenced transactions.
//
// Invalidation falls out for free here: a shape maps to one table, so a
// committed batch's touched-table set is just the shapes that contributed.
//
// Message shapes (verified against real Electric 1.5.1 and 1.7.9, feature
// flags allow_subqueries + tagged_subqueries). Electric 1.6.0 changed
// move-in/out for compound subqueries and broke older clients, so this
// file is the one to re-check on every Electric upgrade. 1.7.9 adds an
// `active_conditions` header to subquery change messages, which is
// ignored harmlessly:
//   insert  - full row (all selected columns); move-in backfills arrive as
//             inserts with `is_move_in: true` + `tags`, possibly for rows
//             already present (idempotent upsert absorbs both; we never
//             LSN-filter, so the stock-client bug of dropping move-ins
//             with stale LSNs cannot occur here).
//   update  - PARTIAL: primary key + changed columns only. A naive
//             full-row upsert would NULL every absent column. Statements
//             are prepared per (operation, column-signature) - updates SET
//             exactly the columns present.
//   delete  - primary key (possibly plus old values; only the pk is used).
//   move-out - headers-ONLY: `{event:'move-out', patterns:[{value,pos}]}`.
//             No key, no value, no operation. Rows whose tags match the
//             pattern leave the shape; the client must delete exactly the
//             rows that lose their LAST tag (see tag bookkeeping below).
//
// Timestamps: every value that looks like a Postgres timestamp is
// normalized to ISO-8601 UTC at ingest - the ONE canonical text format all
// local SQL compares against (never bare `datetime('now')` in queries; use
// strftime('%Y-%m-%dT%H:%M:%fZ','now')).

import type { ModelMeta } from '../types.ts';
import type { Engine, Sqlite3Stmt } from '../worker/engine.ts';
import type { ShapeMessage } from './electric.ts';

/** The subset of ModelMeta the ingest path needs (see LocalDbConfig.models). */
export type ColumnMeta = Pick<ModelMeta, 'boolColumns' | 'jsonColumns' | 'listColumns'>;

export type TableApplier = {
  table: string;
  apply: (messages: ShapeMessage[]) => number;
  /** must-refetch: wipe the table (inside the caller's transaction). */
  truncate: () => void;
};

// Matches Postgres text timestamps: `2026-08-01 05:20:00.123+00` and
// friends. Converted once at ingest, never at query time.
const PG_TS = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}(:?\d{2})?|Z)?$/;
const TZ_SUFFIX = /(Z|[+-]\d{2}(:?\d{2})?)$/;

/**
 * Zero-allocation ISO conversion for the canonical wire format. After the
 * timestamptz migration Electric ships 'YYYY-MM-DD HH:MM:SS[.frac]' with
 * no offset, 'Z', or a ZERO offset ('+00' / '+00:00') - all of which mean
 * UTC, so the ISO form is pure string surgery instead of a Date
 * allocation + re-parse (2 per row on a typical model, 2M on a 1M-row
 * hydration).
 *
 * Returns null - deferring to the Date path - for anything it cannot
 * decide safely: real non-zero offsets, out-of-range components, and any
 * day past 28 (where Date's strict ISO parsing rejects impossible dates
 * like 02-31 and the old behaviour was to leave the raw text alone).
 */
function fastUtcIso(s: string): string | null {
  // Callers gate on PG_TS, so the shape (digit positions, separators) is
  // already validated - charCode arithmetic here replaces the previous
  // regex exec + per-field slice/Number conversions, which allocated ~8
  // intermediates per call in the 2-per-row hydration loop.
  let end = s.length;
  if (s.charCodeAt(end - 1) === 90 /* Z */) {
    end -= 1;
  } else {
    // A tz offset can only be '+HH' / '+HHMM' / '+HH:MM' (sign at one of
    // three distances from the end, always past the 19-char base).
    let sign = -1;
    if (end - 3 >= 19 && (s.charCodeAt(end - 3) === 43 || s.charCodeAt(end - 3) === 45)) {
      sign = end - 3;
    } else if (end - 5 >= 19 && (s.charCodeAt(end - 5) === 43 || s.charCodeAt(end - 5) === 45)) {
      sign = end - 5;
    } else if (end - 6 >= 19 && (s.charCodeAt(end - 6) === 43 || s.charCodeAt(end - 6) === 45)) {
      sign = end - 6;
    }
    if (sign !== -1) {
      // Only a ZERO positive offset means UTC here; anything else defers
      // to the Date path ('-00' included, matching the old allow-list).
      if (s.charCodeAt(sign) !== 43 /* + */) return null;
      for (let i = sign + 1; i < end; i++) {
        const c = s.charCodeAt(i);
        if (c !== 48 /* 0 */ && c !== 58 /* : */) return null;
      }
      end = sign;
    }
  }
  if (end < 19) return null;
  if (end !== 19 && s.charCodeAt(19) !== 46 /* . */) return null;
  const mo = (s.charCodeAt(5) - 48) * 10 + (s.charCodeAt(6) - 48);
  const d = (s.charCodeAt(8) - 48) * 10 + (s.charCodeAt(9) - 48);
  const h = (s.charCodeAt(11) - 48) * 10 + (s.charCodeAt(12) - 48);
  const mi = (s.charCodeAt(14) - 48) * 10 + (s.charCodeAt(15) - 48);
  const se = (s.charCodeAt(17) - 48) * 10 + (s.charCodeAt(18) - 48);
  if (mo < 1 || mo > 12 || d < 1 || d > 28 || h > 23 || mi > 59 || se > 59) return null;
  let frac = end > 19 ? s.slice(20, Math.min(23, end)) : '';
  while (frac.length < 3) frac += '0';
  return `${s.slice(0, 10)}T${s.slice(11, 19)}.${frac}Z`;
}

export function normalizeValue(v: unknown): string | number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number') return v;
  if (typeof v === 'boolean') return v ? 1 : 0;
  const s = String(v);
  // Fast-path guard: the regex only matters for timestamp-shaped strings,
  // and during a 1M-row hydration it would otherwise run on every text
  // cell. Digit first char + '-' at index 4 + plausible length gates it.
  const c0 = s.charCodeAt(0);
  if (
    c0 >= 48 &&
    c0 <= 57 &&
    s.length >= 19 &&
    s.length <= 35 &&
    s.charCodeAt(4) === 45 &&
    PG_TS.test(s)
  ) {
    // Prisma writes naive `timestamp` columns in UTC and Electric ships
    // them WITHOUT an offset ('2026-07-03 12:41:13.736'). new Date() would
    // interpret an offset-less string in the BROWSER'S LOCAL TIMEZONE and
    // toISOString() would then shift every timestamp by the user's UTC
    // offset - invisible in UTC dev containers, wrong everywhere else.
    // Treat offset-less as UTC explicitly. Postgres also emits BARE-HOUR
    // offsets ('+02', '+00') on timestamptz columns, which new Date()
    // rejects (it needs '+02:00') - pad them.
    const fast = fastUtcIso(s);
    if (fast !== null) return fast;
    let iso = s.includes('T') ? s : s.replace(' ', 'T');
    if (!TZ_SUFFIX.test(iso)) iso += 'Z';
    else if (/[+-]\d{2}$/.test(iso)) iso += ':00';
    const d = new Date(iso);
    if (!Number.isNaN(d.getTime())) return d.toISOString();
  }
  return s;
}

/**
 * Postgres array literal -> JSON array text. Electric ships scalar-list
 * columns as PG text form ('{1,2,3}', '{"a","b"}', '{NULL}'). Handles
 * numeric/string/bool elements and double-quote escaping; returns null on
 * anything it can't parse so the raw text never poisons json() consumers.
 */
export function pgArrayToJson(s: string): string | null {
  if (!s.startsWith('{') || !s.endsWith('}')) return null;
  const inner = s.slice(1, -1);
  if (inner === '') return '[]';
  const out: unknown[] = [];
  let i = 0;
  for (;;) {
    if (inner[i] === '"') {
      let val = '';
      i++;
      while (i < inner.length && inner[i] !== '"') {
        if (inner[i] === '\\') {
          val += inner[i + 1];
          i += 2;
        } else {
          val += inner[i];
          i++;
        }
      }
      if (inner[i] !== '"') return null;
      i++; // past closing quote
      out.push(val);
    } else {
      let end = inner.indexOf(',', i);
      if (end === -1) end = inner.length;
      const raw = inner.slice(i, end);
      if (raw === 'NULL') out.push(null);
      else if (raw === 't' || raw === 'true') out.push(true);
      else if (raw === 'f' || raw === 'false') out.push(false);
      else {
        const n = Number(raw);
        if (raw !== '' && !Number.isNaN(n)) out.push(n);
        else out.push(raw);
      }
      i = end;
    }
    if (i === inner.length) break;
    if (inner[i] !== ',') return null;
    i++;
    if (i === inner.length) return null; // trailing comma
  }
  return JSON.stringify(out);
}

type NormalizeFn = (v: unknown) => string | number | null;

function normBool(v: unknown): string | number | null {
  if (v === null || v === undefined) return null;
  if (v === true || v === 1 || v === 't' || v === 'true' || v === '1') return 1;
  if (v === false || v === 0 || v === 'f' || v === 'false' || v === '0') return 0;
  return v ? 1 : 0;
}
function normList(v: unknown): string | number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === 'string') return pgArrayToJson(v) ?? v;
  return JSON.stringify(v);
}
function normJson(v: unknown): string | number | null {
  if (v === null || v === undefined) return null;
  return typeof v === 'string' ? v : JSON.stringify(v);
}

/**
 * Per-COLUMN coercion function for a table, resolved once per column
 * instead of per cell. The bulk hydration loop maps a column list to an
 * array of these up front, so the inner cell loop is a bare indexed call
 * - no Set probes, no string-keyed dispatch (measurable across the ~20M
 * cells of a 1M-row hydration).
 */
export function columnNormalizers(meta?: ColumnMeta): (c: string) => NormalizeFn {
  if (!meta) return () => normalizeValue;
  const bool = new Set(meta.boolColumns);
  const json = new Set(meta.jsonColumns);
  const list = new Set(meta.listColumns);
  return (c) =>
    bool.has(c) ? normBool : list.has(c) ? normList : json.has(c) ? normJson : normalizeValue;
}

/**
 * Column-type-aware ingest coercion, driven by the model metadata.
 * Electric's wire format is text-typed: booleans arrive as 't'/'f' (live
 * stream) or 'true'/'false' (snapshot), JSON columns as JSON text or
 * parsed values, scalar lists as PG array literals, naive timestamps
 * without offsets. Stored raw, each of those breaks something downstream
 * - SQL predicates ('true' is 0 in a numeric context), JS truthiness
 * ('false' is truthy), json() embedding, timezone math. Exported for
 * tests.
 */
export function makeColumnNormalizer(
  meta?: ColumnMeta
): (c: string, v: unknown) => string | number | null {
  const forCol = columnNormalizers(meta);
  const memo = new Map<string, NormalizeFn>();
  return (c, v) => {
    let f = memo.get(c);
    if (!f) {
      f = forCol(c);
      memo.set(c, f);
    }
    return f(v);
  };
}

const q = (name: string) => `"${name.replaceAll('"', '""')}"`;

// Composite pk values are joined with \x01 in the tag bookkeeping table.
const PK_SEP = '\x01';

/**
 * Rows in one bulk statement. A hydration chunk is up to MAX_BATCH_ROWS
 * (50k); chunking bounds the JSON payload held in the wasm heap while
 * keeping the per-statement overhead negligible.
 */
const BULK_CHUNK = 5_000;
/**
 * Below this many consecutive same-shape inserts, per-row binding wins -
 * the JSON payload build isn't worth amortizing. Live updates (1-10 rows)
 * always take the row path; hydration always takes the bulk path.
 */
const BULK_MIN_RUN = 64;

export function makeApplier(
  engine: Engine,
  table: string,
  primaryKey: string[],
  meta?: ColumnMeta
): TableApplier {
  // Prepared statements keyed by operation + sorted column signature.
  const stmts = new Map<string, Sqlite3Stmt>();
  const pkJoinExpr = primaryKey.map(q).join(` || '${PK_SEP}' || `);
  const pkSet = new Set(primaryKey);

  const norm = makeColumnNormalizer(meta);
  const normFor = columnNormalizers(meta);

  // Object.keys() order is stable for rows parsed from one JSON response,
  // so the raw key list is a cheap shape signature: sorting (and the
  // non-pk filtering for updates) happens ONCE per shape per batch instead
  // of once per row. This is the hydration hot loop.
  const insertColsMemo = new Map<string, string[]>();
  const updateColsMemo = new Map<string, string[]>();
  function insertCols(keys: string[], sig: string): string[] {
    let c = insertColsMemo.get(sig);
    if (!c) {
      c = [...keys].sort();
      insertColsMemo.set(sig, c);
    }
    return c;
  }
  function updateCols(keys: string[], sig: string): string[] {
    let c = updateColsMemo.get(sig);
    if (!c) {
      c = keys.filter((k) => !pkSet.has(k)).sort();
      updateColsMemo.set(sig, c);
    }
    return c;
  }

  // ── Tagged-subquery bookkeeping (`_ldb_row_tag`) ────────────────────────
  // Tags are ADD-only per row (a row can be in the shape via several tags;
  // one message's tags never revoke another's). Removal happens via
  // move-out (per tag), delete (per row) and truncate (per table). Created
  // lazily so untagged shapes pay nothing.
  let tagsReady = false;
  function ensureTags(): void {
    if (tagsReady) return;
    engine.db.exec(`
			CREATE TABLE IF NOT EXISTS _ldb_row_tag (
				table_name TEXT NOT NULL, tag TEXT NOT NULL, pk TEXT NOT NULL,
				PRIMARY KEY (table_name, tag, pk)
			);
			CREATE INDEX IF NOT EXISTS idx_ldb_row_tag_pk ON _ldb_row_tag(table_name, pk);
		`);
    tagsReady = true;
  }

  function prepared(key: string, sql: string): Sqlite3Stmt {
    let s = stmts.get(key);
    if (!s) {
      s = engine.db.prepare(sql);
      stmts.set(key, s);
    }
    return s;
  }

  /**
   * Set-based hydration path: one statement per CHUNK instead of one
   * bind+step per row. The batch crosses the wasm boundary as a single
   * JSON array of value arrays and SQLite expands it with json_each, so
   * the per-row work (N bind calls + step + reset, all boundary crossings)
   * becomes C-side iteration. Same upsert semantics as the row path -
   * including last-write-wins for a pk repeated inside one chunk, since
   * json_each preserves array order.
   */
  function bulkInsert(values: Array<Record<string, unknown>>, cols: string[]): void {
    const key = `bulk:${cols.join(',')}`;
    let s = stmts.get(key);
    if (!s) {
      const nonPk = cols.filter((c) => !pkSet.has(c));
      const updates = nonPk.length
        ? `DO UPDATE SET ${nonPk.map((c) => `${q(c)}=excluded.${q(c)}`).join(',')}`
        : 'DO NOTHING';
      const sel = cols.map((_, i) => `json_extract(value,'$[${i}]')`).join(',');
      // `WHERE true` is REQUIRED, not decoration: when an INSERT takes its
      // rows from a SELECT, SQLite's parser cannot tell an upsert's ON
      // from a join's ON, and a WHERE clause is the documented
      // disambiguator. Without it this is a syntax error at prepare time
      // (caught by smoke/patterns: a 300-row seed wedged the shape loop
      // while every <64-row live insert kept working, because they take
      // the per-row path).
      s = prepared(
        key,
        `INSERT INTO ${q(table)} (${cols.map(q).join(',')})
				 SELECT ${sel} FROM json_each(?) WHERE true
				 ON CONFLICT(${primaryKey.map(q).join(',')}) ${updates}`
      );
    }
    // Per-column normalizers resolved once per bulk call - the cell loop
    // below is the single hottest loop in hydration, and a per-cell
    // string-keyed dispatch (3 Set probes) is real time across 20M cells.
    const colFns: NormalizeFn[] = cols.map((c) => normFor(c));
    // Sizes are known and every slot is filled immediately below, so the
    // arrays are preallocated rather than grown - this is the hydration
    // hot loop (200k rows in one pass is normal).
    // oxlint-disable unicorn/no-new-array -- preallocation, every slot written
    for (let start = 0; start < values.length; start += BULK_CHUNK) {
      const end = Math.min(start + BULK_CHUNK, values.length);
      const payload: Array<Array<string | number | null>> = new Array(end - start);
      for (let i = start; i < end; i++) {
        const v = values[i]!;
        const row: Array<string | number | null> = new Array(cols.length);
        for (let c = 0; c < cols.length; c++) row[c] = colFns[c]!(v[cols[c]!]);
        payload[i - start] = row;
      }
      run(s, [JSON.stringify(payload)]);
    }
    // oxlint-enable unicorn/no-new-array
  }

  function stmtFor(kind: 'insert' | 'update' | 'delete', cols: string[]): Sqlite3Stmt {
    const key = `${kind}:${cols.join(',')}`;
    const cached = stmts.get(key);
    if (cached) return cached;
    let sql: string;
    if (kind === 'insert') {
      const nonPk = cols.filter((c) => !pkSet.has(c));
      const updates = nonPk.length
        ? `DO UPDATE SET ${nonPk.map((c) => `${q(c)}=excluded.${q(c)}`).join(',')}`
        : 'DO NOTHING';
      sql = `INSERT INTO ${q(table)} (${cols.map(q).join(',')}) VALUES (${cols.map(() => '?').join(',')})
				 ON CONFLICT(${primaryKey.map(q).join(',')}) ${updates}`;
    } else if (kind === 'update') {
      sql = `UPDATE ${q(table)} SET ${cols.map((c) => `${q(c)}=?`).join(',')}
				 WHERE ${primaryKey.map((c) => `${q(c)}=?`).join(' AND ')}`;
    } else {
      sql = `DELETE FROM ${q(table)} WHERE ${primaryKey.map((c) => `${q(c)}=?`).join(' AND ')}`;
    }
    return prepared(key, sql);
  }

  function run(s: Sqlite3Stmt, values: (string | number | null)[]): void {
    try {
      s.bind(values).stepReset();
    } catch (err) {
      // Leave the cached statement reusable: a failed bind/step keeps
      // bindings attached and the next bind() dies with SQLITE_MISUSE.
      try {
        s.reset();
      } catch {
        for (const [k, v] of stmts) if (v === s) stmts.delete(k);
        try {
          s.finalize();
        } catch {
          // already finalized
        }
      }
      throw err;
    }
  }

  function pkOf(value: Record<string, unknown>): string {
    return primaryKey.map((c) => String(norm(c, value[c]) ?? '')).join(PK_SEP);
  }

  function recordTags(value: Record<string, unknown>, tags: string[]): void {
    ensureTags();
    const pk = pkOf(value);
    const s = prepared('tag-add', `INSERT OR IGNORE INTO _ldb_row_tag VALUES (?,?,?)`);
    for (const tag of tags) run(s, [table, tag, pk]);
  }

  /**
   * Tag components. A shape whose `WHERE` combines subqueries with
   * AND/OR/NOT (the feature Electric 1.6.0 added) tags each row with ONE
   * component per condition, joined by '/'. Measured against 1.7.9:
   *
   *   nested subquery    -> tags ["b2863e12..."]              conditions [true]
   *   compound (A AND B) -> tags ["980c0a5a.../980c0a5a..."]  conditions [true,true]
   *
   * A move-out pattern's `pos` selects WHICH component its value refers
   * to, so a composite tag must be matched component-wise, not whole.
   */
  const tagComponent = (tag: string, pos: number): string | undefined =>
    (tag.includes('/') ? tag.split('/') : [tag])[pos];

  /**
   * Move-out: drop every tag whose component at `pos` matches, then delete
   * rows left with no tags at all.
   *
   * The "last tag wins" rule is what makes this correct for both AND and
   * OR shapes without the client needing to know which it is: a row stays
   * as long as ANY tag still holds it, and leaves when the final one goes.
   */
  function moveOut(patterns: Array<{ value: string; pos: number }>): number {
    ensureTags();
    let removed = 0;
    const del = prepared(
      'tag-orphan-del',
      `DELETE FROM ${q(table)} WHERE (${pkJoinExpr}) = ? AND NOT EXISTS (
					SELECT 1 FROM _ldb_row_tag t WHERE t.table_name = ? AND t.pk = ?
				)`
    );
    /** Tags to drop, resolved to whole stored values. */
    const resolveTags = (p: { value: string; pos: number }): string[] => {
      // Fast path: a flat pattern against flat tags is an exact match, and
      // that is every shape we currently sync - no scan, no allocation.
      if (p.pos === 0 && !p.value.includes('/')) {
        const exact = prepared(
          'tag-exact',
          `SELECT 1 FROM _ldb_row_tag WHERE table_name = ? AND tag = ? LIMIT 1`
        );
        try {
          exact.bind([table, p.value]);
          if (exact.step()) return [p.value];
        } finally {
          exact.reset();
        }
      }
      // Composite: scan this table's distinct tags and match component-wise.
      // Only reached for compound-subquery shapes, and only on a move-out.
      const all = prepared(
        'tag-distinct',
        `SELECT DISTINCT tag FROM _ldb_row_tag WHERE table_name = ?`
      );
      const out: string[] = [];
      try {
        all.bind([table]);
        while (all.step()) {
          const t = String(all.get(0));
          if (tagComponent(t, p.pos) === p.value) out.push(t);
        }
      } finally {
        all.reset();
      }
      return out;
    };

    for (const p of patterns) {
      const rel = prepared(
        'tag-rel',
        `DELETE FROM _ldb_row_tag WHERE table_name = ? AND tag = ? RETURNING pk`
      );
      const pks: string[] = [];
      for (const tag of resolveTags(p)) {
        try {
          rel.bind([table, tag]);
          while (rel.step()) pks.push(String(rel.get(0)));
        } finally {
          rel.reset();
        }
      }
      for (const pk of pks) {
        run(del, [pk, table, pk]);
        removed++;
      }
    }
    return removed;
  }

  /** A plain insert is bulk-eligible: no tags to record afterwards. */
  function bulkSig(m: ShapeMessage): string | null {
    if (m.headers.operation !== 'insert' || !m.value || m.headers.tags?.length) return null;
    return Object.keys(m.value).join(',');
  }

  return {
    table,
    apply(messages) {
      let n = 0;
      for (let i = 0; i < messages.length; i++) {
        const m = messages[i]!;
        // Hydration fast path: a RUN of consecutive same-shape plain
        // inserts goes through one set-based statement per chunk. Live
        // traffic (short runs, tags, mixed ops) falls through unchanged.
        const sig = bulkSig(m);
        if (sig !== null) {
          let j = i + 1;
          while (j < messages.length && bulkSig(messages[j]!) === sig) j++;
          if (j - i >= BULK_MIN_RUN) {
            const cols = insertCols(Object.keys(m.value!), sig);
            // oxlint-disable-next-line unicorn/no-new-array -- preallocation, every slot written
            const values: Array<Record<string, unknown>> = new Array(j - i);
            for (let k = i; k < j; k++) values[k - i] = messages[k]!.value!;
            bulkInsert(values, cols);
            n += j - i;
            i = j - 1;
            continue;
          }
        }
        if (m.headers.event === 'move-out' && m.headers.patterns?.length) {
          n += moveOut(m.headers.patterns);
          continue;
        }
        const op = m.headers.operation;
        if (!op || !m.value) continue;
        if (op === 'delete') {
          run(
            stmtFor('delete', primaryKey),
            primaryKey.map((c) => norm(c, m.value![c]))
          );
          if (tagsReady) {
            run(
              prepared('tag-row-del', `DELETE FROM _ldb_row_tag WHERE table_name = ? AND pk = ?`),
              [table, pkOf(m.value)]
            );
          }
        } else if (op === 'insert') {
          const keys = Object.keys(m.value);
          const cols = insertCols(keys, keys.join(','));
          run(
            stmtFor('insert', cols),
            cols.map((c) => norm(c, m.value![c]))
          );
          if (m.headers.tags?.length) recordTags(m.value, m.headers.tags);
        } else {
          // Partial update: SET exactly the non-pk columns present.
          const keys = Object.keys(m.value);
          const cols = updateCols(keys, keys.join(','));
          if (cols.length > 0) {
            run(stmtFor('update', cols), [
              ...cols.map((c) => norm(c, m.value![c])),
              ...primaryKey.map((c) => norm(c, m.value![c])),
            ]);
          }
          if (m.headers.tags?.length) recordTags(m.value, m.headers.tags);
        }
        n++;
      }
      return n;
    },
    truncate() {
      engine.db.exec(`DELETE FROM ${q(table)}`);
      if (tagsReady) {
        engine.db.exec(
          `DELETE FROM _ldb_row_tag WHERE table_name = '${table.replaceAll("'", "''")}'`
        );
      }
      for (const s of stmts.values()) {
        try {
          s.finalize();
        } catch {
          // already finalized
        }
      }
      stmts.clear();
    },
  };
}
