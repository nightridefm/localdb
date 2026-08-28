// SQL helpers over model metadata (see ModelMeta in types.ts) - the
// SQLite replacement for the Postgres `to_json(alias)` whole-row
// convention. Metadata is normally generated from a Prisma schema
// (scripts/generate-models.mjs), so a schema change regenerates it and
// drift between SQL fragments and row types is impossible by construction.
//
// Nesting convention: name a column `<name>__json` and the engine's JSON
// wrapper embeds it as a real JSON value under `<name>` (instead of an
// escaped string). rowJson() output is JSON text, so nested whole-row
// selects compose:
//
//   SELECT rp.*,
//     (SELECT ${rowJson('artist', 'a')} FROM artist a WHERE a.id = t.artist_id) AS artist__json
//   FROM track t
//
// which delivers rows shaped { ...t, artist: { ...artist row } | null }.

import type { AttachDef, ModelMeta } from './types.ts';

const q = (name: string) => `"${name.replaceAll('"', '""')}"`;

/**
 * Bind the SQL helpers to a model-metadata map (normally the generated
 * `models` object). The generated module calls this and re-exports the
 * result, so app code gets model-name-typed helpers:
 *
 *   export const { rowJson, ddlFor, shapeColumns } = bindModels(models)
 */
export function bindModels<M extends Record<string, ModelMeta>>(models: M) {
  type Name = Extract<keyof M, string>;

  /**
   * `json_object('col', "alias"."col", ...)` over a model's columns (or a
   * subset). The SQLite equivalent of Postgres `to_json(alias)`.
   */
  function rowJson(model: Name, alias?: string, opts?: { columns?: readonly string[] }): string {
    const m = models[model]!;
    const a = alias ?? m.table;
    const cols = opts?.columns ?? m.columns;
    // JSON / scalar-list columns are stored as JSON TEXT - embed them as
    // real JSON values so consumers get arrays/objects, not escaped
    // strings. json_valid guards against pre-coercion residue so a
    // malformed cell degrades to its raw text instead of erroring the
    // row. Boolean columns (stored 1/0 for SQL predicates) deliver as
    // real JSON true/false - React `{x && <jsx>}` gates render a
    // literal 0.
    const jsonish = new Set<string>([...m.jsonColumns, ...m.listColumns]);
    const boolish = new Set<string>(m.boolColumns);
    const parts = cols.map((c) => {
      const ref = `${q(a)}.${q(c)}`;
      const val = jsonish.has(c)
        ? `CASE WHEN json_valid(${ref}) THEN json(${ref}) ELSE ${ref} END`
        : boolish.has(c)
          ? `CASE WHEN ${ref} IS NULL THEN NULL WHEN ${ref} THEN json('true') ELSE json('false') END`
          : ref;
      return `'${c.replaceAll("'", "''")}', ${val}`;
    });
    return `json_object(${parts.join(', ')})`;
  }

  /** Concatenated CREATE TABLE statements for LocalDbConfig.schemaSql. */
  function ddlFor(...names: Name[]): string {
    return names.map((n) => models[n]!.ddl).join('\n');
  }

  /** Column list for an Electric shape's `columns` param (or a subset check). */
  function shapeColumns(model: Name, subset?: readonly string[]): string {
    const m = models[model]!;
    if (subset) {
      for (const c of subset) {
        if (!(m.columns as readonly string[]).includes(c)) {
          throw new Error(`column '${c}' not in model '${model}'`);
        }
      }
      return subset.join(',');
    }
    return m.columns.join(',');
  }

  return { rowJson, ddlFor, shapeColumns };
}

/**
 * Boolean SQL expression -> real JSON true/false for hand-written
 * json_object select items ('has_avatar', ${jsonBool('d.has_avatar')}).
 * Bare 1/0 inside a hand-built json_object ships numbers, and React
 * `{x && <jsx>}` gates render a literal 0.
 */
export function jsonBool(expr: string): string {
  return `CASE WHEN ${expr} IS NULL THEN NULL WHEN ${expr} THEN json('true') ELSE json('false') END`;
}

/** Name of a table's FTS5 trigram mirror (see LocalDbConfig.fts). */
export function ftsName(table: string): string {
  return `${table}_fts`;
}

/**
 * Assert that no column name is boolean/JSON-typed in one model and
 * differently typed in another. The engine's delivery layer maps result-
 * column NAMES to boolean/JSON conversions (it cannot attribute an
 * arbitrary result column to a table), which is only sound under this
 * invariant. Called by the LocalDb constructor; throws with the offending
 * column on violation, turning a schema collision into a boot failure
 * instead of a silent mis-delivery.
 */
export function validateModels(models: Record<string, ModelMeta>): void {
  const boolNames = new Set<string>();
  const jsonNames = new Set<string>();
  for (const m of Object.values(models)) {
    for (const c of m.boolColumns) boolNames.add(c);
    for (const c of m.jsonColumns) jsonNames.add(c);
    for (const c of m.listColumns) jsonNames.add(c);
  }
  for (const m of Object.values(models)) {
    const bools = new Set<string>(m.boolColumns);
    const jsons = new Set<string>([...m.jsonColumns, ...m.listColumns]);
    for (const c of m.columns) {
      if (boolNames.has(c) && !bools.has(c)) {
        throw new Error(
          `localdb models: column '${c}' on '${m.table}' collides with a boolean column of the same name in another model`
        );
      }
      if (jsonNames.has(c) && !jsons.has(c)) {
        throw new Error(
          `localdb models: column '${c}' on '${m.table}' collides with a JSON/list column of the same name in another model`
        );
      }
    }
  }
}

/** A bare SQL identifier - what an ATTACH alias is allowed to be. */
const BARE_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Validate LocalDbConfig.attach at construction. The alias is interpolated
 * into `ATTACH ... AS <alias>` in the worker, so it is restricted to a
 * bare identifier rather than escaped - there is no legitimate reason for
 * an alias to need quoting, and rejecting is safer than quoting.
 */
export function validateAttach(attach: readonly AttachDef[], ownDbName: string): void {
  const seenAlias = new Set<string>();
  const seenDb = new Set<string>();
  for (const a of attach) {
    if (!BARE_IDENT.test(a.alias)) {
      throw new Error(
        `localdb attach: alias '${a.alias}' is not a bare identifier (letters, digits, underscore; not starting with a digit)`
      );
    }
    const lower = a.alias.toLowerCase();
    if (lower === 'main' || lower === 'temp') {
      throw new Error(`localdb attach: alias '${a.alias}' is reserved by SQLite`);
    }
    if (seenAlias.has(lower)) throw new Error(`localdb attach: duplicate alias '${a.alias}'`);
    seenAlias.add(lower);
    if (!a.dbName) throw new Error(`localdb attach: alias '${a.alias}' has no dbName`);
    if (a.dbName === ownDbName) {
      throw new Error(
        `localdb attach: alias '${a.alias}' points at this database's own file ('${ownDbName}')`
      );
    }
    if (seenDb.has(a.dbName)) {
      throw new Error(`localdb attach: '${a.dbName}' is attached more than once`);
    }
    seenDb.add(a.dbName);
  }
}

/**
 * Merge the attached dbs' models into the owning db's map for delivery
 * hints. Keys are namespaced by alias because model NAMES may collide
 * across databases while describing different tables; embedHints only
 * reads the values, so the key shape is free.
 *
 * The merged map is what validateModels must accept: embedHints is
 * name-global, so a column name that is boolean in one db and plain text
 * in another cannot be delivered correctly and has to fail at boot.
 */
export function mergeAttachModels(
  own: Record<string, ModelMeta> | undefined,
  attach: readonly AttachDef[]
): Record<string, ModelMeta> | undefined {
  if (!attach.some((a) => a.models)) return own;
  const merged: Record<string, ModelMeta> = { ...own };
  for (const a of attach) {
    for (const [name, meta] of Object.entries(a.models ?? {})) merged[`${a.alias}:${name}`] = meta;
  }
  return merged;
}
