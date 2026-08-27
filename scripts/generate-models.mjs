#!/usr/bin/env node
// Prisma -> localdb model codegen. Reads a Prisma schema via DMMF and
// emits a self-contained TypeScript module with, per model:
//   - column metadata (ModelMeta: columns, primary key, boolean/JSON/list
//     columns) for ingest coercion and typed delivery
//   - SQLite DDL for the local cache
//   - a row type in which every DateTime is a string (timestamps are
//     normalized to ISO-8601 text at ingest, and results cross the worker
//     boundary as JSON)
// plus bound SQL helpers (rowJson, ddlFor, shapeColumns).
//
// Usage:
//   generate-models <schema.prisma> <out.ts> [--lib <import>] [--check]
//
//   --lib    import specifier for the localdb package in the emitted file
//            (default: '@nightride/localdb')
//   --check  regenerate in memory and fail if <out.ts> is stale
//
// DDL policy: the local DB is a DISPOSABLE SYNC CACHE. Only primary-key
// columns are NOT NULL - shapes routinely sync column subsets, so every
// other column must tolerate absence. Constraint enforcement belongs to
// Postgres; drift here is resolved by resync, not by rejecting writes.
//
// Requires @prisma/internals (a peer dependency of this package).

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import path from 'node:path';

const args = process.argv.slice(2);
const check = args.includes('--check');
const libIdx = args.indexOf('--lib');
const lib = libIdx >= 0 ? args[libIdx + 1] : '@nightride/localdb';
const positional = args.filter((a, i) => !a.startsWith('--') && (libIdx < 0 || i !== libIdx + 1));
const [schemaPath, outPath] = positional;

if (!schemaPath || !outPath) {
  console.error('usage: generate-models <schema.prisma> <out.ts> [--lib <import>] [--check]');
  process.exit(2);
}

let internals;
try {
  internals = (await import('@prisma/internals')).default;
} catch {
  console.error('generate-models: @prisma/internals is not installed (it is a peer dependency)');
  process.exit(2);
}

const SQLITE_TYPE = {
  String: 'TEXT',
  Boolean: 'INTEGER',
  Int: 'INTEGER',
  BigInt: 'INTEGER',
  Float: 'REAL',
  Decimal: 'TEXT',
  DateTime: 'TEXT',
  Json: 'TEXT',
  Bytes: 'BLOB',
};

// Row types as delivered by the read path: DateTime is an ISO string,
// Json is parsed but untyped, everything crosses the boundary as JSON.
const TS_TYPE = {
  String: 'string',
  Boolean: 'boolean',
  Int: 'number',
  BigInt: 'number',
  Float: 'number',
  Decimal: 'string',
  DateTime: 'string',
  Json: 'unknown',
  Bytes: 'unknown',
};

const q = (name) => `"${name.replaceAll('"', '""')}"`;

const dmmf = await internals.getDMMF({ datamodel: readFileSync(schemaPath, 'utf8') });
const enumNames = new Set(dmmf.datamodel.enums.map((e) => e.name));

const metaEntries = [];
const rowTypes = [];
let skipped = 0;

for (const model of dmmf.datamodel.models) {
  const table = model.dbName ?? model.name;
  const scalarFields = model.fields.filter((f) => f.kind === 'scalar' || f.kind === 'enum');
  const pk = model.primaryKey?.fields ?? model.fields.filter((f) => f.isId).map((f) => f.name);
  if (pk.length === 0) {
    skipped++;
    continue; // shapes need a primary key; un-keyed models are not syncable
  }
  const columns = scalarFields.map((f) => f.dbName ?? f.name);
  const dateColumns = scalarFields
    .filter((f) => f.type === 'DateTime')
    .map((f) => f.dbName ?? f.name);
  // Ingest coercion metadata (see sync/apply.ts): Electric ships booleans
  // as text ('t'/'f' live, 'true'/'false' snapshot), JSON columns as text
  // or parsed values, and scalar lists as Postgres array literals.
  const boolColumns = scalarFields
    .filter((f) => f.type === 'Boolean' && !f.isList)
    .map((f) => f.dbName ?? f.name);
  const jsonColumns = scalarFields
    .filter((f) => f.type === 'Json' && !f.isList)
    .map((f) => f.dbName ?? f.name);
  const listColumns = scalarFields.filter((f) => f.isList).map((f) => f.dbName ?? f.name);
  const colDefs = scalarFields.map((f) => {
    const col = f.dbName ?? f.name;
    const sqlType = f.isList
      ? 'TEXT' // scalar lists land as JSON text
      : enumNames.has(f.type)
        ? 'TEXT'
        : (SQLITE_TYPE[f.type] ?? 'TEXT');
    const notNull = pk.includes(col) ? ' NOT NULL' : '';
    return `\t${q(col)} ${sqlType}${notNull}`;
  });
  const ddl = `CREATE TABLE IF NOT EXISTS ${q(table)} (\n${colDefs.join(',\n')},\n\tPRIMARY KEY (${pk.map(q).join(', ')})\n);`;

  metaEntries.push(`  ${model.name}: {
    table: ${JSON.stringify(table)},
    primaryKey: ${JSON.stringify(pk)},
    columns: ${JSON.stringify(columns)},
    dateColumns: ${JSON.stringify(dateColumns)},
    boolColumns: ${JSON.stringify(boolColumns)},
    jsonColumns: ${JSON.stringify(jsonColumns)},
    listColumns: ${JSON.stringify(listColumns)},
    ddl: ${JSON.stringify(ddl)},
  },`);

  const fields = scalarFields.map((f) => {
    const col = f.dbName ?? f.name;
    const base = enumNames.has(f.type) ? 'string' : (TS_TYPE[f.type] ?? 'unknown');
    const t = f.isList ? `${base}[]` : base;
    return `    ${col}: ${t}${f.isRequired ? '' : ' | null'};`;
  });
  rowTypes.push(`  ${model.name}: {\n${fields.join('\n')}\n  };`);
}

const file = `// AUTO-GENERATED by localdb generate-models - DO NOT EDIT.
// Source: ${path.basename(schemaPath)} (${dmmf.datamodel.models.length} models, ${skipped} skipped for lacking a primary key).
//
// DDL policy: primary keys NOT NULL, everything else nullable - the local
// DB is a disposable sync cache and shapes may sync column subsets.
// DateTime columns are typed string: timestamps are ISO-8601 TEXT at
// ingest and results cross the worker boundary as JSON.

/* eslint-disable */
/* oxlint-disable */
import { bindModels, type ModelMeta } from ${JSON.stringify(lib)};

/** Row types as delivered by localdb reads (all dates are strings). */
export type ModelRows = {
${rowTypes.join('\n')}
};

export type ModelName = keyof ModelRows;
export type ModelRow<M extends ModelName> = ModelRows[M];

export const models = {
${metaEntries.join('\n')}
} satisfies Record<ModelName, ModelMeta>;

export const { rowJson, ddlFor, shapeColumns } = bindModels(models);
`;

if (check) {
  const current = existsSync(outPath) ? readFileSync(outPath, 'utf8') : '';
  // Compare modulo formatting: a formatter may rewrap the generated file.
  const strip = (s) => s.replaceAll(/\s+/g, ' ').replaceAll("'", '"');
  if (strip(current) !== strip(file)) {
    console.error(
      `generate-models: ${outPath} is STALE with respect to ${schemaPath}.\n` +
        `Regenerate it with:  generate-models ${schemaPath} ${outPath}` +
        (lib === '@nightride/localdb' ? '' : ` --lib ${lib}`)
    );
    process.exit(1);
  }
  console.log(`generate-models: ok - ${outPath} matches ${schemaPath}`);
} else {
  mkdirSync(path.dirname(outPath), { recursive: true });
  writeFileSync(outPath, file);
  console.log(
    `generated ${outPath}: ${dmmf.datamodel.models.length - skipped} models (${skipped} skipped, no pk)`
  );
}
