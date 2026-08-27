#!/usr/bin/env node
// Build the publishable dist/: JS + .d.ts + source maps, with the source's
// explicit .ts import specifiers rewritten to .js.
// rewriteRelativeImportExtensions (tsconfig.build.json) covers the emitted
// JS; the declaration output keeps the .ts specifiers (compiler gap in the
// current tsc), so they are rewritten here - without this, a consumer's
// type resolution dead-ends on './client/LocalDb.ts' inside dist/.

import { execSync } from 'node:child_process';
import { readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';

rmSync('dist', { recursive: true, force: true });
execSync('tsc -p tsconfig.build.json', { stdio: 'inherit' });

function* walk(dir) {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) yield* walk(p);
    else yield p;
  }
}

const RELATIVE_TS = /(['"])(\.{1,2}\/[^'"]*)\.ts\1/g;
let rewritten = 0;
for (const f of walk('dist')) {
  if (!f.endsWith('.d.ts')) continue;
  const src = readFileSync(f, 'utf8');
  const out = src.replace(RELATIVE_TS, (_, quote, spec) => `${quote}${spec}.js${quote}`);
  if (out !== src) {
    writeFileSync(f, out);
    rewritten++;
  }
}
console.log(`dist built (${rewritten} declaration files rewritten to .js specifiers)`);

// Guard: no emitted file may still reference a relative .ts specifier.
for (const f of walk('dist')) {
  if (!f.endsWith('.js') && !f.endsWith('.d.ts')) continue;
  if (RELATIVE_TS.test(readFileSync(f, 'utf8'))) {
    console.error(`build-dist: ${f} still imports a .ts specifier`);
    process.exit(1);
  }
}
