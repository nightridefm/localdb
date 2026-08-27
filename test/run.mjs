// Test runner: esbuild-bundles each test/*.test.ts and runs it in a child
// node process with TZ=America/New_York - a deliberately non-UTC zone so
// any local-time leakage in timestamp handling fails loudly instead of
// passing in UTC-only CI/dev containers. A test file fails by throwing
// (non-zero exit).
import { build } from 'esbuild';
import { spawnSync } from 'node:child_process';
import { readdirSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const outDir = path.join(here, '.build');
mkdirSync(outDir, { recursive: true });

const tests = readdirSync(here).filter((f) => f.endsWith('.test.ts'));
let failed = 0;

for (const t of tests) {
  const outfile = path.join(outDir, t.replace(/\.ts$/, '.mjs'));
  await build({
    entryPoints: [path.join(here, t)],
    bundle: true,
    platform: 'node',
    format: 'esm',
    outfile,
    logLevel: 'silent',
    // sqlite-wasm's node build stays external - it locates its .wasm
    // relative to its own module path.
    external: ['@sqlite.org/sqlite-wasm'],
  });
  const res = spawnSync(process.execPath, ['--no-warnings', outfile], {
    stdio: 'inherit',
    env: { ...process.env, TZ: 'America/New_York' },
  });
  if (res.status === 0) {
    console.log(`PASS ${t}`);
  } else {
    console.error(`FAIL ${t} (exit ${res.status})`);
    failed++;
  }
}

if (failed > 0) {
  console.error(`\n${failed}/${tests.length} test files failed`);
  process.exit(1);
}
console.log(`\nall ${tests.length} test files passed (TZ=America/New_York)`);
