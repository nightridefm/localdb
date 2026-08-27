// Hydration-ingest A/B: per-row bind path vs the json_each set-based path,
// through the REAL applier (src/sync/apply.ts) into an in-memory SQLite.
// Bundles itself and runs the inner half under node.
//
// Run: node bench/ingest-bench.mjs   (INGEST_N=200000 to change the size)
import { build } from 'esbuild';
import { spawnSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));

if (!process.env.INGEST_BENCH_INNER) {
  const outfile = path.join(here, '.build', 'ingest-bench.inner.mjs');
  mkdirSync(path.dirname(outfile), { recursive: true });
  await build({
    entryPoints: [path.join(here, 'ingest-bench.inner.mjs')],
    bundle: true,
    platform: 'node',
    format: 'esm',
    outfile,
    logLevel: 'silent',
    external: ['@sqlite.org/sqlite-wasm'],
  });
  const res = spawnSync(process.execPath, ['--no-warnings', outfile], {
    stdio: 'inherit',
    env: { ...process.env, INGEST_BENCH_INNER: '1', TZ: 'America/New_York' },
  });
  process.exit(res.status ?? 1);
}
