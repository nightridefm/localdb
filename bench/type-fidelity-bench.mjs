// A/B benchmark for the type-fidelity machinery: what does typed ingest
// and delivery cost over the plain path, in one process:
//
//   1. value normalization (pure JS): generic normalizeValue vs the
//      column-aware normalizer
//   2. full applier ingest: identical wire messages into `artist` (bool +
//      json coercion active) vs a metadata-less clone table (plain
//      normalizeValue fallback) - isolates coercion cost through the
//      whole stack (prepared stmts + sqlite insert dominate)
//   3. read delivery: plain json_object embed vs embedExpr (bool CASE +
//      json_valid CASE per row) at grid-page and bulk sizes
//   4. rowJson aggregate: json_group_array over CASE'd vs plain row
//      objects
//
// Run: node bench/type-fidelity-bench.mjs   (bundles itself via esbuild)
import { build } from 'esbuild';
import { spawnSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));

if (!process.env.TYPE_BENCH_INNER) {
  const outfile = path.join(here, '.build', 'type-fidelity-bench.inner.mjs');
  mkdirSync(path.dirname(outfile), { recursive: true });
  await build({
    entryPoints: [path.join(here, 'type-fidelity-bench.inner.mjs')],
    bundle: true,
    platform: 'node',
    format: 'esm',
    outfile,
    logLevel: 'silent',
    external: ['@sqlite.org/sqlite-wasm'],
  });
  const res = spawnSync(process.execPath, ['--no-warnings', outfile], {
    stdio: 'inherit',
    env: { ...process.env, TYPE_BENCH_INNER: '1', TZ: 'America/New_York' },
  });
  process.exit(res.status ?? 1);
}
