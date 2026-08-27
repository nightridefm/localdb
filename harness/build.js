// Shared bundler for every browser harness (bench, smoke, fuzz).
//
// Each harness keeps its OWN entry list - that is the part worth reading
// at the call site. What lives here is only the boilerplate all three had
// copied verbatim: esbuild settings plus the sqlite-wasm resolution rule.
//
// sqlite-wasm must stay EXTERNAL and be rewritten to its servable
// /node_modules URL, so its .wasm resolves relative to the real module at
// runtime. Bundling it breaks import.meta.url-relative wasm loading.
import { build } from 'esbuild';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const PKG_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const sqliteWasmExternal = {
  name: 'sqlite-wasm-external',
  setup(b) {
    b.onResolve({ filter: /^@sqlite\.org\/sqlite-wasm$/ }, () => ({
      path: '/node_modules/@sqlite.org/sqlite-wasm/dist/index.mjs',
      external: true,
    }));
  },
};

/**
 * Bundle `entries` ({ in, out } paths relative to the package root).
 * `jsx: true` enables automatic JSX for the React harness.
 */
export async function bundle(entries, opts = {}) {
  for (const e of entries) {
    await build({
      bundle: true,
      format: 'esm',
      platform: 'browser',
      target: 'es2022',
      sourcemap: true,
      plugins: [sqliteWasmExternal],
      logLevel: 'warning',
      ...(opts.jsx || e.jsx ? { jsx: 'automatic' } : {}),
      entryPoints: [path.join(PKG_ROOT, e.in)],
      outfile: path.join(PKG_ROOT, e.out),
    });
  }
}

/** The two worker entries every browser harness needs. */
export const WORKER_ENTRIES = (distDir) => [
  { in: 'src/worker/writer.worker.ts', out: `${distDir}/writer.worker.js` },
  { in: 'src/worker/reader.worker.ts', out: `${distDir}/reader.worker.js` },
];
