// Shared sqlite-wasm bootstrap for the node tests. The upstream .d.ts
// types the initializer as zero-arg, but it accepts an Emscripten module
// config at runtime (same cast as src/worker/engine.ts); the print
// overrides silence the wasm banner on stdout.
import sqlite3InitModule from '@sqlite.org/sqlite-wasm';

type Init = (opts?: {
  print?: (s: string) => void;
  printErr?: (s: string) => void;
}) => ReturnType<typeof sqlite3InitModule>;

export function initSqlite(): ReturnType<typeof sqlite3InitModule> {
  return (sqlite3InitModule as unknown as Init)({ print: () => {}, printErr: () => {} });
}
