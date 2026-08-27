# localdb

Reactive local-first SQLite for the browser. localdb syncs
[ElectricSQL](https://electric-sql.com) shapes into an OPFS-backed
[sqlite-wasm](https://sqlite.org/wasm) database and serves live queries
from worker threads. The main thread stays thin: it subscribes, receives
UTF-8 JSON bytes, and parses only when a result actually changed.

- **Off-thread everything.** One writer worker owns sync. A pool of reader
  workers executes queries. A slow query never blocks sync, and a sync
  burst never blocks reads.
- **Raw SQLite SQL.** Queries are plain SQL strings with `?` params. No
  query builder, no dialect translation layer.
- **Change-driven updates.** Commits invalidate only the queries that read
  the touched tables. Results are hashed in the worker; unchanged results
  ship nothing.
- **Multi-tab.** One tab syncs, every tab reads the same OPFS file. Leader
  failover is automatic.
- **Disposable cache.** The local database can always be wiped and
  re-synced. Durability is Postgres's job.

## How it works

```
each tab   hooks / signals -- LocalDb client (subscriptions, snapshots, delta cache)
                 | one batched postMessage per tick (UTF-8 bytes, parse on change only)
           reader worker pool - own OPFS handles, query registry,
                 throttled scheduler, hash dedupe, priority lanes
                 ^ commit notices via BroadcastChannel (cross-tab)
ONE tab    writer worker (Web Locks leader election; hot standbys elsewhere)
           Electric shapes -> coalesced fenced transactions -> flush -> notice
           deferred indexes + FTS mirrors rebuilt after hydration
```

- The writer applies each shape batch in one transaction and persists the
  shape cursor in the same transaction. A crash can never desync cursor
  and data.
- Invalidation is structural: a shape maps to one table, so each commit
  notice carries the touched-table set. Reader workers extract each
  query's table dependencies with `sqlite3_set_authorizer` at prepare
  time.
- Every result crosses the WASM boundary as one JSON string built by
  SQLite (`json_group_array(json_object(...))`), is hashed against the
  previous run inside the wasm heap, and is copied out only on change.
- A Web Locks fence keeps readers off half-written B-tree pages: readers
  hold it shared per tick, the writer holds it exclusive per commit.
  Measured overhead is ~0.05 ms per read.

## Browser support

The writer probes OPFS capabilities at boot and picks a connection mode:

| Mode     | Requires                                  | Browsers                | Topology                                                       |
| -------- | ----------------------------------------- | ----------------------- | -------------------------------------------------------------- |
| `multi`  | `createSyncAccessHandle` mode `readwrite-unsafe` | Chrome/Edge 121+  | Writer + N readers, each with a persistent handle. Multi-tab.  |
| `baton`  | mode `read-only`                          | Safari 16.4+, Firefox   | Readers hold read-only handles; the writer opens an exclusive handle only inside its write fence. Multi-tab. |
| `single` | OPFS SAHPool                              | Older OPFS browsers     | One connection; the writer worker hosts reads. Single tab.     |

Environments without OPFS (or Safari Private Browsing) fail loudly: the
client reports a terminal `unsupported` status with a reason instead of
spinning.

## Install

```
npm install @nightride/localdb
```

The package ships compiled JS + type declarations (`dist/`, plus the
TypeScript source for source maps) and is consumed through a bundler
(Vite, esbuild, webpack, ...). `@sqlite.org/sqlite-wasm` is its only
runtime dependency. React is an optional peer dependency for the hook;
`@prisma/internals` is an optional peer dependency for the codegen CLI.

Installing straight from git (`github:nightridefm/localdb`) also works:
a `prepare` script builds `dist/` at install time. pnpm 10+ blocks
git-dependency build scripts by default, so allowlist the package once
in the consumer's `pnpm-workspace.yaml`:

```yaml
onlyBuiltDependencies:
  - '@nightride/localdb'
```

## Quick start

### 1. Generate model metadata (optional but recommended)

If your Postgres schema lives in Prisma, generate typed model metadata:

```
npx localdb-generate-models schema.prisma src/gen/models.ts
```

The emitted module contains per-model column metadata, SQLite DDL, row
types, and bound SQL helpers. Metadata is what makes booleans, JSON
columns, scalar lists, and timestamps arrive with correct types; tables
without such columns work without it.

### 2. Create the database

```ts
import { LocalDb } from '@nightride/localdb';
import { models, ddlFor, shapeColumns } from './gen/models.ts';

const db = new LocalDb(
  {
    dbName: 'app.db',
    schemaVersion: 3, // bump to wipe + resync
    schemaSql: ddlFor('artist', 'track') + `
      CREATE INDEX IF NOT EXISTS idx_track_artist ON track(artist_id);
    `,
    models,
    shapes: [
      { table: 'artist', primaryKey: ['id'], url: '/shape/artist' },
      {
        table: 'track',
        primaryKey: ['id'],
        url: '/shape/track',
        params: { columns: shapeColumns('track') },
      },
    ],
    fts: [{ table: 'track', columns: ['title'] }],
  },
  {
    writer: new Worker(new URL('@nightride/localdb/writer.worker', import.meta.url), { type: 'module' }),
    spawnReader: () =>
      new Worker(new URL('@nightride/localdb/reader.worker', import.meta.url), { type: 'module' }),
  }
);
```

The shape URLs point at your Electric instance, usually through a proxy
that adds auth. localdb speaks the shape protocol directly.

### 3. Read

```ts
// Live subscription: fires on first result, then only on change.
const stop = db.subscribe(
  { sql: 'SELECT * FROM track WHERE artist_id = ? ORDER BY created_at DESC', params: [id] },
  (result) => render(result.rows)
);

// One-shot read (route loaders). Calls issued in the same microtask are
// batched into one worker message and share one consistent snapshot.
const rows = await db.query('SELECT count(*) AS n FROM track');

// Sync state for boot UI. The current state is replayed on subscribe, so
// a late listener still sees a terminal `unsupported`.
db.onSyncStatus((s) => console.log(s.state));

// Manual sync control (e.g. a "work offline" toggle). Reads keep serving
// the local data; resume picks up from the stored cursors.
db.pauseSync();
db.resumeSync();
```

A failing query (broken SQL, torn topology) delivers a result with
`error` set instead of never firing; the previous good `rows` are kept,
and the field clears on the next successful delivery. A terminal
`unsupported` environment rejects `ready()` and all pending `query()`
calls, so nothing awaits forever.

React:

```tsx
import { useLiveQuery } from '@nightride/localdb/react';

function Tracks({ artistId }: { artistId: string }) {
  const { rows, isLoading, error } = useLiveQuery<TrackRow>(db, {
    sql: 'SELECT * FROM track WHERE artist_id = ? ORDER BY created_at DESC',
    params: [artistId],
  });
  // ...
}
```

Components that share `(sql, params)` share one worker-side execution.
For large lists with small churn, set `delta: true` on the spec: the
worker then ships only changed rows, and unchanged row objects keep
referential identity across updates (so `memo` rows reconcile for free).

## Query dialect

Queries are native SQLite. Conventions worth knowing:

- **Timestamps are ISO-8601 UTC text**, normalized at ingest. Compare with
  `strftime('%Y-%m-%dT%H:%M:%fZ','now')`, never bare `datetime('now')`.
  With model metadata, normalization is scoped to declared DateTime
  columns; without it, any timestamp-shaped text cell is normalized.
- **Booleans are stored as 1/0** (so `WHERE is_active` works) and
  **delivered as real `true`/`false`** when model metadata declares the
  column.
- **JSON and scalar-list columns** are stored as JSON text and delivered
  as parsed arrays/objects.
- **`__json` aliasing:** any select item whose value is JSON text (a
  `rowJson(...)` subselect, a `json_group_array(...)` aggregate) must be
  aliased `AS <name>__json`. The engine embeds it as a real JSON value and
  strips the suffix, so consumers read `row.<name>`:

  ```ts
  const sql = `
    SELECT t.*,
      (SELECT ${rowJson('artist', 'a')} FROM artist a WHERE a.id = t.artist_id) AS artist__json
    FROM track t
  `;
  // rows are { ...track, artist: { ...artist } | null }
  ```

- **FTS5 trigram search:** declare `fts: [{ table, columns }]` and query
  the mirror for indexed substring search:
  `t.rowid IN (SELECT rowid FROM track_fts WHERE title LIKE '%syn%')`.
  Commits to the base table invalidate mirror queries automatically.
- **Fuzzy search:** every connection registers pg_trgm-compatible UDFs.
  `similarity(a, b)` is exact pg_trgm parity for name-vs-name comparison.
  `word_similarity(query, text)` scores the best word window, for ORDER BY
  ranking. `search_similarity(query, text)` ANDs over query words, for
  WHERE arms - adding a word always narrows the result.

## Configuration

All fields of `LocalDbConfig` (see `src/types.ts` for full doc comments):

| Field                 | Default   | Purpose                                                        |
| --------------------- | --------- | -------------------------------------------------------------- |
| `dbName`              | required  | OPFS file name.                                                |
| `schemaSql`           | required  | DDL applied to a fresh database.                               |
| `schemaVersion`       | required  | Bump to wipe the cache and resync every shape.                 |
| `shapes`              | required  | Electric shape subscriptions.                                  |
| `models`              | -         | Model metadata for typed ingest + delivery.                    |
| `maxUpdateHz`         | 10        | Ceiling for reactive delivery.                                 |
| `writeCoalesceMs`     | 50        | Write-side commit coalescing window for live traffic.          |
| `readers`             | cores - 2, max 4 | Reader pool size.                                       |
| `readMode`            | `'fence'` | Read consistency mode (see below).                             |
| `alignShapes`         | off       | Cross-shape LSN alignment: joins never observe skew between actively syncing tables. |
| `advise`              | off       | Index advisor: surfaces unindexed full-table scans per new query. |
| `fts`                 | -         | FTS5 trigram mirrors to maintain.                              |
| `parkHiddenAfterMs`   | 0 (off)   | Tear down the reader pool after the tab is hidden this long.   |
| `pauseSyncWhenHidden` | off       | Pause Electric streams while every tab of this DB is hidden.   |
| `forceConnMode`       | -         | Testing override for the connection-mode probe.                |

## Read consistency

**`fence` (default) - the screen is always self-consistent.** Every query
in a tick runs inside one shared lock hold, so a list and its counter can
never disagree, and a read issued after a commit always reflects it. The
cost is waiting: reads can queue behind a commit and vice versa
(sub-millisecond at small sizes, milliseconds at millions of rows).

**`seqlock` - nothing waits, but validation is weaker.** Reads take no
locks and validate with `PRAGMA data_version` sampled around each query.
It gives tighter read tails and full writer/reader decoupling, but at
multi-million-row scale the `data_version` signal was measured lagging
~114 ms behind commits on this VFS, which allows a stale read to pass
validation. Treat `seqlock` as experimental and keep `fence` wherever a
stale row costs a wrong decision.

## Multi-tab

Every tab spawns the writer worker; a never-released exclusive Web Lock
elects one sync leader. Non-leaders idle as hot standbys and relay the
leader's sync status. The read path needs no election: the OPFS file is
shared storage, so a new tab's reader pool has the full dataset instantly
(no second hydration, no second Electric stream), and commit notices ride
BroadcastChannel, which is cross-tab by nature. When the leader tab
closes, the lock transfers and the new leader resumes from the
crash-consistent cursor.

Each tab logs its connection mode, leader/standby role, promotions, and
sync-state transitions to the console, so "which tab is making the shape
requests" is answerable without a debugger.

## Electric protocol coverage

Verified against Electric 1.5.1 and 1.7.9, including live instances:

- offset/handle chaining, snapshot pages, `up-to-date`, live long-poll
- `electric-cursor` echo (CDN request collapsing)
- 409 handle rotation and `must-refetch` (table truncate + re-hydrate)
- permanent 4xx responses back off immediately (no hot loop)
- partial updates (primary key + changed columns only)
- tagged subqueries: move-in backfills and headers-only move-out
  messages, including composite tags from compound predicates, with full
  tag-lifecycle bookkeeping (`_ldb_row_tag`)
- LSN-based dedup is intentionally absent: ordered idempotent upserts
  make redelivery and move-in backfill safe without it
- not consumed yet: `old_value` / `replica=full`; SSE transport (the
  transport is pluggable, long-poll is implemented)

## Performance

Headline numbers from the included harnesses (headless Chrome, 1M-row
seed unless noted; see `bench/` to reproduce):

| Metric                                             | Result           |
| -------------------------------------------------- | ---------------- |
| Reader p50, 4 readers under continuous writes      | 0.24 ms (100k rows), 2.9 ms (1M) |
| Aggregate read qps, 4 readers                      | ~13,000 (100k-row seed) |
| Writer batch commit (1,000 rows, incl. flush)      | ~19 ms           |
| Reactive delivery p50 (commit -> subscriber)       | 3 ms (100k), 23 ms (1M) |
| Hydration ingest (bulk json_each path)             | ~150k rows/s     |
| Isolated mutation, end to end                      | ~10 ms           |
| Delta shipping, 1-row change in a 2,051-row list   | 83 B vs 107 KB full |
| Staleness violations (fence mode, all runs)        | 0                |

Scale checks at 10M rows (~2 GB OPFS) stay correct (0 staleness,
integrity ok); latency is then dominated by per-query cost and fence hold
time.

## Development

```
pnpm install
pnpm test           # node test suite (runs under TZ=America/New_York)
pnpm check          # tsc --noEmit over src, tests, smoke, bench
pnpm lint           # oxlint --type-aware
pnpm format         # oxfmt (format:check in CI)
pnpm check:example-models  # codegen drift check for examples/models.ts
pnpm build          # publishable dist/ (JS + d.ts, .ts imports rewritten)

pnpm build:smoke && pnpm smoke     # headless-Chrome e2e, all three connection tiers
pnpm build:smoke && pnpm harness   # manual: open http://127.0.0.1:8077/smoke/
pnpm build:bench && pnpm harness   # then open /bench/concurrency.html?readers=4&seed=100000
pnpm bench:ingest   # node-side hydration ingest A/B
pnpm bench:types    # node-side type-fidelity A/B
```

CI runs check, lint, format:check, codegen drift, the node tests, the
dist build, and the headless browser smoke (multi / baton / single).

Layout:

```
src/            the library (client, workers, sync, VFS, SQL helpers)
scripts/        generate-models.mjs - Prisma -> models codegen CLI
examples/       example schema + generated models (used by tests and benches)
test/           node tests: ingest coercion, tag lifecycle, registry, similarity
harness/        static server + mock Electric shape endpoint + CDP poller
smoke/          scripted browser scenarios against the mock endpoint
bench/          concurrency and ingest/type-fidelity benchmarks
```

The mock Electric server (`harness/server.js`) implements the shape
protocol: snapshot pages, live long-poll, `up-to-date`, `must-refetch`,
handle rotation, and a shared cross-table LSN for alignment testing.
Drive it with POSTs to `/shape/upload/{insert,update,seed}` and
`/shape/pair`.

## Status and caveats

- Extracted from a production music-streaming platform, where it serves
  the admin and creator tooling (it replaced a PGlite-based sync layer).
- `baton` mode is verified on Chromium via `forceConnMode`; a validation
  pass on real WebKit (handle caps, acquisition latency) is still open.
- `seqlock` read mode is experimental (see Read consistency).
- Cross-shape LSN alignment is opt-in and bounded by `maxSkewMs`; without
  it, brief cross-table skew between shapes is possible.

## License

MIT
