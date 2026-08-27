# Benchmarks

Three harnesses, each with a deliberately narrow scope. Numbers vary with
hardware; treat the shapes of the results (scaling, ratios, zero-counts)
as the signal, not the absolute values.

## Concurrency (browser)

How reads and commits interfere under load: reader latency percentiles,
aggregate qps, writer lock waits and flush cost, freshness checks, and
reactive delivery latency (commit notice to subscriber callback). Uses
the real engine, VFS, fence, and `ReadService`.

```
pnpm build:bench
pnpm harness
# open http://127.0.0.1:8077/bench/concurrency.html?readers=4&seed=100000
```

Query parameters:

- `readers=N` - reader worker count (default 2)
- `seed=N` - seeded row count (default 100,000)
- `mode=fence|seqlock` - read consistency mode (default fence)
- `payload=N` - adds a live N-row subscription per reader, so the run
  measures wire cost (hash, transfer, parse) instead of only the scheduler

Results land in `window.__bench` (JSON); progress in `window.__progress`.
`node harness/poll-cdp.js` polls a headless Chrome session over CDP and
prints the result.

Every tenth read asserts freshness: the row count must reflect every
commit noticed before the read began. `staleness.violations` must be 0 in
fence mode. The run also measures `data_version` detection lag per
reader, which is the soundness limit of seqlock validation.

**Scope note:** the bench writer inserts through its own prepared
statements. It does not exercise the sync path (ingest coercion, bulk
hydration, partial updates, tags, coalescing, cursor persistence,
deferred indexes, FTS). For the real writer worker, use `smoke/`.

## Ingest (node)

A/B for the set-based hydration path: identical wire messages applied
through the real applier, once forced down the per-row bind path and once
through the `json_each` bulk path. Asserts the two paths produce
byte-identical tables.

```
pnpm bench:ingest          # INGEST_N=500000 to change the size
```

## Type fidelity (node)

What typed ingest and delivery cost over the plain path: column-aware
normalization throughput, applier ingest with and without model metadata,
and the delivery wrapper (`embedExpr` CASE guards, `rowJson` aggregates)
against plain `json_object` embeds.

```
pnpm bench:types
```
