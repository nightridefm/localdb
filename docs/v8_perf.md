# V8 performance pass

A checklist for an agent (or a person) to run over a package now and then:
find the code that runs every frame / tick / sample / message, check it
against how V8 actually optimises JavaScript, measure, fix what the numbers
justify, and log the pass at the bottom.

This copy lives in localdb, which has no frames: its hot paths are per
commit, per row and per page (listed in section 1). The checklist and the
older log entries came over from lissen (`packages/app`, `cyberspace`, ...)
and are kept as precedent. Cold code is not in scope: everywhere outside a
hot path, readable code wins over anything here.

## Ground rules

- **Behaviour must not change.** Same output, same timing semantics. Run
  the checks that prove it before and after: `pnpm test` (node, sqlite-wasm),
  `pnpm smoke` (headless Chrome, all three connection tiers), and for a
  rewrite of a pure function, a fuzzed old-vs-new comparison in the
  scratchpad (`Object.is` on every output).
- **Evidence first.** Every fix needs either a measurement (CPU ms, allocation
  MB/s, frame time, deopt trace) or a plainly visible per-frame allocation /
  shape problem in code that is proven hot. "Looks slow" in cold code is not
  a finding.
- **Measure before and after**, same scenario, and put both numbers in the
  report. A fix that does not move a number is reverted.
- **Hot means it runs at least per frame (rAF, 60-144 Hz), per audio block,
  per sim tick, per network message, or per row/cell in a list that is
  scrolled.** Per click, per route load, per mount is not hot.
- Format only touched files. No git state changes; leave the work unstaged.
  No em-dashes.

## 1. Find the hot paths

localdb's, by rate:

- **Reader tick** (`ReadService.runTickInner`, `QueryRegistry.due` /
  `recordRun` / `recordDeltaRun`): once per commit, at most `maxUpdateHz`
  (10 Hz) per reader, times the dirty subscriptions.
- **Result wrapper SQL** (`engine.wrapJsonSql`, `execRows`): per row of
  every query run; mostly C, but the SQL shape decides how much JSON work
  SQLite does.
- **SQL UDFs** (`similarity.ts`): per row of a scan, per search keystroke.
- **VFS** (`vfs.ts` `xRead` / `xWrite`): per page not in the cache.
- **Ingest** (`sync/apply.ts` `apply`, `bulkInsert`, the normalizers): per
  message and per cell during hydration (1M rows x ~20 cells).
- **Main thread** (`LocalDb.onReaderMessage`, `useLiveQuery`'s
  `getSnapshot`): per delivered result and per render of every subscribed
  component.

Measure node-side code (UDFs, wrapper SQL, ingest) against an in-memory
sqlite-wasm db (`test/init-sqlite.ts`, bundled with esbuild, sqlite-wasm
external); `pnpm bench:ingest`, `pnpm bench:types` and the browser
concurrency bench (`bench/README.md`) cover the rest.

For lissen-style packages, start from the entry points that run repeatedly, then follow what they call:

```bash
rg -n "requestAnimationFrame|setInterval\(|registerProcessor|AudioWorkletProcessor|onaudioprocess|getByteFrequencyData|getFloatTimeDomainData|getFloat32Array|onmessage|addEventListener\('(message|pointermove|mousemove|scroll|wheel|timeupdate|progress)'" packages/<pkg>/src
rg -n "useFrame|useAnimationFrame|new ResizeObserver|IntersectionObserver" packages/<pkg>/src
rg -n "\.onmessage\s*=|EventSource|ReadableStream|getReader\(\)" packages/<pkg>/src
```

Also hot: canvas paint functions, per-row renderers of virtualised lists /
canvas grids, SSE/WebSocket message handlers, MSE append loops, Web Worker
message loops, `useSyncExternalStore` subscriptions that fire per tick,
selectors/derived signals recomputed per tick.

For React: a component re-rendering per animation frame or per audio tick
is a hot path even if the code looks declarative. Find the state that
changes at that rate (`setState` inside rAF/timers/stream handlers) and the
tree under it.

Write the list down with the rate each one runs at; that list is the scope.

## 2. The checklist

Work through each item for every hot path. Each has: what to look for, why
V8 cares, and how to confirm.

### 2.1 Function size: hot loops in huge functions are never optimised
- **Look for:** a per-frame/per-element loop inside a very large function
  (hundreds of lines). Rough cue: any function over ~300 lines that contains
  a hot loop.
- **Why:** TurboFan skips functions over `--max-optimized-bytecode-size`
  (60 KB of bytecode). They stay in the interpreter/baseline tier however hot
  they are, where every non-integer number is a heap-allocated box: each float
  op in the loop allocates.
- **Confirm:** `node --allow-natives-syntax` + `%GetOptimizationStatus(fn)`,
  or `--trace-opt` showing the function never optimised (or "function too
  big" in `--trace-opt-verbose`).
- **Fix:** move each hot loop into its own small named function with plain
  arguments. Precedent: cyberspace `frame()` -> `blendPeers` / `packSolids` /
  `slotPalette`, 5.6 -> 2.0 ms/s CPU (2026-10-08).

### 2.2 Stable object shapes (monomorphic property access)
- **Look for:** objects in hot paths created in different places with
  different property sets or orders; properties added after creation
  (`o.x = ...` on an object that did not have `x`); `delete o.x`; optional
  fields (`x?:` types) on hot objects; one function receiving several
  "kinds" of object.
- **Why:** each property access caches the object shapes (hidden classes) it
  has seen. One shape is fast, 2-4 is slower, more is megamorphic and slow,
  and it blocks inlining.
- **Confirm:** `node --log-ic --log-maps` (or Chrome `--js-flags`), then load
  the log in V8's system analyzer (https://v8.dev/tools/head/system-analyzer)
  and look for polymorphic/megamorphic ICs in the hot functions.
- **Fix:** one factory or literal per type with every field present (use
  `null`/`0` for "absent"), same order; never delete, set to null.

### 2.3 Array element kinds
- **Look for:** number arrays that start as small integers and later hold
  decimals (`const v = [0, 0, 0]` then `v[0] = 0.5`); `undefined`/`null`/`NaN`
  stored into number arrays; `new Array(n)` then filled (holey); arrays that
  mix numbers and objects; `arr.length = 0` patterns on hot scratch arrays.
- **Why:** packed SMI -> packed DOUBLE -> generic ELEMENTS, one way only;
  holey variants are slower; each transition is a copy.
- **Fix:** typed arrays (`Float64Array`, `Int32Array`) for numeric scratch and
  state; build plain arrays densely with real values; keep one element type.

### 2.4 Typed arrays and struct-of-arrays
- **Look for:** per-entity objects (`{ x, y, z, vx, ... }`) iterated every
  frame in large numbers.
- **Why:** typed arrays have no shapes, no boxing, contiguous memory.
- **Notes:** `Float32Array` reads convert to double (fine for GPU/audio
  buffers, use `Float64Array` for CPU scratch maths). Reads past the end
  return `undefined` and can deopt: keep loop bounds exact.

### 2.5 Number representation and type-stable variables
- **Look for:** a variable or parameter that is sometimes a number and
  sometimes `undefined`/string/object; integers beyond 31 bits in hot
  integer code; producing `-0` or `NaN` in integer paths.
- **Why:** V8 specialises on observed types; a new type deopts the function
  (visible in `--trace-deopt`). Small integers (Smi, 31-bit) are unboxed.
- **Fix:** initialise with the real type (`let t = 0` not `let t`), default
  early, keep ints in int range (`| 0` where bit-exact semantics are wanted).

### 2.6 Allocation in hot paths
- **Look for, inside per-frame/per-element code:** closures created per
  iteration (`arr.forEach(x => ...)` building a new function each frame is
  fine; a NEW closure per element is not), spreads (`[...a]`, `{...o}`),
  `.map`/`.filter`/`.slice`/`.concat`, `Object.entries/keys/values`,
  destructuring into new objects, template strings, `new Float32Array` per
  call, `Array.from`, iterating a `Map` (entry arrays), returning small
  objects/tuples from helpers, `Date` objects, `JSON.stringify/parse`.
- **Why:** young-gen GC is cheap per object but at 60-144 Hz it becomes
  frame-time jitter. Escape analysis only removes allocations in optimised,
  inlined code.
- **Fix:** reuse scratch (module-level typed arrays/objects), compute into
  out-parameters, cache derived values and rebuild on change (dirty flag),
  throttle what does not need per-frame freshness (text, rosters, labels).
- **Measure:** Chrome DevTools Memory > Allocation sampling, or a
  Performance trace with the memory track; MB/s per function.

### 2.7 Inlining-friendly helpers
- **Look for:** small helpers in hot loops that are called with mixed
  argument types, or that grew large.
- **Why:** TurboFan inlines callees up to a few hundred bytes of bytecode
  (with a cumulative budget). Inlined + type-stable = the allocation of a
  returned temporary can be removed. Polymorphic call sites do not inline.
- **Fix:** keep hot helpers tiny and monomorphic; split a helper used from
  two very different call sites into two.

### 2.8 Dictionary-mode objects and computed keys
- **Look for:** plain objects used as growing maps (`cache[id] = ...` with
  many ids), `obj[key]` with many different keys in hot code, `delete` on
  them.
- **Why:** they fall into slow dictionary mode; computed-key access is
  megamorphic.
- **Fix:** `Map` for dynamic keys; arrays/typed arrays indexed by slot for
  per-entity state.
- **Seen here:** `geo-map.tsx` `draw` looped `for...in` over a per-country
  weights object every frame (30 Hz). V8's Maglev tier deopted on it every
  ~1.3 s ("insufficient type feedback for generic keyed access") and `draw`
  never reached TurboFan. Building the list once per data change into an
  array: 0 deopts, TurboFan, ~15% less CPU (2026-10-09).

### 2.9 Strings
- **Look for:** building strings per frame (labels, CSS values, transforms,
  canvas text) that do not change most frames; `+=` loops.
- **Why:** concatenation builds ropes that are flattened on use; each one is
  an allocation.
- **Fix:** cache and rebuild on change; for canvas, cache measured text.

### 2.10 React-specific hot paths (packages/app)
- **Look for:** `setState`/signal writes at frame or audio rate driving
  re-renders; new object/array/function props created each render on
  components under such state (defeats `memo`); context values recreated per
  render; selectors recomputed per tick; lists not virtualised.
- **Fix:** move per-frame visuals out of React state (refs + direct canvas/DOM
  writes, or a signal read inside the canvas loop); stabilise props
  (`useMemo`/`useCallback` only where it is a measured hot path); throttle UI
  updates to what the eye needs (4-10 Hz for numbers/text).
- **Measure:** React Profiler (commits per second), Chrome Performance
  (scripting ms per frame).

### 2.11 Web Audio / workers / messaging
- **Look for:** `getByteFrequencyData` into a new array each frame (allocate
  the `Uint8Array` once); `AudioWorkletProcessor.process` allocating per block
  (128 frames, ~375 Hz at 48 kHz: any allocation there is a GC risk on the
  audio thread); `postMessage` of large objects per frame (use transferables
  or a `SharedArrayBuffer`); `ScriptProcessorNode` (deprecated, main thread).
- **Fix:** preallocate; transfer; keep worklet `process()` allocation-free.
- **DOM getters that build objects:** `audio.buffered` builds a fresh
  `TimeRanges` on every read (~6 us with two MSE ranges); `ctx.font` costs
  ~0.4 us per read. Read once per frame into a local. The scrubbers read
  `buffered` 1 + 3n times per frame (2026-10-09). In worklets, keep per-sample
  state in locals for the block and write it back once.

### 2.12 Node services
- `Buffer.allocUnsafe` + in-place writes over `Buffer.concat` / `alloc` per
  message; a read cursor over re-concatenating chunks; `setTimeout` to the
  next due step over a tight `setInterval`; for allocation-heavy services,
  measure `--max-semi-space-size=<MB>` (fewer scavenges).

## 3. Measure

Pick what fits the hot path; record the scenario so the "after" run is the
same.

- **Pure modules (node):** import the module in a small bench in the
  scratchpad, warm it, time N iterations; run with `--trace-opt
  --trace-deopt` to see what optimises and what bails out.
- **In the browser:** Chrome DevTools MCP `performance_start_trace` /
  `performance_stop_trace` around a fixed interaction (e.g. 10 s of playback
  with the visualiser open), then `performance_analyze_insight`; heap
  snapshots / allocation sampling for MB/s by function. Follow CLAUDE.md's
  Playwright / Chrome MCP startup protocol for logged-in flows.
- **Shapes/ICs:** `--log-ic --log-maps` + the V8 system analyzer.
- Report per hot path: rate, CPU ms per second (or per frame), allocation
  MB/s, deopt count; before and after.

## 4. Report

One table per pass, ranked by measured impact:

| Hot path (file:line) | Rate | Finding (checklist item) | Before | After | Status |
| -------------------- | ---- | ------------------------ | ------ | ----- | ------ |

Status: fixed / proposed (needs a decision) / not worth it (measured, small).
Then append a dated entry to the log below.

## Log

<!-- newest first: date, package, scope, headline numbers, follow-ups -->

- 2026-10-09, localdb (first pass in this repo). Fixed:
  - `engine.ts` result wrapper: `json_object` now feeds `json_group_array`
    directly instead of through a subquery column + `json(__row)`, which
    re-parsed every row. Byte-identical output; 20k rows 51.8 -> 25.1 ms,
    2k rows 28.6 -> 26.4 ms, small query 1.43 -> 1.32 ms.
  - `similarity.ts` UDFs: Set-per-word-per-row replaced by typed-array
    scratch, generation stamps and a per-query membership table (2.6).
    50k-row scan: search_similarity 198 -> 134 ms, word_similarity 277 ->
    106 ms, similarity 143 -> 93 ms; the UDF call + string decode floor is
    76 ms, so JS work is 2-6x less. Scavenges 3135 -> 1066 per bench run.
    Bit-identical over 600k fuzzed calls incl. non-ASCII case folding.
  Not worth it (measured): bulk-run signature `Object.keys().join()` in
  `apply()` (0.2 -> 0.04 us/row, ~1.5% of a ~10 us/row hydration); VFS
  per-read view/options allocation (I/O bound, 32 MB page cache).
  Fixed (measured, headless Chrome, React prod build, fake workers):
  `useLiveQuery` `getSnapshot` re-hashes the SQL and stringifies params
  via `peek()` on every call, 0.67 us each (a 400-char SQL, 2 params) vs
  0.01 us for a lookup by the render's `specKey`. React calls it 3x per
  component per data update and 1x per re-render. 1000 subscribed rows:
  update 8-10 -> 6.3-8.3 ms, parent re-render 2.6-3.4 -> 1.8-2.5 ms; 200
  rows: ~0.4 / ~0.2 ms. Now `LocalDb.peekKey` (internal); rerun against
  the shipped hook matches the prototype.

- 2026-10-09, `packages/app` follow-ups. Geo map: flat-map hex Path2D cache
  per (builtKey, w, agg) + rounded/cached alpha strings: flat 28 -> 10-13
  ms/s, 5.1 -> 0.9-1.7 MB/s; globe 22.4 -> 19.5 ms/s; 0% pixels changed
  (canvas alpha is 8-bit already). Butterchurn 2.6.7: equation-var objects
  were dictionary-mode (`Object.assign({}, ...)` of ~250 keys), so every
  float store allocated; spread + stable base shapes + reused buffers,
  bit-identical over 9 presets, heaviest preset 613 -> 380 ms/s and 82 -> 28
  MB/s. Patch written (scratchpad) but applying it via `pnpm patch-commit`
  is a shared-resource change left to the user.

- 2026-10-09, `packages/app` (first pass with this checklist; three areas in
  parallel). Fixed:
  - scrubbers (`layouts/player.tsx`, `track-scrubber.tsx`, `media-hero.tsx`,
    forensics `footer-player.tsx`): `audio.buffered` read once per frame,
    ~29 -> 4 us/frame per scrubber; `slider.tsx` buffered mask rebuilt only
    when the ranges change.
  - `bitcrusher-worklet.js`: per-sample state in locals, JS share ~36 -> 10 ms
    per 120 s rendered, output byte-identical.
  - `geo-map.tsx` `draw`: for-in deopt loop removed (38 deopts/30 s -> 0,
    reaches TurboFan), projections into a scratch Float64Array, `rgbOf`
    cached: 21.7-22.9 -> 18.6-19.3 ms/s incl. GC, 3.56 -> 2.53 MB/s.
  - `limiter-indicator.tsx`: skips unchanged frames, 0.65 -> 0.1 ms/s;
    `visualiser-frequency.tsx` radii hoisted.
  - canvas grid (`text.ts`, `paint.ts`): string-keyed cache and `ctx.font`
    reads replaced, one cell context per paint: 83 us / 93 KB -> 31.5 us /
    6.5 KB per paint (synthetic 20k-row grid; no route uses it yet).
  - `chat-messages.tsx`: rows no longer one big subgrid, forced layout per
    message 10.6 -> 1.3 ms.
  - `contexts/signals/sync.ts`: playhead copied to other tabs at most 1/s
    (was 20/s), discrete changes still 50 ms, nothing sent with no other tab
    open (presence handshake): 46 -> 0 messages per 3 s alone, 46 -> 3 with a
    peer. Other tabs' progress bars now step once a second.
  - `packages/cyberspace` beat tracker (driven from the app at 250 Hz):
    reused result objects, `setBeat` closures hoisted, identical output over
    20k steps.
  Proposed, not done: butterchurn allocates ~17 MB/s in `cloneVars` (needs a
  pnpm patch of the library); geo-map per-hex `Path2D`/`rgba()` caching
  (~1.6 MB/s left); extrapolating the playhead in follower tabs between the
  1/s updates. Not worth it (measured): modfx worklet (Math.sin/random bound),
  logo `checkVisibility` (cost only moves), MSE read loop, peaks analysis,
  now-playing SSE, live-query SQL building.

- 2026-10-08, `packages/cyberspace` (before this checklist existed): `frame()`
  over the optimisation size cap -> loops split out (5.6 -> 2.0 ms/s CPU,
  ~4x less allocation); `peerBoxes` closure/tuple churn -> pooled scratch +
  direct `instData` writes (3.96 -> 1.05 ms/s); HUD rebuilt only on change;
  ACKs only on new sequence numbers; relay snapshot build 0 allocs steady
  state; walker broadphase (41 -> 4 us/step).
