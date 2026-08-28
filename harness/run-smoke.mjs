#!/usr/bin/env node
// Headless end-to-end smoke runner. For each connection tier it starts a
// FRESH mock-Electric harness server, loads /smoke/?mode=<tier> in
// headless Chrome (fresh profile = fresh OPFS), and asserts the reactive
// lifecycle recorded in window.__events:
//   - the forced tier actually boots ('mode' event) and sync reaches live
//   - initial hydration lands (count=50, top5 has 5 rows) and the
//     one-shot loader path answers
//   - a POSTed insert flows through to a new count delivery
//   - a POSTed update reorders top5 (reactive re-run + delta of content)
//   - broken SQL delivers an error result (not an eternal spinner)
//   - pauseSync stops deliveries; resumeSync catches up from the cursor
//   - the constant query is delivered exactly once (hash dedupe)
//
// Then a SECOND page per tier (smoke/attach.html) covers cross-database
// ATTACH: a project db joined to a tenant db, asserting that a commit on
// the ATTACHED file refreshes a live query - and that 'single' mode fails
// those queries closed while leaving single-db queries working.
//
// Usage: node harness/run-smoke.mjs [multi baton single]   (default: all)
// Env:   CHROME_BIN (browser binary), SMOKE_PORT, SMOKE_CDP_PORT
//
// Requires `pnpm build:smoke` first (CI runs it as a separate step).

import { execFileSync, spawn } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PORT = Number(process.env.SMOKE_PORT || 8077);
const CDP = Number(process.env.SMOKE_CDP_PORT || 9223);
const MODES = process.argv.slice(2).length ? process.argv.slice(2) : ['multi', 'baton', 'single'];
const BOOT_TIMEOUT_MS = 90_000; // cold wasm fetch/compile on CI runners
const STEP_TIMEOUT_MS = 15_000;

if (!existsSync(path.join(ROOT, 'smoke/dist/main.js'))) {
  console.error('run-smoke: smoke/dist is missing - run `pnpm build:smoke` first');
  process.exit(2);
}

function chromeBin() {
  if (process.env.CHROME_BIN) return process.env.CHROME_BIN;
  for (const c of ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser']) {
    try {
      execFileSync('sh', ['-c', `command -v ${c}`], { stdio: 'pipe' });
      return c;
    } catch {
      // try the next candidate
    }
  }
  console.error('run-smoke: no Chrome/Chromium binary found (set CHROME_BIN)');
  process.exit(2);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(fn, what, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`timeout waiting for ${what}`);
    await sleep(250);
  }
}

async function httpOk(url, opts) {
  try {
    const res = await fetch(url, opts);
    return res.ok ? res : null;
  } catch {
    return null;
  }
}

/** Minimal CDP client over the /json HTTP endpoints + one page WebSocket. */
async function openPage(url) {
  const res = await waitFor(
    () => httpOk(`http://127.0.0.1:${CDP}/json/new`, { method: 'PUT' }),
    'CDP target creation',
    15_000
  );
  const target = await res.json();
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = () => reject(new Error('CDP websocket failed'));
  });
  let id = 0;
  const pending = new Map();
  ws.onmessage = (e) => {
    const m = JSON.parse(e.data);
    if (m.id && pending.has(m.id)) {
      pending.get(m.id)(m);
      pending.delete(m.id);
    }
  };
  const send = (method, params) =>
    new Promise((resolve) => {
      const mid = ++id;
      pending.set(mid, resolve);
      ws.send(JSON.stringify({ id: mid, method, params }));
    });
  // /json/new?url= no longer navigates on current Chrome - do it via CDP.
  await send('Page.navigate', { url });
  const evaluate = async (expression) => {
    const m = await send('Runtime.evaluate', { expression, returnByValue: true });
    return m.result?.result?.value;
  };
  const close = async () => {
    ws.close();
    await httpOk(`http://127.0.0.1:${CDP}/json/close/${target.id}`);
  };
  return { evaluate, close };
}

async function runMode(mode, page) {
  const state = async () => {
    const raw = await page.evaluate(
      'JSON.stringify({err: window.__error, events: window.__events || []})'
    );
    const v = JSON.parse(raw || '{"err":null,"events":[]}');
    if (v.err) throw new Error(`page reported __error: ${v.err}`);
    const bad = v.events.find((e) => e[0] === 'oneshot-error');
    if (bad) throw new Error(`one-shot query failed: ${bad[1]}`);
    return v.events;
  };
  const eventually = (pred, what, timeoutMs = STEP_TIMEOUT_MS) =>
    waitFor(async () => pred(await state()), what, timeoutMs);

  await eventually(
    (ev) => ev.some((e) => e[0] === 'mode' && e[1] === mode),
    `connMode '${mode}' to boot`,
    BOOT_TIMEOUT_MS
  );
  await eventually(
    (ev) => ev.some((e) => e[0] === 'status' && e[1] === 'live'),
    'sync to reach live',
    BOOT_TIMEOUT_MS
  );
  await eventually(
    (ev) =>
      ev.some((e) => e[0] === 'count' && e[2] === 50) &&
      ev.some((e) => e[0] === 'top5' && e[2] === 5),
    'initial hydration (count=50, top5 len 5)'
  );
  await eventually(
    (ev) => ev.some((e) => e[0] === 'oneshot' && e[1] === 50),
    'one-shot loader query'
  );

  // Live insert -> the count subscription must re-deliver.
  await fetch(`http://127.0.0.1:${PORT}/shape/upload/insert`, { method: 'POST' });
  await eventually(
    (ev) => ev.some((e) => e[0] === 'count' && e[2] === 51),
    'count=51 after live insert'
  );

  // Live update jumps a row by +1000 plays -> new top5 head.
  await fetch(`http://127.0.0.1:${PORT}/shape/upload/update`, { method: 'POST' });
  await eventually(
    (ev) => ev.some((e) => e[0] === 'top5' && e[3] !== null && e[3] >= 1000),
    'top5 reorder after live update'
  );

  // Broken SQL surfaces as an error result on the subscription.
  await eventually(
    (ev) => ev.some((e) => e[0] === 'badquery' && e[1] === 'error'),
    'error delivery for broken SQL'
  );

  // Manual pause: the stream stops (no delivery for a new server-side
  // row), and resume catches up from the stored cursor.
  await page.evaluate('window.__db.pauseSync()');
  await eventually((ev) => ev.some((e) => e[0] === 'status' && e[1] === 'paused'), 'paused status');
  await fetch(`http://127.0.0.1:${PORT}/shape/upload/insert`, { method: 'POST' });
  await sleep(2000);
  if ((await state()).some((e) => e[0] === 'count' && e[2] === 52)) {
    throw new Error('count updated while sync was paused');
  }
  await page.evaluate('window.__db.resumeSync()');
  await eventually(
    (ev) => ev.some((e) => e[0] === 'count' && e[2] === 52),
    'count=52 after resume'
  );

  // Hash dedupe: the constant query was dirtied by every commit above but
  // its content never changed - exactly ONE delivery is allowed.
  const consts = (await state()).filter((e) => e[0] === 'const');
  if (consts.length !== 1) {
    throw new Error(`constant query delivered ${consts.length} times (want exactly 1)`);
  }
}

/**
 * Cross-database ATTACH (smoke/attach.html). project.db attaches
 * tenant.db; the mock server's two shapes are split one per file.
 */
async function runAttach(mode, page) {
  const state = async () => {
    const raw = await page.evaluate(
      'JSON.stringify({err: window.__error, events: window.__events || []})'
    );
    const v = JSON.parse(raw || '{"err":null,"events":[]}');
    if (v.err) throw new Error(`page reported __error: ${v.err}`);
    return v.events;
  };
  const eventually = (pred, what, timeoutMs = STEP_TIMEOUT_MS) =>
    waitFor(async () => pred(await state()), what, timeoutMs);
  const last = (ev, k) => [...ev].reverse().find((e) => e[0] === k);

  await eventually(
    (ev) => ev.some((e) => e[0] === 'mode' && e[1] === mode),
    `connMode '${mode}' to boot`,
    BOOT_TIMEOUT_MS
  );
  await eventually(
    (ev) => ev.some((e) => e[0] === 'tenant-status' && e[1] === 'live'),
    'tenant db sync to reach live',
    BOOT_TIMEOUT_MS
  );
  await eventually(
    (ev) => ev.some((e) => e[0] === 'project-status' && e[1] === 'live'),
    'project db sync to reach live',
    BOOT_TIMEOUT_MS
  );

  // Both tiers, attached or not: the project db's OWN queries and the
  // tenant db's own readers must be completely unaffected. This is the
  // regression guard - attaching must not disturb the ordinary path.
  await eventually((ev) => last(ev, 'tenant-own')?.[2] === 50, 'tenant db serves its own readers');
  await eventually((ev) => last(ev, 'local') !== undefined, 'project db serves its own queries');

  if (mode === 'single') {
    // SAHPool access handles are exclusive per file, so there is no way to
    // open the second db. The requirement is that this fails CLOSED with
    // the reason, rather than surfacing as "no such table".
    const crossFailed = (ev, key) => {
      const e = last(ev, key);
      return e && typeof e[3] === 'string' && e[3].includes("'single' connection mode");
    };
    await eventually((ev) => crossFailed(ev, 'xjoin'), 'cross-db join rejected in single mode');
    await eventually(
      (ev) => crossFailed(ev, 'tenant-only'),
      'attached-only query rejected in single mode'
    );
    await eventually(
      (ev) => ev.some((e) => e[0] === 'oneshot-error' && String(e[1]).includes('single')),
      'cross-db one-shot rejected in single mode'
    );
    return;
  }

  // Hydration reaches the query THROUGH the attach.
  await eventually(
    (ev) => last(ev, 'tenant-only')?.[2] === 50,
    'attached table hydrated (50 rows)'
  );
  await eventually((ev) => last(ev, 'oneshot')?.[1] === 50, 'cross-db one-shot loader query');

  // THE assertion: a commit on the ATTACHED db, on its own commit channel,
  // must re-run a query that reads only attached tables. Nothing on this
  // db's own channel can refresh it.
  await fetch(`http://127.0.0.1:${PORT}/shape/upload/insert`, { method: 'POST' });
  await eventually(
    (ev) => last(ev, 'tenant-only')?.[2] === 51,
    'attached-db commit refreshes a live query'
  );

  // Cross-FILE transactions: 5 pairs, one upload row + one link row each.
  for (let i = 0; i < 5; i++) {
    await fetch(`http://127.0.0.1:${PORT}/shape/pair`, { method: 'POST' });
  }
  await eventually((ev) => last(ev, 'xjoin')?.[2] === 5, 'cross-db join sees all 5 paired rows');
  await eventually((ev) => last(ev, 'local')?.[2] === 5, 'project-only query sees all 5 links');

  // Cross-file joins are not LSN-aligned (alignShapes spans one writer, and
  // there are two here), so the orphan probe may legitimately observe an
  // intermediate state. What it must NOT do is stay there.
  await eventually(
    (ev) => last(ev, 'orphans')?.[2] === 0,
    'cross-file join converges (no lasting orphans)'
  );
}

const bin = chromeBin();
let server = null;
let chrome = null;
let profile = null;
let failed = false;

const cleanup = () => {
  chrome?.kill('SIGKILL');
  server?.kill('SIGKILL');
  // Best-effort: chrome can still be flushing profile files as it dies,
  // and a leftover temp dir must never flip the run's exit code.
  try {
    if (profile) rmSync(profile, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  } catch {
    // leave the temp profile behind
  }
};
process.on('exit', cleanup);
process.on('SIGINT', () => process.exit(130));

try {
  profile = mkdtempSync(path.join(os.tmpdir(), 'ldb-smoke-'));
  chrome = spawn(
    bin,
    [
      '--headless=new',
      '--disable-gpu',
      '--no-sandbox',
      '--no-first-run',
      '--disable-dev-shm-usage',
      `--user-data-dir=${profile}`,
      `--remote-debugging-port=${CDP}`,
      'about:blank',
    ],
    { stdio: 'ignore' }
  );
  await waitFor(
    () => httpOk(`http://127.0.0.1:${CDP}/json/version`),
    'Chrome CDP endpoint',
    30_000
  );

  // Each scenario gets a FRESH server: the shape log resets to the 50-row
  // seed, so every tier (and every scenario) asserts against identical data.
  const SCENARIOS = [
    { name: '', path: 'smoke/', run: runMode },
    { name: 'attach', path: 'smoke/attach.html', run: runAttach },
  ];
  for (const mode of MODES) {
    for (const sc of SCENARIOS) {
      const label = sc.name ? `${mode}/${sc.name}` : mode;
      server = spawn(process.execPath, [path.join(ROOT, 'harness/server.js')], {
        env: { ...process.env, PORT: String(PORT) },
        stdio: 'ignore',
      });
      await waitFor(
        () => httpOk(`http://127.0.0.1:${PORT}/shape/upload?offset=-1`),
        'harness server',
        15_000
      );
      const page = await openPage(`http://127.0.0.1:${PORT}/${sc.path}?mode=${mode}`);
      const t0 = Date.now();
      try {
        await sc.run(mode, page);
        console.log(`SMOKE PASS [${label}] (${((Date.now() - t0) / 1000).toFixed(1)}s)`);
      } catch (err) {
        failed = true;
        console.error(`SMOKE FAIL [${label}]: ${err.message}`);
        try {
          const tail = await page.evaluate('JSON.stringify((window.__events||[]).slice(-15))');
          console.error(`  events tail: ${tail}`);
        } catch {
          // page already dead
        }
      }
      await page.close();
      server.kill('SIGKILL');
      server = null;
      await sleep(200);
    }
  }
} finally {
  cleanup();
}

process.exit(failed ? 1 : 0);
