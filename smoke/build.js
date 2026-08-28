// Smoke harness bundles: one page per scripted scenario + both real
// worker entries.
import { bundle, WORKER_ENTRIES } from '../harness/build.js';

await bundle([
  { in: 'smoke/smoke-app.ts', out: 'smoke/dist/main.js' },
  { in: 'smoke/patterns-app.ts', out: 'smoke/dist/patterns.js' },
  { in: 'smoke/align-app.ts', out: 'smoke/dist/align.js' },
  { in: 'smoke/attach-app.ts', out: 'smoke/dist/attach.js' },
  { in: 'smoke/react-smoke.tsx', out: 'smoke/dist/react.js', jsx: true },
  ...WORKER_ENTRIES('smoke/dist'),
]);
console.log('smoke bundles built');
