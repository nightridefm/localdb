// Concurrency-bench bundles. These import the real src/ engine, fence,
// and ReadService - but NOT the writer worker; see the scope note at the
// top of concurrency-writer.ts before quoting a number here as a
// write-path result.
import { bundle } from '../harness/build.js';

await bundle([
  { in: 'bench/concurrency-writer.ts', out: 'bench/dist/writer.js' },
  { in: 'bench/concurrency-reader.ts', out: 'bench/dist/reader.js' },
]);
console.log('bench bundles built');
