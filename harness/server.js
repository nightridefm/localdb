import fs from 'node:fs';
import http from 'node:http';
import http2 from 'node:http2';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Serve the whole package dir so ../node_modules resolves for worker imports.
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const MIME = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.mjs': 'text/javascript',
  '.wasm': 'application/wasm',
  '.data': 'application/octet-stream',
  '.json': 'application/json',
  '.map': 'application/json',
  '.tar.gz': 'application/gzip',
};

// ── Mock Electric shape endpoint (smoke tests) ─────────────────────────────
// Two tables (upload, link) sharing ONE global LSN counter, mirroring how
// Electric stamps a global replication position across shapes:
//   GET  /shape/{upload|link}?offset=&handle=&live=[&_delay=ms]
//        messages carry headers.lsn; up-to-date carries global_last_seen_lsn.
//        _delay postpones 200 responses (simulates a slow shape stream).
//   POST /shape/upload/insert|update      - single-table ops (test driver)
//   POST /shape/upload/seed?n=N           - bulk rows (delta benching)
//   POST /shape/pair                      - upload row + link row at the
//        SAME lsn (a cross-table transaction, for alignment testing)
// Offsets are per-table message-log indexes; handle is static ('h1').
let glsn = 0;
let shapeRowSeq = 0;
let currentHandle = 'h1';
let handleSeq = 1;
const shapeLogs = { upload: [], link: [] }; // {op, row, lsn}
const shapeWaiters = { upload: [], link: [] };

function wake(table) {
  for (const w of shapeWaiters[table].splice(0)) w();
}
function shapeInsert() {
  glsn++;
  shapeRowSeq++;
  const row = {
    id: `row-${String(shapeRowSeq).padStart(6, '0')}`,
    title: `title ${shapeRowSeq}`,
    plays: (shapeRowSeq * 7) % 1000,
  };
  shapeLogs.upload.push({ op: 'insert', row, lsn: glsn });
  wake('upload');
  return row;
}
function shapeUpdate() {
  if (!shapeLogs.upload.length) return null;
  glsn++;
  const src = shapeLogs.upload[Math.floor(shapeLogs.upload.length / 2)].row;
  const row = { ...src, plays: src.plays + 1000 };
  shapeLogs.upload.push({ op: 'update', row, lsn: glsn });
  wake('upload');
  return row;
}
function shapePair() {
  // One "transaction": an upload row and a link row at the SAME lsn.
  glsn++;
  shapeRowSeq++;
  const up = {
    id: `pair-${String(shapeRowSeq).padStart(6, '0')}`,
    title: `pair ${shapeRowSeq}`,
    plays: 1,
  };
  const link = {
    id: `link-${String(shapeRowSeq).padStart(6, '0')}`,
    upload_id: up.id,
    position: shapeRowSeq,
  };
  shapeLogs.upload.push({ op: 'insert', row: up, lsn: glsn });
  shapeLogs.link.push({ op: 'insert', row: link, lsn: glsn });
  wake('upload');
  wake('link');
  return { up, link, lsn: glsn };
}
for (let i = 0; i < 50; i++) shapeInsert(); // initial dataset

function shapeMessages(table, from) {
  return shapeLogs[table].slice(from).map((e) => ({
    key: e.row.id,
    value: e.row,
    headers: { operation: e.op, lsn: String(e.lsn), last: true },
  }));
}
function serveShape(req, res, url) {
  if (req.method === 'POST') {
    if (url.pathname === '/shape/rotate') {
      // Invalidate the current handle: subsequent polls with the old one
      // get 409 (must-refetch), like an Electric shape rotation.
      currentHandle = 'h' + ++handleSeq;
      for (const t of Object.keys(shapeWaiters)) wake(t);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: true, handle: currentHandle }));
    }
    if (url.pathname === '/shape/pair') {
      const r = shapePair();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: true, ...r }));
    }
    if (url.pathname.endsWith('/seed')) {
      const n = Math.min(100000, Number(url.searchParams.get('n') || 1000));
      for (let i = 0; i < n; i++) shapeInsert();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: true, rows: n, log: shapeLogs.upload.length }));
    }
    const row = url.pathname.endsWith('/update') ? shapeUpdate() : shapeInsert();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, row, log: shapeLogs.upload.length }));
  }
  const table = url.pathname.split('/')[2];
  if (!shapeLogs[table]) {
    res.writeHead(404);
    return res.end('unknown shape table');
  }
  const clientHandle = url.searchParams.get('handle');
  if (clientHandle && clientHandle !== currentHandle) {
    res.writeHead(409, { 'electric-handle': currentHandle, 'Content-Type': 'application/json' });
    return res.end(JSON.stringify([{ headers: { control: 'must-refetch' } }]));
  }
  const offset =
    url.searchParams.get('offset') === '-1' ? 0 : Number(url.searchParams.get('offset') || 0);
  const live = url.searchParams.get('live') === 'true';
  const delay = Number(url.searchParams.get('_delay') || 0);
  const respond = () => {
    const send = () => {
      const log = shapeLogs[table];
      const msgs = shapeMessages(table, offset);
      msgs.push({ headers: { control: 'up-to-date', global_last_seen_lsn: String(glsn) } });
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'electric-handle': currentHandle,
        'electric-offset': String(log.length),
      });
      res.end(JSON.stringify(msgs));
    };
    if (delay > 0) setTimeout(send, delay);
    else send();
  };
  if (live && offset >= shapeLogs[table].length) {
    const timer = setTimeout(() => {
      const i = shapeWaiters[table].indexOf(onWake);
      if (i >= 0) shapeWaiters[table].splice(i, 1);
      res.writeHead(204, {
        'electric-handle': currentHandle,
        'electric-offset': String(shapeLogs[table].length),
      });
      res.end();
    }, 15000);
    const onWake = () => {
      clearTimeout(timer);
      respond();
    };
    shapeWaiters[table].push(onWake);
    return;
  }
  respond();
}

function handler(req, res) {
  const url = new URL(req.url, 'http://x');
  if (url.pathname.startsWith('/shape/')) return serveShape(req, res, url);
  const urlPath = decodeURIComponent(url.pathname);
  let file = path.join(ROOT, urlPath === '/' ? 'smoke/index.html' : urlPath);
  if (urlPath.endsWith('/') && urlPath !== '/') file = path.join(file, 'index.html');
  if (!file.startsWith(ROOT)) {
    res.writeHead(403);
    return res.end();
  }
  fs.readFile(file, (err, buf) => {
    if (err) {
      res.writeHead(404);
      return res.end('not found: ' + urlPath);
    }
    const ext = file.endsWith('.tar.gz') ? '.tar.gz' : path.extname(file);
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cache-Control': 'no-cache',
    });
    res.end(buf);
  });
}

const PORT = Number(process.env.PORT || 8077);
http
  .createServer(handler)
  .listen(PORT, '127.0.0.1', () => console.log(`harness server on http://127.0.0.1:${PORT}`));

// ── HTTP/2 (TLS) on 8443, optional ─────────────────────────────────────────
// Over HTTP/1.1 a browser allows only ~6 connections per origin, so N idle
// shape long-polls starve every other request from the page. HTTP/2
// multiplexes them onto one connection, which matches how Electric shapes
// are served in production.
//
// Chrome does NOT implement h2c (cleartext HTTP/2) and has no flag for it,
// so TLS is mandatory - hence the self-signed cert. Launch Chrome with
// --ignore-certificate-errors to use this endpoint. Generate the cert with:
//   openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
//     -keyout harness/certs/key.pem -out harness/certs/cert.pem \
//     -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
const CERT_DIR = path.join(ROOT, 'harness/certs');
try {
  const key = fs.readFileSync(path.join(CERT_DIR, 'key.pem'));
  const cert = fs.readFileSync(path.join(CERT_DIR, 'cert.pem'));
  http2
    // allowHTTP1 so plain-HTTP/1.1 clients (curl, node fetch) still work
    // against the same port and the two paths stay comparable.
    .createSecureServer({ key, cert, allowHTTP1: true }, handler)
    .listen(8443, '127.0.0.1', () => console.log('harness server (h2) on https://127.0.0.1:8443'));
} catch {
  console.log('harness server: no harness/certs - HTTP/2 endpoint disabled');
}
