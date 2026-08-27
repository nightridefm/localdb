// React-mount smoke for useLiveQuery: proves the hook layer end to end.
//   - components re-render ONLY when their query's result content changed
//   - delta-mode lists keep referential identity for unchanged rows across
//     updates (React.memo rows reconcile for free)
//   - shared subscriptions: two components on the same (sql, params) share
//     one worker execution
// Driven against the mock shape server; window.__react exposes counters
// and row-identity samples for the headless driver.

import { StrictMode, memo, useRef } from 'react';
import { createRoot } from 'react-dom/client';

import { useLiveQuery } from '../src/client/react.ts';
import { LocalDb } from '../src/index.ts';

type UploadRow = { id: string; title: string; plays: number };

const state = {
  renders: {
    count: 0,
    deltaList: 0,
    fullList: 0,
    constant: 0,
    twin: 0,
    rowComponent: 0,
    rowFull: 0,
  },
  rowsHistory: [] as UploadRow[][],
  events: [] as unknown[][],
};
(globalThis as unknown as { __react: typeof state; __db: LocalDb }).__react = state;

const db = new LocalDb(
  {
    dbName: 'react-smoke.db',
    schemaSql: `CREATE TABLE IF NOT EXISTS upload (id TEXT PRIMARY KEY, title TEXT NOT NULL, plays INTEGER NOT NULL);`,
    schemaVersion: 1,
    shapes: [{ table: 'upload', primaryKey: ['id'], url: '/shape/upload' }],
    readers: 1,
  },
  {
    writer: new Worker('/smoke/dist/writer.worker.js', { type: 'module' }),
    spawnReader: () => new Worker('/smoke/dist/reader.worker.js', { type: 'module' }),
  }
);
(globalThis as unknown as { __db: LocalDb }).__db = db;

// Memoized row: renders only when its row PROP identity changes - the
// direct beneficiary of delta-mode identity stability.
const Row = memo(function Row({ row }: { row: UploadRow }) {
  state.renders.rowComponent++;
  return (
    <li>
      {row.title}: {row.plays}
    </li>
  );
});

function DeltaList() {
  const { rows } = useLiveQuery<UploadRow>(db, {
    sql: 'SELECT id, title, plays FROM upload ORDER BY id',
    delta: true,
    deltaKey: 'id',
  });
  state.renders.deltaList++;
  const prev = useRef<UploadRow[] | undefined>(undefined);
  if (rows && rows !== prev.current) {
    state.rowsHistory.push(rows);
    if (state.rowsHistory.length > 4) state.rowsHistory.shift();
    prev.current = rows;
  }
  return (
    <ul>
      {(rows ?? []).slice(0, 500).map((r) => (
        <Row key={r.id} row={r} />
      ))}
    </ul>
  );
}

// Same SQL as DeltaList but WITHOUT delta mode - the storm-comparison
// baseline. Full deliveries re-parse everything, so every row object is a
// fresh identity and memo() cannot save the Row renders.
const RowF = memo(function RowF({ row }: { row: UploadRow }) {
  state.renders.rowFull++;
  return (
    <li>
      {row.title}: {row.plays}
    </li>
  );
});

function FullList() {
  const { rows } = useLiveQuery<UploadRow>(db, {
    sql: 'SELECT id, title, plays FROM upload ORDER BY id',
  });
  state.renders.fullList++;
  return (
    <ul>
      {(rows ?? []).slice(0, 500).map((r) => (
        <RowF key={r.id} row={r} />
      ))}
    </ul>
  );
}

function CountView() {
  const { rows } = useLiveQuery<{ n: number }>(db, { sql: 'SELECT count(*) AS n FROM upload' });
  state.renders.count++;
  return <p>count: {rows?.[0]?.n ?? '...'}</p>;
}

// Twin of CountView - same (sql, params): must SHARE the subscription.
function CountTwin() {
  const { rows } = useLiveQuery<{ n: number }>(db, { sql: 'SELECT count(*) AS n FROM upload' });
  state.renders.twin++;
  return <p>twin: {rows?.[0]?.n ?? '...'}</p>;
}

function Constant() {
  const { rows } = useLiveQuery<{ one: number }>(db, { sql: 'SELECT 1 AS one' });
  state.renders.constant++;
  return <p>const: {rows?.[0]?.one ?? '...'}</p>;
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <CountView />
    <CountTwin />
    <Constant />
    <DeltaList />
    <FullList />
  </StrictMode>
);
