// Reader worker entry ('multi' and 'baton' modes). Own connection via the
// opfs-modes VFS, own ReadService; listens to the writer's commit channel
// directly. In baton mode the OPFS handle is only held inside read-fence
// windows (ReadService manages that); the DB open itself needs one, so
// boot happens under the shared fence.

import type { FromReader, ToReader } from '../protocol.ts';
import type { CommitNotice } from '../types.ts';

import { commitChannel, fenceName } from '../protocol.ts';
import { openEngine, warmModule } from './engine.ts';
import { withReadFence } from './fence.ts';
import { ReadService } from './read-service.ts';

// The client spawns this worker BEFORE the writer has resolved the
// connection mode, so start the sqlite-wasm fetch/compile immediately:
// it overlaps the writer's OPFS probe + engine open + schema init instead
// of serializing after r-init.
warmModule();

const post: (m: FromReader, transfer?: Transferable[]) => void = (m, transfer) =>
  (postMessage as (m: unknown, t?: Transferable[]) => void)(m, transfer);

let svc: ReadService | null = null;
// Module-scoped on purpose: a BroadcastChannel referenced only from a
// handler's local scope is eligible for GC, after which commit notices
// silently stop arriving (observed in Chrome during the smoke test).
let commits: BroadcastChannel | null = null;

onmessage = async (ev: MessageEvent<ToReader>) => {
  const m = ev.data;
  try {
    if (m.t === 'r-init') {
      const connMode = m.connMode ?? 'multi';
      const fence = fenceName(m.dbName);
      let engine;
      if (connMode === 'baton') {
        engine = await withReadFence(fence, async () => {
          const e = await openEngine({
            dbName: m.dbName,
            role: 'reader',
            connMode,
            models: m.models,
          });
          e.release();
          return e;
        });
      } else {
        engine = await openEngine({ dbName: m.dbName, role: 'reader', connMode, models: m.models });
      }
      svc = new ReadService(engine, fence, post, {
        readMode: m.readMode,
        advise: m.advise,
        maxUpdateHz: m.maxUpdateHz,
      });
      commits = new BroadcastChannel(commitChannel(m.dbName));
      commits.onmessage = (e: MessageEvent<CommitNotice>) => svc!.onCommit(e.data);
      post({ t: 'r-ready' });
    } else if (m.t === 'subscribe') {
      await svc!.subscribe(m.key, m.spec);
    } else if (m.t === 'visibility') {
      svc?.setHidden(m.hidden);
    } else if (m.t === 'unsubscribe') {
      svc!.unsubscribe(m.key);
    } else if (m.t === 'query') {
      await svc!.oneShot(m.queries);
    } else if (m.t === 'analyze') {
      await svc!.analyze(m.id, m.queries);
    }
  } catch (err) {
    if (m.t === 'query') {
      post({
        t: 'r-query-results',
        results: m.queries.map((q) => ({ id: q.id, error: String(err) })),
      });
    } else post({ t: 'r-error', error: String(err) });
  }
};
