// Reader worker entry ('multi' and 'baton' modes). Own connection via the
// opfs-modes VFS, own ReadService; listens to the writer's commit channel
// directly. In baton mode the OPFS handle is only held inside read-fence
// windows (ReadService manages that); the DB open itself needs one, so
// boot happens under the shared fence.

import type { FromReader, ToReader } from '../protocol.ts';
import type { CommitNotice } from '../types.ts';

import { commitChannel, fenceName } from '../protocol.ts';
import { openEngine, warmModule } from './engine.ts';
import { withReadFences } from './fence.ts';
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
/** One per attached db - module-scoped for the same GC reason as `commits`. */
let attachedCommits: BroadcastChannel[] = [];

onmessage = async (ev: MessageEvent<ToReader>) => {
  const m = ev.data;
  try {
    if (m.t === 'r-init') {
      const connMode = m.connMode ?? 'multi';
      // Attached dbs (LocalDbConfig.attach) reach only the "wide" pool
      // members; every other reader still opens exactly one file.
      const attach = m.attach ?? [];
      // Fence against every writer whose data a query here can touch, not
      // just this db's. Sorted to match ReadService's acquisition order.
      const fences = [m.dbName, ...attach.map((a) => a.dbName)].map(fenceName).sort();
      const open = () =>
        openEngine({ dbName: m.dbName, role: 'reader', connMode, models: m.models, attach });
      let engine;
      if (connMode === 'baton') {
        // Every file's handle is opened by the ATTACH, so the boot has to
        // hold every fence - a 'read-only' acquire while that db's writer
        // holds its exclusive readwrite handle throws at the platform layer.
        engine = await withReadFences(fences, async () => {
          const e = await open();
          e.release();
          return e;
        });
      } else {
        // Under the fence too: the open reads the header and schema pages,
        // and a commit landing mid-read tears them. In fence mode nothing
        // reopens a poisoned pager, so every later query on this reader
        // would fail SQLITE_CORRUPT (seen when the first sync frame lands
        // while the readers boot).
        engine = await withReadFences(fences, open);
      }
      svc = new ReadService(engine, fences, post, {
        readMode: m.readMode,
        advise: m.advise,
        maxUpdateHz: m.maxUpdateHz,
      });
      // r-init is idempotent: close any channels from a previous init so a
      // re-init cannot leave two handlers feeding one ReadService.
      commits?.close();
      for (const bc of attachedCommits) bc.close();
      commits = new BroadcastChannel(commitChannel(m.dbName));
      commits.onmessage = (e: MessageEvent<CommitNotice>) => svc!.onCommit(e.data);
      // An attached db commits on its OWN channel; without this a
      // tenant-only change would leave a joined query stale until some
      // main-db commit happened to re-run it.
      attachedCommits = attach.map((a) => {
        const bc = new BroadcastChannel(commitChannel(a.dbName));
        bc.onmessage = (e: MessageEvent<CommitNotice>) => svc!.onCommit(e.data, a.alias);
        return bc;
      });
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
