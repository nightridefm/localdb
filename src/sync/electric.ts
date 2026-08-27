// Electric shape stream client - written against the HTTP shape protocol,
// not ported from anywhere. One ShapeStream per shape; the writer worker
// owns all of them.
//
// Protocol essentials:
//   GET {url}?offset=-1                 -> initial snapshot pages
//   GET {url}?offset=&handle=&live=true -> long-poll for changes (or SSE)
//   response headers: electric-handle, electric-offset
//   messages: {key, value, headers:{operation: insert|update|delete}}
//             {headers:{control: 'up-to-date' | 'must-refetch'}}
//
// The transport is a pluggable async generator so long-poll (below) and SSE
// can be swapped without touching consumers. Offset/handle persistence is
// the caller's job (writer stores them in the _ldb_shape meta table inside
// the SAME transaction as the data - crash-consistent by construction).

export type ShapeMessage = {
  key?: string;
  value?: Record<string, unknown>;
  headers: {
    operation?: 'insert' | 'update' | 'delete';
    control?: 'up-to-date' | 'must-refetch' | 'snapshot-end';
    /**
     * Tagged-subquery shapes (`tagged_subqueries` feature flag): change
     * messages list the tags (subquery-match reasons) currently keeping
     * the row in the shape.
     */
    tags?: string[];
    /** Move-in backfill insert - row entered the shape via a subquery match. */
    is_move_in?: boolean;
    /**
     * Headers-ONLY message (no key/value/operation): rows whose tags match
     * the patterns leave the shape. The client owns tag->row bookkeeping;
     * see sync/apply.ts.
     */
    event?: 'move-out';
    patterns?: Array<{ value: string; pos: number }>;
    /** Replication position of a change message (live mode). */
    lsn?: string;
    /** Global replication position, carried by up-to-date controls. */
    global_last_seen_lsn?: string;
  };
};

export type ShapeCursor = { offset: string; handle: string | null };

export type ShapeBatch = {
  messages: ShapeMessage[];
  cursor: ShapeCursor;
  upToDate: boolean;
  mustRefetch: boolean;
  /** global_last_seen_lsn from this batch's up-to-date control, if any. */
  upToDateLsn: string | null;
};

export type ShapeStreamOptions = {
  url: string;
  params?: Record<string, string>;
  cursor?: ShapeCursor | null;
  signal?: AbortSignal;
  /** Called between retries with the pending delay. */
  /**
   * Called before each retry sleep. `httpStatus` is set only when the failure
   * was an HTTP status (not a transport error), so callers can react to
   * auth-shaped failures specifically.
   */
  onRetry?: (error: unknown, delayMs: number, httpStatus?: number) => void;
  fetchImpl?: typeof fetch;
};

const INITIAL_CURSOR: ShapeCursor = { offset: '-1', handle: null };

/**
 * Async generator yielding ShapeBatch forever (until aborted). Handles
 * retries with capped exponential backoff internally; a yielded batch is
 * always a successfully-fetched page. `must-refetch` resets the cursor and
 * is surfaced so the applier can truncate the table.
 */
export async function* shapeStream(opts: ShapeStreamOptions): AsyncGenerator<ShapeBatch> {
  const f = opts.fetchImpl ?? fetch;
  let cursor = opts.cursor ?? INITIAL_CURSOR;
  let live = false;
  let backoff = 500;
  // electric-cursor: opaque value echoed back on live polls so CDN-level
  // request collapsing works; refreshed from every response that sends it.
  let liveCursor: string | null = null;

  while (!opts.signal?.aborted) {
    const url = new URL(opts.url, globalThis.location?.href);
    for (const [k, v] of Object.entries(opts.params ?? {})) url.searchParams.set(k, v);
    url.searchParams.set('offset', cursor.offset);
    if (cursor.handle) url.searchParams.set('handle', cursor.handle);
    if (live) {
      url.searchParams.set('live', 'true');
      if (liveCursor) url.searchParams.set('cursor', liveCursor);
    }

    let res: Response;
    try {
      res = await f(url, { signal: opts.signal });
    } catch (err) {
      if (opts.signal?.aborted) return;
      opts.onRetry?.(err, backoff);
      await sleep(backoff, opts.signal);
      backoff = Math.min(backoff * 2, 15_000);
      continue;
    }

    liveCursor = res.headers.get('electric-cursor') ?? liveCursor;
    if (res.status === 204) {
      // Live long-poll timed out with no changes - re-poll immediately.
      cursor = cursorFrom(res, cursor);
      backoff = 500;
      continue;
    }
    if (res.status === 409) {
      // Shape rotated (handle expired): server sends must-refetch.
      yield {
        messages: [],
        cursor: INITIAL_CURSOR,
        upToDate: false,
        mustRefetch: true,
        upToDateLsn: null,
      };
      cursor = cursorFrom(res, INITIAL_CURSOR);
      live = false;
      continue;
    }
    if (!res.ok) {
      // Permanent request errors (bad shape definition, auth) must not
      // hot-loop: jump straight to the max backoff instead of 500ms.
      if ([400, 401, 403, 404, 422].includes(res.status)) backoff = 15_000;
      opts.onRetry?.(new Error(`shape fetch ${res.status}`), backoff, res.status);
      await sleep(backoff, opts.signal);
      backoff = Math.min(backoff * 2, 15_000);
      continue;
    }
    backoff = 500;

    const messages = (await res.json()) as ShapeMessage[];
    cursor = cursorFrom(res, cursor);
    let upToDate = false;
    let mustRefetch = false;
    let upToDateLsn: string | null = null;
    for (const m of messages) {
      if (m.headers.control === 'up-to-date') {
        upToDate = true;
        upToDateLsn = m.headers.global_last_seen_lsn ?? null;
      } else if (m.headers.control === 'must-refetch') mustRefetch = true;
    }
    if (mustRefetch) {
      yield {
        messages: [],
        cursor: INITIAL_CURSOR,
        upToDate: false,
        mustRefetch: true,
        upToDateLsn: null,
      };
      cursor = INITIAL_CURSOR;
      live = false;
      continue;
    }
    yield { messages, cursor, upToDate, mustRefetch: false, upToDateLsn };
    if (upToDate) live = true;
  }
}

function cursorFrom(res: Response, prev: ShapeCursor): ShapeCursor {
  return {
    offset: res.headers.get('electric-offset') ?? prev.offset,
    handle: res.headers.get('electric-handle') ?? prev.handle,
  };
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(t);
        resolve();
      },
      { once: true }
    );
  });
}
