// React binding. useSyncExternalStore over LocalDb.subscribe - stable
// snapshots, no tearing, shared subscriptions for identical (sql, params).

import { useCallback, useMemo, useSyncExternalStore } from 'react';

import type { QueryResult, QuerySpec } from '../types.ts';
import type { LocalDb } from './LocalDb.ts';

import { queryKey } from '../protocol.ts';

export type LiveQueryState<Row> = {
  rows: Row[] | undefined;
  isLoading: boolean;
  version: number;
  /**
   * Set when the query's last run failed (see QueryResult.error). `rows`
   * keep the last good result; clears on the next successful delivery.
   */
  error?: string;
};

const LOADING: LiveQueryState<never> = { rows: undefined, isLoading: true, version: 0 };

/**
 * Reactive raw-SQL hook.
 *
 *   const { rows, isLoading } = useLiveQuery<TrackRow>(db, {
 *     sql: 'SELECT * FROM track WHERE artist_id = ? ORDER BY created_at DESC',
 *     params: [artistId],
 *   })
 *
 * Results update at most at the scheduler tick rate, only when content
 * actually changed, and N components sharing the same (sql, params) share
 * one worker-side execution.
 */
export function useLiveQuery<Row = unknown>(db: LocalDb, spec: QuerySpec): LiveQueryState<Row> {
  // Key the memo on content, not object identity - callers inline specs.
  // queryKey covers everything that splits a worker-side subscription
  // (sql, params, delta mode), so toggling those re-subscribes.
  const specKey = queryKey(spec.sql, spec.params, spec);
  // oxlint-disable-next-line react-hooks/exhaustive-deps -- content-keyed on purpose
  const stableSpec = useMemo(() => spec, [specKey]);

  // Cache the mapped state per snapshot so getSnapshot is referentially
  // stable between updates (useSyncExternalStore contract).
  // oxlint-disable react-hooks/exhaustive-deps -- reset cache when the query content changes
  const cache = useMemo(
    () => ({ last: null as QueryResult<Row> | null, mapped: LOADING as LiveQueryState<Row> }),
    [specKey]
  );
  // oxlint-enable react-hooks/exhaustive-deps

  const subscribe = useCallback(
    (onChange: () => void) => db.subscribe<Row>(stableSpec, onChange),
    [db, stableSpec]
  );
  const getSnapshot = useCallback((): LiveQueryState<Row> => {
    // By the render's key: the same key subscribe() derives from
    // stableSpec, without re-hashing the SQL on every call.
    const snap = db.peekKey<Row>(specKey);
    if (snap === cache.last) return cache.mapped;
    cache.last = snap;
    cache.mapped = snap
      ? {
          rows: snap.rows,
          isLoading: false,
          version: snap.version,
          ...(snap.error !== undefined ? { error: snap.error } : {}),
        }
      : (LOADING as LiveQueryState<Row>);
    return cache.mapped;
  }, [db, specKey, cache]);

  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}
