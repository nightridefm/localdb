// Web Locks commit fence. NOT a freshness mechanism - freshness is the
// scheduler's job. The fence exists so a reader never walks a half-written
// B-tree: without it, a read overlapping a commit can return silently wrong
// rows (or SQLITE_CORRUPT). Benchmarked overhead: ~0.05ms per read, sub-ms
// writer waits with 1-4 readers (bench/results).
//
// Rules encoded here:
//   - Writer: exclusive, held for the WHOLE write transaction. With
//     journal_mode=MEMORY + big page cache, file writes cluster at commit,
//     so the hold is ~batch-commit-time (bench: ~10ms per 1000 rows).
//   - Readers: shared, held per query execution. Web Locks queue fairly,
//     so a stream of reads cannot starve the writer.

export function withReadFence<T>(fence: string, fn: () => T | Promise<T>): Promise<T> {
  return navigator.locks.request(fence, { mode: 'shared' }, async () => fn());
}

export function withWriteFence<T>(fence: string, fn: () => T | Promise<T>): Promise<T> {
  return navigator.locks.request(fence, { mode: 'exclusive' }, async () => fn());
}
