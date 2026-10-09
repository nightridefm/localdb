// "opfs-modes" VFS - the multi-connection architecture (bench results:
// near-linear read scaling to 4 workers, 0.24ms reads, 0 errors).
//
// Every worker holds its OWN FileSystemSyncAccessHandle on the shared DB
// file, opened with {mode:'readwrite-unsafe'} (the platform refuses to mix
// 'read-only' with a live writer - lock modes must match - so readers open
// readwrite-unsafe and never write). SQLite-level locking is a no-op: the
// Web Locks commit fence is the only cross-worker coordination, and it is
// sufficient because readers never overlap a write transaction.
//
// Constraints (enforced by openEngine):
//   - journal_mode=MEMORY and temp_store=MEMORY: only the main DB file is
//     ever opened, so xOpen can be a synchronous map lookup over handles
//     pre-opened asynchronously in init.
//   - The DB is a disposable sync cache; durability is not a goal.

type Sqlite3 = {
  capi: Record<string, number> & {
    sqlite3_vfs_find: (name: string) => number;
    sqlite3_file: { structInfo: { sizeof: number } } & (new (ptr?: number) => {
      $pMethods: number;
      dispose: () => void;
    });
    sqlite3_io_methods: new () => { $iVersion: number; pointer: number };
    sqlite3_vfs: new () => {
      $iVersion: number;
      $szOsFile: number;
      $mxPathname: number;
      pointer: number;
    };
  };
  wasm: {
    heap8u: () => Uint8Array;
    poke: (ptr: number, value: number | bigint, type: string) => void;
    cstrToJs: (ptr: number) => string;
    cstrncpy: (dst: number, src: number, n: number) => number;
  };
  vfs: {
    installVfs: (opt: {
      io: { struct: unknown; methods: Record<string, unknown> };
      vfs: { struct: unknown; methods: Record<string, unknown>; name: string };
    }) => void;
  };
};

export type HandleMode = 'read-only' | 'readwrite' | 'readwrite-unsafe';

type PreopenedFile = {
  /** null while released in baton mode - IO methods fail loudly on null. */
  handle: FileSystemSyncAccessHandle | null;
  readonly: boolean;
  /** Baton mode: handle is acquired/released around fence windows. */
  baton: boolean;
  fileHandle: FileSystemFileHandle;
  mode: HandleMode;
  /**
   * Baton mode: how many read windows in THIS worker currently want the
   * handle. Windows overlap - a subscribe's dep extraction, a scheduler
   * tick and a one-shot batch can all be in flight at once under the same
   * shared fence - so the handle is opened by the first and closed by the
   * last. Without the count the first window to finish closes a handle
   * the others are still reading through, and their reads fail with
   * SQLITE_IOERR.
   */
  holds: number;
  /**
   * In-flight open, so concurrent acquires await the SAME one. Without
   * it both callers pass the `handle` check before either assigns, both
   * call createSyncAccessHandle, and the second silently overwrites the
   * first - leaking a handle that nothing can ever close, which blocks
   * the owning database's writer permanently (every later write window
   * fails with NoModificationAllowedError).
   */
  opening: Promise<void> | null;
};

export const OPFS_MODES_VFS = 'opfs-modes';

// Module state: one VFS registration and one preopen map per worker.
const preopened = new Map<string, PreopenedFile>();
const openFiles = new Map<number, PreopenedFile>();
let installed = false;

async function resolveFileHandle(path: string, create: boolean): Promise<FileSystemFileHandle> {
  const root = await navigator.storage.getDirectory();
  const segs = path.replace(/^\//, '').split('/');
  let dir = root;
  for (const s of segs.slice(0, -1)) dir = await dir.getDirectoryHandle(s, { create });
  return dir.getFileHandle(segs[segs.length - 1]!, { create });
}

/**
 * Pre-open an access handle for `path` so the VFS can open it
 * synchronously. Returns true if THIS call opened it, false if it was
 * already open - the caller needs that to know what it may clean up if a
 * later step of the same open fails.
 */
export async function preopenFile(
  path: string,
  opts: { mode: HandleMode; create?: boolean; baton?: boolean }
): Promise<boolean> {
  if (preopened.has(path)) return false;
  const fh = await resolveFileHandle(path, opts.create ?? false);
  const handle = await (
    fh as FileSystemFileHandle & {
      createSyncAccessHandle: (o?: { mode?: HandleMode }) => Promise<FileSystemSyncAccessHandle>;
    }
  ).createSyncAccessHandle({ mode: opts.mode });
  preopened.set(path, {
    handle,
    readonly: opts.mode === 'read-only',
    baton: opts.baton ?? false,
    fileHandle: fh,
    mode: opts.mode,
    // The pre-open itself is not a window: openEngine's caller releases
    // once to hand the baton back, which takes the count 0 -> closed.
    holds: 0,
    opening: null,
  });
  return true;
}

/**
 * Baton mode: re-open the access handle for `path` (no-op if held or not
 * a baton file). MUST be called under the matching fence - a 'readwrite'
 * acquire while read-only holders exist (or vice versa) throws at the
 * platform layer; the shared/exclusive fence provides exactly that
 * mutual exclusion.
 */
export async function acquirePreopened(path: string): Promise<void> {
  const f = preopened.get(path);
  if (!f || !f.baton) return;
  f.holds++;
  if (f.handle) return;
  if (!f.opening) f.opening = openHandle(path, f);
  const inFlight = f.opening;
  try {
    await inFlight;
  } catch (err) {
    f.holds--;
    throw err;
  } finally {
    if (f.opening === inFlight) f.opening = null;
  }
}

async function openHandle(path: string, f: PreopenedFile): Promise<void> {
  try {
    f.handle = await (
      f.fileHandle as FileSystemFileHandle & {
        createSyncAccessHandle: (o?: { mode?: HandleMode }) => Promise<FileSystemSyncAccessHandle>;
      }
    ).createSyncAccessHandle({ mode: f.mode });
  } catch (err) {
    // The platform error names neither the file nor the mode, which makes
    // a handle conflict between two databases (an ATTACHed one, say)
    // undebuggable from a log line. Say which.
    throw new Error(
      `localdb: cannot acquire '${f.mode}' access handle for ${path}: ${String((err as Error)?.message ?? err)}`,
      { cause: err }
    );
  }
}

/**
 * Baton mode: close the access handle so holders of the OTHER mode can
 * open theirs. Call before releasing the fence. Writers flush first so
 * the bytes are visible to the next read-only holders.
 */
export function releasePreopened(path: string): void {
  const f = preopened.get(path);
  if (!f || !f.baton) return;
  if (f.holds > 0) f.holds--;
  // Only the LAST window hands the baton back.
  if (f.holds > 0 || !f.handle) return;
  try {
    if (!f.readonly) f.handle.flush();
    f.handle.close();
  } catch {
    // already closed
  }
  f.handle = null;
}

export function closePreopened(path?: string): void {
  for (const [p, f] of preopened) {
    if (path && p !== path) continue;
    try {
      f.handle?.close();
    } catch {
      // already closed
    }
    f.holds = 0;
    preopened.delete(p);
  }
}

/**
 * Flush a preopened file's access handle. REQUIRED after every commit on
 * the writer: with synchronous=OFF SQLite never calls xSync, and writes
 * through one OPFS access handle are not reliably visible to other
 * workers' handles until flush() (observed: readers ticking 100ms after a
 * commit read stale pages; later reads saw fresh data).
 */
export function flushPreopened(path: string): void {
  const f = preopened.get(path);
  if (f && !f.readonly) f.handle?.flush();
}

/** Truncate a preopened file to zero bytes (cache wipe without reopening). */
export function truncatePreopened(path: string): void {
  const f = preopened.get(path);
  if (!f || f.readonly || !f.handle) throw new Error(`cannot truncate ${path}`);
  f.handle.truncate(0);
  f.handle.flush();
}

/**
 * Register the VFS in this worker (idempotent). Call after the first
 * preopenFile so opening a DB via `new oo1.DB(path, flags, OPFS_MODES_VFS)`
 * resolves the handle synchronously.
 */
export function installOpfsModesVfs(sqlite3: Sqlite3): void {
  if (installed || sqlite3.capi.sqlite3_vfs_find(OPFS_MODES_VFS)) {
    installed = true;
    return;
  }
  const capi = sqlite3.capi;
  const wasm = sqlite3.wasm;

  const ioStruct = new capi.sqlite3_io_methods();
  ioStruct.$iVersion = 1;
  const vfsStruct = new capi.sqlite3_vfs();
  vfsStruct.$iVersion = 1;
  vfsStruct.$szOsFile = capi.sqlite3_file.structInfo.sizeof;
  vfsStruct.$mxPathname = 512;

  const ioMethods = {
    xClose: (pFile: number) => {
      openFiles.delete(pFile);
      return 0;
    },
    xRead: (pFile: number, pDest: number, n: number, offset64: bigint) => {
      const f = openFiles.get(pFile);
      if (!f || !f.handle) return capi.SQLITE_IOERR_READ as number;
      try {
        const dst = wasm.heap8u().subarray(Number(pDest), Number(pDest) + n);
        const nRead = f.handle.read(dst, { at: Number(offset64) });
        if (nRead < n) {
          wasm.heap8u().fill(0, Number(pDest) + nRead, Number(pDest) + n);
          return capi.SQLITE_IOERR_SHORT_READ as number;
        }
        return 0;
      } catch {
        return capi.SQLITE_IOERR_READ as number;
      }
    },
    xWrite: (pFile: number, pSrc: number, n: number, offset64: bigint) => {
      const f = openFiles.get(pFile);
      if (!f || f.readonly || !f.handle) return capi.SQLITE_READONLY as number;
      try {
        const src = wasm.heap8u().subarray(Number(pSrc), Number(pSrc) + n);
        return f.handle.write(src, { at: Number(offset64) }) === n
          ? 0
          : (capi.SQLITE_IOERR_WRITE as number);
      } catch {
        return capi.SQLITE_IOERR_WRITE as number;
      }
    },
    xTruncate: (pFile: number, sz64: bigint) => {
      const f = openFiles.get(pFile);
      if (!f || f.readonly || !f.handle) return capi.SQLITE_READONLY as number;
      f.handle.truncate(Number(sz64));
      return 0;
    },
    xSync: (pFile: number) => {
      const f = openFiles.get(pFile);
      if (f && !f.readonly) f.handle?.flush();
      return 0;
    },
    xFileSize: (pFile: number, pSz64: number) => {
      const f = openFiles.get(pFile);
      if (!f || !f.handle) return capi.SQLITE_IOERR_FSTAT as number;
      wasm.poke(pSz64, BigInt(f.handle.getSize()), 'i64');
      return 0;
    },
    xLock: () => 0,
    xUnlock: () => 0,
    xCheckReservedLock: (_pFile: number, pOut: number) => {
      wasm.poke(pOut, 0, 'i32');
      return 0;
    },
    xFileControl: () => capi.SQLITE_NOTFOUND as number,
    xSectorSize: () => 4096,
    xDeviceCharacteristics: () => capi.SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN as number,
  };

  const vfsMethods = {
    xOpen: (_pVfs: number, zName: number, pFile: number, flags: number, pOutFlags: number) => {
      const name = zName ? wasm.cstrToJs(zName) : null;
      const f = name ? preopened.get(name) : null;
      if (!f) return capi.SQLITE_CANTOPEN as number;
      const sq3File = new capi.sqlite3_file(pFile);
      sq3File.$pMethods = ioStruct.pointer;
      sq3File.dispose();
      openFiles.set(pFile, f);
      wasm.poke(pOutFlags, flags, 'i32');
      return 0;
    },
    xAccess: (_pVfs: number, zName: number, _flags: number, pOut: number) => {
      const name = zName ? wasm.cstrToJs(zName) : '';
      wasm.poke(pOut, preopened.has(name) ? 1 : 0, 'i32');
      return 0;
    },
    xFullPathname: (_pVfs: number, zName: number, nOut: number, pOut: number) => {
      const n = wasm.cstrncpy(pOut, zName, nOut);
      return n < nOut ? 0 : (capi.SQLITE_CANTOPEN as number);
    },
    xDelete: () => 0,
    xRandomness: (_pVfs: number, n: number, pOut: number) => {
      const heap = wasm.heap8u();
      for (let i = 0; i < n; i++) heap[pOut + i] = (Math.random() * 256) | 0;
      return n;
    },
    xSleep: () => 0,
    xCurrentTime: (_pVfs: number, pOut: number) => {
      wasm.poke(pOut, 2440587.5 + Date.now() / 86400000, 'double');
      return 0;
    },
    xGetLastError: () => 0,
  };

  sqlite3.vfs.installVfs({
    io: { struct: ioStruct, methods: ioMethods },
    vfs: { struct: vfsStruct, methods: vfsMethods, name: OPFS_MODES_VFS },
  });
  installed = true;
}

/** Is OPFS present at all? False in Safari Private Browsing and pre-OPFS browsers. */
export async function opfsAvailable(): Promise<boolean> {
  try {
    if (typeof navigator === 'undefined' || !navigator.storage?.getDirectory) return false;
    await navigator.storage.getDirectory();
    return true;
  } catch {
    return false;
  }
}

/**
 * PRIVATE, per-probe file name. Probes sharing a fixed name race when
 * several tabs boot at once: one worker's removeEntry can delete the file
 * while another still holds a handle (or throw NotFoundError after the
 * first cleanup), the outer catch swallows the exception, and the probe
 * reports "capability not supported". Concurrent tabs then resolve to
 * DIFFERENT connection modes on the same browser and file, and a tab that
 * falls to 'single' hard-fails with `storage-failed` because SAHPool
 * handles are exclusive. A random per-probe name removes the shared
 * resource entirely.
 */
function probeName(kind: string): string {
  const rand =
    typeof crypto !== 'undefined' && crypto.randomUUID
      ? crypto.randomUUID()
      : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return `__ldb_probe_${kind}_${rand}`;
}

/** Best-effort cleanup: a failure here must never change a probe verdict. */
async function removeProbe(root: FileSystemDirectoryHandle, name: string): Promise<void> {
  try {
    await root.removeEntry(name);
  } catch {
    // Another context may have raced us, or a handle is still settling.
    // The file is a few bytes in OPFS and is re-created per probe.
  }
}

let readOnlyModeSupported: boolean | null = null;
/**
 * Baton-tier probe: can two 'read-only' handles coexist on one file?
 * Safari 16.4+ and Firefox support this without readwrite-unsafe - it
 * enables the read-only-readers + fenced-readwrite-writer baton mode.
 */
export async function detectReadOnlyMode(): Promise<boolean> {
  if (readOnlyModeSupported !== null) return readOnlyModeSupported;
  const root = await navigator.storage.getDirectory();
  const name = probeName('ro');
  try {
    const fh = await root.getFileHandle(name, { create: true });
    const open = () =>
      (
        fh as FileSystemFileHandle & {
          createSyncAccessHandle: (o?: {
            mode?: HandleMode;
          }) => Promise<FileSystemSyncAccessHandle>;
        }
      ).createSyncAccessHandle({ mode: 'read-only' });
    const a = await open();
    let ok = false;
    try {
      const b = await open();
      b.close();
      ok = true;
    } catch {
      ok = false;
    }
    a.close();
    readOnlyModeSupported = ok;
  } catch {
    readOnlyModeSupported = false;
  }
  await removeProbe(root, name);
  return readOnlyModeSupported;
}

let handleModesSupported: boolean | null = null;
/**
 * Does THE mode the multi tier actually uses work here? Probes by opening
 * two 'readwrite-unsafe' handles on a scratch file - that is exactly what
 * multi mode needs (writer + readers all hold readwrite-unsafe; lock modes
 * on a file must match, so nothing weaker proves the capability).
 * Chromium 121+ passes. Cached per worker.
 *
 * Deliberately NOT probed with 'read-only': Safari supports read-only's
 * shared semantics WITHOUT supporting readwrite-unsafe, so a read-only
 * probe would pass and then multi-mode boot would throw on the first real
 * handle (WebIDL rejects the unknown enum value) - a silent-hang
 * misdetection instead of a clean fall to 'single'.
 */
export async function detectHandleModes(): Promise<boolean> {
  if (handleModesSupported !== null) return handleModesSupported;
  // Per-probe file name + cleanup outside the verdict - see probeName().
  const root = await navigator.storage.getDirectory();
  const name = probeName('mode');
  try {
    const fh = await root.getFileHandle(name, { create: true });
    const open = (m: HandleMode) =>
      (
        fh as FileSystemFileHandle & {
          createSyncAccessHandle: (o?: {
            mode?: HandleMode;
          }) => Promise<FileSystemSyncAccessHandle>;
        }
      ).createSyncAccessHandle({ mode: m });
    const a = await open('readwrite-unsafe');
    let ok = false;
    try {
      const b = await open('readwrite-unsafe');
      b.close();
      ok = true;
    } catch {
      ok = false;
    }
    a.close();
    handleModesSupported = ok;
  } catch {
    handleModesSupported = false;
  }
  await removeProbe(root, name);
  return handleModesSupported;
}
