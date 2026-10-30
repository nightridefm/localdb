// Pluggable sync transports. The writer worker runs Electric shape streams
// by default; a transport registered here replaces them when
// LocalDbConfig.sync names it. Registration happens in the APP's writer
// worker entry, so localdb never depends on a transport package:
//
//   import { registerSyncTransport } from '@nightride/localdb/writer.worker';
//   import { localsyncTransport } from '@nightride/localsync';
//   registerSyncTransport('localsync', localsyncTransport);
//
// The contract is deliberately small: the transport gets exclusive write
// access (inside the write fence, so readers never see a half-applied
// transaction), reports each committed transaction so localdb can
// broadcast it, and reports status.

import type { SyncStatus } from '../types.ts';

export type SqlValue = string | number | bigint | null;

/** The SQLite surface a transport writes through. */
export type TransportDb = {
  exec(sql: string, bind?: SqlValue[]): void;
  all(sql: string, bind?: SqlValue[]): SqlValue[][];
};

export type TransportContext = {
  /** Runs fn with exclusive write access (write fence + connection handle). */
  run<T>(fn: (db: TransportDb) => T): Promise<T>;
  /** A transaction the transport applied has committed. */
  committed(tables: string[], rows: number): void;
  status(s: SyncStatus): void;
  log?(message: string): void;
};

export type SyncTransport = {
  start(): void;
  stop(): void;
  /** Payload from LocalDb.syncCommand (e.g. a new subscription set). */
  command?(payload: unknown): void;
};

export type SyncTransportFactory = (ctx: TransportContext, options: unknown) => SyncTransport;

const registry = new Map<string, SyncTransportFactory>();

export function registerSyncTransport(name: string, factory: SyncTransportFactory): void {
  registry.set(name, factory);
}

export function syncTransport(name: string): SyncTransportFactory | undefined {
  return registry.get(name);
}
