/**
 * Minimal interface for DO SQLite storage.
 * Matches DurableObjectStorage.sql from @cloudflare/workers-types.
 */
export interface SqlStorage {
  exec<T extends Record<string, SqlStorageValue>>(
    query: string,
    ...bindings: unknown[]
  ): SqlStorageCursor<T>;
}

export interface SqlStorageCursor<T> {
  toArray(): T[];
}

export type SqlStorageValue =
  | ArrayBuffer
  | string
  | number
  | null;

export interface Storage {
  sql: SqlStorage;
  transactionSync: (fn: () => void) => void;
}

export const INIT_SQL = `CREATE TABLE IF NOT EXISTS jfs_kv (
  k BLOB PRIMARY KEY,
  v BLOB NOT NULL,
  ver INTEGER NOT NULL DEFAULT 1
) WITHOUT ROWID`;

/** Create the jfs_kv table if it does not already exist. */
export function ensureTable(storage: Storage): void {
  storage.sql.exec(INIT_SQL);
}
