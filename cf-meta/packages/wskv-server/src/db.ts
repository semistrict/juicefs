import type { Storage } from "./schema.js";

/** Result of a single key lookup. */
export interface GetResult {
  found: boolean;
  value: Uint8Array | null;
  ver: number;
}

/** A single key-value entry returned from a list query. */
export interface ListEntry {
  key: Uint8Array;
  value: Uint8Array | null;
  ver: number;
}

export interface ListOptions {
  start: Uint8Array;
  end: Uint8Array;
  keysOnly: boolean;
  limit: number;
}

/** Result of a commit operation. */
export interface CommitResult {
  ok: boolean;
  error: string;
}

export interface ObservedEntry {
  key: Uint8Array;
  ver: number;
}

export interface PutEntry {
  key: Uint8Array;
  value: Uint8Array;
}

/** Fetch a single key from storage. */
export function dbGet(storage: Storage, key: Uint8Array): GetResult {
  const rows = storage.sql
    .exec<{ v: ArrayBuffer; ver: number }>(
      "SELECT v, ver FROM jfs_kv WHERE k = ?",
      key,
    )
    .toArray();

  if (rows.length > 0) {
    return {
      found: true,
      value: new Uint8Array(rows[0].v),
      ver: rows[0].ver,
    };
  }
  return { found: false, value: null, ver: 0 };
}

/** List entries in a key range. */
export function dbList(storage: Storage, opts: ListOptions): ListEntry[] {
  let query: string;
  const bindings: unknown[] = [opts.start, opts.end];

  if (opts.keysOnly) {
    query = "SELECT k, ver FROM jfs_kv WHERE k >= ? AND k < ? ORDER BY k";
  } else {
    query = "SELECT k, v, ver FROM jfs_kv WHERE k >= ? AND k < ? ORDER BY k";
  }

  if (opts.limit > 0) {
    query += " LIMIT ?";
    bindings.push(opts.limit);
  }

  const rows = storage.sql
    .exec<{ k: ArrayBuffer; v: ArrayBuffer | null; ver: number }>(query, ...bindings)
    .toArray();

  return rows.map((row) => ({
    key: new Uint8Array(row.k),
    value: !opts.keysOnly && row.v != null ? new Uint8Array(row.v) : null,
    ver: row.ver,
  }));
}

/** Atomically commit puts and deletes with OCC version checks. */
export function dbCommit(
  storage: Storage,
  observed: ObservedEntry[],
  puts: PutEntry[],
  dels: Uint8Array[],
): CommitResult {
  let ok = true;
  let error = "";

  storage.transactionSync(() => {
    // Check observed versions (OCC)
    for (const obs of observed) {
      const rows = storage.sql
        .exec<{ ver: number }>("SELECT ver FROM jfs_kv WHERE k = ?", obs.key)
        .toArray();
      const curVer = rows.length > 0 ? rows[0].ver : 0;
      if (curVer !== obs.ver) {
        ok = false;
        error = "write conflict";
        return;
      }
    }

    // Apply puts
    for (const put of puts) {
      storage.sql.exec(
        `INSERT INTO jfs_kv (k, v, ver) VALUES (?, ?, 1)
           ON CONFLICT(k) DO UPDATE SET v = excluded.v, ver = ver + 1`,
        put.key,
        put.value,
      );
    }

    // Apply deletes
    for (const del of dels) {
      storage.sql.exec("DELETE FROM jfs_kv WHERE k = ?", del);
    }
  });

  return { ok, error };
}

/** Delete all rows from the kv table. */
export function dbReset(storage: Storage): void {
  storage.sql.exec("DELETE FROM jfs_kv");
}
