import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  type WskvMessage,
  WskvMessageSchema,
  type GetRequest,
  type ListRequest,
  type CommitRequest,
  type ResetRequest,
  GetResponseSchema,
  ListResponseSchema,
  CommitResponseSchema,
  ResetResponseSchema,
  EntrySchema,
} from "./gen/wskv_pb.js";

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

const INIT_SQL = `CREATE TABLE IF NOT EXISTS jfs_kv (
  k BLOB PRIMARY KEY,
  v BLOB NOT NULL,
  ver INTEGER NOT NULL DEFAULT 1
) WITHOUT ROWID`;

export class WskvServer {
  private storage: Storage;
  private initialized = false;

  constructor(storage: Storage) {
    this.storage = storage;
  }

  private ensureTable(): void {
    if (this.initialized) return;
    this.storage.sql.exec(INIT_SQL);
    this.initialized = true;
  }

  /**
   * Handle a binary WebSocket frame. Returns the response bytes to send back,
   * or null for control messages (like ready_notify) that need no response.
   */
  handleMessage(data: ArrayBuffer): ArrayBuffer | null {
    const msg = fromBinary(WskvMessageSchema, new Uint8Array(data));
    if (msg.msg.case === "readyNotify") {
      return null;
    }
    this.ensureTable();
    const resp = this.dispatch(msg);
    return toBinary(WskvMessageSchema, resp).buffer as ArrayBuffer;
  }

  private dispatch(msg: WskvMessage): WskvMessage {
    switch (msg.msg.case) {
      case "getReq":
        return this.handleGet(msg.msg.value);
      case "listReq":
        return this.handleList(msg.msg.value);
      case "commitReq":
        return this.handleCommit(msg.msg.value);
      case "resetReq":
        return this.handleReset(msg.msg.value);
      default:
        throw new Error(`unknown message case: ${msg.msg.case}`);
    }
  }

  private handleGet(req: GetRequest): WskvMessage {
    const rows = this.storage.sql
      .exec<{ v: ArrayBuffer; ver: number }>(
        "SELECT v, ver FROM jfs_kv WHERE k = ?",
        req.key,
      )
      .toArray();

    const resp = create(GetResponseSchema);
    resp.id = req.id;
    if (rows.length > 0) {
      resp.value = new Uint8Array(rows[0].v);
      resp.ver = rows[0].ver;
      resp.found = true;
    }
    return create(WskvMessageSchema, { msg: { case: "getResp", value: resp } });
  }

  private handleList(req: ListRequest): WskvMessage {
    let query: string;
    const bindings: unknown[] = [req.start, req.end];

    if (req.keysOnly) {
      query = "SELECT k, ver FROM jfs_kv WHERE k >= ? AND k < ? ORDER BY k";
    } else {
      query = "SELECT k, v, ver FROM jfs_kv WHERE k >= ? AND k < ? ORDER BY k";
    }

    if (req.limit > 0) {
      query += " LIMIT ?";
      bindings.push(req.limit);
    }

    const rows = this.storage.sql
      .exec<{ k: ArrayBuffer; v: ArrayBuffer | null; ver: number }>(query, ...bindings)
      .toArray();

    const entries = rows.map((row) => {
      const entry = create(EntrySchema);
      entry.key = new Uint8Array(row.k);
      entry.ver = row.ver;
      if (!req.keysOnly && row.v != null) {
        entry.value = new Uint8Array(row.v);
      }
      return entry;
    });

    const resp = create(ListResponseSchema);
    resp.id = req.id;
    resp.entries = entries;
    return create(WskvMessageSchema, { msg: { case: "listResp", value: resp } });
  }

  private handleCommit(req: CommitRequest): WskvMessage {
    let ok = true;
    let error = "";

    this.storage.transactionSync(() => {
      // Check observed versions (OCC)
      for (const obs of req.observed) {
        const rows = this.storage.sql
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
      for (const put of req.puts) {
        this.storage.sql.exec(
          `INSERT INTO jfs_kv (k, v, ver) VALUES (?, ?, 1)
           ON CONFLICT(k) DO UPDATE SET v = excluded.v, ver = ver + 1`,
          put.key,
          put.value,
        );
      }

      // Apply deletes
      for (const del of req.dels) {
        this.storage.sql.exec("DELETE FROM jfs_kv WHERE k = ?", del);
      }
    });

    const resp = create(CommitResponseSchema);
    resp.id = req.id;
    resp.ok = ok;
    resp.error = error;
    return create(WskvMessageSchema, { msg: { case: "commitResp", value: resp } });
  }

  private handleReset(req: ResetRequest): WskvMessage {
    this.storage.sql.exec("DELETE FROM jfs_kv");
    const resp = create(ResetResponseSchema);
    resp.id = req.id;
    resp.ok = true;
    return create(WskvMessageSchema, { msg: { case: "resetResp", value: resp } });
  }
}
