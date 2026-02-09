import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import type { GenMessage } from "@bufbuild/protobuf/codegenv2";
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
import type { Storage } from "./schema.js";
import { ensureTable } from "./schema.js";
import { dbGet, dbList, dbCommit, dbReset } from "./db.js";

export class WskvServer {
  private storage: Storage;
  private initialized = false;

  constructor(storage: Storage) {
    this.storage = storage;
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
    if (!this.initialized) {
      ensureTable(this.storage);
      this.initialized = true;
    }
    const resp = this.dispatch(msg);
    return toBinary(WskvMessageSchema, resp).buffer as ArrayBuffer;
  }

  /** Create a proto message and wrap it in the WskvMessage envelope. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private wrap(case_: WskvMessage["msg"]["case"], schema: GenMessage<any>, fields: object): WskvMessage {
    return create(WskvMessageSchema, {
      msg: { case: case_, value: create(schema, fields) } as WskvMessage["msg"],
    });
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
    const result = dbGet(this.storage, req.key);
    return this.wrap("getResp", GetResponseSchema, {
      id: req.id,
      ...(result.found
        ? { value: result.value!, ver: result.ver, found: true }
        : {}),
    });
  }

  private handleList(req: ListRequest): WskvMessage {
    const rows = dbList(this.storage, {
      start: req.start,
      end: req.end,
      keysOnly: req.keysOnly,
      limit: req.limit,
    });
    return this.wrap("listResp", ListResponseSchema, {
      id: req.id,
      entries: rows.map((row) =>
        create(EntrySchema, {
          key: row.key,
          ver: row.ver,
          ...(row.value != null ? { value: row.value } : {}),
        }),
      ),
    });
  }

  private handleCommit(req: CommitRequest): WskvMessage {
    const result = dbCommit(
      this.storage,
      req.observed.map((obs) => ({ key: obs.key, ver: obs.ver })),
      req.puts.map((put) => ({ key: put.key, value: put.value })),
      req.dels,
    );
    return this.wrap("commitResp", CommitResponseSchema, {
      id: req.id,
      ok: result.ok,
      error: result.error,
    });
  }

  private handleReset(req: ResetRequest): WskvMessage {
    dbReset(this.storage);
    return this.wrap("resetResp", ResetResponseSchema, {
      id: req.id,
      ok: true,
    });
  }
}
