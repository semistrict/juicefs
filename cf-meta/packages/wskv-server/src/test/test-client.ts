import { create, toBinary, fromBinary } from "@bufbuild/protobuf";
import {
  WskvMessageSchema,
  GetRequestSchema,
  ListRequestSchema,
  CommitRequestSchema,
  ResetRequestSchema,
  type GetResponse,
  type ListResponse,
  type CommitResponse,
  type ResetResponse,
  type WskvMessage,
  ReadRangeSchema,
  ReadEntrySchema,
  PutSchema,
} from "../gen/wskv_pb.js";
import { WskvServer } from "../wskv-server.js";
import type { Storage } from "../schema.js";

const encoder = new TextEncoder();

/** Encode a string to Uint8Array for use as keys/values. */
export function b(s: string): Uint8Array {
  return encoder.encode(s);
}

/**
 * Test helper that wraps WskvServer.handleMessage() with protobuf
 * encode/decode, providing a clean API for tests.
 */
export class WskvTestClient {
  private server: WskvServer;
  private nextId = 1n;

  constructor(storage: Storage) {
    this.server = new WskvServer(storage);
  }

  private send(msg: WskvMessage): WskvMessage {
    const bytes = toBinary(WskvMessageSchema, msg);
    const respBytes = this.server.handleMessage(bytes.buffer as ArrayBuffer);
    if (respBytes === null) {
      throw new Error("unexpected null response");
    }
    return fromBinary(WskvMessageSchema, new Uint8Array(respBytes));
  }

  private id(): bigint {
    return this.nextId++;
  }

  /** Get a single key. */
  get(key: Uint8Array): GetResponse {
    const id = this.id();
    const msg = create(WskvMessageSchema, {
      msg: {
        case: "getReq",
        value: create(GetRequestSchema, { id, key }),
      },
    });
    const resp = this.send(msg);
    if (resp.msg.case !== "getResp") {
      throw new Error(`expected getResp, got ${resp.msg.case}`);
    }
    return resp.msg.value;
  }

  /** List entries in [start, end). */
  list(
    start: Uint8Array,
    end: Uint8Array,
    opts?: { keysOnly?: boolean; limit?: number },
  ): ListResponse {
    const id = this.id();
    const msg = create(WskvMessageSchema, {
      msg: {
        case: "listReq",
        value: create(ListRequestSchema, {
          id,
          start,
          end,
          keysOnly: opts?.keysOnly ?? false,
          limit: opts?.limit ?? 0,
        }),
      },
    });
    const resp = this.send(msg);
    if (resp.msg.case !== "listResp") {
      throw new Error(`expected listResp, got ${resp.msg.case}`);
    }
    return resp.msg.value;
  }

  /**
   * Commit with explicit read ranges, puts, and deletes.
   * Read ranges capture what was observed during the "read phase" so the
   * server can validate no concurrent modifications occurred.
   */
  commit(
    reads: Array<{
      start: Uint8Array;
      end: Uint8Array;
      entries: Array<{ key: Uint8Array; ver: number }>;
      keysOnly?: boolean;
      limit?: number;
    }>,
    puts: Array<{ key: Uint8Array; value: Uint8Array }>,
    dels: Uint8Array[],
  ): CommitResponse {
    const id = this.id();
    const msg = create(WskvMessageSchema, {
      msg: {
        case: "commitReq",
        value: create(CommitRequestSchema, {
          id,
          reads: reads.map((r) =>
            create(ReadRangeSchema, {
              start: r.start,
              end: r.end,
              entries: r.entries.map((e) =>
                create(ReadEntrySchema, { key: e.key, ver: e.ver }),
              ),
              keysOnly: r.keysOnly ?? false,
              limit: r.limit ?? 0,
            }),
          ),
          puts: puts.map((p) =>
            create(PutSchema, { key: p.key, value: p.value }),
          ),
          dels,
        }),
      },
    });
    const resp = this.send(msg);
    if (resp.msg.case !== "commitResp") {
      throw new Error(`expected commitResp, got ${resp.msg.case}`);
    }
    return resp.msg.value;
  }

  /** Simple helper: put a single key with no read validation. */
  put(key: Uint8Array, value: Uint8Array): CommitResponse {
    return this.commit([], [{ key, value }], []);
  }

  /** Simple helper: delete a single key with no read validation. */
  del(key: Uint8Array): CommitResponse {
    return this.commit([], [], [key]);
  }

  /** Reset (wipe) all data. */
  reset(): ResetResponse {
    const id = this.id();
    const msg = create(WskvMessageSchema, {
      msg: {
        case: "resetReq",
        value: create(ResetRequestSchema, { id }),
      },
    });
    const resp = this.send(msg);
    if (resp.msg.case !== "resetResp") {
      throw new Error(`expected resetResp, got ${resp.msg.case}`);
    }
    return resp.msg.value;
  }
}
