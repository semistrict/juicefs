import { create, toBinary } from "@bufbuild/protobuf";
import { WskvServer } from "./wskv-server.js";
import type { Storage } from "./wskv-server.js";
import { WskvMessageSchema, InitNotificationSchema } from "./gen/wskv_pb.js";

export { WskvServer };
export type { Storage, SqlStorage, SqlStorageCursor, SqlStorageValue } from "./wskv-server.js";

export interface StorageConfig {
  storage: string;
  bucket: string;
  accessKey: string;
  secretKey: string;
  volumeName: string;
}

/** Methods we need from the Container/Sandbox DO. */
export interface ContainerHandle {
  exec(cmd: string): Promise<unknown>;
  waitForPort(opts: { portToCheck: number; retries: number; waitInterval: number }): Promise<unknown>;
  containerFetch(request: Request, port: number): Promise<Response>;
}

export interface StartWskvOptions {
  storage: Storage;
  mountpoint?: string;
  port?: number;
  config: StorageConfig;
}

/**
 * Manages the JuiceFS cfmount lifecycle inside a Cloudflare Container.
 *
 * Call `launch()` from onStart() to start the Go binary (fast, safe for
 * blockConcurrencyWhile), then call `connect()` from fetch() or elsewhere
 * to establish the WebSocket and wire up wskv.
 */
export class WskvMount {
  private container: ContainerHandle;
  private opts: StartWskvOptions;
  private ws: WebSocket | null = null;
  private connecting: Promise<WebSocket> | null = null;

  constructor(container: ContainerHandle, opts: StartWskvOptions) {
    this.container = container;
    this.opts = opts;
  }

  get mountpoint(): string {
    return this.opts.mountpoint ?? "/mnt/jfs";
  }

  get port(): number {
    return this.opts.port ?? 9876;
  }

  /** Start the Go binary in the background. Safe to call from onStart(). */
  launch(): void {
    this.container.exec(
      `juicefs-cfmount ${this.mountpoint} --port ${this.port} > /tmp/cfmount.log 2>&1 &`,
    ).catch((err) => {
      console.error("cfmount background start failed:", err);
    });
  }

  /** Wait for the binary, upgrade to WebSocket, wire up wskv. Idempotent. */
  async connect(): Promise<WebSocket> {
    if (this.ws) return this.ws;
    if (this.connecting) return this.connecting;
    this.connecting = this.doConnect();
    try {
      this.ws = await this.connecting;
      return this.ws;
    } finally {
      this.connecting = null;
    }
  }

  private async doConnect(): Promise<WebSocket> {
    // 1. Wait for the Go binary's HTTP server.
    await this.container.waitForPort({
      portToCheck: this.port,
      retries: 30,
      waitInterval: 1000,
    });

    // 2. Upgrade to WebSocket.
    const resp = await this.container.containerFetch(
      new Request("http://localhost/ws", {
        headers: { Upgrade: "websocket", Connection: "Upgrade" },
      }),
      this.port,
    );
    const ws = resp.webSocket;
    if (!ws) {
      throw new Error(`failed to upgrade to websocket on port ${this.port}`);
    }
    ws.accept();

    // 3. Send init_notify with storage credentials.
    const initNotify = create(InitNotificationSchema, {
      storage: this.opts.config.storage,
      bucket: this.opts.config.bucket,
      accessKey: this.opts.config.accessKey,
      secretKey: this.opts.config.secretKey,
      volumeName: this.opts.config.volumeName,
    });
    ws.send(toBinary(WskvMessageSchema, create(WskvMessageSchema, {
      msg: { case: "initNotify", value: initNotify },
    })));

    // 4. Wire up wskv message handling.
    const server = new WskvServer(this.opts.storage);
    ws.addEventListener("message", (event) => {
      const data = event.data;
      if (typeof data === "string") return;
      const resp = server.handleMessage(data as ArrayBuffer);
      if (resp) ws.send(resp);
    });

    ws.addEventListener("close", () => {
      this.ws = null;
    });

    return ws;
  }
}
