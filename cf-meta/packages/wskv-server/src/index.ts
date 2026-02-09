import { WskvServer } from "./wskv-server.js";
import type { Storage } from "./schema.js";
import { WsKvClient } from "./ws-client.js";

export { WskvServer, WsKvClient };
export type { Storage, SqlStorage, SqlStorageCursor, SqlStorageValue } from "./schema.js";

export interface StorageConfig {
  storage: string;
  bucket: string;
  /** Optional path prefix within the bucket (e.g. "subdir/foo"). */
  bucketPath?: string;
  accessKey: string;
  secretKey: string;
  volumeName: string;
}

/** Minimal subset of the Sandbox Process handle we depend on. */
export interface ContainerProcess {
  readonly id: string;
  readonly status: string;
  kill(signal?: string): Promise<void>;
  getStatus(): Promise<string>;
  getLogs(): Promise<{ stdout: string; stderr: string }>;
  waitForPort(port: number, options?: { mode?: "http" | "tcp"; timeout?: number }): Promise<void>;
}

/** Methods we need from the Container/Sandbox DO. */
export interface ContainerHandle {
  exec(cmd: string): Promise<unknown>;
  startProcess(cmd: string, opts?: { processId?: string }): Promise<ContainerProcess>;
  containerFetch(request: Request, port: number): Promise<Response>;
}

export interface StartWskvOptions {
  storage: Storage;
  mountpoint: string;
  port?: number;
  config: StorageConfig;
}

/**
 * Manages the JuiceFS cfmount lifecycle inside a Cloudflare Container.
 *
 * Call `start()` from onStart() to start the Go binary as a managed
 * background process, then call `connect()` from fetch() or elsewhere
 * to establish the WebSocket and wire up wskv.
 *
 * If the WebSocket closes unexpectedly, the mount will automatically
 * reconnect with exponential backoff (1s, 2s, 4s, 8s, capped at 10s).
 * Call `close()` to stop reconnection attempts and shut down cleanly.
 */
export class WskvMount {
  private container: ContainerHandle;
  private opts: StartWskvOptions;
  private ws: WebSocket | null = null;
  private connecting: Promise<WebSocket> | null = null;
  private process: ContainerProcess | null = null;
  private closed = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;

  constructor(container: ContainerHandle, opts: StartWskvOptions) {
    this.container = container;
    this.opts = opts;
  }

  get mountpoint(): string {
    return this.opts.mountpoint;
  }

  get port(): number {
    return this.opts.port ?? 14234;
  }

  /**
   * Start the cfmount binary as a managed background process.
   *
   * Uses the container's `startProcess()` API so the process is tracked,
   * its logs are captured, and it can be inspected or killed later.
   * Safe to call from onStart().
   */
  async start(): Promise<void> {
    const cmd = `juicefs-cfmount ${this.mountpoint} --address 0.0.0.0:${this.port}`;
    this.process = await this.container.startProcess(cmd, {
      processId: "cfmount",
    });
    console.log(`cfmount process started (id=${this.process.id})`);
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

  /**
   * Stop reconnection attempts and close the current WebSocket.
   * After calling close(), the mount will not attempt to reconnect.
   */
  close(): void {
    this.closed = true;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  /** Return the managed process handle, if started. */
  getProcess(): ContainerProcess | null {
    return this.process;
  }

  /** Calculate backoff delay: 1s, 2s, 4s, 8s, capped at 10s. */
  private getBackoffMs(): number {
    const base = 1000;
    const delay = base * Math.pow(2, this.reconnectAttempt);
    return Math.min(delay, 10_000);
  }

  private scheduleReconnect(): void {
    if (this.closed) return;
    const delay = this.getBackoffMs();
    this.reconnectAttempt++;
    console.log(`WebSocket closed, reconnecting in ${delay}ms (attempt ${this.reconnectAttempt})`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (this.closed) return;
      // Trigger reconnect; connect() is idempotent and handles the connecting state.
      this.connect().catch((err) => {
        console.error(`Reconnect attempt ${this.reconnectAttempt} failed: ${err}`);
        // Schedule another attempt on failure.
        this.scheduleReconnect();
      });
    }, delay);
  }

  private async doConnect(): Promise<WebSocket> {
    // 1. Wait for the Go binary's HTTP server via the managed process handle.
    if (this.process) {
      await this.process.waitForPort(this.port, { mode: "tcp", timeout: 30_000 });
    } else {
      // Fallback: binary was started outside our control.
      throw new Error("cfmount process not started — call start() first");
    }

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

    // 3. Wire up protocol client.
    const client = new WsKvClient(ws);
    client.sendInit(this.opts.config);

    // 4. Wire up wskv message handling.
    const server = new WskvServer(this.opts.storage);
    client.onMessage((data) => server.handleMessage(data));
    client.onClose(() => {
      this.ws = null;
      this.scheduleReconnect();
    });

    // Connection succeeded; reset backoff counter.
    this.reconnectAttempt = 0;

    return ws;
  }
}
