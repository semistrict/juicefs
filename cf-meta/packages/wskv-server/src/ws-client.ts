import { create, toBinary } from "@bufbuild/protobuf";
import { WskvMessageSchema, InitNotificationSchema } from "./gen/wskv_pb.js";
import type { StorageConfig } from "./index.js";

/**
 * Protocol abstraction for the WebSocket KV client.
 *
 * Encapsulates protobuf serialization for control messages and provides
 * a clean API for wiring up message and close handlers on the WebSocket.
 */
export class WsKvClient {
  constructor(private ws: WebSocket) {}

  /** Build and send the InitNotification protobuf message with storage credentials. */
  sendInit(config: StorageConfig): void {
    let bucket = config.bucket;
    if (config.bucketPath) {
      bucket = bucket + "/" + config.bucketPath;
    }
    const initNotify = create(InitNotificationSchema, {
      storage: config.storage,
      bucket,
      accessKey: config.accessKey,
      secretKey: config.secretKey,
      volumeName: config.volumeName,
    });
    this.ws.send(
      toBinary(
        WskvMessageSchema,
        create(WskvMessageSchema, {
          msg: { case: "initNotify", value: initNotify },
        }),
      ),
    );
  }

  /**
   * Set up the message listener. Binary frames are passed to `handler`;
   * if the handler returns a response buffer, it is sent back over the socket.
   * String frames are silently ignored.
   */
  onMessage(handler: (data: ArrayBuffer) => ArrayBuffer | null): void {
    this.ws.addEventListener("message", (event) => {
      const data = event.data;
      if (typeof data === "string") return;
      const resp = handler(data as ArrayBuffer);
      if (resp) this.ws.send(resp);
    });
  }

  /** Set up the close listener. */
  onClose(handler: () => void): void {
    this.ws.addEventListener("close", handler);
  }
}
