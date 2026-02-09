import { DurableObject } from "cloudflare:workers";
import { WskvServer } from "../wskv-server.js";
import type { Storage } from "../schema.js";

interface Env {
  WSKV_TEST: DurableObjectNamespace;
}

export class WskvTestDO extends DurableObject {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // /connect?target=ws://host:port/ws — connect to an external WS server
    // and wire up WskvServer to handle messages over that connection.
    if (url.pathname === "/connect") {
      const target = url.searchParams.get("target");
      if (!target) {
        return new Response("missing target param", { status: 400 });
      }

      // Workers fetch() doesn't support ws:// scheme; convert to http://.
      const httpTarget = target.replace(/^ws:\/\//, "http://").replace(/^wss:\/\//, "https://");
      const resp = await fetch(httpTarget, {
        headers: { Upgrade: "websocket" },
      });
      const ws = resp.webSocket;
      if (!ws) {
        return new Response("failed to upgrade to websocket", { status: 502 });
      }
      ws.accept();

      const server = new WskvServer(this.ctx.storage as unknown as Storage);
      ws.addEventListener("message", (event) => {
        const data = event.data;
        if (typeof data === "string") return;
        const respBytes = server.handleMessage(data as ArrayBuffer);
        if (respBytes) ws.send(respBytes);
      });

      return new Response("connected");
    }

    return new Response("ok");
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/connect") {
      // Each caller can pass ?name=<unique> to get a fresh DO instance.
      const name = url.searchParams.get("name") ?? "test";
      const id = env.WSKV_TEST.idFromName(name);
      const stub = env.WSKV_TEST.get(id);
      return stub.fetch(request);
    }

    return new Response("test worker");
  },
};
