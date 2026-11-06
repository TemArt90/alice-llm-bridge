// Hibernatable agent socket: the transport that keeps the Durable Object idle.
import test from "node:test";
import assert from "node:assert/strict";
import { loadWorker } from "./load-worker.mjs";

const worker = await loadWorker();
const KEY = "agent-key-for-tests";
const BASE = "https://alice-code-bridge.example.workers.dev";
const HOOK = "/alice/webhook/webhook-secret-for-tests";
const ENV = { AGENT_KEY: KEY, WEBHOOK_SECRET: "webhook-secret-for-tests" };

class StubResponse {
  constructor(body, init = {}) {
    this.status = init.status ?? 200;
    this.headers = init.headers ?? new Headers();
    this.webSocket = init.webSocket ?? null;
    this.body = body ?? null;
  }

  async text() {
    if (this.body == null) return "";
    return typeof this.body === "string" ? this.body : new TextDecoder().decode(this.body);
  }

  async json() {
    return JSON.parse(await this.text());
  }
}

class StubPair {
  constructor() {
    const client = { sent: [], close() {} };
    const server = {
      sent: [],
      closed: false,
      send(data) {
        this.sent.push(JSON.parse(data));
      },
      close() {
        this.closed = true;
      },
    };
    return [client, server];
  }
}

globalThis.Response = StubResponse;
globalThis.WebSocketPair = StubPair;

// getWebSockets() drops a closed socket, exactly like the real runtime.
function context() {
  const sockets = [];
  return {
    acceptWebSocket(socket) {
      const close = socket.close.bind(socket);
      socket.close = () => {
        close();
        sockets.splice(sockets.indexOf(socket), 1);
      };
      sockets.push(socket);
    },
    getWebSockets: () => sockets,
  };
}

const headers = pairs => ({
  get: name => pairs[name.toLowerCase()] ?? null,
  [Symbol.iterator]: () => Object.entries(pairs)[Symbol.iterator](),
});
const request = (path, { method = "GET", pairs = {}, body = null } = {}) => ({
  url: BASE + path,
  method,
  headers: headers(pairs),
  body,
});
const auth = (extra = {}) => ({ authorization: `Bearer ${KEY}`, ...extra });
const upgrade = () => request("/_agent/ws", { pairs: auth({ upgrade: "websocket" }) });
const hook = () => request(HOOK, { method: "POST", pairs: auth() });
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
const encode = text => Buffer.from(text, "utf8").toString("base64");
const result = (id, text, status = 200) =>
  JSON.stringify({ id, status, headers: [["content-type", "application/json"]], body: encode(text) });

async function connected() {
  const ctx = context();
  const bridge = new worker.AliceCodeBridge(ctx, ENV);
  const handshake = await bridge.fetch(upgrade());
  return { ctx, bridge, handshake, socket: ctx.getWebSockets()[0] };
}

test("the socket needs an upgrade request and the shared secret", async () => {
  const ctx = context();
  const bridge = new worker.AliceCodeBridge(ctx, ENV);
  assert.equal((await bridge.fetch(request("/_agent/ws", { pairs: auth() }))).status, 426);
  const denied = await bridge.fetch(request("/_agent/ws", { pairs: auth({ upgrade: "websocket", authorization: "Bearer nope" }) }));
  assert.equal(denied.status, 401);
  assert.equal(ctx.getWebSockets().length, 0);
});

test("no socket means Alice is told the agent is offline before the deadline", async () => {
  const ctx = context();
  const bridge = new worker.AliceCodeBridge(ctx, ENV);
  const response = await bridge.fetch(hook());
  assert.equal(response.status, 503);
  assert.equal(await response.text(), "Agent offline");
});

test("a handshake registers one hibernatable socket", async () => {
  const { ctx, handshake } = await connected();
  assert.equal(handshake.status, 101);
  assert.ok(handshake.webSocket, "the client half of the pair is returned");
  assert.equal(ctx.getWebSockets().length, 1);
});

test("the webhook job travels over the socket and its result answers Alice", async () => {
  const { bridge, socket } = await connected();
  const pending = bridge.fetch(hook());
  await wait(20);
  const job = socket.sent.at(-1);
  assert.equal(job.path, HOOK);
  assert.equal(job.method, "POST");
  assert.equal(typeof job.id, "string");
  await bridge.webSocketMessage(socket, result(job.id, "ok"));
  const response = await pending;
  assert.equal(response.status, 200);
  assert.equal(await response.text(), "ok");
});

test("pings are answered without a job", async () => {
  const { bridge, socket } = await connected();
  await bridge.webSocketMessage(socket, JSON.stringify({ type: "ping" }));
  assert.equal(socket.sent.at(-1).type, "pong");
});

test("a foreign close leaves an in-flight job alone, the owning close fails it", async () => {
  const { bridge, socket } = await connected();
  const pending = bridge.fetch(hook());
  await wait(20);
  const job = socket.sent.at(-1);
  bridge.webSocketClose({ stranger: true });
  await bridge.webSocketMessage(socket, result(job.id, "still here"));
  const answered = await pending;
  assert.equal(answered.status, 200);
  assert.equal(await answered.text(), "still here");

  const lost = bridge.fetch(hook());
  await wait(20);
  socket.close();
  bridge.webSocketClose(socket);
  const failed = await lost;
  assert.equal(failed.status, 503);
  assert.equal(await failed.text(), "Agent disconnected");
  assert.equal((await bridge.fetch(hook())).status, 503);
});

test("a reconnect replaces the stale socket and keeps the relay usable", async () => {
  const { ctx, bridge, socket } = await connected();
  const again = await bridge.fetch(upgrade());
  assert.equal(again.status, 101);
  assert.equal(socket.closed, true);
  assert.equal(ctx.getWebSockets().length, 1);
  const fresh = ctx.getWebSockets()[0];
  const pending = bridge.fetch(hook());
  await wait(20);
  const job = fresh.sent.at(-1);
  await bridge.webSocketMessage(fresh, result(job.id, "new socket"));
  assert.equal(await (await pending).text(), "new socket");
});

test("malformed results fail the caller with 502 instead of hanging", async () => {
  const { bridge, socket } = await connected();
  const pending = bridge.fetch(hook());
  await wait(20);
  const job = socket.sent.at(-1);
  await bridge.webSocketMessage(socket, JSON.stringify({ id: job.id, status: 99, headers: [] }));
  const response = await pending;
  assert.equal(response.status, 502);
});
