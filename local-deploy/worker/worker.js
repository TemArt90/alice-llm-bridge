// Alice coding bridge: fixed-route relay with a hibernatable agent socket.
// Relay pattern adapted from zhi_guang/pc/alice_worker/worker-poll.js (ChandelierBridge),
// original project eluceon/alice-llm-bridge, MIT License, Copyright (c) 2026 eluceon.
import { DurableObject } from "cloudflare:workers";

const BATCH_MAX = 8;
const PENDING_MAX = 16;
const REQ_MAX = 65536;
const RES_MAX = 16384;
const DEAD_MS = 60000;
const PUBLIC_TIMEOUT_MS = 3900;
const POLL_MS = 2000;
const HOOK_PREFIX = "/alice/webhook/";
const INSTANCE = "alice-code-bridge-v1";
const IN_HEADERS = new Set(["authorization", "content-type", "cookie", "x-request-id"]);
const OUT_HEADERS = new Set(["content-type", "cache-control", "location", "allow"]);

const reply = (text, status) => new Response(text, { status, headers: { "cache-control": "no-store" } });
const json = (value, status = 200) =>
  new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json", "cache-control": "no-store" } });

async function digest(value) {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(String(value))));
}

// Constant-time comparison over fixed-length digests.
async function sameSecret(value, expected) {
  if (!value || !expected) return false;
  const [left, right] = await Promise.all([digest(value), digest(expected)]);
  let difference = 0;
  for (let i = 0; i < left.length; i += 1) difference |= left[i] ^ right[i];
  return difference === 0;
}

async function readBody(request, limit) {
  if (Number(request.headers.get("content-length") || 0) > limit) throw new RangeError("large body");
  if (!request.body) return new Uint8Array();
  const reader = request.body.getReader();
  const chunks = [];
  let length = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      length += value.length;
      if (length > limit) throw new RangeError("large body");
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.length;
  }
  return body;
}

function encode(bytes) {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 0x8000) binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(binary);
}

function decode(text) {
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

export class AliceCodeBridge extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.pending = new Map();
    this.queue = [];
    this.poll = null;
    this.lastAgent = 0;
    // Hibernated agent sockets belong to ctx.getWebSockets(); closing them here
    // would drop the connection every time the object wakes up.
  }

  agentSockets() {
    return this.ctx.getWebSockets();
  }

  online() {
    return this.agentSockets().length > 0 || Date.now() - this.lastAgent <= DEAD_MS;
  }

  abandon(socket) {
    for (const [id, entry] of this.pending) {
      if (entry.socket !== socket) continue;
      clearTimeout(entry.timer);
      this.pending.delete(id);
      entry.resolve(reply("Agent disconnected", 503));
    }
  }

  offlineIfLast(socket) {
    // Otherwise the relay keeps claiming a live agent for a minute after the only
    // socket dropped, so Alice waits out its whole deadline instead of a 503.
    if (!this.agentSockets().some(item => item !== socket)) this.lastAgent = 0;
  }

  dispatch() {
    this.queue = this.queue.filter(job => this.pending.has(job.id));
    if (!this.poll || !this.queue.length) return;
    const poll = this.poll;
    this.poll = null;
    clearTimeout(poll.timer);
    poll.resolve(json({ jobs: this.queue.splice(0, BATCH_MAX) }));
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/_agent/ws") return this.onSocket(request);
    if (url.pathname === "/_agent/poll") return this.onPoll(request);
    if (url.pathname === "/_agent/result") return this.onResult(request);
    return this.enqueue(request, url);
  }

  async authorized(request) {
    return sameSecret(request.headers.get("authorization"), this.env.AGENT_KEY ? `Bearer ${this.env.AGENT_KEY}` : "");
  }

  async onSocket(request) {
    if ((request.headers.get("upgrade") || "").toLowerCase() !== "websocket") return reply("Upgrade required", 426);
    if (!await this.authorized(request)) return reply("Unauthorized", 401);
    // One PC agent: replace a stale or half-open socket before accepting the new one.
    for (const stale of this.agentSockets()) {
      try {
        stale.close(1000, "replaced");
      } catch {
        // A socket that is already gone needs no cleanup.
      }
    }
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    this.lastAgent = Date.now();
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  settle(entry, data) {
    try {
      if (!Number.isInteger(data.status) || data.status < 200 || data.status > 599 || !Array.isArray(data.headers)) throw new Error("shape");
      const headers = new Headers();
      for (const pair of data.headers) {
        if (!Array.isArray(pair) || pair.length !== 2) throw new Error("shape");
        const [key, value] = pair;
        if (OUT_HEADERS.has(String(key).toLowerCase())) headers.append(key, value);
      }
      const body = decode(data.body || "");
      if (body.length > RES_MAX) throw new Error("large body");
      entry.resolve(new Response(body, { status: data.status, headers }));
    } catch {
      entry.resolve(reply("Invalid response", 502));
    }
  }

  async onPoll(request) {
    if (!await this.authorized(request)) return reply("Unauthorized", 401);
    this.lastAgent = Date.now();
    if (this.poll) {
      clearTimeout(this.poll.timer);
      this.poll.resolve(json({ idle: true }));
      this.poll = null;
    }
    const response = new Promise(resolve => {
      const timer = setTimeout(() => {
        this.poll = null;
        resolve(json({ idle: true }));
      }, POLL_MS);
      this.poll = { resolve, timer };
    });
    this.dispatch();
    return response;
  }

  async onResult(request) {
    if (!await this.authorized(request)) return reply("Unauthorized", 401);
    let data;
    try {
      data = JSON.parse(new TextDecoder().decode(await readBody(request, RES_MAX)));
    } catch (error) {
      return error instanceof RangeError ? reply("Too large", 413) : reply("Invalid response", 400);
    }
    const entry = data && typeof data.id === "string" ? this.pending.get(data.id) : undefined;
    if (!entry) return reply("Request expired", 409);
    this.pending.delete(data.id);
    clearTimeout(entry.timer);
    this.settle(entry, data);
    return json({ accepted: true });
  }

  async enqueue(request, url) {
    let body;
    try {
      body = await readBody(request, REQ_MAX);
    } catch (error) {
      return error instanceof RangeError ? reply("Too large", 413) : reply("Invalid request", 400);
    }
    if (!this.online()) return reply("Agent offline", 503);
    if (this.pending.size >= PENDING_MAX) return reply("Busy", 503);
    const id = crypto.randomUUID();
    const socket = this.agentSockets()[0];
    const response = new Promise(resolve => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        resolve(reply("Agent timeout", 504));
      }, PUBLIC_TIMEOUT_MS);
      this.pending.set(id, { resolve, timer, socket });
    });
    const job = {
      id,
      method: request.method,
      path: url.pathname,
      headers: [...request.headers].filter(([key]) => IN_HEADERS.has(key.toLowerCase())),
      body: encode(body),
    };
    if (socket) {
      try {
        socket.send(JSON.stringify(job));
      } catch {
        const entry = this.pending.get(id);
        if (entry) clearTimeout(entry.timer);
        this.pending.delete(id);
        return reply("Agent offline", 503);
      }
      return response;
    }
    this.queue.push(job);
    this.dispatch();
    return response;
  }

  async webSocketMessage(ws, message) {
    this.lastAgent = Date.now();
    let data;
    try {
      data = JSON.parse(typeof message === "string" ? message : new TextDecoder().decode(message));
    } catch {
      return;
    }
    if (!data || typeof data !== "object") return;
    if (data.type === "hello" || data.type === "ping") {
      try {
        ws.send(JSON.stringify({ type: "pong" }));
      } catch {
        // A socket that is already gone needs no cleanup.
      }
      return;
    }
    const entry = typeof data.id === "string" ? this.pending.get(data.id) : undefined;
    if (!entry) return; // The request already timed out; drop the late result.
    this.pending.delete(data.id);
    clearTimeout(entry.timer);
    this.settle(entry, data);
  }

  webSocketClose(ws) {
    this.abandon(ws);
    this.offlineIfLast(ws);
  }

  webSocketError(ws) {
    this.abandon(ws);
    this.offlineIfLast(ws);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (path === "/health") {
      if (request.method !== "GET") return reply("Method not allowed", 405);
      return json({ ok: true, service: "alice-code-bridge" });
    }
    if (path.startsWith(HOOK_PREFIX)) {
      if (request.method !== "POST") return reply("Method not allowed", 405);
      if (url.search || !await sameSecret(path.slice(HOOK_PREFIX.length), env.WEBHOOK_SECRET)) return reply("Not found", 404);
      return env.BRIDGE.get(env.BRIDGE.idFromName(INSTANCE)).fetch(request);
    }
    if (path === "/_agent/poll" || path === "/_agent/result" || path === "/_agent/ws") {
      const method = path === "/_agent/result" ? "POST" : "GET";
      if (request.method !== method) return reply("Method not allowed", 405);
      return env.BRIDGE.get(env.BRIDGE.idFromName(INSTANCE)).fetch(request);
    }
    return reply("Not found", 404);
  },
};
