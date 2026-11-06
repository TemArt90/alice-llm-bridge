import test from "node:test";
import assert from "node:assert/strict";
import { loadWorker, makeEnv } from "./load-worker.mjs";

const worker = await loadWorker();
const BASE = "https://alice-code-bridge.example.workers.dev";
const AGENT_KEY = "agent-key-for-tests";
const WEBHOOK_SECRET = "webhook-secret-for-tests";
const AGENT = { Authorization: `Bearer ${AGENT_KEY}` };
const HOOK = "/alice/webhook/" + WEBHOOK_SECRET;

const relay = (env, path, init) => worker.default.fetch(new Request(BASE + path, init), env);
const poll = (env, headers = AGENT) => relay(env, "/_agent/poll", { headers });
const hook = (env, init = {}) => relay(env, HOOK, { method: "POST", ...init });
const result = (env, payload, headers = AGENT) =>
  relay(env, "/_agent/result", { method: "POST", headers, body: typeof payload === "string" ? payload : JSON.stringify(payload) });
const settle = (ms = 60) => new Promise(resolve => setTimeout(resolve, ms));
const encode = text => Buffer.from(text, "utf8").toString("base64");

test("GET /health reports readiness without exposing secrets", async () => {
  const { env } = makeEnv(worker);
  const response = await relay(env, "/health", { method: "GET" });
  assert.equal(response.status, 200);
  const body = await response.text();
  assert.deepEqual(JSON.parse(body), { ok: true, service: "alice-code-bridge" });
  assert.ok(!body.includes(WEBHOOK_SECRET) && !body.includes(AGENT_KEY));
});

test("unknown routes, wrong webhook secret and query strings return 404 before the Durable Object", async () => {
  const { env, state } = makeEnv(worker);
  const cases = [
    ["/", {}],
    ["/nope", {}],
    ["/alice/webhook/", { method: "POST" }],
    ["/alice/webhook/wrong-secret", { method: "POST" }],
    [HOOK + "?debug=1", { method: "POST" }],
    [HOOK + "/", { method: "POST" }],
  ];
  for (const [path, init] of cases) {
    const response = await relay(env, path, init);
    assert.equal(response.status, 404, path);
    const body = await response.text();
    assert.ok(!body.includes(WEBHOOK_SECRET), path);
  }
  assert.equal(state.instance, null);
});

test("known routes reject wrong methods with 405", async () => {
  const { env, state } = makeEnv(worker);
  assert.equal((await relay(env, "/health", { method: "POST" })).status, 405);
  assert.equal((await relay(env, HOOK, { method: "GET" })).status, 405);
  assert.equal((await relay(env, "/_agent/poll", { method: "POST", headers: AGENT })).status, 405);
  assert.equal((await relay(env, "/_agent/result", { method: "GET", headers: AGENT })).status, 405);
  assert.equal(state.instance, null);
});

test("agent endpoints reject missing, malformed and wrong bearer tokens", async () => {
  const { env } = makeEnv(worker);
  const headers = [{}, { Authorization: "Bearer wrong" }, { Authorization: AGENT_KEY }, { Authorization: `Basic ${AGENT_KEY}` }];
  for (const header of headers) {
    const response = await poll(env, header);
    assert.equal(response.status, 401);
    assert.ok(!(await response.text()).includes(AGENT_KEY));
  }
  assert.equal((await result(env, { id: "x", status: 200, headers: [], body: "" }, { Authorization: "Bearer wrong" })).status, 401);
});

test("oversized public bodies are rejected with 413", async () => {
  const { env } = makeEnv(worker);
  assert.equal((await hook(env, { body: "x".repeat(70000) })).status, 413);
  assert.equal((await hook(env, { body: "x".repeat(65537) })).status, 413);
});

test("public requests fail fast while no agent has polled", async () => {
  const { env } = makeEnv(worker);
  const response = await hook(env, { body: "{}" });
  assert.equal(response.status, 503);
});

test("an idle poll waits briefly and reports no work", async () => {
  const { env } = makeEnv(worker);
  const started = Date.now();
  const response = await poll(env);
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { idle: true });
  assert.ok(Date.now() - started >= 1900);
});

test("authenticated roundtrip relays one job and returns the agent result", async () => {
  const { env } = makeEnv(worker);
  await (await poll(env)).json(); // first poll marks the agent online

  const pending = hook(env, {
    headers: { "content-type": "application/json", authorization: "Bearer alice", "x-dropped": "1" },
    body: JSON.stringify({ request: { command: "hi" } }),
  });
  await settle();

  const batch = await (await poll(env)).json();
  assert.equal(batch.jobs.length, 1);
  const job = batch.jobs[0];
  assert.equal(job.method, "POST");
  assert.equal(job.path, HOOK);
  const headers = Object.fromEntries(job.headers);
  assert.equal(headers["content-type"], "application/json");
  assert.equal(headers.authorization, "Bearer alice");
  assert.equal("x-dropped" in headers, false);
  assert.deepEqual(JSON.parse(Buffer.from(job.body, "base64").toString("utf8")), { request: { command: "hi" } });

  const accepted = await result(env, {
    id: job.id,
    status: 200,
    headers: [["content-type", "application/json"], ["x-dropped", "1"]],
    body: encode('{"ok":true}'),
  });
  assert.equal(accepted.status, 200);
  assert.deepEqual(await accepted.json(), { accepted: true });

  const reply = await pending;
  assert.equal(reply.status, 200);
  assert.equal(reply.headers.get("content-type"), "application/json");
  assert.equal(reply.headers.get("x-dropped"), null);
  assert.equal(await reply.text(), '{"ok":true}');
});

test("batches are bounded to eight jobs and sixteen pending requests", async () => {
  const { env } = makeEnv(worker);
  await (await poll(env)).json(); // agent online

  const pending = [];
  for (let i = 0; i < 16; i++) pending.push(hook(env, { body: `{"n":${i}}` }));
  await settle(150);
  assert.equal((await hook(env, { body: "{}" })).status, 503);

  const first = (await (await poll(env)).json()).jobs;
  assert.equal(first.length, 8);
  const second = (await (await poll(env)).json()).jobs;
  assert.equal(second.length, 8);

  for (const job of [...first, ...second]) {
    assert.equal((await result(env, { id: job.id, status: 200, headers: [["content-type", "text/plain"]], body: encode("ok") })).status, 200);
  }
  for (const reply of await Promise.all(pending)) {
    assert.equal(reply.status, 200);
    assert.equal(await reply.text(), "ok");
  }
});

test("results for unknown or already settled jobs are rejected as expired", async () => {
  const { env } = makeEnv(worker);
  const response = await result(env, { id: "missing", status: 200, headers: [], body: "" });
  assert.equal(response.status, 409);
});

test("malformed and oversized agent results are rejected", async () => {
  const { env } = makeEnv(worker);
  assert.equal((await result(env, "not json")).status, 400);
  assert.equal((await result(env, "x".repeat(20000))).status, 413);
});

test("an invalid agent result status fails the waiting caller with 502", async () => {
  const { env } = makeEnv(worker);
  await (await poll(env)).json();
  const pending = hook(env, { body: "{}" });
  await settle();
  const job = (await (await poll(env)).json()).jobs[0];
  assert.equal((await result(env, { id: job.id, status: 99, headers: [], body: "" })).status, 200);
  assert.equal((await pending).status, 502);
});

test("public requests give up after the 3900 ms budget", async () => {
  const { env } = makeEnv(worker);
  await (await poll(env)).json();
  const started = Date.now();
  const response = await hook(env, { body: "{}" });
  const elapsed = Date.now() - started;
  assert.equal(response.status, 504);
  assert.ok(elapsed >= 3800 && elapsed < 4600, `elapsed ${elapsed}ms`);
});
