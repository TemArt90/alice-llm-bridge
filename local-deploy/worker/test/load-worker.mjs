// Loads worker.js in plain Node by stubbing the cloudflare:workers import.
import { readFile, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const STUB = `class DurableObject {
  constructor(ctx, env) { this.ctx = ctx; this.env = env; }
}`;

const IMPORT = /^import\s*\{\s*DurableObject\s*\}\s*from\s*["']cloudflare:workers["'];?\s*$/m;

export async function loadWorker() {
  const source = await readFile(new URL("../worker.js", import.meta.url), "utf8");
  const transformed = source.replace(IMPORT, STUB);
  if (transformed === source) throw new Error("worker.js must import DurableObject from cloudflare:workers");
  const file = join(tmpdir(), `alice-code-bridge-${randomUUID()}.mjs`);
  await writeFile(file, transformed, "utf8");
  return import(pathToFileURL(file).href);
}

// Minimal DurableObjectNamespace stand-in; one instance keeps relay state.
export function makeEnv(worker, overrides = {}) {
  const env = { AGENT_KEY: "agent-key-for-tests", WEBHOOK_SECRET: "webhook-secret-for-tests", ...overrides };
  const state = { instance: null, gets: 0 };
  env.BRIDGE = {
    idFromName: name => name,
    get: () => {
      state.gets += 1;
      return {
        fetch(request) {
          if (!state.instance) state.instance = new worker.AliceCodeBridge({ getWebSockets: () => [] }, env);
          return state.instance.fetch(request);
        },
      };
    },
  };
  return { env, state };
}
