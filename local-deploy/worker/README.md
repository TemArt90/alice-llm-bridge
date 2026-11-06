# alice-code-bridge Worker

Minimal Cloudflare Worker that lets Alice (Yandex Dialogs) reach a coding bridge
running on a PC behind NAT. It is a **separate service** from the lamp Worker in
`zhi_guang`: its own script name (`alice-code-bridge`), its own Durable Object
class (`AliceCodeBridge`), its own secrets. Nothing in the lamp deployment is
read-modify-written by this directory.

## Public surface

| Route | Method | Purpose |
| --- | --- | --- |
| `/health` | GET | `{"ok":true,"service":"alice-code-bridge"}` |
| `/alice/webhook/{WEBHOOK_SECRET}` | POST | Alice webhook; exact path, no query string |
| `/_agent/poll` | GET | Agent long-poll, `Authorization: Bearer <AGENT_KEY>` |
| `/_agent/result` | POST | Agent result upload, same bearer auth |

Everything else is rejected before the Durable Object is touched. Unknown
paths, a wrong or missing webhook secret, and any query string all return 404
so the secret path cannot be discovered.

## Relay protocol

Poll returns either `{"idle":true}` (after ~2 s) or a batch:

```json
{ "jobs": [ { "id": "uuid", "method": "POST", "path": "/alice/webhook/…",
              "headers": [["content-type", "application/json"]], "body": "<base64>" } ] }
```

The agent replays each job against its local bridge and uploads:

```json
{ "id": "uuid", "status": 200, "headers": [["content-type","application/json"]], "body": "<base64>" }
```

`{"accepted":true}` acknowledges; the waiting public request receives the
relayed status, allowlisted headers and body.

## Socket transport

The agent can hold one WebSocket instead of polling:

| Route | Method | Purpose |
| --- | --- | --- |
| `/_agent/ws` | GET + `Upgrade: websocket` | Agent socket, `Authorization: Bearer AGENT_KEY` |

Each public request is delivered as one frame (`{id, method, path, headers,
body}`) and the agent answers with the same result object it would upload to
`/_agent/result`. The socket is accepted with `acceptWebSocket()`, so the
Durable Object **hibernates while idle**.

That is not an optimisation. Workers Free grants 13 000 GB-s of Durable Object
duration per day for the whole account (reset 00:00 UTC). A two-second
long-poll keeps the object active around the clock and eats that budget; once
it is spent every request answers `500` with `error code: 1101` and
`Exceeded allowed duration in Durable Objects free tier`. The lamp skill in
`zhi_guang` shares the same account quota and was taken down by exactly this.
A hibernating socket is billed for the few milliseconds of an actual command.

The sidecar picks the transport in `settings.json`:

```json
{ "relay_transport": "websocket" }
```

The code default stays `poll`, and `/_agent/poll` + `/_agent/result` keep
working, so the switch is reversible without another deploy.

## Limits

| Bound | Value |
| --- | --- |
| Jobs per batch | 8 |
| Concurrent pending public requests | 16 |
| Public request body | 64 KiB |
| Agent response body | 16 KiB |
| Public request timeout | 3900 ms (Alice needs < 4.5 s) |
| Agent considered dead after | 60 s without a poll |
| Poll hold time | 2000 ms |

Inbound headers forwarded to the agent: `authorization`, `content-type`,
`cookie`, `x-request-id`. Response headers passed back: `content-type`,
`cache-control`, `location`, `allow`.

## Security

* Two independent credentials: the exact webhook path segment
  (`WEBHOOK_SECRET`) for public traffic, and `Bearer AGENT_KEY` for the agent
  channel. Both are compared as SHA-256 digests with a constant-time loop, so
  neither value leaks through timing or error text.
* Secret values never appear in responses, logs or `deployment.json`. There is
  no observability/tail configured on the Worker.
* Model usage stays locked behind the original Rust bridge's user allowlist;
  this Worker only forwards HTTP and never talks to a model provider itself.

## Tests

Offline, no network, no dependencies — Node's built-in test runner. The loader
in `test/load-worker.mjs` replaces the `cloudflare:workers` import with a stub
`DurableObject` base class so the module runs on real Web APIs in Node.

```sh
npm test        # node --test "test/**/*.test.mjs"
```

`test/worker.test.mjs` covers the public surface and the poll fallback;
`test/ws-relay.test.mjs` covers the socket transport: handshake, job delivery,
socket replacement, cancellation scope and malformed results.

## Deploy

Requires the existing Wrangler OAuth grant used by `zhi_guang/pc`. Run with that
project's interpreter (it has `requests`):

```sh
H:/workspace/zhi_guang/pc/.venv/Scripts/python.exe deploy_worker.py --check
H:/workspace/zhi_guang/pc/.venv/Scripts/python.exe deploy_worker.py
H:/workspace/zhi_guang/pc/.venv/Scripts/python.exe deploy_worker.py --verify
```

The script imports only `access_token()` from `alice_cloudflare.py` (refresh of
the existing grant); `prepare()` and `deploy_api()` — the functions that mutate
the lamp Worker — are never called. It reads `agent_key` and `webhook_secret`
from `../secrets.json`, refuses to update an existing `alice-code-bridge`
script, then writes module + metadata (DO migration `v1`, secret bindings,
`BRIDGE` namespace binding, observability off) and enables the workers.dev
route with previews disabled. It then reads settings back and probes the public
URL. Exit codes: `0` deployed/verified, `3` secrets missing, `4` Worker already
exists, `5` failure.

`deployment.json` (gitignored) records `public_url` and `account_id` for the
local sidecar.

**Creating needs a missing script; changing it needs `--update`.** Without the
flag the script refuses to touch an existing Worker (exit 4), and with it the
script refuses when the deployed bindings differ from
`{BRIDGE, AGENT_KEY, WEBHOOK_SECRET}` — nothing is dropped silently. Secret
values are re-sent from `../secrets.json`, so the Windows and Ubuntu copies must
match (compare SHA-256 before deploying; they did on 06.11.2026).

## Attribution

The relay pattern — polling and the hibernating socket — is adapted from
`zhi_guang/pc/alice_worker/worker-poll.js` (`ChandelierBridge`), which derives
from **eluceon/alice-llm-bridge** — MIT License, Copyright (c) 2026 eluceon.
The adaptation keeps the batching, timeout and result-validation model, and
adds the exact-secret public route, separate worker identity and explicit
route/method allowlists.
