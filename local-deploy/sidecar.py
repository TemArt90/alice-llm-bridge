#!/usr/bin/env python3
"""Alice <-> OpenCode Go sidecar.

Runs next to the original Rust Alice bridge and provides:

* an authenticated OpenAI-compatible ``POST /v1/chat/completions`` proxy that
  talks to a fixed OpenCode Go endpoint with the user's own subscription key;
* a thin ``GET /health`` + ``POST /alice/webhook/{secret}`` front door for the
  bridge behind Cloudflare;
* an outgoing long-poll worker (``GET /_agent/poll``, ``batch-v1``) that
  executes only the two whitelisted bridge routes and uploads results to
  ``POST /_agent/result``.

Configuration lives in ``settings.json`` and ``secrets.json`` next to this
file.  Nothing is fetched from the network at startup.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from aiohttp import ClientError, ClientSession, ClientTimeout, WSMsgType, web

DEFAULT_UPSTREAM_URL = (
    "https://opencode.ai/inference/go/openai/v1/chat/completions"
)

SPEECH_LIMIT = 1000
MAX_BODY_BYTES = 64 * 1024
UPSTREAM_TIMEOUT = 45.0
BRIDGE_TIMEOUT = 3.0
POLL_TIMEOUT = 6.0
# Leave room for a retry before the Worker's 3.9 second public deadline.
RESULT_TIMEOUT = 1.0
RESULT_RETRY_DELAY = 0.1
# The worker accepts at most 16 KiB of JSON per result upload; base64 inflates
# the payload, so the raw bridge body is capped well below that.
RELAY_BODY_LIMIT = 8000
POLL_ERROR_BACKOFF = 2.0
# The Worker already long-polls for two seconds; a second idle sleep adds latency.
POLL_IDLE_INTERVAL = 0.0
MAX_JOBS_PER_BATCH = 8
MAX_CONCURRENT_JOBS = 8

USER_AGENT = "alice-code-bridge/1.0"
PROTOCOL_HEADER = "X-Bridge-Protocol"
PROTOCOL_VERSION = "batch-v1"

# The agent socket keeps the Durable Object hibernating between commands, so the
# Cloudflare free tier is billed for a few milliseconds per request instead of
# around the clock.  The poll loop stays available for networks without upgrades.
AGENT_WS_PATH = "/_agent/ws"
WS_HEARTBEAT = 20.0
WS_BACKOFF_START = 1.0
WS_BACKOFF_MAX = 30.0
WS_MAX_MESSAGE = 512 * 1024

WEBHOOK_PREFIX = "/alice/webhook/"
HEALTH_PATH = "/health"

log = logging.getLogger("alice-sidecar")


class ConfigError(RuntimeError):
    """Raised when settings.json / secrets.json are missing or invalid."""


class JobError(ValueError):
    """Raised when a polled job is malformed or targets a non-whitelisted URL."""

    def __init__(self, reason: str, *, job_id: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.job_id = job_id


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------

_REQUIRED_SETTINGS = {
    "public_url": str,
    "bridge_url": str,
    "proxy_listen": str,
    "proxy_port": int,
    "allowed_models": list,
    "session_id": str,
}
_REQUIRED_SECRETS = ("agent_key", "webhook_secret", "proxy_key", "opencode_go_key")


@dataclass(frozen=True)
class Config:
    settings: Mapping[str, Any]
    secrets: Mapping[str, Any]
    base_dir: Path

    @property
    def public_url(self) -> str:
        return str(self.settings["public_url"]).rstrip("/")

    @property
    def bridge_url(self) -> str:
        return str(self.settings["bridge_url"]).rstrip("/")

    @property
    def allowed_models(self) -> tuple[str, ...]:
        return tuple(str(m) for m in self.settings["allowed_models"])

    def secret(self, name: str) -> str:
        return str(self.secrets[name])


def _read_json(path: Path, label: str) -> dict:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ConfigError(f"{label} not found next to sidecar.py") from None
    except OSError as exc:
        raise ConfigError(f"{label} is unreadable: {type(exc).__name__}") from None
    try:
        data = json.loads(raw)
    except ValueError:
        raise ConfigError(f"{label} is not valid JSON") from None
    if not isinstance(data, dict):
        raise ConfigError(f"{label} must contain a JSON object")
    return data


def load_config(directory: str | Path) -> Config:
    """Load and validate settings.json + secrets.json from *directory*."""
    base = Path(directory)
    settings = _read_json(base / "settings.json", "settings.json")
    secrets = _read_json(base / "secrets.json", "secrets.json")

    for key, expected in _REQUIRED_SETTINGS.items():
        if key not in settings:
            raise ConfigError(f"settings.json is missing '{key}'")
        value = settings[key]
        if expected is int:
            if not isinstance(value, int) or isinstance(value, bool):
                raise ConfigError(f"settings.json '{key}' must be an integer")
        elif not isinstance(value, expected):
            raise ConfigError(f"settings.json '{key}' has the wrong type")
        # public_url stays empty until the tunnel domain is provisioned; that
        # only disables agent polling, it must not stop the local proxy.
        if key != "public_url" and isinstance(value, str) and not value.strip():
            raise ConfigError(f"settings.json '{key}' must not be empty")

    if "greeting_text" in settings:
        greeting = settings["greeting_text"]
        if not isinstance(greeting, str) or not greeting.strip() or len(greeting) > SPEECH_LIMIT:
            raise ConfigError("settings.json 'greeting_text' must contain 1 to 1000 characters")

    models = settings["allowed_models"]
    if not models or not all(isinstance(m, str) and m for m in models):
        raise ConfigError("settings.json 'allowed_models' must list model ids")
    port = settings["proxy_port"]
    if not (1 <= port <= 65535):
        raise ConfigError("settings.json 'proxy_port' is out of range")

    for key in _REQUIRED_SECRETS:
        if key not in secrets:
            raise ConfigError(f"secrets.json is missing '{key}'")
        if not isinstance(secrets[key], str) or not secrets[key]:
            raise ConfigError(f"secrets.json '{key}' must be a non-empty string")

    return Config(settings=settings, secrets=secrets, base_dir=base)


# ---------------------------------------------------------------------------
# application state
# ---------------------------------------------------------------------------


@dataclass
class Runtime:
    session: ClientSession
    semaphore: asyncio.Semaphore
    poll_task: asyncio.Task | None = None
    job_tasks: set[asyncio.Task] = field(default_factory=set)
    shutting_down: bool = False
    upstream_url: str = DEFAULT_UPSTREAM_URL
    bridge_url: str = ""
    poll_interval: float = POLL_IDLE_INTERVAL
    poll_error_backoff: float = POLL_ERROR_BACKOFF


STATE: web.AppKey[Runtime] = web.AppKey("state", Runtime)
CONFIG: web.AppKey[Config] = web.AppKey("config", Config)


@web.middleware
async def error_middleware(request: web.Request, handler):
    """Never let aiohttp echo a traceback (paths, secrets) to the caller."""
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except Exception as exc:
        log.error("request failed: %s", type(exc).__name__)
        return _error(500, "internal_error", "internal error")


def create_app(
    config: Config,
    *,
    upstream_url: str = DEFAULT_UPSTREAM_URL,
    bridge_url: str | None = None,
    enable_poll: bool = True,
    poll_interval: float = POLL_IDLE_INTERVAL,
    poll_error_backoff: float = POLL_ERROR_BACKOFF,
    transport: str = "poll",
) -> web.Application:
    app = web.Application(client_max_size=MAX_BODY_BYTES, middlewares=[error_middleware])
    app[CONFIG] = config
    resolved_bridge = (bridge_url or config.bridge_url).rstrip("/")

    app.router.add_get(HEALTH_PATH, handle_health)
    app.router.add_post("/v1/chat/completions", handle_chat)
    app.router.add_post(WEBHOOK_PREFIX + "{secret}", handle_webhook)

    async def _startup(app: web.Application) -> None:
        await _on_startup(
            app,
            upstream_url=upstream_url,
            bridge_url=resolved_bridge,
            enable_poll=enable_poll,
            poll_interval=poll_interval,
            poll_error_backoff=poll_error_backoff,
            transport=transport,
        )

    app.on_startup.append(_startup)
    app.on_cleanup.append(_on_cleanup)
    return app


async def _on_startup(
    app: web.Application,
    *,
    upstream_url: str,
    bridge_url: str,
    enable_poll: bool,
    poll_interval: float,
    poll_error_backoff: float,
    transport: str,
) -> None:
    # The webhook secret lives in the URL path, so neither aiohttp's access log
    # nor our own logs may ever see a request line.  run_app also disables the
    # access log, but a test runner or an embedding app may not.
    logging.getLogger("aiohttp.access").setLevel(logging.CRITICAL)

    session = ClientSession(
        timeout=ClientTimeout(total=None), trust_env=False, auto_decompress=True
    )
    runtime = Runtime(
        session=session,
        semaphore=asyncio.Semaphore(MAX_CONCURRENT_JOBS),
        upstream_url=upstream_url,
        bridge_url=bridge_url,
        poll_interval=poll_interval,
        poll_error_backoff=poll_error_backoff,
    )
    app[STATE] = runtime
    if enable_poll and not app[CONFIG].public_url:
        log.warning("agent polling disabled: public_url is not configured")
    elif enable_poll and transport == "websocket":
        runtime.poll_task = asyncio.create_task(ws_loop(runtime, app[CONFIG]))
    elif enable_poll:
        runtime.poll_task = asyncio.create_task(poll_loop(runtime, app[CONFIG]))


async def _on_cleanup(app: web.Application) -> None:
    runtime = app[STATE]
    runtime.shutting_down = True
    if runtime.poll_task is not None:
        runtime.poll_task.cancel()
        try:
            await runtime.poll_task
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # a crashed loop must not block shutdown
            log.debug("poll task ended with %s", type(exc).__name__)
    for task in list(runtime.job_tasks):
        task.cancel()
    if runtime.job_tasks:
        await asyncio.gather(*runtime.job_tasks, return_exceptions=True)
        runtime.job_tasks.clear()
    if not runtime.session.closed:
        await runtime.session.close()


# ---------------------------------------------------------------------------
# handlers
# ---------------------------------------------------------------------------


async def handle_health(request: web.Request) -> web.Response:
    return web.Response(text="ok", content_type="text/plain")


def _error(status: int, kind: str, message: str) -> web.Response:
    return web.json_response(
        {"error": {"type": kind, "message": message}}, status=status
    )


def _bearer_matches(header: str, expected: str) -> bool:
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer":
        return False
    return hmac.compare_digest(token.strip().encode("utf-8"), expected.encode("utf-8"))


def thinking_override(model: str) -> dict[str, str] | None:
    """Per-model thinking flag required by OpenCode Go.

    GLM Flash and DeepSeek Flash get an explicit flag so short voice replies
    do not exhaust the output budget on unspoken reasoning. ``glm-5.2`` keeps
    its previous behavior; all other models are forwarded untouched.
    """
    if model in ("glm-5.3-flash", "deepseek-v4.1-flash"):
        return {"type": "disabled"}
    return None


_SENTENCE_END = re.compile(r"[.!?…](?:\s|$)")

def cap_speech(text: str, limit: int = SPEECH_LIMIT) -> str:
    """Trim *text* to at most *limit* chars, preferring a sentence boundary."""
    text = text.strip()
    if len(text) <= limit:
        return text
    window = text[:limit]
    boundary = -1
    for match in _SENTENCE_END.finditer(window):
        if match.end() >= limit // 3:
            boundary = match.end()
    if boundary > 0:
        return window[:boundary].strip()
    cut = window.rstrip()
    space = cut.rfind(" ")
    if space > limit // 2:
        cut = cut[:space]
    return cut.rstrip()


async def handle_chat(request: web.Request) -> web.Response:
    started = time.monotonic()
    config: Config = request.app[CONFIG]
    runtime: Runtime = request.app[STATE]

    if request.query_string:
        return _error(400, "bad_request", "query strings are not supported")
    if not _bearer_matches(request.headers.get("Authorization", ""), config.secret("proxy_key")):
        log.warning("chat rejected: unauthorized")
        return _error(401, "unauthorized", "invalid API key")

    raw = await request.read()
    if len(raw) > MAX_BODY_BYTES:
        return _error(413, "payload_too_large", "request body is too large")
    try:
        payload = json.loads(raw)
    except ValueError:
        return _error(400, "bad_request", "body must be JSON")
    if not isinstance(payload, dict):
        return _error(400, "bad_request", "body must be a JSON object")

    model = payload.get("model")
    if not isinstance(model, str) or model not in config.allowed_models:
        log.warning("chat rejected: model not allowed")
        return _error(400, "model_not_allowed", "requested model is not allowed")
    if payload.get("stream"):
        return _error(400, "stream_not_supported", "streaming is not supported")
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        return _error(400, "bad_request", "messages must be a non-empty list")

    upstream_payload = dict(payload)
    override = thinking_override(model)
    if override is not None:
        upstream_payload["thinking"] = override

    headers = {
        "Authorization": f"Bearer {config.secret('opencode_go_key')}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "x-opencode-session": str(config.settings["session_id"]),
    }
    try:
        async with runtime.session.post(
            runtime.upstream_url,
            json=upstream_payload,
            headers=headers,
            timeout=ClientTimeout(total=UPSTREAM_TIMEOUT),
            allow_redirects=False,
        ) as response:
            status = response.status
            body = await response.read()
    except asyncio.TimeoutError:
        log.warning("chat upstream timeout after %.0fs", UPSTREAM_TIMEOUT)
        return _error(502, "upstream_unavailable", "upstream request failed")
    except ClientError as exc:
        log.warning("chat upstream transport error: %s", type(exc).__name__)
        return _error(502, "upstream_unavailable", "upstream request failed")

    elapsed_ms = int((time.monotonic() - started) * 1000)
    if status != 200:
        log.warning("chat upstream status=%s in %dms", status, elapsed_ms)
        return _error(502, "upstream_error", f"upstream returned status {status}")

    try:
        data = json.loads(body)
        message = data["choices"][0]["message"]
        content = message.get("content")
    except (ValueError, KeyError, IndexError, TypeError):
        log.warning("chat upstream invalid body in %dms", elapsed_ms)
        return _error(502, "invalid_upstream_response", "upstream returned no usable answer")

    if not isinstance(content, str) or not content.strip():
        log.warning("chat upstream empty content in %dms", elapsed_ms)
        return _error(502, "empty_completion", "model returned no answer text")

    message["content"] = cap_speech(content)
    log.info("chat ok model=%s in %dms", model, elapsed_ms)
    return web.json_response(data)


UPSTREAM_GREETING = "Привет! Я мост к нейросетям. Задай любой вопрос или скажи: помощь."


def override_greeting(config: Config, request_body: bytes, response_body: bytes) -> bytes:
    """Customize only an already authorized empty-session Rust greeting.

    Never fabricate a response before the Rust allowlist check, and never
    rewrite refusals, answers, help, malformed payloads or ongoing sessions.
    """
    greeting = config.settings.get("greeting_text")
    if not isinstance(greeting, str) or not greeting.strip():
        return response_body
    try:
        request = json.loads(request_body)
        if request["session"]["new"] is not True:
            return response_body
        utterance = request["request"]["original_utterance"]
        if not isinstance(utterance, str) or utterance.strip():
            return response_body
        reply = json.loads(response_body)
        speech = reply["response"]
        if speech.get("end_session") is not False or speech.get("text") != UPSTREAM_GREETING:
            return response_body
        speech["text"] = greeting
        speech["tts"] = greeting
        return json.dumps(reply, ensure_ascii=False).encode("utf-8")
    except (ValueError, KeyError, TypeError, AttributeError):
        return response_body


async def handle_webhook(request: web.Request) -> web.Response:
    started = time.monotonic()
    config: Config = request.app[CONFIG]
    runtime: Runtime = request.app[STATE]
    secret = request.match_info.get("secret", "")

    # Hide the endpoint entirely for anything but the exact configured path.
    if request.query_string or not hmac.compare_digest(
        secret.encode("utf-8"), config.secret("webhook_secret").encode("utf-8")
    ):
        log.warning("webhook rejected: unknown route")
        return web.Response(status=404, text="not found")

    if request.content_length is not None and request.content_length > MAX_BODY_BYTES:
        return _error(413, "payload_too_large", "request body is too large")
    raw = await request.read()
    if len(raw) > MAX_BODY_BYTES:
        return _error(413, "payload_too_large", "request body is too large")

    url = runtime.bridge_url + WEBHOOK_PREFIX + secret
    try:
        async with runtime.session.post(
            url,
            data=raw,
            headers={"Content-Type": "application/json"},
            timeout=ClientTimeout(total=BRIDGE_TIMEOUT),
            allow_redirects=False,
        ) as response:
            status = response.status
            body = await response.read()
            content_type = response.headers.get("Content-Type", "application/json")
    except asyncio.TimeoutError:
        log.warning("webhook bridge timeout after %.0fs", BRIDGE_TIMEOUT)
        return _error(502, "bridge_unavailable", "bridge request failed")
    except ClientError as exc:
        log.warning("webhook bridge transport error: %s", type(exc).__name__)
        return _error(502, "bridge_unavailable", "bridge request failed")

    elapsed_ms = int((time.monotonic() - started) * 1000)
    if status >= 400:
        log.warning("webhook bridge status=%s in %dms", status, elapsed_ms)
        return _error(502, "bridge_error", "bridge request failed")

    if status == 200:
        body = override_greeting(config, raw, body)
    log.info("webhook ok status=%s in %dms", status, elapsed_ms)
    return web.Response(body=body, status=status, headers={"Content-Type": content_type})


# ---------------------------------------------------------------------------
# outgoing agent relay
# ---------------------------------------------------------------------------

JOB_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
AGENT_POLL_PATH = "/_agent/poll"
AGENT_RESULT_PATH = "/_agent/result"


@dataclass(frozen=True)
class RelayRequest:
    job_id: str
    method: str
    path: str
    body: bytes


def parse_job(config: Config, job: Any) -> RelayRequest:
    """Validate a polled job.  Only the two bridge routes are addressable."""
    if not isinstance(job, dict):
        raise JobError("job must be an object")
    job_id = job.get("id")
    if not isinstance(job_id, str) or not JOB_ID_RE.match(job_id):
        raise JobError("invalid job id")

    method = job.get("method")
    if method not in ("GET", "POST"):
        raise JobError("method not allowed", job_id=job_id)

    path = job.get("path")
    if not isinstance(path, str) or not path.startswith("/"):
        raise JobError("path must start with /", job_id=job_id)
    if "?" in path or "#" in path or "//" in path or "\\" in path:
        raise JobError("query strings, fragments and joins are not allowed", job_id=job_id)
    if ".." in path:
        raise JobError("relative segments are not allowed", job_id=job_id)

    webhook_path = WEBHOOK_PREFIX + config.secret("webhook_secret")
    if method == "GET":
        if path != HEALTH_PATH:
            raise JobError("path not allowed", job_id=job_id)
    else:
        if path != webhook_path:
            raise JobError("path not allowed", job_id=job_id)

    raw_body = job.get("body", "")
    if raw_body is None:
        raw_body = ""
    if not isinstance(raw_body, str):
        raise JobError("body must be base64 text", job_id=job_id)
    try:
        body = base64.b64decode(raw_body, validate=True) if raw_body else b""
    except (binascii.Error, ValueError):
        raise JobError("body is not valid base64", job_id=job_id) from None
    if len(body) > MAX_BODY_BYTES:
        raise JobError("body too large", job_id=job_id)
    if method == "GET" and body:
        raise JobError("GET jobs must not carry a body", job_id=job_id)

    return RelayRequest(job_id=job_id, method=method, path=path, body=body)


def _local_result(job_id: str, status: int, body: bytes, content_type: str) -> dict:
    return {
        "id": job_id,
        "status": status,
        "headers": [["content-type", content_type]],
        "body": base64.b64encode(body).decode("ascii"),
    }


def _rejection_result(job_id: str, status: int) -> dict:
    payload = json.dumps({"error": {"type": "job_rejected"}}).encode("utf-8")
    return _local_result(job_id, status, payload, "application/json")


async def relay_job(runtime: Runtime, config: Config, job: Any) -> dict:
    """Execute one whitelisted job against the local bridge (no retries)."""
    parsed = parse_job(config, job)
    url = runtime.bridge_url + parsed.path
    headers = {"Content-Type": "application/json"} if parsed.method == "POST" else {}
    try:
        async with runtime.session.request(
            parsed.method,
            url,
            data=parsed.body if parsed.method == "POST" else None,
            headers=headers,
            timeout=ClientTimeout(total=BRIDGE_TIMEOUT),
            allow_redirects=False,
        ) as response:
            status = response.status
            body = await response.read()
            content_type = response.headers.get("Content-Type", "application/json")
    except (asyncio.TimeoutError, ClientError) as exc:
        log.warning("relay bridge error: %s", type(exc).__name__)
        return _rejection_result(parsed.job_id, 502)

    if status >= 400:
        log.warning("relay bridge status=%s job=%s", status, parsed.job_id)
        return _rejection_result(parsed.job_id, 502)
    if status == 200 and parsed.method == "POST":
        body = override_greeting(config, parsed.body, body)
    if len(body) > RELAY_BODY_LIMIT:
        log.warning("relay bridge body too large job=%s", parsed.job_id)
        return _rejection_result(parsed.job_id, 502)
    return _local_result(parsed.job_id, status, body, content_type)


async def upload_result(runtime: Runtime, config: Config, result: dict) -> bool:
    """Upload a job result, retrying at most once (two attempts total)."""
    url = config.public_url + AGENT_RESULT_PATH
    headers = {
        "Authorization": f"Bearer {config.secret('agent_key')}",
        "Content-Type": "application/json",
    }
    for attempt in (1, 2):
        try:
            async with runtime.session.post(
                url,
                json=result,
                headers=headers,
                timeout=ClientTimeout(total=RESULT_TIMEOUT),
                proxy=config.settings.get("relay_proxy_url") or None,
                allow_redirects=False,
            ) as response:
                if 200 <= response.status < 300:
                    return True
                log.warning("result upload status=%s attempt=%d", response.status, attempt)
        except asyncio.TimeoutError:
            log.warning("result upload timeout attempt=%d", attempt)
        except ClientError as exc:
            log.warning("result upload error=%s attempt=%d", type(exc).__name__, attempt)
        if attempt == 1:
            await asyncio.sleep(RESULT_RETRY_DELAY)
    return False


async def handle_batch(runtime: Runtime, config: Config, jobs: list, send=None) -> None:
    """Run up to eight jobs concurrently (bounded by the shared semaphore).

    ``send`` delivers one result: an HTTP upload for a polled batch, a socket
    frame for an agent socket.
    """

    if send is None:
        async def send(result: dict) -> None:
            await upload_result(runtime, config, result)

    async def run(job: Any) -> None:
        async with runtime.semaphore:
            try:
                result = await relay_job(runtime, config, job)
            except JobError as exc:
                log.warning("relay rejected job: %s", exc.reason)
                if not exc.job_id:
                    return
                result = _rejection_result(exc.job_id, 400)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # never let one bad job kill the batch
                log.warning("relay unexpected failure: %s", type(exc).__name__)
                job_id = job.get("id") if isinstance(job, dict) else None
                if not isinstance(job_id, str) or not JOB_ID_RE.match(job_id):
                    return
                result = _rejection_result(job_id, 500)
            await send(result)

    await asyncio.gather(*(run(job) for job in jobs[:MAX_JOBS_PER_BATCH]))


async def poll_once(runtime: Runtime, config: Config) -> dict:
    url = config.public_url + AGENT_POLL_PATH
    headers = {
        "Authorization": f"Bearer {config.secret('agent_key')}",
        PROTOCOL_HEADER: PROTOCOL_VERSION,
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    async with runtime.session.get(
        url,
        headers=headers,
        timeout=ClientTimeout(total=POLL_TIMEOUT),
        proxy=config.settings.get("relay_proxy_url") or None,
        allow_redirects=False,
    ) as response:
        if response.status != 200:
            raise RuntimeError(f"poll status {response.status}")
        payload = await response.json(content_type=None)
    if not isinstance(payload, dict):
        raise RuntimeError("poll payload is not an object")
    return payload


async def poll_loop(runtime: Runtime, config: Config) -> None:
    """Long-poll the agent endpoint until the runner shuts down."""
    while not runtime.shutting_down:
        try:
            payload = await poll_once(runtime, config)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("poll failed: %s", type(exc).__name__)
            await asyncio.sleep(runtime.poll_error_backoff)
            continue

        if payload.get("idle") is True:
            await asyncio.sleep(runtime.poll_interval)
            continue

        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            log.warning("poll payload has no job list")
            await asyncio.sleep(runtime.poll_error_backoff)
            continue
        if not jobs:
            await asyncio.sleep(runtime.poll_interval)
            continue
        try:
            await handle_batch(runtime, config, jobs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("batch failed: %s", type(exc).__name__)
            await asyncio.sleep(runtime.poll_error_backoff)


def relay_socket_url(config: Config) -> str:
    """Map the public HTTPS relay address onto its agent socket."""
    base = config.public_url
    for scheme, socket_scheme in (("https://", "wss://"), ("http://", "ws://")):
        if base.startswith(scheme):
            return socket_scheme + base[len(scheme):].rstrip("/") + AGENT_WS_PATH
    raise RuntimeError("public_url must start with http:// or https://")


def socket_sender(socket: Any):
    async def send(result: dict) -> None:
        try:
            await socket.send_json(result)
        except Exception as exc:  # a dead socket must not kill the job task
            log.warning("socket result upload failed: %s", type(exc).__name__)

    return send


def dispatch_socket_job(runtime: Runtime, config: Config, socket: Any, job: dict) -> None:
    """Relay one socket job without blocking the reader of the next frame."""
    task = asyncio.create_task(handle_batch(runtime, config, [job], socket_sender(socket)))
    runtime.job_tasks.add(task)
    task.add_done_callback(runtime.job_tasks.discard)


def dispatch_message(runtime: Runtime, config: Config, socket: Any, text: str) -> None:
    try:
        job = json.loads(text)
    except ValueError:
        log.warning("socket frame is not json")
        return
    if not isinstance(job, dict) or job.get("type") in {"hello", "ping", "pong"}:
        return
    dispatch_socket_job(runtime, config, socket, job)


async def ws_loop(runtime: Runtime, config: Config) -> None:
    """Hold the hibernatable agent socket until shutdown."""
    url = relay_socket_url(config)
    backoff = WS_BACKOFF_START
    while not runtime.shutting_down:
        try:
            async with runtime.session.ws_connect(
                url,
                headers={
                    "Authorization": f"Bearer {config.secret('agent_key')}",
                    "User-Agent": USER_AGENT,
                },
                heartbeat=WS_HEARTBEAT,
                max_msg_size=WS_MAX_MESSAGE,
                proxy=config.settings.get("relay_proxy_url") or None,
            ) as socket:
                log.info("relay socket connected")
                backoff = WS_BACKOFF_START
                async for message in socket:
                    if message.type is WSMsgType.TEXT:
                        dispatch_message(runtime, config, socket, message.data)
                    elif message.type is WSMsgType.ERROR:
                        raise ConnectionError("socket reported an error")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("socket connection failed: %s", type(exc).__name__)
        if runtime.shutting_down:
            break
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, WS_BACKOFF_MAX)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        config = load_config(Path(__file__).resolve().parent)
    except ConfigError as exc:
        log.error("configuration error: %s", exc)
        raise SystemExit(1) from None
    app = create_app(config, transport=config.settings.get("relay_transport", "poll"))
    web.run_app(
        app,
        host=config.settings["proxy_listen"],
        port=config.settings["proxy_port"],
        access_log=None,
        print=None,
    )


if __name__ == "__main__":
    main()
