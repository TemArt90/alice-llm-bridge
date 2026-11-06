"""Tests for the Alice/OpenCode Go sidecar.

All HTTP traffic here is local: fake upstream, fake bridge and fake Cloudflare
agent endpoints are spun up in-process.  No external API calls are made.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import json
import os
import tempfile
import unittest
from pathlib import Path

from aiohttp import ClientSession, web
from aiohttp.test_utils import TestClient, TestServer

import sidecar


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def make_config(tmpdir: str, **settings_overrides) -> "sidecar.Config":
    settings = {
        "public_url": "https://alice.example.test",
        "bridge_url": "http://127.0.0.1:18090",
        "proxy_listen": "127.0.0.1",
        "proxy_port": 18091,
        "allowed_models": ["glm-5.3-flash", "glm-5.2"],
        "session_id": "3f5c1d2e-0000-4000-8000-000000000001",
    }
    settings.update(settings_overrides)
    secrets = {
        "agent_key": "agent-key-test-value",
        "webhook_secret": "webhook-secret-test-value",
        "proxy_key": "proxy-key-test-value",
        "opencode_go_key": "go-key-test-value",
    }
    directory = Path(tmpdir)
    (directory / "settings.json").write_text(json.dumps(settings), encoding="utf-8")
    (directory / "secrets.json").write_text(json.dumps(secrets), encoding="utf-8")
    return sidecar.load_config(directory)


class FakeServer:
    """Tiny aiohttp server that records requests and replies with a fixture."""

    def __init__(self, routes):
        self.routes = routes
        self.requests = []
        self.server = None

    async def handler(self, request: web.Request):
        raw = await request.read()
        record = {
            "method": request.method,
            "path": request.path,
            "query": request.query_string,
            "headers": dict(request.headers),
            "raw": raw,
            "json": None,
        }
        if raw:
            try:
                record["json"] = json.loads(raw)
            except ValueError:
                pass
        self.requests.append(record)
        route = self.routes.get((request.method, request.path))
        if route is None:
            return web.Response(status=404, text="not found")
        if asyncio.iscoroutinefunction(route):
            return await route(request, record)
        status, body, content_type = route
        if isinstance(body, (dict, list)):
            return web.json_response(body, status=status)
        return web.Response(status=status, text=body, content_type=content_type)

    async def __aenter__(self):
        app = web.Application()
        for method, path in self.routes:
            app.router.add_route(method, path, self.handler)
        # a catch-all so unexpected paths are recorded, not 405'd.
        app.router.add_route("GET", "/{tail:.*}", self.handler)
        app.router.add_route("POST", "/{tail:.*}", self.handler)
        self.server = TestServer(app)
        await self.server.start_server()
        return self

    async def __aexit__(self, *exc):
        await self.server.close()

    @property
    def base_url(self) -> str:
        return str(self.server.make_url("")).rstrip("/")


class SidecarTestCase(unittest.IsolatedAsyncioTestCase):
    extra_settings: dict = {}

    async def asyncSetUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.config = make_config(self._tmp.name, **self.extra_settings)
        self.http = ClientSession()
        self.addCleanup(self.http.close)

    async def start_sidecar(self, **kwargs):
        kwargs.setdefault("enable_poll", False)
        app = sidecar.create_app(self.config, **kwargs)
        self.client = TestClient(TestServer(app))
        await self.client.start_server()
        self.addCleanup(self.client.close)
        return self.client


# --------------------------------------------------------------------------
# cycle 1: configuration and health
# --------------------------------------------------------------------------


class TestConfig(unittest.TestCase):
    def test_load_config_reads_settings_and_secrets(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
        self.assertEqual(config.settings["bridge_url"], "http://127.0.0.1:18090")
        self.assertEqual(config.settings["proxy_port"], 18091)
        self.assertEqual(config.settings["allowed_models"], ["glm-5.3-flash", "glm-5.2"])
        self.assertEqual(config.secrets["proxy_key"], "proxy-key-test-value")

    def test_load_config_allows_empty_public_url_before_tunnel_setup(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp, public_url="")
        self.assertEqual(config.public_url, "")

    def test_load_config_rejects_missing_secret_without_echoing_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = make_config(tmp)
            secrets_path = Path(tmp) / "secrets.json"
            secrets = json.loads(secrets_path.read_text(encoding="utf-8"))
            del secrets["opencode_go_key"]
            secrets_path.write_text(json.dumps(secrets), encoding="utf-8")
            with self.assertRaises(sidecar.ConfigError) as ctx:
                sidecar.load_config(tmp)
        message = str(ctx.exception)
        self.assertIn("opencode_go_key", message)
        self.assertNotIn("agent-key-test-value", message)


class TestHealth(SidecarTestCase):
    async def test_health_returns_ok_without_auth(self):
        client = await self.start_sidecar()
        response = await client.get("/health")
        self.assertEqual(response.status, 200)
        self.assertEqual((await response.text()).strip(), "ok")

    async def test_unknown_path_is_not_found(self):
        client = await self.start_sidecar()
        response = await client.get("/nope")
        self.assertEqual(response.status, 404)


# --------------------------------------------------------------------------
# cycle 2: chat completions proxy
# --------------------------------------------------------------------------

GOOD_COMPLETION = {
    "id": "cmpl-test-1",
    "object": "chat.completion",
    "model": "glm-5.3-flash",
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": "Готово: функция написана."},
            "finish_reason": "stop",
        }
    ],
    "usage": {"prompt_tokens": 42, "completion_tokens": 9, "total_tokens": 51},
}


class ChatProxyTestCase(SidecarTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.upstream = FakeServer(
            {("POST", "/v1/chat/completions"): (200, GOOD_COMPLETION, "application/json")}
        )
        await self.upstream.__aenter__()
        self.addAsyncCleanup(self.upstream.__aexit__, None, None, None)
        await self.start_sidecar(
            upstream_url=self.upstream.base_url + "/v1/chat/completions"
        )

    def payload(self, **overrides):
        body = {
            "model": "glm-5.3-flash",
            "messages": [
                {"role": "system", "content": "You help with coding."},
                {"role": "user", "content": "Напиши fizzbuzz на Python"},
            ],
            "max_tokens": 300,
            "temperature": 0.3,
        }
        body.update(overrides)
        return body

    async def post_chat(self, body=None, headers=None, raw=None):
        request_headers = {
            "Authorization": "Bearer proxy-key-test-value",
            "Content-Type": "application/json",
        }
        if headers:
            request_headers.update(headers)
        data = raw if raw is not None else json.dumps(body or self.payload())
        return await self.client.post(
            "/v1/chat/completions", data=data, headers=request_headers
        )


class TestChatAuth(ChatProxyTestCase):
    async def test_missing_authorization_is_rejected_without_upstream_call(self):
        response = await self.post_chat(headers={"Authorization": ""})
        self.assertEqual(response.status, 401)
        self.assertEqual(self.upstream.requests, [])

    async def test_wrong_key_is_rejected_without_upstream_call(self):
        response = await self.post_chat(headers={"Authorization": "Bearer nope"})
        self.assertEqual(response.status, 401)
        self.assertEqual(self.upstream.requests, [])


class TestChatForwarding(ChatProxyTestCase):
    async def test_cloudflare_proxy_is_not_used_for_model_requests(self):
        self.client.app[sidecar.CONFIG] = dataclasses.replace(
            self.config, settings={**self.config.settings, 'relay_proxy_url': 'http://127.0.0.1:1'})
        response = await self.post_chat()
        self.assertEqual(response.status, 200)
        self.assertEqual(len(self.upstream.requests), 1)

    async def test_valid_request_returns_content_and_preserves_usage(self):
        response = await self.post_chat()
        self.assertEqual(response.status, 200)
        body = await response.json()
        self.assertEqual(
            body["choices"][0]["message"]["content"], "Готово: функция написана."
        )
        self.assertEqual(body["usage"], GOOD_COMPLETION["usage"])

    async def test_upstream_call_uses_required_headers_and_disables_thinking(self):
        await self.post_chat()
        self.assertEqual(len(self.upstream.requests), 1)
        sent = self.upstream.requests[0]
        self.assertEqual(sent["headers"].get("User-Agent"), "alice-code-bridge/1.0")
        self.assertEqual(
            sent["headers"].get("x-opencode-session"),
            "3f5c1d2e-0000-4000-8000-000000000001",
        )
        self.assertEqual(
            sent["headers"].get("Authorization"), "Bearer go-key-test-value"
        )
        self.assertEqual(sent["json"]["model"], "glm-5.3-flash")
        self.assertEqual(sent["json"]["temperature"], 0.3)
        self.assertEqual(sent["json"]["max_tokens"], 300)
        self.assertEqual(sent["json"]["thinking"], {"type": "disabled"})
        self.assertEqual(
            sent["json"]["messages"][1]["content"], "Напиши fizzbuzz на Python"
        )

    async def test_glm_5_2_omits_thinking_field(self):
        await self.post_chat(self.payload(model="glm-5.2"))
        sent = self.upstream.requests[0]
        self.assertEqual(sent["json"]["model"], "glm-5.2")
        self.assertNotIn("thinking", sent["json"])

    async def test_other_models_get_no_thinking_override(self):
        self.assertIsNone(sidecar.thinking_override("kimi-k2.7-code"))
        self.assertIsNone(sidecar.thinking_override("glm-5.2"))
        self.assertEqual(sidecar.thinking_override("glm-5.3-flash"), {"type": "disabled"})

    async def test_session_id_is_stable_across_requests(self):
        await self.post_chat()
        await self.post_chat()
        sessions = [r["headers"].get("x-opencode-session") for r in self.upstream.requests]
        self.assertEqual(len(sessions), 2)
        self.assertEqual(sessions[0], sessions[1])

    async def test_model_outside_allowlist_is_rejected_without_upstream_call(self):
        response = await self.post_chat(self.payload(model="gpt-4o"))
        self.assertEqual(response.status, 400)
        self.assertEqual(self.upstream.requests, [])

    async def test_oversized_body_is_rejected_without_upstream_call(self):
        raw = b'{"model":"glm-5.3-flash","pad":"' + b"x" * (64 * 1024) + b'"}'
        response = await self.post_chat(raw=raw)
        self.assertEqual(response.status, 413)
        self.assertEqual(self.upstream.requests, [])

    async def test_query_string_is_rejected(self):
        response = await self.client.post(
            "/v1/chat/completions?target=http://evil.test",
            data=json.dumps(self.payload()),
            headers={"Authorization": "Bearer proxy-key-test-value"},
        )
        self.assertEqual(response.status, 400)
        self.assertEqual(self.upstream.requests, [])

    async def test_incoming_body_cannot_redirect_upstream_endpoint(self):
        evil = FakeServer({("POST", "/v1/chat/completions"): (200, GOOD_COMPLETION, "application/json")})
        async with evil:
            body = self.payload(
                url=evil.base_url + "/v1/chat/completions",
                base_url=evil.base_url,
                endpoint=evil.base_url,
                upstream_url=evil.base_url,
            )
            response = await self.post_chat(body)
        self.assertEqual(response.status, 200)
        self.assertEqual(evil.requests, [])
        self.assertEqual(len(self.upstream.requests), 1)


# --------------------------------------------------------------------------
# cycle 3: failure containment, no retries, speech cap
# --------------------------------------------------------------------------


class TestChatFailures(ChatProxyTestCase):
    def set_upstream(self, status, body, content_type="application/json"):
        self.upstream.routes[("POST", "/v1/chat/completions")] = (
            status,
            body,
            content_type,
        )

    async def test_upstream_error_body_is_not_leaked_to_client_or_logs(self):
        self.set_upstream(
            500,
            {"error": "internal", "detail": "SECRET-UPSTREAM-TRACE sk-live-DEADBEEF"},
        )
        with self.assertLogs("alice-sidecar", level="DEBUG") as captured:
            response = await self.post_chat()
        text = await response.text()
        self.assertEqual(response.status, 502)
        for marker in ("SECRET-UPSTREAM-TRACE", "DEADBEEF", "sk-live"):
            self.assertNotIn(marker, text)
            self.assertNotIn(marker, "\n".join(captured.output))

    async def test_upstream_error_is_not_retried(self):
        self.set_upstream(500, {"error": "boom"})
        await self.post_chat()
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.upstream.requests), 1)

    async def test_invalid_json_from_upstream_is_rejected_without_leak(self):
        self.set_upstream(200, "<html>SECRET-UPSTREAM-TRACE</html>", "text/html")
        response = await self.post_chat()
        self.assertEqual(response.status, 502)
        self.assertNotIn("SECRET-UPSTREAM-TRACE", await response.text())

    async def test_empty_content_is_rejected_cleanly(self):
        body = json.loads(json.dumps(GOOD_COMPLETION))
        body["choices"][0]["message"]["content"] = "   \n "
        self.set_upstream(200, body)
        response = await self.post_chat()
        self.assertEqual(response.status, 502)
        self.assertEqual((await response.json())["error"]["type"], "empty_completion")

    async def test_reasoning_only_answer_is_rejected_without_echoing_reasoning(self):
        body = json.loads(json.dumps(GOOD_COMPLETION))
        body["choices"][0]["message"]["content"] = ""
        body["choices"][0]["message"]["reasoning_content"] = "SECRET-REASONING-TRACE"
        self.set_upstream(200, body)
        response = await self.post_chat()
        self.assertEqual(response.status, 502)
        self.assertNotIn("SECRET-REASONING-TRACE", await response.text())

    async def test_chat_logs_never_contain_api_keys(self):
        with self.assertLogs("alice-sidecar", level="DEBUG") as captured:
            await self.post_chat()
        output = "\n".join(captured.output)
        self.assertNotIn("go-key-test-value", output)
        self.assertNotIn("proxy-key-test-value", output)

    async def test_non_list_messages_are_rejected_without_upstream_call(self):
        response = await self.post_chat(self.payload(messages="hello"))
        self.assertEqual(response.status, 400)
        self.assertEqual(self.upstream.requests, [])


class TestSpeechCap(ChatProxyTestCase):
    async def test_long_answer_is_capped_at_sentence_boundary_and_usage_kept(self):
        sentence = "Это предложение про код. "
        long_text = sentence * 80
        body = json.loads(json.dumps(GOOD_COMPLETION))
        body["choices"][0]["message"]["content"] = long_text
        body["usage"] = {"prompt_tokens": 10, "completion_tokens": 400, "total_tokens": 410}
        self.upstream.routes[("POST", "/v1/chat/completions")] = (
            200,
            body,
            "application/json",
        )
        response = await self.post_chat()
        self.assertEqual(response.status, 200)
        data = await response.json()
        content = data["choices"][0]["message"]["content"]
        self.assertLessEqual(len(content), 1000)
        self.assertTrue(content.endswith("."))
        self.assertEqual(data["usage"], body["usage"])

    def test_cap_speech_leaves_short_text_untouched(self):
        self.assertEqual(sidecar.cap_speech("Короткий ответ."), "Короткий ответ.")

    def test_cap_speech_hard_cuts_unbroken_text(self):
        capped = sidecar.cap_speech("x" * 5000)
        self.assertEqual(len(capped), 1000)


# --------------------------------------------------------------------------
# cycle 4: webhook front door
# --------------------------------------------------------------------------

ALICE_REPLY = {
    "response": {"text": "Привет! Я мост.", "end_session": False},
    "version": "1.0",
}
WEBHOOK_PATH = "/alice/webhook/webhook-secret-test-value"


class WebhookTestCase(SidecarTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.bridge = FakeServer(
            {("POST", WEBHOOK_PATH): (200, ALICE_REPLY, "application/json")}
        )
        await self.bridge.__aenter__()
        self.addAsyncCleanup(self.bridge.__aexit__, None, None, None)
        await self.start_sidecar(bridge_url=self.bridge.base_url)

    def alice_body(self):
        return {
            "meta": {"locale": "ru-RU", "timezone": "Europe/Moscow"},
            "session": {
                "message_id": 1,
                "session_id": "s-1",
                "skill_id": "skill",
                "new": False,
                "application": {"application_id": "app"},
            },
            "request": {
                "command": "привет",
                "original_utterance": "привет",
                "type": "SimpleUtterance",
            },
            "version": "1.0",
        }


class TestWebhookForwarding(WebhookTestCase):
    async def test_wrong_secret_is_hidden_and_not_forwarded(self):
        response = await self.client.post(
            "/alice/webhook/wrong-secret",
            data=json.dumps(self.alice_body()),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status, 404)
        self.assertEqual(self.bridge.requests, [])

    async def test_valid_webhook_is_forwarded_and_reply_relayed(self):
        response = await self.client.post(
            WEBHOOK_PATH,
            data=json.dumps(self.alice_body()),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(await response.json(), ALICE_REPLY)
        self.assertEqual(len(self.bridge.requests), 1)
        sent = self.bridge.requests[0]
        self.assertEqual(sent["json"]["request"]["original_utterance"], "привет")
        self.assertEqual(sent["headers"].get("Content-Type"), "application/json")

    async def test_hop_headers_cookies_and_auth_are_not_forwarded(self):
        await self.client.post(
            WEBHOOK_PATH,
            data=json.dumps(self.alice_body()),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer should-not-leak",
                "Cookie": "session=should-not-leak",
                "X-Forwarded-For": "203.0.113.9",
            },
        )
        sent_headers = self.bridge.requests[0]["headers"]
        for name in ("Authorization", "Cookie", "X-Forwarded-For"):
            self.assertNotIn(name, sent_headers)

    async def test_webhook_query_string_is_rejected(self):
        response = await self.client.post(
            WEBHOOK_PATH + "?x=1",
            data=json.dumps(self.alice_body()),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status, 404)
        self.assertEqual(self.bridge.requests, [])

    async def test_oversized_webhook_body_is_rejected(self):
        response = await self.client.post(
            WEBHOOK_PATH,
            data=b"x" * (64 * 1024 + 10),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status, 413)
        self.assertEqual(self.bridge.requests, [])

    async def test_bridge_error_is_sanitized(self):
        self.bridge.routes[("POST", WEBHOOK_PATH)] = (
            500,
            {"detail": "SECRET-BRIDGE-TRACE"},
            "application/json",
        )
        response = await self.client.post(
            WEBHOOK_PATH,
            data=json.dumps(self.alice_body()),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status, 502)
        self.assertNotIn("SECRET-BRIDGE-TRACE", await response.text())

    async def test_secret_is_not_logged(self):
        with self.assertLogs("alice-sidecar", level="DEBUG") as captured:
            await self.client.post(
                WEBHOOK_PATH,
                data=json.dumps(self.alice_body()),
                headers={"Content-Type": "application/json"},
            )
        self.assertNotIn("webhook-secret-test-value", "\n".join(captured.output))


# --------------------------------------------------------------------------
# cycle 5: agent relay (outgoing poll) and SSRF containment
# --------------------------------------------------------------------------


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


class RelayTestCase(SidecarTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.bridge = FakeServer(
            {
                ("GET", "/health"): (200, "ok", "text/plain"),
                ("POST", WEBHOOK_PATH): (200, ALICE_REPLY, "application/json"),
            }
        )
        await self.bridge.__aenter__()
        self.addAsyncCleanup(self.bridge.__aexit__, None, None, None)
        await self.start_sidecar(enable_poll=False, bridge_url=self.bridge.base_url)
        self.runtime = self.client.server.app[sidecar.STATE]

    def job(self, **overrides):
        job = {
            "id": "job-1",
            "method": "POST",
            "path": WEBHOOK_PATH,
            "headers": [["content-type", "application/json"]],
            "body": b64(json.dumps({"request": {"original_utterance": "привет"}}).encode()),
        }
        job.update(overrides)
        return job


class TestRelayValidation(RelayTestCase):
    async def assert_rejected(self, job):
        with self.assertRaises(sidecar.JobError):
            await sidecar.relay_job(self.runtime, self.config, job)
        self.assertEqual(self.bridge.requests, [])

    async def test_arbitrary_paths_are_rejected(self):
        for path in ("/admin", "/alice/webhook/other", "/health/../admin", "/v1/chat/completions"):
            await self.assert_rejected(self.job(path=path))

    async def test_absolute_url_path_is_rejected(self):
        await self.assert_rejected(self.job(path="http://169.254.169.254/latest/meta-data"))

    async def test_query_string_and_fragment_are_rejected(self):
        await self.assert_rejected(self.job(path=WEBHOOK_PATH + "?a=1"))
        await self.assert_rejected(self.job(path=WEBHOOK_PATH + "#x"))

    async def test_wrong_method_is_rejected(self):
        await self.assert_rejected(self.job(method="DELETE"))
        await self.assert_rejected(self.job(method="GET", path=WEBHOOK_PATH))
        await self.assert_rejected(self.job(method="POST", path="/health"))

    async def test_invalid_id_is_rejected(self):
        await self.assert_rejected(self.job(id=""))
        await self.assert_rejected(self.job(id="../x"))

    async def test_invalid_base64_is_rejected(self):
        await self.assert_rejected(self.job(body="not base64 !!"))

    async def test_oversized_body_is_rejected(self):
        await self.assert_rejected(self.job(body=b64(b"x" * (64 * 1024 + 1))))


class TestRelayExecution(RelayTestCase):
    async def test_health_job_is_relayed_with_base64_body(self):
        result = await sidecar.relay_job(
            self.runtime, self.config, self.job(id="h1", method="GET", path="/health", body="")
        )
        self.assertEqual(result["id"], "h1")
        self.assertEqual(result["status"], 200)
        self.assertEqual(base64.b64decode(result["body"]), b"ok")
        self.assertTrue(
            any(name.lower() == "content-type" and "text/plain" in value for name, value in result["headers"])
        )

    async def test_webhook_job_does_not_forward_job_auth_or_cookies(self):
        job = self.job(
            headers=[
                ["content-type", "application/json"],
                ["authorization", "Bearer should-not-leak"],
                ["cookie", "sid=should-not-leak"],
                ["host", "evil.test"],
            ]
        )
        await sidecar.relay_job(self.runtime, self.config, job)
        self.assertEqual(len(self.bridge.requests), 1)
        sent = self.bridge.requests[0]["headers"]
        for name in ("Authorization", "Cookie"):
            self.assertNotIn(name, sent)
        # aiohttp sets its own Host for the bridge address; the job's value must not survive.
        self.assertNotIn("evil.test", sent.get("Host", ""))
        self.assertEqual(sent.get("Content-Type"), "application/json")

    async def test_unknown_job_id_does_not_execute_or_upload(self):
        with self.assertRaises(sidecar.JobError):
            await sidecar.relay_job(self.runtime, self.config, {"method": "GET", "path": "/health"})
        self.assertEqual(self.bridge.requests, [])

    async def test_oversized_bridge_reply_is_replaced_with_502(self):
        self.bridge.routes[("POST", WEBHOOK_PATH)] = (
            200,
            "x" * (sidecar.RELAY_BODY_LIMIT + 1),
            "text/plain",
        )
        result = await sidecar.relay_job(self.runtime, self.config, self.job())
        self.assertEqual(result["status"], 502)

    async def test_bridge_failure_becomes_sanitized_result(self):
        self.bridge.routes[("POST", WEBHOOK_PATH)] = (
            500,
            {"detail": "SECRET-BRIDGE-TRACE"},
            "application/json",
        )
        result = await sidecar.relay_job(self.runtime, self.config, self.job())
        self.assertEqual(result["status"], 502)
        self.assertNotIn(b"SECRET-BRIDGE-TRACE", base64.b64decode(result["body"]))


class TestResultUpload(RelayTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.cloud = FakeServer(
            {("POST", "/_agent/result"): (200, {"ok": True}, "application/json")}
        )
        await self.cloud.__aenter__()
        self.addAsyncCleanup(self.cloud.__aexit__, None, None, None)
        self.config.settings["public_url"] = self.cloud.base_url

    async def test_result_is_uploaded_with_agent_key(self):
        result = {"id": "job-1", "status": 200, "headers": [], "body": ""}
        await sidecar.upload_result(self.runtime, self.config, result)
        self.assertEqual(len(self.cloud.requests), 1)
        sent = self.cloud.requests[0]
        self.assertEqual(sent["headers"].get("Authorization"), "Bearer agent-key-test-value")
        self.assertEqual(sent["json"], result)

    async def test_failed_upload_is_retried_at_most_once(self):
        attempts = []

        async def flaky(request, record):
            attempts.append(1)
            if len(attempts) == 1:
                return web.Response(status=500, text="nope")
            return web.json_response({"ok": True})

        self.cloud.routes[("POST", "/_agent/result")] = flaky
        await sidecar.upload_result(self.runtime, self.config, {"id": "j", "status": 200})
        self.assertEqual(len(attempts), 2)

    async def test_upload_gives_up_after_one_retry(self):
        attempts = []

        async def always_fails(request, record):
            attempts.append(1)
            return web.Response(status=503, text="nope")

        self.cloud.routes[("POST", "/_agent/result")] = always_fails
        await sidecar.upload_result(self.runtime, self.config, {"id": "j", "status": 200})
        self.assertEqual(len(attempts), 2)


class TestPollLoop(RelayTestCase):
    async def test_poll_uses_protocol_headers_and_executes_jobs(self):
        uploads = []
        polled = asyncio.Event()
        uploaded = asyncio.Event()

        polls = []

        async def poll(request, record):
            polls.append(record)
            polled.set()
            if len(polls) == 1:
                return web.json_response(
                    {"jobs": [self.job(id="loop-1", method="GET", path="/health", body="")]}
                )
            return web.json_response({"idle": True})

        async def result_upload(request, record):
            uploads.append(record["json"])
            uploaded.set()
            return web.json_response({"ok": True})

        cloud = FakeServer(
            {
                ("GET", "/_agent/poll"): poll,
                ("POST", "/_agent/result"): result_upload,
            }
        )
        async with cloud:
            app = sidecar.create_app(
                self.config,
                bridge_url=self.bridge.base_url,
                enable_poll=True,
                poll_interval=0.01,
                poll_error_backoff=0.01,
            )
            app[sidecar.CONFIG] = dataclasses.replace(
                self.config, settings={**self.config.settings, "public_url": cloud.base_url}
            )
            client = TestClient(TestServer(app))
            await client.start_server()
            try:
                await asyncio.wait_for(polled.wait(), timeout=5)
                await asyncio.wait_for(uploaded.wait(), timeout=5)
                await asyncio.sleep(0.05)
            finally:
                await client.close()

        self.assertTrue(cloud.requests)
        first_poll = next(r for r in cloud.requests if r["path"] == "/_agent/poll")
        self.assertEqual(first_poll["method"], "GET")
        self.assertEqual(
            first_poll["headers"].get("Authorization"), "Bearer agent-key-test-value"
        )
        self.assertEqual(first_poll["headers"].get("X-Bridge-Protocol"), "batch-v1")
        self.assertEqual(len(uploads), 1)
        self.assertEqual(uploads[0]["id"], "loop-1")
        self.assertEqual(uploads[0]["status"], 200)
        self.assertEqual(base64.b64decode(uploads[0]["body"]), b"ok")


# --------------------------------------------------------------------------
# cycle 6: batch bounds, rejection uploads, backoff, cleanup
# --------------------------------------------------------------------------


class TestBatchBounds(RelayTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.cloud = FakeServer(
            {("POST", "/_agent/result"): (200, {"ok": True}, "application/json")}
        )
        await self.cloud.__aenter__()
        self.addAsyncCleanup(self.cloud.__aexit__, None, None, None)
        self.config.settings["public_url"] = self.cloud.base_url

    async def test_at_most_eight_jobs_are_executed_per_batch(self):
        self.bridge.routes[("GET", "/health")] = (200, "ok", "text/plain")
        jobs = [
            self.job(id=f"j{i}", method="GET", path="/health", body="") for i in range(12)
        ]
        await sidecar.handle_batch(self.runtime, self.config, jobs)
        await asyncio.sleep(0.05)
        health_calls = [r for r in self.bridge.requests if r["path"] == "/health"]
        self.assertEqual(len(health_calls), 8)
        self.assertEqual(len(self.cloud.requests), 8)

    async def test_concurrency_never_exceeds_eight(self):
        active = 0
        peak = 0
        gate = asyncio.Event()

        async def slow_health(request, record):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await gate.wait()
            finally:
                active -= 1
            return web.Response(text="ok", content_type="text/plain")

        self.bridge.routes[("GET", "/health")] = slow_health
        jobs = [
            self.job(id=f"c{i}", method="GET", path="/health", body="") for i in range(8)
        ]
        task = asyncio.create_task(sidecar.handle_batch(self.runtime, self.config, jobs))
        for _ in range(200):
            if len(self.bridge.requests) >= 8:
                break
            await asyncio.sleep(0.01)
        gate.set()
        await asyncio.wait_for(task, timeout=5)
        self.assertEqual(peak, 8)

    async def test_invalid_job_with_known_id_uploads_400_without_executing(self):
        job = self.job(id="bad-path", path="/admin")
        await sidecar.handle_batch(self.runtime, self.config, [job])
        await asyncio.sleep(0.05)
        self.assertEqual(self.bridge.requests, [])
        self.assertEqual(len(self.cloud.requests), 1)
        uploaded = self.cloud.requests[0]["json"]
        self.assertEqual(uploaded["id"], "bad-path")
        self.assertEqual(uploaded["status"], 400)

    async def test_job_with_invalid_id_is_dropped_silently(self):
        await sidecar.handle_batch(self.runtime, self.config, [{"method": "GET", "path": "/health"}])
        await asyncio.sleep(0.05)
        self.assertEqual(self.cloud.requests, [])
        self.assertEqual(self.bridge.requests, [])


class TestNoRetry(RelayTestCase):
    async def test_bridge_error_is_not_retried(self):
        self.bridge.routes[("POST", WEBHOOK_PATH)] = (500, {"detail": "boom"}, "application/json")
        await sidecar.relay_job(self.runtime, self.config, self.job())
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.bridge.requests), 1)


class TestPollBackoff(RelayTestCase):
    async def test_poll_error_backs_off_and_loop_recovers(self):
        uploaded = asyncio.Event()
        polls = []

        async def poll(request, record):
            polls.append(record)
            if len(polls) == 1:
                return web.Response(status=503, text="nope")
            return web.json_response(
                {"jobs": [self.job(id="after-error", method="GET", path="/health", body="")]}
            )

        async def result_upload(request, record):
            uploaded.set()
            return web.json_response({"ok": True})

        cloud = FakeServer(
            {
                ("GET", "/_agent/poll"): poll,
                ("POST", "/_agent/result"): result_upload,
            }
        )
        async with cloud:
            config = dataclasses.replace(
                self.config, settings={**self.config.settings, "public_url": cloud.base_url}
            )
            app = sidecar.create_app(
                config,
                bridge_url=self.bridge.base_url,
                enable_poll=True,
                poll_interval=0.01,
                poll_error_backoff=0.02,
            )
            client = TestClient(TestServer(app))
            await client.start_server()
            try:
                await asyncio.wait_for(uploaded.wait(), timeout=5)
            finally:
                await client.close()
        self.assertGreaterEqual(len(polls), 2)


class TestCleanup(RelayTestCase):
    async def test_closing_app_cancels_poll_and_closes_session(self):
        app = sidecar.create_app(
            self.config,
            bridge_url=self.bridge.base_url,
            enable_poll=True,
            poll_interval=0.01,
            poll_error_backoff=0.01,
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        runtime = app[sidecar.STATE]
        self.assertIsNotNone(runtime.poll_task)
        await client.close()
        self.assertTrue(runtime.poll_task.done())
        self.assertTrue(runtime.session.closed)


class TestPollDisabled(SidecarTestCase):
    async def test_empty_public_url_disables_poll_without_failing_startup(self):
        self.config.settings["public_url"] = ""
        app = sidecar.create_app(
            self.config, bridge_url="http://127.0.0.1:1", enable_poll=True
        )
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            self.assertEqual((await client.get("/health")).status, 200)
            self.assertIsNone(app[sidecar.STATE].poll_task)
        finally:
            await client.close()


class TestErrorMiddleware(SidecarTestCase):
    async def test_unhandled_error_is_sanitized_and_path_is_not_logged(self):
        app = sidecar.create_app(self.config, enable_poll=False)

        async def boom(request):
            raise RuntimeError("SECRET-TRACE " + request.path)

        app.router.add_get("/boom/{secret}", boom)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            with self.assertLogs(level="DEBUG") as captured:
                response = await client.get("/boom/webhook-secret-test-value")
                text = await response.text()
        finally:
            await client.close()
        self.assertEqual(response.status, 500)
        logs = "\n".join(captured.output)
        self.assertNotIn("SECRET-TRACE", text)
        self.assertNotIn("SECRET-TRACE", logs)
        self.assertNotIn("webhook-secret-test-value", logs)


class TestSpecConstants(unittest.TestCase):
    def test_limits_match_the_deployment_contract(self):
        self.assertEqual(sidecar.UPSTREAM_TIMEOUT, 45.0)
        self.assertEqual(sidecar.BRIDGE_TIMEOUT, 3.0)
        self.assertEqual(sidecar.POLL_TIMEOUT, 6.0)
        self.assertEqual(sidecar.POLL_ERROR_BACKOFF, 2.0)
        self.assertEqual(sidecar.MAX_BODY_BYTES, 64 * 1024)
        self.assertEqual(sidecar.SPEECH_LIMIT, 1000)
        self.assertEqual(sidecar.MAX_CONCURRENT_JOBS, 8)
        self.assertLessEqual(sidecar.RELAY_BODY_LIMIT, 8000)
        self.assertEqual(sidecar.PROTOCOL_VERSION, "batch-v1")
        self.assertEqual(sidecar.USER_AGENT, "alice-code-bridge/1.0")


class TestConfigLocation(unittest.TestCase):
    def test_load_config_is_independent_of_cwd(self):
        with tempfile.TemporaryDirectory() as tmp:
            make_config(tmp)
            previous = os.getcwd()
            os.chdir(tempfile.gettempdir())
            try:
                config = sidecar.load_config(Path(tmp))
            finally:
                os.chdir(previous)
        self.assertEqual(config.settings["proxy_port"], 18091)


if __name__ == "__main__":
    unittest.main()
