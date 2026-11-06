"""Agent socket transport: jobs arrive as frames and results go back the same way."""
import asyncio
import contextlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import WSMsgType

import unittest

import sidecar


class Message:
    def __init__(self, kind, data):
        self.type = kind
        self.data = data


class FakeSocket:
    def __init__(self, messages=(), on_end=None):
        self._messages = list(messages)
        self._on_end = on_end
        self.sent = []

    async def send_json(self, payload):
        self.sent.append(payload)

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in self._messages:
            yield message
        if self._on_end is not None:
            self._on_end()


class FakeConnection:
    def __init__(self, socket):
        self._socket = socket

    async def __aenter__(self):
        return self._socket

    async def __aexit__(self, *exc_info):
        return False


def make_config(**settings):
    values = {
        "public_url": "https://relay.example.test",
        "relay_proxy_url": "http://127.0.0.1:10808",
    }
    values.update(settings)
    return sidecar.Config(settings=values, secrets={"agent_key": "test-agent"}, base_dir=Path("."))


def make_runtime():
    return sidecar.Runtime(session=MagicMock(), semaphore=asyncio.Semaphore(4))


JOB = {
    "id": "11111111-1111-4111-8111-111111111111",
    "method": "POST",
    "path": "/alice/webhook/secret",
    "headers": [["content-type", "application/json"]],
    "body": "",
}
RESULT = {"id": JOB["id"], "status": 200, "headers": [["content-type", "application/json"]], "body": ""}


class SocketUrlTests(unittest.TestCase):
    def test_https_address_maps_to_wss(self):
        self.assertEqual(sidecar.relay_socket_url(make_config()), "wss://relay.example.test/_agent/ws")

    def test_http_address_maps_to_ws_without_a_double_slash(self):
        self.assertEqual(
            sidecar.relay_socket_url(make_config(public_url="http://127.0.0.1:8787/")),
            "ws://127.0.0.1:8787/_agent/ws",
        )

    def test_addresses_without_a_scheme_are_rejected(self):
        with self.assertRaises(RuntimeError):
            sidecar.relay_socket_url(make_config(public_url="relay.example.test"))


class SocketLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_frame_job_is_relayed_and_answered_over_the_socket(self):
        runtime = make_runtime()
        socket = FakeSocket([Message(WSMsgType.TEXT, json.dumps(JOB))],
                            on_end=lambda: setattr(runtime, "shutting_down", True))
        runtime.session.ws_connect = MagicMock(return_value=FakeConnection(socket))
        with patch.object(sidecar, "relay_job", new=AsyncMock(return_value=RESULT)):
            await sidecar.ws_loop(runtime, make_config())
            if runtime.job_tasks:
                await asyncio.gather(*runtime.job_tasks, return_exceptions=True)
        self.assertEqual(socket.sent, [RESULT])
        call = runtime.session.ws_connect.call_args
        self.assertEqual(call.args[0], "wss://relay.example.test/_agent/ws")
        self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer test-agent")
        self.assertEqual(call.kwargs["proxy"], "http://127.0.0.1:10808")

    async def test_control_frames_and_garbage_are_ignored(self):
        runtime = make_runtime()
        socket = FakeSocket([Message(WSMsgType.TEXT, '{"type":"pong"}'),
                             Message(WSMsgType.TEXT, "not json")],
                            on_end=lambda: setattr(runtime, "shutting_down", True))
        runtime.session.ws_connect = MagicMock(return_value=FakeConnection(socket))
        with patch.object(sidecar, "relay_job", new=AsyncMock(return_value=RESULT)) as relay:
            await sidecar.ws_loop(runtime, make_config())
        relay.assert_not_awaited()
        self.assertEqual(socket.sent, [])

    async def test_a_failed_connect_backs_off_and_retries(self):
        runtime = make_runtime()
        attempts = {"n": 0}

        def connect(*args, **kwargs):
            attempts["n"] += 1
            if attempts["n"] >= 3:
                runtime.shutting_down = True
            raise ConnectionError("relay is down")

        runtime.session.ws_connect = connect
        with patch.object(sidecar.asyncio, "sleep", new=AsyncMock()) as sleep:
            await sidecar.ws_loop(runtime, make_config())
        self.assertEqual(attempts["n"], 3)
        self.assertEqual([call.args[0] for call in sleep.await_args_list], [sidecar.WS_BACKOFF_START, 2.0])
