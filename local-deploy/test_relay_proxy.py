"""Only Cloudflare polling/result uploads may use the configured relay proxy."""
import asyncio
from pathlib import Path
from typing import cast
from aiohttp import ClientSession
import unittest
from unittest.mock import AsyncMock, MagicMock
import sidecar

PROXY = 'http://127.0.0.1:10808'

class RelayProxyTests(unittest.IsolatedAsyncioTestCase):
    def make_runtime(self, proxied=True):
        settings = {'public_url': 'https://relay.example.test'}
        if proxied:
            settings['relay_proxy_url'] = PROXY
        config = sidecar.Config(settings=settings, secrets={'agent_key': 'test-only-agent'}, base_dir=Path('.'))
        session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={'idle': True})
        manager = AsyncMock()
        manager.__aenter__.return_value = response
        session.get.return_value = manager
        session.post.return_value = manager
        return sidecar.Runtime(session=cast(ClientSession, session), semaphore=asyncio.Semaphore(8)), config

    async def test_poll_uses_explicit_relay_proxy(self):
        runtime, config = self.make_runtime()
        self.assertEqual(await sidecar.poll_once(runtime, config), {'idle': True})
        self.assertEqual(cast(MagicMock, runtime.session).get.call_args.kwargs.get('proxy'), PROXY)

    async def test_result_upload_uses_explicit_relay_proxy(self):
        runtime, config = self.make_runtime()
        self.assertTrue(await sidecar.upload_result(runtime, config, {'id': 'test'}))
        self.assertEqual(cast(MagicMock, runtime.session).post.call_args.kwargs.get('proxy'), PROXY)

    async def test_no_proxy_setting_preserves_direct_connection(self):
        runtime, config = self.make_runtime(proxied=False)
        await sidecar.poll_once(runtime, config)
        self.assertIsNone(cast(MagicMock, runtime.session).get.call_args.kwargs.get('proxy'))

if __name__ == '__main__':
    unittest.main(verbosity=2)
