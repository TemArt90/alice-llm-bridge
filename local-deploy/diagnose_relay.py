"""Bounded live relay diagnosis; no model generation and no secrets printed."""
import asyncio
import json
import time
from pathlib import Path
import sidecar
from aiohttp import ClientSession, ClientTimeout, web

async def main():
    config = sidecar.load_config(Path(__file__).resolve().parent)
    original = sidecar.poll_once
    async def traced(runtime, cfg):
        t = time.monotonic()
        try:
            result = await original(runtime, cfg)
            print('poll', round(time.monotonic()-t, 3), 'idle', result.get('idle'), 'jobs', len(result.get('jobs', [])), flush=True)
            return result
        except Exception as exc:
            text = str(exc)
            for secret in cfg.secrets.values():
                text = text.replace(str(secret), '[hidden]')
            print('poll_error', type(exc).__name__, text, round(time.monotonic()-t, 3), flush=True)
            raise
    sidecar.poll_once = traced
    app = sidecar.create_app(config)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, '127.0.0.1', 18091).start()
        await asyncio.sleep(4)
        payload = {'meta': {'locale':'ru-RU','timezone':'Europe/Moscow'},
                   'session': {'message_id':1,'session_id':'relay-diagnostic','skill_id':'relay-diagnostic','new':True,
                               'user':{'user_id':'unauthorized-relay-diagnostic'},'application':{'application_id':'relay-diagnostic'}},
                   'request':{'command':'','original_utterance':'','type':'SimpleUtterance'},'version':'1.0'}
        async with ClientSession() as client:
            t = time.monotonic()
            async with client.post(config.public_url + sidecar.WEBHOOK_PREFIX + config.secret('webhook_secret'), json=payload, timeout=ClientTimeout(total=10)) as response:
                text = await response.text()
                print('public_probe', response.status, round(time.monotonic()-t, 3), text[:500], flush=True)
        await asyncio.sleep(3)
    finally:
        await runner.cleanup()

if __name__ == '__main__':
    asyncio.run(main())
