"""Bounded real relay diagnosis. Does not reset history or change model presets."""
import asyncio
import json
import logging
import time
import tomllib
import uuid
from pathlib import Path

import aiohttp
from aiohttp import web
import sidecar

async def main():
    root = Path(__file__).resolve().parent
    cfg = sidecar.load_config(root)
    user = tomllib.loads((root / 'config.toml').read_text())['server']['allowed_user_ids'][0]
    started = time.monotonic()
    def event(kind, **fields):
        print(json.dumps({'t': round(time.monotonic()-started, 3), 'event': kind, **fields}, ensure_ascii=False), flush=True)
    original_poll, original_relay, original_upload = sidecar.poll_once, sidecar.relay_job, sidecar.upload_result
    async def poll(runtime, config):
        t = time.monotonic()
        try:
            data = await original_poll(runtime, config)
            event('poll', seconds=round(time.monotonic()-t,3), idle=data.get('idle'), jobs=len(data.get('jobs',[])))
            return data
        except Exception as exc:
            text = str(exc)
            for value in config.secrets.values():
                text = text.replace(str(value), '[hidden]')
            event('poll_error', type=type(exc).__name__, detail=text)
            raise
    async def relay(runtime, config, job):
        event('relay_start', input_bytes=len(job.get('body','')))
        result = await original_relay(runtime, config, job)
        event('relay_end', status=result['status'], output_bytes=len(result['body']))
        return result
    async def upload(runtime, config, result):
        event('upload_start')
        value = await original_upload(runtime, config, result)
        event('upload_end', accepted=value)
        return value
    sidecar.poll_once, sidecar.relay_job, sidecar.upload_result = poll, relay, upload
    app = sidecar.create_app(cfg)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, '127.0.0.1', cfg.settings['proxy_port']).start()
        await asyncio.sleep(3)
        session_id = 'network-diagnostic-' + str(uuid.uuid4())
        async with aiohttp.ClientSession() as client:
            for i, question in enumerate(('кто я', 'В одном предложении объясни назначение git status.', 'ну что')):
                payload = {'meta':{'locale':'ru-RU','timezone':'Europe/Moscow'},
                    'session':{'message_id':i+1,'session_id':session_id,'skill_id':'50e5bf26-dd9f-481c-872a-356b21c96569','new':False,
                               'user':{'user_id':user},'application':{'application_id':session_id}},
                    'request':{'command':question.lower(),'original_utterance':question,'type':'SimpleUtterance'},'version':'1.0'}
                t = time.monotonic()
                try:
                    async with client.post(cfg.public_url+sidecar.WEBHOOK_PREFIX+cfg.secret('webhook_secret'),json=payload,
                                           timeout=aiohttp.ClientTimeout(total=8)) as response:
                        text = await response.text()
                        event('public', question=question, status=response.status, seconds=round(time.monotonic()-t,3), body=text[:450])
                except Exception as exc:
                    event('public_error', type=type(exc).__name__)
                await asyncio.sleep(1)
    finally:
        await runner.cleanup()

if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
