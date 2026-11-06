"""Configurable greeting replacement must preserve the Rust access-control result."""
import base64
import copy
import json
from pathlib import Path
import tempfile
import unittest
import sidecar
import test_sidecar as fixtures

OLD = 'Привет! Я мост к нейросетям. Задай любой вопрос или скажи: помощь.'
NEW = 'Привет! Я Ерёма. Чем могу помочь?'
UPSTREAM = {'response': {'text': OLD, 'tts': OLD, 'end_session': False}, 'version': '1.0'}

class GreetingWebhookTests(fixtures.WebhookTestCase):
    extra_settings = {'greeting_text': NEW}

    async def send(self, *, new=True, utterance='', reply=None):
        self.bridge.routes[('POST', fixtures.WEBHOOK_PATH)] = (200, reply or UPSTREAM, 'application/json')
        body = self.alice_body()
        body['session']['new'] = new
        body['request']['original_utterance'] = utterance
        body['request']['command'] = utterance.lower()
        response = await self.client.post(fixtures.WEBHOOK_PATH, json=body)
        self.assertEqual(response.status, 200)
        self.assertEqual(len(self.bridge.requests), 1, 'Rust authorization must happen before replacement')
        return await response.json()

    async def test_authorized_new_session_replaces_text_and_tts(self):
        data = await self.send()
        self.assertEqual(data['response'], {'text': NEW, 'tts': NEW, 'end_session': False})
        self.assertEqual(data['version'], '1.0')

    async def test_access_denial_is_never_replaced(self):
        denied = {'response': {'text': 'Извини, это семейный навык. Я отвечаю только своим.', 'end_session': True}, 'version': '1.0'}
        self.assertEqual(await self.send(reply=denied), denied)

    async def test_existing_session_is_not_rewritten(self):
        self.assertEqual(await self.send(new=False), UPSTREAM)

    async def test_new_session_with_real_utterance_is_not_rewritten(self):
        self.assertEqual(await self.send(utterance='помощь'), UPSTREAM)

    async def test_other_reply_is_not_rewritten(self):
        other = copy.deepcopy(UPSTREAM)
        other['response'].update(text='Другой ответ', tts='Другой ответ')
        self.assertEqual(await self.send(reply=other), other)

class GreetingRelayTests(fixtures.RelayTestCase):
    extra_settings = {'greeting_text': NEW}

    async def test_polled_webhook_replaces_greeting_after_bridge_response(self):
        self.bridge.routes[('POST', fixtures.WEBHOOK_PATH)] = (200, UPSTREAM, 'application/json')
        body = {'session': {'new': True}, 'request': {'original_utterance': ''}}
        result = await sidecar.relay_job(self.runtime, self.config, self.job(body=fixtures.b64(json.dumps(body).encode())))
        self.assertEqual(result['status'], 200)
        data = json.loads(base64.b64decode(result['body']))
        self.assertEqual(data['response'], {'text': NEW, 'tts': NEW, 'end_session': False})
        self.assertEqual(len(self.bridge.requests), 1)

class GreetingConfigTests(unittest.TestCase):
    def test_invalid_greeting_is_rejected_at_startup(self):
        for value in (None, 12, False, '', '  ', 'x' * (sidecar.SPEECH_LIMIT + 1)):
            with self.subTest(value_type=type(value).__name__), tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(sidecar.ConfigError):
                    fixtures.make_config(tmp, greeting_text=value)

if __name__ == '__main__':
    unittest.main(verbosity=2)
