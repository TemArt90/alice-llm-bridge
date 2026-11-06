"""Live end-to-end check against the running service; preserves conversation history."""
import json
import time
import tomllib
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
WAIT_PHRASES = ('нужно немного подумать', 'мне нужно подумать', 'хороший вопрос', 'ещё думаю', 'пару секунд', 'готовлю ответ')

def run():
    settings = json.loads((ROOT / 'settings.json').read_text())
    secrets = json.loads((ROOT / 'secrets.json').read_text())
    config = tomllib.loads((ROOT / 'config.toml').read_text())
    user = config['server']['allowed_user_ids'][0]
    report = {'kind': 'public_webhook_real_model_no_history_reset', 'utc': datetime.now(timezone.utc).isoformat(),
              'expected_model': config['models']['fast']['model'], 'checks': [], 'verified': False}
    session_id = 'acceptance-' + str(uuid.uuid4())
    client = requests.Session()
    counter = 0
    def ask(text, actor=user):
        nonlocal counter
        counter += 1
        payload = {'meta': {'locale': 'ru-RU', 'timezone': 'Europe/Moscow'},
                   'session': {'message_id': counter, 'session_id': session_id,
                       'skill_id': '50e5bf26-dd9f-481c-872a-356b21c96569', 'new': False,
                       'user': {'user_id': actor}, 'application': {'application_id': session_id}},
                   'request': {'command': text.lower(), 'original_utterance': text, 'type': 'SimpleUtterance'}, 'version': '1.0'}
        started = time.monotonic()
        try:
            response = client.post(settings['public_url'] + '/alice/webhook/' + secrets['webhook_secret'], json=payload, timeout=8)
        except requests.RequestException as exc:
            raise RuntimeError(type(exc).__name__) from None
        elapsed = round(time.monotonic()-started, 3)
        item = {'command': text, 'http': response.status_code, 'seconds': elapsed}
        report['checks'].append(item)
        if response.status_code != 200:
            raise RuntimeError('public webhook HTTP ' + str(response.status_code))
        body = response.json()
        answer = body['response']
        item.update(answer=answer['text'], end_session=answer['end_session'])
        print(json.dumps(item, ensure_ascii=False), flush=True)
        assert elapsed < 4.5, 'Alice deadline exceeded'
        assert body['version'] == '1.0'
        return answer
    try:
        denied = ask('кто я', actor='stranger-' + str(uuid.uuid4()))
        assert denied['end_session'], 'stranger was not rejected'
        identity = ask('кто я')
        # A finished answer from a previous utterance is intentionally delivered
        # before the new command, even across Alice sessions. Drain it without
        # deleting conversation history, then repeat the harmless identity query.
        for _ in range(3):
            if 'Артём' in identity['text']:
                break
            assert not identity['end_session']
            time.sleep(1)
            identity = ask('кто я')
        assert not identity['end_session'] and 'Артём' in identity['text']
        reply = ask('Одним предложением объясни, что показывает git status.')
        for _ in range(12):
            if not any(phrase in reply['text'].lower() for phrase in WAIT_PHRASES):
                break
            time.sleep(1)
            reply = ask('ну что')
        assert not reply['end_session']
        assert 0 < len(reply['text']) <= 1000
        assert 'git' in reply['text'].lower() and any(word in reply['text'].lower() for word in ('файл', 'репозитор', 'состоян'))
        report['verified'] = True
    except Exception as exc:
        report['error'] = str(exc).replace(secrets['webhook_secret'], '[hidden]')
        raise
    finally:
        (ROOT / 'verification-end-to-end.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
        client.close()
    return report

if __name__ == '__main__':
    run()
