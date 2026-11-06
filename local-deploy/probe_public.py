"""Explicit live smoke: public relay + real Go completions, using synthetic Alice requests.
Does not claim to be a physical speaker test. Preserves existing conversation history.
"""
import json
from pathlib import Path
import time
import uuid
import tomllib

import requests

ROOT = Path(__file__).resolve().parent
settings = json.loads((ROOT / 'settings.json').read_text())
secrets = json.loads((ROOT / 'secrets.json').read_text())
url = settings['public_url'] + '/alice/webhook/' + secrets['webhook_secret']
report = {'kind': 'synthetic_alice_public_webhook_real_go', 'checks': []}
session = requests.Session()
message_id = 0
TEST_USER = tomllib.loads((ROOT / 'config.toml').read_text(encoding='utf-8'))['server']['allowed_user_ids'][0]

def ask(text, user=TEST_USER, new=False):
    global message_id
    message_id += 1
    payload = {
        'meta': {'locale': 'ru-RU', 'timezone': 'Europe/Moscow'},
        'session': {'message_id': message_id, 'session_id': 'deployment-smoke',
                    'skill_id': 'deployment-smoke', 'user': {'user_id': user},
                    'application': {'application_id': 'deployment-smoke'}, 'new': new},
        'request': {'command': text.lower(), 'original_utterance': text,
                    'type': 'SimpleUtterance'}, 'version': '1.0'}
    started = time.monotonic()
    try:
        response = session.post(url, json=payload, timeout=10)
    except requests.RequestException as exc:
        raise RuntimeError(type(exc).__name__) from None
    elapsed = round(time.monotonic() - started, 3)
    if response.status_code != 200:
        raise RuntimeError('webhook HTTP ' + str(response.status_code))
    data = response.json()
    answer = data['response']['text']
    item = {'command': text, 'seconds': elapsed, 'answer': answer,
            'end_session': data['response']['end_session']}
    report['checks'].append(item)
    print(json.dumps(item, ensure_ascii=False), flush=True)
    assert elapsed < 4.5, 'Alice response deadline exceeded'
    assert data['version'] == '1.0'
    return data['response']

def completion(question):
    result = ask(question)
    for _ in range(6):
        text = result['text'].lower()
        if not any(word in text for word in ('думаю', 'подумать', 'секунд', 'готовлю', 'готовится')):
            break
        time.sleep(1)
        result = ask('ну что')
    assert not result['end_session']
    assert len(result['text']) <= 1000
    assert not any(word in result['text'].lower() for word in (
        'не удалось', 'ошибка', 'неполад', 'попробуй позже', 'секунд', 'думаю'))
    return result['text']

try:
    denied = ask('Что делает git status?', user='stranger-' + str(uuid.uuid4()))
    assert denied['end_session'] is True
    assert ask('', new=True)['end_session'] is False
    assert 'Артём' in ask('кто я')['text']
    fast = completion('Что делает git status? Ответь одним предложением.')
    assert 'git' in fast.lower() or 'репозитор' in fast.lower()
    assert 'умную' in ask('переключись на умную модель')['text'].lower()
    smart = completion('Зачем нужен индекс в SQL? Ответь одним предложением.')
    assert any(s in smart.lower() for s in ('индекс', 'поиск', 'выборк'))
    assert 'быструю' in ask('смени модель на быструю')['text'].lower()
    # Preserve the owner's existing conversation history; never reset it in smoke tests.
    report['verified'] = True
except Exception as exc:
    report['verified'] = False
    report['error'] = str(exc).replace(secrets['webhook_secret'], '[hidden]')
    raise
finally:
    (ROOT / 'verification-public.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
