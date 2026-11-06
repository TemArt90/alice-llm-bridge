"""Dependency regression: Cloudflare compresses real poll jobs using Brotli."""
import unittest
import tomllib
from pathlib import Path
import sidecar
import brotli
from aiohttp.compression_utils import BrotliDecompressor

class RuntimeCompressionTests(unittest.TestCase):
    def test_retry_budget_fits_cloudflare_deadline(self):
        config = tomllib.loads((Path(__file__).parent / 'config.toml').read_text(encoding='utf-8'))
        # Reserve 400 ms for local work and network outside result upload.
        worst_ms = (config['defaults']['reply_budget_ms']
                    + sidecar.POLL_IDLE_INTERVAL * 1000
                    + 2 * sidecar.RESULT_TIMEOUT * 1000
                    + sidecar.RESULT_RETRY_DELAY * 1000 + 400)
        self.assertLess(worst_ms, 3900)

    def test_brotli_compressed_poll_payload_is_decodable(self):
        body = b'{"jobs":[{"id":"probe","method":"POST","body":"payload"}]}'
        compressed = brotli.compress(body)
        decoder = BrotliDecompressor()
        self.assertEqual(decoder.decompress_sync(compressed) + decoder.flush(), body)

class DeployedModelTests(unittest.TestCase):
    def test_requested_deepseek_model_is_used_by_both_presets(self):
        import json
        root = Path(__file__).parent
        config = tomllib.loads((root / 'config.toml').read_text(encoding='utf-8'))
        settings = json.loads((root / 'settings.json').read_text(encoding='utf-8'))
        for preset in ('fast', 'smart'):
            self.assertEqual(config['models'][preset]['model'], 'deepseek-v4.1-flash')
        self.assertEqual(settings['allowed_models'], ['deepseek-v4.1-flash'])

    def test_deepseek_disables_reasoning_for_short_voice_answers(self):
        self.assertEqual(sidecar.thinking_override('deepseek-v4.1-flash'), {'type': 'disabled'})

class VoicePersonaTests(unittest.TestCase):
    def test_persona_is_general_purpose_and_honest_about_tools(self):
        config = tomllib.loads((Path(__file__).parent / 'config.toml').read_text(encoding='utf-8'))
        persona = config['profiles'][0]['persona'].lower()
        self.assertIn('универсальный голосовой ассистент', persona)
        self.assertNotIn('помощник по программированию', persona)
        self.assertNotIn('здесь обсуждаем программирование', persona)
        self.assertIn('нет инструментов', persona)

if __name__ == '__main__':
    unittest.main(verbosity=2)
