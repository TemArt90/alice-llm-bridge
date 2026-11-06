"""Read-only checks for this installation; no model calls or account changes."""
import json
from pathlib import Path
import shutil
import subprocess
import tomllib
import unittest

import yaml

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent


class DeploymentConfigTests(unittest.TestCase):
    def test_config_access_is_locked_or_explicitly_allowlisted(self):
        config = tomllib.loads((ROOT / 'config.toml').read_text(encoding='utf-8'))
        self.assertTrue(config['server']['allowed_user_ids'])
        self.assertEqual(config['server']['listen'], '127.0.0.1:18090')
        self.assertEqual(config['providers']['opencode_go']['base_url'],
                         'http://127.0.0.1:18091/v1')
        for name in ('fast', 'smart'):
            self.assertIn(config['models'][name]['provider'], config['providers'])
            self.assertGreater(config['models'][name]['max_tokens'], 0)

    def test_compose_is_isolated_and_image_pinned(self):
        config = yaml.safe_load((ROOT / 'compose.yaml').read_text(encoding='utf-8'))
        self.assertEqual(config['name'], 'alice-code-bridge')
        app = config['services']['app']
        self.assertIn('@sha256:', app['image'])
        self.assertTrue(app['read_only'])
        self.assertEqual(app['restart'], 'unless-stopped')
        self.assertEqual(app['environment']['CONFIG_PATH'], '/app/config.toml')
        self.assertEqual(config['services']['postgres']['ports'],
                         ['127.0.0.1:15434:5432'])

    def test_secrets_are_gitignored(self):
        paths = ['local-deploy/' + name for name in (
            'secrets.json', 'settings.json', 'config.toml',
            'bridge.env', 'postgres.env')]
        result = subprocess.run([shutil.which('git') or 'git', '-C', str(REPO),
                                 'check-ignore', '--', *paths],
                                capture_output=True, text=True, check=True)
        self.assertEqual(set(result.stdout.splitlines()), set(paths))

    def test_init_script_uses_linux_line_endings(self):
        script = (ROOT / 'init-app-db.sh').read_bytes()
        self.assertNotIn(b'\r', script)
        self.assertTrue(script.startswith(b'#!/bin/bash\n'))

    def test_sidecar_unit_restarts_and_uses_private_permissions(self):
        unit = (ROOT / 'alice-code-sidecar.service').read_text(encoding='utf-8')
        for setting in ('Restart=always', 'UMask=0077', 'NoNewPrivileges=true',
                        'WantedBy=default.target'):
            self.assertIn(setting, unit)


class LiveDeploymentTests(unittest.TestCase):
    def test_remote_config_unit_and_original_bridge_health(self):
        command = (
            'cd ~/alice-llm-bridge/local-deploy && '
            'docker compose config --quiet && '
            'systemd-analyze --user verify ./alice-code-sidecar.service && '
            'python3 -c "import urllib.request; '
            "assert urllib.request.urlopen('http://127.0.0.1:18090/health',timeout=5).status==200; "
            "print('PASS: remote compose, systemd unit and original Rust bridge health')\""
        )
        result = subprocess.run(
            [shutil.which('ssh') or 'ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=7',
             '-p', '2222', 'temart@192.168.1.44', command],
            capture_output=True, text=True, timeout=25)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('PASS:', result.stdout)


if __name__ == '__main__':
    unittest.main(verbosity=2)
