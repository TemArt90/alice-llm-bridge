"""Offline coverage for the production service entry point."""
from pathlib import Path
import unittest
from unittest.mock import patch
import run_service
import sidecar


class ServiceEntrypointTests(unittest.TestCase):
    def test_systemd_uses_the_observed_service_entrypoint(self):
        unit = (Path(__file__).parent / 'alice-code-sidecar.service').read_text()
        self.assertIn('/.venv/bin/python %h/alice-llm-bridge/local-deploy/run_service.py', unit)

    def test_entrypoint_wraps_and_restores_the_original_handlers(self):
        originals = (sidecar.poll_once, sidecar.relay_job, sidecar.upload_result)
        def fake_main():
            self.assertIsNot(sidecar.poll_once, originals[0])
            self.assertIsNot(sidecar.relay_job, originals[1])
            self.assertIsNot(sidecar.upload_result, originals[2])
        with patch.object(sidecar, 'main', side_effect=fake_main) as main:
            run_service.main()
            main.assert_called_once_with()
        self.assertEqual((sidecar.poll_once, sidecar.relay_job, sidecar.upload_result), originals)

if __name__ == '__main__':
    unittest.main(verbosity=2)
