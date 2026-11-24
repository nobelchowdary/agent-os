"""Bootstrap prerequisite checks; no real cluster or isolation is exercised."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('kind_cluster_prerequisites', ROOT / 'scripts' / 'kind_cluster.py')
bootstrap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bootstrap)


class BootstrapPrerequisiteTests(unittest.TestCase):
    def test_effective_pid_limit_is_checked_on_every_node(self):
        def read(args):
            self.assertIn(args[-1], ('/api/v1/nodes/first/proxy/configz', '/api/v1/nodes/second/proxy/configz'))
            return json.dumps({'kubeletconfig': {'podPidsLimit': bootstrap.POD_PIDS_LIMIT}})
        with patch.object(bootstrap, 'command', side_effect=read) as command:
            self.assertEqual(bootstrap.verify_pid_configuration(['kubectl'], ['first', 'second']), 2)
        self.assertEqual(command.call_count, 2)

    def test_missing_unlimited_wrong_or_invalid_pid_limit_refuses(self):
        for value in (None, -1, 0, 129, '128', True):
            with self.subTest(value=value), patch.object(bootstrap, 'command', return_value=json.dumps({
                    'kubeletconfig': {'podPidsLimit': value}})), self.assertRaisesRegex(RuntimeError, 'pod_pids_limit_not_configured'):
                bootstrap.verify_pid_configuration(['kubectl'], ['worker'])
        with self.assertRaisesRegex(RuntimeError, 'no_disposable_nodes'):
            bootstrap.verify_pid_configuration(['kubectl'], [])

    def test_a_second_misconfigured_node_cannot_be_ignored(self):
        readings = [{'kubeletconfig': {'podPidsLimit': 128}}, {'kubeletconfig': {'podPidsLimit': -1}}]
        with patch.object(bootstrap, 'command', side_effect=map(json.dumps, readings)), self.assertRaisesRegex(RuntimeError, 'pod_pids_limit_not_configured'):
            bootstrap.verify_pid_configuration(['kubectl'], ['first', 'second'])


if __name__ == '__main__':
    unittest.main()
