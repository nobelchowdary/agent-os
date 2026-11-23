"""Dry-run/failure-boundary checks; these do not execute a cluster campaign."""
import importlib.util
import io
from pathlib import Path
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / (name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


bootstrap = module('kind_cluster')
campaign = module('kind_integration')


class KindSafetyTests(unittest.TestCase):
    def test_multiple_kubectl_resource_documents_preserve_deployment_and_storage(self):
        import json
        items = [{'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim'},
                 {'apiVersion': 'apps/v1', 'kind': 'Deployment'},
                 {'apiVersion': 'v1', 'kind': 'Service'}]
        concatenated = '\n'.join(json.dumps(item) for item in items)
        self.assertEqual(campaign.rendered_resources(concatenated)['items'], items)
        self.assertEqual(campaign.rendered_resources(json.dumps({'kind': 'List', 'items': items}))['items'], items)

    def test_default_invocations_contact_nothing(self):
        for tool in (bootstrap, campaign):
            with self.subTest(tool=tool.__name__), patch('sys.argv', ['tool']), patch.object(tool.subprocess, 'run', side_effect=AssertionError('external command')), patch.object(tool.subprocess, 'Popen', side_effect=AssertionError('external process')), redirect_stdout(io.StringIO()) as output:
                self.assertEqual(tool.main(), 0)
                self.assertIn('NOT_EXECUTED', output.getvalue())

    def test_wrong_host_refuses_before_download_or_cluster_creation(self):
        with patch('sys.argv', ['tool', '--execute']), patch.object(bootstrap.platform, 'system', return_value='Darwin'), patch.object(bootstrap, 'command', side_effect=AssertionError('external command')), self.assertRaisesRegex(RuntimeError, 'requires_linux_amd64'):
            bootstrap.main()

    def test_bad_download_never_installs_a_binary(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def read(self, *_): return b'altered executable'
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / 'kind'
            with patch.object(bootstrap.urllib.request, 'urlopen', return_value=Response()), self.assertRaisesRegex(RuntimeError, 'checksum_mismatch'):
                bootstrap.fetch_verified('kind', target)
            self.assertFalse(target.exists())

    def test_invalid_counts_refuse_before_any_kubernetes_call(self):
        for count in ('0', '-1', '1001'):
            with self.subTest(count=count), patch('sys.argv', ['tool', '--execute', '--sequential-runs', count]), patch.object(campaign, 'Cluster', side_effect=AssertionError('cluster initialization')), self.assertRaisesRegex(campaign.CheckFailed, 'invalid_campaign_counts'):
                campaign.main()

    def test_lost_watch_cannot_claim_unique_executions(self):
        class Stopped:
            def poll(self): return 1
        watch = campaign.ObjectWatch.__new__(campaign.ObjectWatch)
        watch.process, watch.errors = Stopped(), []
        with self.assertRaisesRegex(campaign.CheckFailed, 'watch_continuity_lost'):
            watch.unique(['synthetic-id'])

    def test_old_request_failure_cannot_terminate_replacement_forward(self):
        class Forward:
            def __init__(self): self.stopped = False
            def terminate(self): self.stopped = True
            def wait(self, timeout=None): return 0
        cluster = campaign.Cluster.__new__(campaign.Cluster)
        old, current = Forward(), Forward()
        cluster.forward_lock = threading.Lock()
        cluster.forward = cluster.forward_ready = current
        cluster.stop_forward(expected=old)
        self.assertIs(cluster.forward, current)
        self.assertFalse(current.stopped)
        cluster.stop_forward(expected=current)
        self.assertTrue(current.stopped)
        self.assertIsNone(cluster.forward)

    def test_failure_diagnostics_omit_sensitive_pod_fields(self):
        cluster = campaign.Cluster.__new__(campaign.Cluster)
        pod = {'metadata': {'namespace': 'test', 'name': 'pod', 'annotations': {'token': 'private-value'}},
               'spec': {'containers': [{'name': 'agent', 'env': [{'value': 'private-value'}]}]},
               'status': {'phase': 'Pending', 'message': 'private-value', 'containerStatuses': [
                   {'name': 'agent', 'state': {'waiting': {'reason': 'ImagePullBackOff', 'message': 'private-value'}}}]}}
        import json
        with patch.object(cluster, 'kubectl', return_value=json.dumps({'items': [pod]})):
            value = cluster.diagnostics()
        self.assertNotIn('private-value', json.dumps(value))
        self.assertEqual(value['pods'][0]['containers'][0]['waiting_reason'], 'ImagePullBackOff')


if __name__ == '__main__':
    unittest.main()
