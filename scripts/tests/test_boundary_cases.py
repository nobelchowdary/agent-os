"""Probe evidence/cleanup contracts, without executing submitted code locally."""
import ast
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('boundary_cases', ROOT / 'scripts' / 'boundary_cases.py')
boundary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(boundary)


def observation(reachable):
    return {'network': {name: {'reachable': value} for name, value in reachable.items()}}


def admitted_pod():
    return {'spec': {'runtimeClassName': 'gvisor', 'automountServiceAccountToken': False, 'nodeName': 'different-worker-a',
        'securityContext': {'runAsNonRoot': True}, 'volumes': [{'name': 'tmp', 'emptyDir': {}}],
        'containers': [{'name': 'agent', 'securityContext': {'readOnlyRootFilesystem': True,
            'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}}}]}}


class FakeCluster:
    def __init__(self):
        self.applied, self.commands, self.submitted = [], [], []
        self.worker_nodes = {'different-worker-a', 'different-worker-b'}
        self.pid_limit = 128
        self.pod = admitted_pod()
        self.restricted = observation(dict(controlled_peer=False, kubernetes_api=False, dns=False, metadata=False))
        self.restricted['process_and_filesystem'] = dict(uid_nonzero=True, service_account_token_absent=True,
            root_write_denied=True, root_mount_read_only=True, pid_one_is_python=True)
        self.positive = observation(dict(controlled_peer=True, kubernetes_api=True, dns=True, metadata=False))

    def obj(self, kind, name=None, namespace=None):
        if kind == 'configmap':
            return {'data': {'config.json': json.dumps({'templates': {'python': {'image': 'python@sha256:' + '1' * 64}}})}}
        if kind == 'nodes':
            return {'items': [{'metadata': {'name': name, 'labels': {'kubernetes.io/hostname': name + '-host'}}}
                              for name in ('different-worker-b', 'different-worker-a')]}
        if kind == 'service':
            return {'spec': {'clusterIP': '10.96.0.1' if name == 'kubernetes' else '10.96.0.10'}}
        if kind == 'pod':
            return {'status': {'podIP': '10.244.2.50'}}
        raise AssertionError('unexpected object request')

    def apply(self, value): self.applied.append(value)

    def kubectl(self, *args, **kwargs):
        self.commands.append(args)
        if '--raw' in args:
            return json.dumps({'kubeletconfig': {'podPidsLimit': self.pid_limit}})
        if 'exec' in args:
            return boundary.PREFIX + json.dumps(self.positive) + '\n'
        return ''

    def submit(self, code, key, timeout):
        # Generated workload code is inspected, never executed on the test host.
        ast.parse(code)
        self.submitted.append(code)
        return {'id': len(self.submitted)}

    def running_pod(self, run): return self.pod

    def finish(self, run, state, timeout):
        value = self.restricted if run['id'] == 1 else {'attempt_limit': boundary.MAX_CHILDREN,
            'spawned': 10, 'children_reaped': 10, 'resource_denied': True}
        return {'result': {'output_head': boundary.PREFIX + json.dumps(value) + '\n'}}


class BoundaryTests(unittest.TestCase):
    def test_network_denial_requires_reachable_controls_on_both_sides(self):
        targets = {'controlled_peer': {}}
        yes, no = observation({'controlled_peer': True}), observation({'controlled_peer': False})
        result = boundary.evaluate_network(targets, yes, no, yes)
        self.assertEqual(result['controlled_peer']['qualification'], 'measured_denial')
        for first, last in ((no, yes), (yes, no), (no, no)):
            with self.subTest(first=first, last=last):
                result = boundary.evaluate_network(targets, first, no, last)
                self.assertEqual(result['controlled_peer']['qualification'], 'not_qualified')
        self.assertEqual(result['external_control']['qualification'], 'not_qualified')

    def test_any_workload_connection_is_a_failure_even_when_control_failed(self):
        with self.assertRaisesRegex(boundary.BoundaryFailure, 'boundary_open_controlled_peer'):
            boundary.evaluate_network({'controlled_peer': {}}, observation({'controlled_peer': False}),
                observation({'controlled_peer': True}), observation({'controlled_peer': False}))

    def test_missing_observation_cannot_count_as_denial(self):
        with self.assertRaisesRegex(boundary.BoundaryFailure, 'incomplete_boundary_network_observation'):
            boundary.evaluate_network({'controlled_peer': {}}, observation({'controlled_peer': True}),
                {'network': {}}, observation({'controlled_peer': True}))

    def test_external_probes_require_an_explicit_literal_endpoint(self):
        self.assertIsNone(boundary.external_target(''))
        self.assertEqual(boundary.external_target('192.0.2.20:18080')['port'], 18080)
        for value in ('https://example.com', 'example.com:443', '127.0.0.1:80', '169.254.169.254:80',
                      '0.0.0.0:80', '224.0.0.1:80', '192.0.2.20:65536'):
            with self.subTest(value=value), self.assertRaisesRegex(boundary.BoundaryFailure, 'invalid_boundary_external_endpoint'):
                boundary.external_target(value)

    def test_actual_admitted_pod_rejects_privileges_host_sources_or_runtime_fallback(self):
        boundary.inspect_pod(admitted_pod())
        changes = [dict(hostPID=True), dict(runtimeClassName='runc'), dict(automountServiceAccountToken=True),
            dict(volumes=[{'name': 'host', 'hostPath': {'path': '/'}}]),
            dict(volumes=[{'name': 'creds', 'secret': {'secretName': 'other'}}]),
            dict(initContainers=[{'name': 'injected'}])]
        for change in changes:
            pod = admitted_pod()
            pod['spec'].update(change)
            with self.subTest(change=change), self.assertRaises(boundary.BoundaryFailure):
                boundary.inspect_pod(pod)

    def test_generated_process_exercise_is_bounded_and_cleans_up_in_finally(self):
        code = boundary.fork_code()
        tree = ast.parse(code)
        self.assertLessEqual(boundary.MAX_CHILDREN, 128)
        self.assertIn('resource.RLIMIT_AS', code)
        self.assertIn('signal.alarm(15)', code)
        attempt = next(node for node in ast.walk(tree) if isinstance(node, ast.For) and isinstance(node.iter, ast.Call)
                       and isinstance(node.iter.func, ast.Name) and node.iter.func.id == 'range')
        self.assertEqual(ast.literal_eval(attempt.iter.args[0]), boundary.MAX_CHILDREN)
        cleanup = next(node.finalbody for node in ast.walk(tree) if isinstance(node, ast.Try) and node.finalbody)
        cleanup_source = '\n'.join(ast.unparse(node) for node in cleanup)
        self.assertIn('os.kill(child, signal.SIGKILL)', cleanup_source)
        self.assertIn('os.waitpid(child, 0)', cleanup_source)
        ast.parse(boundary.probe_code({'dns': {'kind': 'dns', 'address': '10.0.0.10', 'port': 53}}, filesystem=True))

    def test_partial_results_and_cleanup_preserve_unqualified_paths(self):
        cluster = FakeCluster()
        with patch.dict(boundary.os.environ, {}, clear=True):
            result = boundary.boundary_checks(cluster)
        self.assertEqual(result['qualification'], 'partial')
        self.assertEqual(result['control_node_count'], 2)
        self.assertEqual(result['network']['dns']['qualification'], 'measured_denial')
        self.assertEqual(result['network']['metadata']['qualification'], 'not_qualified')
        self.assertEqual(result['network']['external_control']['qualification'], 'not_qualified')
        self.assertEqual(result['host_escape_resistance'], 'not_qualified')
        self.assertEqual(result['guest_pid_ceiling'], 'not_qualified')
        self.assertEqual(result['bounded_process_exercise']['qualification'], 'exercise_only')
        self.assertEqual(cluster.commands[-1][:2], ('delete', 'namespace'))
        fixtures = [item for item in cluster.applied if item['kind'] == 'Pod']
        self.assertTrue(all(item['spec']['runtimeClassName'] == 'gvisor' for item in fixtures))
        self.assertEqual({item['spec']['nodeSelector']['kubernetes.io/hostname'] for item in fixtures},
                         {'different-worker-a-host', 'different-worker-b-host'})

    def test_open_boundary_or_missing_control_fails_and_always_cleans_fixtures(self):
        for mode in ('open', 'unreachable_control', 'host_namespace'):
            cluster = FakeCluster()
            if mode == 'open': cluster.restricted['network']['controlled_peer']['reachable'] = True
            if mode == 'unreachable_control': cluster.positive['network']['dns']['reachable'] = False
            if mode == 'host_namespace': cluster.pod['spec']['hostNetwork'] = True
            with self.subTest(mode=mode), patch.dict(boundary.os.environ, {}, clear=True), self.assertRaises(boundary.BoundaryFailure):
                boundary.boundary_checks(cluster)
            self.assertEqual(cluster.commands[-1][:2], ('delete', 'namespace'))

    def test_positive_control_policy_is_fixture_scoped_and_precedes_pods(self):
        cluster = FakeCluster()
        with patch.dict(boundary.os.environ, {}, clear=True):
            boundary.boundary_checks(cluster)
        policies = [item for item in cluster.applied if item['kind'] == 'NetworkPolicy']
        self.assertEqual(len(policies), 1)
        policy = policies[0]
        self.assertNotEqual(policy['metadata']['namespace'], boundary.NAMESPACE)
        self.assertTrue(policy['metadata']['namespace'].startswith('agentos-boundary-'))
        self.assertEqual(policy['spec']['podSelector'], {'matchLabels': {boundary.CONTROL_LABEL: 'true'}})
        self.assertEqual(set(policy['spec']['policyTypes']), {'Ingress', 'Egress'})
        self.assertEqual(policy['spec']['ingress'], [{}])
        self.assertEqual(policy['spec']['egress'], [{}])
        for pod in [item for item in cluster.applied if item['kind'] == 'Pod']:
            self.assertEqual(pod['metadata']['namespace'], policy['metadata']['namespace'])
            self.assertEqual(pod['metadata']['labels'][boundary.CONTROL_LABEL], 'true')
            self.assertLess(cluster.applied.index(policy), cluster.applied.index(pod))

    def test_no_child_processes_are_created_without_finite_node_pid_prerequisite(self):
        for limit in (-1, None, 4096, True):
            cluster = FakeCluster()
            cluster.pid_limit = limit
            with self.subTest(limit=limit), patch.dict(boundary.os.environ, {}, clear=True):
                result = boundary.boundary_checks(cluster)
            self.assertEqual(result['bounded_process_exercise']['qualification'], 'not_qualified')
            self.assertEqual(len(cluster.submitted), 1)  # Network/filesystem probe only.

    def test_namespace_creation_collision_does_not_authorize_cleanup(self):
        cluster = FakeCluster()
        original = cluster.kubectl
        def command(*args, **kwargs):
            if args[0] == 'create':
                raise RuntimeError('namespace_already_exists')
            return original(*args, **kwargs)
        with patch.object(cluster, 'kubectl', side_effect=command), patch.dict(boundary.os.environ, {}, clear=True), self.assertRaises(RuntimeError):
            boundary.boundary_checks(cluster)
        self.assertFalse(cluster.applied)
        self.assertFalse(any(args[0] == 'delete' for args in cluster.commands))

    def test_a_broken_route_on_any_selected_worker_cannot_qualify_denial(self):
        cluster = FakeCluster()
        original = cluster.kubectl
        def command(*args, **kwargs):
            if 'exec' in args and args[args.index('exec') + 1] == 'control-1':
                return boundary.PREFIX + json.dumps(observation(dict(controlled_peer=True, kubernetes_api=False, dns=True, metadata=False)))
            return original(*args, **kwargs)
        with patch.object(cluster, 'kubectl', side_effect=command), patch.dict(boundary.os.environ, {}, clear=True), self.assertRaisesRegex(
                boundary.BoundaryFailure, 'required_boundary_positive_control_unavailable'):
            boundary.boundary_checks(cluster)

    def test_unrelated_output_cannot_become_probe_evidence(self):
        for output in ('{}', boundary.PREFIX + 'malformed', boundary.PREFIX + '[]'):
            with self.subTest(output=output), self.assertRaises(boundary.BoundaryFailure):
                boundary.result_from_text(output)


if __name__ == '__main__':
    unittest.main()
