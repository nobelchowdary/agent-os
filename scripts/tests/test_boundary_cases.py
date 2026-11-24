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
    return {'metadata': {'uid': '11111111-2222-4333-8444-555555555555'}, 'status': {'qosClass': 'Guaranteed'},
        'spec': {'runtimeClassName': 'gvisor', 'automountServiceAccountToken': False, 'nodeName': 'different-worker-a',
        'securityContext': {'runAsNonRoot': True}, 'volumes': [{'name': 'tmp', 'emptyDir': {}}],
        'containers': [{'name': 'agent', 'securityContext': {'readOnlyRootFilesystem': True,
            'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}}}]}}


class FakeCluster:
    def __init__(self):
        self.applied, self.commands, self.submitted = [], [], []
        self.finished_states = []
        self.worker_nodes = {'different-worker-a', 'different-worker-b'}
        self.api_node, self.dedicated_kind = 'selected-api', False
        self.drains, self.ready = 0, True
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

    def drain(self): self.drains += 1

    def request(self, *args, **kwargs): return {'status': 'ready' if self.ready else 'unready'}

    def finish(self, run, state, timeout):
        self.finished_states.append((run['id'], state))
        value = self.restricted if run['id'] == 1 else {'stage': 'complete', 'attempt_limit': boundary.MAX_CHILDREN,
            'spawned': 10, 'children_reaped': 10, 'resource_denied': True}
        if run['id'] == 3:
            value = {'gvisor': True}
        return {'state': 'succeeded', 'reason_code': 'completed',
                'result': {'exit_code': 0, 'output_head': boundary.PREFIX + json.dumps(value) + '\n'}}


def observed_host_limit(pod):
    return {'qualification': 'host_pid_limit_hit', 'pod_uid_verified': True, 'pod_uid': pod['metadata']['uid'],
        'node_name': pod['spec']['nodeName'], 'qos_class': pod['status']['qosClass'],
        'pids_max': 128, 'pids_current_peak': 128, 'pids_peak': 128, 'pids_events_max_before': 0,
        'pids_events_max_after': 1, 'pids_events_max_increase': 1, 'readings_complete': True, 'sample_count': 4,
        'cgroup_gone': True}


class FakeObserver:
    def __init__(self, value):
        self.value, self.started, self.closed = value, False, False
    def start(self):
        self.started = True
        return self
    def close(self):
        self.closed = True
        return self.value


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
        self.assertLess(code.index('"stage": "fork_progress", "spawned": 0'), code.index('os.fork()'))
        self.assertLess(code.index('"stage": "fork_progress", "spawned": 0'), code.index('time.sleep(5)'))
        self.assertLess(code.index('time.sleep(5)'), code.index('os.fork()'))
        self.assertIn('len(children) % 8 == 0', code)
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
        self.assertIn((2, None), cluster.finished_states)
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

    def test_process_diagnostics_export_only_valid_bounded_progress_and_fixed_flags(self):
        secret = 'credential-value-must-never-escape'
        lines = [boundary.PREFIX + json.dumps({'stage': 'fork_progress', 'spawned': 0}),
                 boundary.PREFIX + json.dumps({'stage': 'fork_progress', 'spawned': 8, 'extra': secret}),
                 boundary.PREFIX + json.dumps({'stage': 'fork_progress', 'spawned': 65}),
                 boundary.PREFIX + json.dumps({'stage': secret, 'spawned': 16}),
                 boundary.PREFIX + json.dumps({'stage': 'fork_progress', 'spawned': True}),
                 boundary.PREFIX + 'malformed',
                 'Traceback (most recent call last): ' + secret,
                 'Resource temporarily unavailable; Cannot allocate memory; MemoryError; TimeoutError; fatal error:']
        record = {'state': 'failed', 'reason_code': 'task_error',
                  'result': {'exit_code': 128, 'output_head': '\n'.join(lines), 'output_tail': secret}}
        summary = boundary.process_diagnostics(record)
        self.assertEqual(summary['last_progress_spawned'], 8)
        self.assertEqual(summary['last_progress_stage'], 'fork_progress')
        self.assertEqual(summary['exit_code'], 128)
        self.assertTrue(summary['error_markers']['resource_temporarily_unavailable'])
        self.assertTrue(summary['error_markers']['cannot_allocate_memory'])
        self.assertTrue(summary['error_markers']['traceback'])
        self.assertTrue(summary['error_markers']['memory_error'])
        self.assertTrue(summary['error_markers']['timeout'])
        self.assertFalse(summary['error_markers']['failed_to_create_os_thread'])
        self.assertNotIn(secret, json.dumps(summary))
        self.assertNotIn('output_head', summary)
        invalid = boundary.process_diagnostics({'state': secret, 'reason_code': secret,
            'result': {'exit_code': True, 'output_head': boundary.PREFIX + '[]'}})
        self.assertEqual(invalid['state'], 'unrecognized')
        self.assertEqual(invalid['reason_code'], 'unrecognized')
        self.assertIsNone(invalid['exit_code'])
        self.assertIsNone(invalid['last_progress_spawned'])

    def test_unexpected_exit_stays_failed_and_preserves_only_safe_earlier_evidence(self):
        cluster = FakeCluster()
        original = cluster.finish
        secret = 'private-workload-text-must-not-escape'
        def finish(run, state, timeout):
            if run['id'] != 2:
                return original(run, state, timeout)
            self.assertIsNone(state)
            return {'state': 'failed', 'reason_code': 'task_error', 'result': {'exit_code': 128,
                'output_head': boundary.PREFIX + json.dumps({'stage': 'fork_progress', 'spawned': 8, 'secret': secret})
                               + '\nResource temporarily unavailable ' + secret}}
        with patch.object(cluster, 'finish', side_effect=finish), patch.dict(boundary.os.environ, {}, clear=True), self.assertRaisesRegex(
                boundary.BoundaryFailure, 'unexpected_bounded_process_outcome') as raised:
            boundary.boundary_checks(cluster)
        evidence = raised.exception.evidence
        self.assertEqual(evidence['network']['controlled_peer']['qualification'], 'measured_denial')
        self.assertTrue(evidence['admitted_pod_checks']['required_runtime'])
        self.assertTrue(evidence['process_and_filesystem']['root_mount_read_only'])
        self.assertEqual(evidence['host_escape_resistance'], 'not_qualified')
        process = evidence['bounded_process_exercise']
        self.assertEqual(process['qualification'], 'not_qualified')
        self.assertEqual(process['fixture']['state'], 'failed')
        self.assertEqual(process['fixture']['reason_code'], 'task_error')
        self.assertEqual(process['fixture']['exit_code'], 128)
        self.assertEqual(process['fixture']['last_progress_spawned'], 8)
        self.assertTrue(process['fixture']['error_markers']['resource_temporarily_unavailable'])
        self.assertNotIn(secret, json.dumps(evidence))
        self.assertEqual(len(cluster.submitted), 2)  # No followup success can hide this failure.
        self.assertEqual(cluster.commands[-1][:2], ('delete', 'namespace'))

    def test_pid_limit_termination_requires_bound_counter_evidence_and_recovery(self):
        cluster = FakeCluster()
        cluster.dedicated_kind = True
        observer = FakeObserver(observed_host_limit(cluster.pod))
        original = cluster.finish
        def finish(run, state, timeout):
            if run['id'] != 2:
                return original(run, state, timeout)
            self.assertTrue(observer.started)
            self.assertIsNone(state)
            return {'state': 'failed', 'reason_code': 'task_error', 'result': {'exit_code': 128,
                'output_head': boundary.PREFIX + json.dumps({'stage': 'fork_progress', 'spawned': 16})}}
        with patch.object(boundary, 'create_process_observer', return_value=observer), patch.object(cluster, 'finish', side_effect=finish), patch.dict(
                boundary.os.environ, {}, clear=True):
            result = boundary.boundary_checks(cluster)
        process = result['bounded_process_exercise']
        self.assertTrue(observer.closed)
        self.assertEqual(process['qualification'], 'host_limit_observed')
        self.assertEqual(process['outcome'], 'sandbox_terminated_at_host_pid_limit')
        self.assertEqual(process['fixture']['exit_code'], 128)
        self.assertTrue(process['selected_nodes_ready'])
        self.assertTrue(process['api_ready'])
        self.assertTrue(process['followup_actual_gvisor_succeeded'])
        self.assertTrue(process['process_and_followup_cleaned_up'])
        self.assertEqual(cluster.drains, 2)
        self.assertEqual(result['guest_pid_ceiling'], 'not_qualified')
        serialized = json.dumps(process['host_pid_observation'])
        self.assertNotIn(cluster.pod['metadata']['uid'], serialized)
        self.assertNotIn(cluster.pod['spec']['nodeName'], serialized)

    def test_exit_128_without_every_actor_bound_counter_condition_stays_failed(self):
        invalid = [dict(qualification='not_qualified'), dict(pod_uid_verified=False), dict(pod_uid='wrong-pod'),
            dict(node_name='different-worker-b'), dict(qos_class='Burstable'), dict(readings_complete=False),
            dict(pids_max=129), dict(pids_peak=129), dict(pids_current_peak=129), dict(pids_peak=None),
            dict(pids_events_max_before=1, pids_events_max_after=1, pids_events_max_increase=0),
            dict(pids_events_max_increase=2), dict(sample_count=1)]
        for change in invalid:
            cluster = FakeCluster()
            cluster.dedicated_kind = True
            report = observed_host_limit(cluster.pod)
            report.update(change)
            observer = FakeObserver(report)
            original = cluster.finish
            def finish(run, state, timeout):
                return original(run, state, timeout) if run['id'] != 2 else {
                    'state': 'failed', 'reason_code': 'task_error', 'result': {'exit_code': 128}}
            with self.subTest(change=change), patch.object(boundary, 'create_process_observer', return_value=observer), patch.object(
                    cluster, 'finish', side_effect=finish), patch.dict(boundary.os.environ, {}, clear=True), self.assertRaisesRegex(
                    boundary.BoundaryFailure, 'unexpected_bounded_process_outcome'):
                boundary.boundary_checks(cluster)
            self.assertTrue(observer.closed)
            self.assertEqual(cluster.drains, 0)

    def test_other_failure_codes_and_failed_recovery_do_not_qualify_resource_pressure(self):
        for state, reason, exit_code, ready in [('failed', 'oom_killed', 128, True), ('failed', 'task_error', 137, True),
                                                ('failed', 'task_error', 128, False)]:
            cluster = FakeCluster()
            cluster.dedicated_kind, cluster.ready = True, ready
            observer = FakeObserver(observed_host_limit(cluster.pod))
            original = cluster.finish
            def finish(run, expected, timeout):
                return original(run, expected, timeout) if run['id'] != 2 else {
                    'state': state, 'reason_code': reason, 'result': {'exit_code': exit_code}}
            # The real API is called with a named state argument for the pressure
            # result; keep that interface explicit in the fake too.
            def dispatch(run, state, timeout):
                return finish(run, state, timeout)
            category = 'boundary_process_recovery_failed' if not ready else 'unexpected_bounded_process_outcome'
            with self.subTest(reason=reason, exit_code=exit_code, ready=ready), patch.object(boundary, 'create_process_observer', return_value=observer), patch.object(
                    cluster, 'finish', side_effect=dispatch), patch.dict(boundary.os.environ, {}, clear=True), self.assertRaisesRegex(
                    boundary.BoundaryFailure, category):
                boundary.boundary_checks(cluster)
            self.assertTrue(observer.closed)

    def test_observer_is_closed_when_terminal_read_fails_and_never_runs_for_external(self):
        cluster = FakeCluster()
        with patch.object(boundary, 'create_process_observer', side_effect=AssertionError('external host access')), patch.dict(
                boundary.os.environ, {}, clear=True):
            result = boundary.boundary_checks(cluster)
        self.assertEqual(result['bounded_process_exercise']['host_pid_observation']['qualification'], 'not_qualified')
        cluster = FakeCluster()
        cluster.dedicated_kind = True
        observer = FakeObserver(observed_host_limit(cluster.pod))
        original = cluster.finish
        def finish(run, state, timeout):
            if run['id'] == 2:
                raise RuntimeError('terminal_read_unavailable')
            return original(run, state, timeout)
        with patch.object(boundary, 'create_process_observer', return_value=observer), patch.object(cluster, 'finish', side_effect=finish), patch.dict(
                boundary.os.environ, {}, clear=True), self.assertRaisesRegex(RuntimeError, 'terminal_read_unavailable'):
            boundary.boundary_checks(cluster)
        self.assertTrue(observer.closed)

    def test_observer_start_failure_cannot_supply_limit_evidence_from_close(self):
        cluster = FakeCluster()
        cluster.dedicated_kind = True
        observer = FakeObserver(observed_host_limit(cluster.pod))
        original = cluster.finish
        def finish(run, state, timeout):
            return original(run, state, timeout) if run['id'] != 2 else {
                'state': 'failed', 'reason_code': 'task_error', 'result': {'exit_code': 128}}
        with patch.object(observer, 'start', side_effect=RuntimeError('baseline_unavailable')), patch.object(
                boundary, 'create_process_observer', return_value=observer), patch.object(cluster, 'finish', side_effect=finish), patch.dict(
                boundary.os.environ, {}, clear=True), self.assertRaisesRegex(boundary.BoundaryFailure, 'unexpected_bounded_process_outcome') as raised:
            boundary.boundary_checks(cluster)
        self.assertTrue(observer.closed)
        self.assertEqual(raised.exception.evidence['bounded_process_exercise']['host_pid_observation']['qualification'], 'not_qualified')

    def test_real_observer_factory_loads_without_starting_host_access(self):
        cluster = FakeCluster()
        observer = boundary.create_process_observer(cluster, cluster.pod)
        self.assertEqual(observer.__class__.__name__, 'ProcessObserver')
        self.assertEqual(observer.pod_uid, cluster.pod['metadata']['uid'])
        self.assertEqual(observer.close()['qualification'], 'not_qualified')
        self.assertEqual(cluster.commands, [])


if __name__ == '__main__':
    unittest.main()
