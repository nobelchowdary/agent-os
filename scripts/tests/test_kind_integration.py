"""Dry-run/failure-boundary checks; these do not execute a cluster campaign."""
import importlib.util
import io
from pathlib import Path
import tempfile
import threading
import tomllib
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
    def test_cancellation_waits_for_handler_and_never_accepts_a_remaining_pod(self):
        class FakeCluster:
            def __init__(self, premature=False):
                self.ready = self.cancelled = False
                self.log_reads = self.polls = 0
                self.premature = premature
            def submit(self, *args, **kwargs): return {'id': 'synthetic'}
            def running_pod(self, run): return {'spec': {'terminationGracePeriodSeconds': 10}}
            def request(self, method, path, **kwargs):
                if path.endswith('/logs'):
                    self.log_reads += 1
                    self.ready = self.log_reads > 1
                    return {'logs': 'ignoring-term' if self.ready else ''}
                if not self.ready:
                    raise AssertionError('cancelled before handler readiness')
                self.cancelled = True
                return {}
            def get(self, run):
                self.polls += 1
                return {'state': 'cancelled' if self.premature or self.polls > 1 else 'cancelling'}
            def pods(self, run):
                return [{'metadata': {'deletionGracePeriodSeconds': 10}}] if self.polls == 1 else []
        cluster = FakeCluster()
        with patch.object(campaign.time, 'sleep'):
            result = campaign.cancel_ignoring_term(cluster)
        self.assertTrue(cluster.cancelled)
        self.assertEqual(cluster.log_reads, 2)
        self.assertTrue(result['handler_ready'])
        self.assertEqual(result['configured_grace_seconds'], 10)
        self.assertEqual(result['observed_delete_grace_seconds'], [10])
        with patch.object(campaign.time, 'sleep'), self.assertRaisesRegex(campaign.CheckFailed, 'cancelled_before_pod_disappeared'):
            campaign.cancel_ignoring_term(FakeCluster(premature=True))

    def test_targeted_report_cannot_claim_the_full_stress_counts(self):
        import json
        from unittest.mock import Mock
        cluster = Mock(configuration={}, worker_nodes={'worker'}, api_node='api', node_inventory=[])
        with tempfile.TemporaryDirectory() as temporary, patch.object(campaign, 'WORK', Path(temporary)), patch.object(campaign, 'Cluster', return_value=cluster), patch.object(campaign, 'run_campaign', return_value=[]), patch('sys.argv', ['tool', '--execute', '--only-case', 'cancel_ignores_sigterm_until_gone']):
            self.assertEqual(campaign.main(), 0)
            report = json.loads((Path(temporary) / 'result.json').read_text())
        self.assertEqual(report['selection'], 'cancel_ignores_sigterm_until_gone')
        self.assertEqual(report['sequential_runs'], 0)
        self.assertEqual(report['crash_submits'], 0)

    def test_external_cluster_requires_opt_in_and_selected_nodes_before_commands(self):
        with patch.object(campaign.subprocess, 'run', side_effect=AssertionError('external command')):
            with self.assertRaisesRegex(campaign.CheckFailed, 'dedicated_kind_requires_kind_profile'):
                campaign.Cluster(cluster_profile='eks')
            with self.assertRaisesRegex(campaign.CheckFailed, 'requires_destructive_opt_in'):
                campaign.Cluster(kubeconfig='/private/config', context='test-eks')
            with self.assertRaisesRegex(campaign.CheckFailed, 'requires_nodes_and_images'):
                campaign.Cluster(kubeconfig='/private/config', context='test-eks', allow_destructive_external=True)
            with self.assertRaisesRegex(campaign.CheckFailed, 'workload_image_requires_sha256_digest'):
                campaign.Cluster(workload_image='python:latest')
            cluster = campaign.Cluster(kubectl='/private/bin/kubectl', kubeconfig='/private/config', context='test-eks',
                worker_nodes=['sandbox-a', 'sandbox-b'], api_node='api-c', api_image='private/agentos@sha256:' + 'a' * 64,
                workload_image='private/python@sha256:' + 'b' * 64,
                api_egress_cidrs=['192.0.2.5/32'],
                cluster_profile='eks', allow_destructive_external=True)
        self.assertFalse(cluster.dedicated_kind)
        self.assertEqual(cluster.k, ['/private/bin/kubectl', '--kubeconfig', '/private/config', '--context', 'test-eks'])
        self.assertEqual(cluster.worker_nodes, {'sandbox-a', 'sandbox-b'})
        self.assertEqual(cluster.profile, 'eks')

    def test_api_egress_accepts_only_explicit_host_cidrs_and_valid_tcp_ports(self):
        self.assertEqual(campaign.api_egress_destinations(['2001:db8::1/128', '192.0.2.5/32', '192.0.2.5/32'], 443),
                         ('192.0.2.5/32', '2001:db8::1/128'))
        for cidr in ['192.0.2.5', '192.0.2.5/24', '192.0.2.0/24', '0.0.0.0/0', '2001:db8::/64', '::/0', 'hostname/32', '']:
            with self.subTest(cidr=cidr), self.assertRaisesRegex(campaign.CheckFailed, 'invalid_api_egress_host_cidr'):
                campaign.api_egress_destinations([cidr], 443)
        for port in [0, -1, 65536, True, '443']:
            with self.subTest(port=port), self.assertRaisesRegex(campaign.CheckFailed, 'invalid_api_egress_port'):
                campaign.api_egress_destinations(['192.0.2.5/32'], port)
        for port in [1, 65535]:
            self.assertEqual(campaign.api_egress_destinations(['192.0.2.5/32'], port), ('192.0.2.5/32',))

    def test_external_cli_requires_api_destinations_before_any_cluster_call(self):
        base = ['tool', '--execute', '--context', 'test-eks', '--kubeconfig', '/private/config',
                '--worker-node', 'worker', '--api-node', 'api', '--api-image', 'prepared/api',
                '--workload-image', 'private/python@sha256:' + 'b' * 64, '--allow-destructive-external-cluster']
        for extra, reason in [([], 'external_cluster_requires_api_egress_destinations'),
                              (['--api-egress-cidr', '192.0.2.0/24'], 'invalid_api_egress_host_cidr'),
                              (['--api-egress-cidr', '192.0.2.5/32', '--api-egress-port', '0'], 'invalid_api_egress_port')]:
            with self.subTest(extra=extra), patch('sys.argv', base + extra), patch.object(campaign.subprocess, 'run',
                    side_effect=AssertionError('external command')), patch.object(campaign.subprocess, 'Popen',
                    side_effect=AssertionError('external process')), self.assertRaisesRegex(campaign.CheckFailed, reason):
                campaign.main()

    def test_external_kubelet_case_reports_a_skip_without_host_commands(self):
        from unittest.mock import Mock
        cluster = Mock(dedicated_kind=False)
        result = campaign.finished_kubelet_unavailable(cluster)
        self.assertEqual(result['status'], 'skipped')
        cluster.command.assert_not_called()
        cluster.submit.assert_not_called()

    def test_external_install_preserves_prepared_runtime_and_records_load_policy(self):
        import json
        class PreparedCluster(campaign.Cluster):
            def __init__(self, handler='runsc', extra_worker=False, existing_namespaces=()):
                super().__init__(kubeconfig='/private/config', context='test-eks', worker_nodes=['pool-a'],
                    api_node='control-a', api_image='private/prepared-image', cluster_profile='eks',
                    workload_image='private/python@sha256:' + 'b' * 64,
                    api_egress_cidrs=['192.0.2.5/32', '2001:db8::1/128'], api_egress_port=6443,
                    allow_destructive_external=True, load_sizes=(64,))
                self.handler, self.extra_worker = handler, extra_worker
                self.existing_namespaces = existing_namespaces
                self.applied, self.commands, self.operations = [], [], []
                self.quota = {'spec': {'hard': {'pods': '16', 'count/jobs.batch': '64', 'limits.memory': '8Gi'}}}
            def obj(self, kind, name=None, namespace=None):
                if kind == 'nodes':
                    nodes = [{'metadata': {'name': 'pool-a', 'labels': {'sandbox': 'yes', 'kubernetes.io/hostname': 'host-a'}}},
                             {'metadata': {'name': 'control-a', 'labels': {'kubernetes.io/hostname': 'host-control'}}}]
                    if self.extra_worker:
                        nodes.append({'metadata': {'name': 'unselected', 'labels': {'sandbox': 'yes'}}})
                    return {'items': nodes}
                if kind == 'runtimeclass':
                    return {'handler': self.handler, 'scheduling': {'nodeSelector': {'sandbox': 'yes'}}}
                if kind == 'namespaces':
                    return {'items': [{'metadata': {'name': name}} for name in self.existing_namespaces]}
                if kind == 'resourcequota': return self.quota
                raise AssertionError(kind)
            def kubectl(self, *args, **kwargs):
                self.commands.append(args)
                self.operations.append(('command', args))
                if args[0] == 'create' and '--dry-run=client' in args:
                    return json.dumps({'kind': 'Deployment', 'spec': {'template': {'spec': {'containers': [{}]}}}})
                return ''
            def apply(self, value):
                self.applied.append(value)
                self.operations.append(('apply', value))
            def rollout(self): pass
            def drain(self): pass
        cluster = PreparedCluster()
        cluster.install()
        self.assertFalse(any(value.get('kind') == 'RuntimeClass' for value in cluster.applied))
        deployment = next(value['items'][0] for value in cluster.applied if value.get('kind') == 'List')
        self.assertEqual(deployment['spec']['template']['spec']['nodeSelector'], {'kubernetes.io/hostname': 'host-control'})
        self.assertEqual(deployment['spec']['template']['spec']['containers'][0]['image'], 'private/prepared-image')
        policy = next(value for value in cluster.applied if value.get('kind') == 'NetworkPolicy')
        self.assertEqual(policy['metadata'], {'name': 'agentos-api-kubernetes-egress', 'namespace': 'agentos-system'})
        self.assertEqual(policy['spec'], {'podSelector': {'matchLabels': {'app.kubernetes.io/name': 'agent-os',
            'app.kubernetes.io/component': 'api'}}, 'policyTypes': ['Egress'], 'egress': [{
                'to': [{'ipBlock': {'cidr': '192.0.2.5/32'}}, {'ipBlock': {'cidr': '2001:db8::1/128'}}],
                'ports': [{'protocol': 'TCP', 'port': 6443}]}]})
        static_policy_command = ('apply', '-f', str(ROOT / 'deploy' / 'api-policy.yaml'))
        deployment_apply = next(value for value in cluster.applied if value.get('kind') == 'List')
        self.assertLess(cluster.operations.index(('command', static_policy_command)),
                        cluster.operations.index(('apply', deployment_apply)))
        self.assertLess(cluster.operations.index(('apply', policy)), cluster.operations.index(('apply', deployment_apply)))
        self.assertEqual(cluster.configuration['max_active_total'], 4)
        self.assertEqual(cluster.configuration['max_active_per_tenant'], 4)
        self.assertEqual(cluster.configuration['startup_timeout_seconds'], 20)
        self.assertEqual(cluster.configuration['resource_quota']['pods'], '16')
        self.assertEqual(cluster.configuration['templates']['python']['termination_grace_period_seconds'], 10)
        self.assertEqual(cluster.configuration['templates']['python']['image'], 'private/python@sha256:' + 'b' * 64)
        self.assertEqual(cluster.configuration['api_kubernetes_egress'],
                         {'host_cidrs': ['192.0.2.5/32', '2001:db8::1/128'], 'tcp_port': 6443})
        self.assertEqual(cluster.configuration['templates']['load']['resources'], {'cpu_milli': 25, 'memory_mib': 64, 'disk_mib': 16})
        self.assertNotIn(cluster.token, json.dumps(cluster.configuration))
        loaded = cluster.configure_load(64)
        self.assertEqual(loaded['max_active_total'], 64)
        self.assertEqual(loaded['max_active_per_tenant'], 64)
        self.assertEqual(loaded['startup_timeout_seconds'], 240)
        self.assertEqual(loaded['resource_quota']['pods'], '64')
        self.assertEqual(cluster.configuration['resource_quota']['pods'], '16')
        self.assertEqual(cluster.configuration['startup_timeout_seconds'], 20)
        self.assertEqual(cluster.commands[-1], ('-n', 'agentos-system', 'rollout', 'restart', 'deployment/agentos-api'))
        for cluster, reason in [(PreparedCluster(handler='runc'), 'external_runtime_not_runsc'),
                                (PreparedCluster(extra_worker=True), 'external_runtime_nodes_must_match_selection'),
                                (PreparedCluster(existing_namespaces=['agentos-system']), 'external_campaign_requires_fresh_namespaces'),
                                (PreparedCluster(existing_namespaces=[campaign.NAMESPACE]), 'external_campaign_requires_fresh_namespaces')]:
            with self.assertRaisesRegex(campaign.CheckFailed, reason): cluster.install()
            self.assertEqual(cluster.commands, [])
            self.assertEqual(cluster.applied, [])
        dedicated = PreparedCluster()
        dedicated.dedicated_kind = True
        dedicated.install()
        self.assertFalse(any(value.get('kind') == 'NetworkPolicy' for value in dedicated.applied))
        self.assertNotIn(static_policy_command, dedicated.commands)
        self.assertIsNone(dedicated.configuration['api_kubernetes_egress'])

    def test_finished_task_with_stopped_kubelet_restores_worker_and_api_even_on_failure(self):
        class FakeCluster:
            dedicated_kind = True
            worker_nodes, api_node = {'selected-worker'}, 'selected-api'
            def __init__(self, fail=False, label='agentos-ci'):
                self.steps, self.reads = [], 0
                self.fail, self.label = fail, label
            def submit(self, *args, **kwargs): return {'id': 'synthetic'}
            def running_pod(self, run): return {'spec': {'nodeName': 'selected-worker'}}
            def command(self, args):
                if 'inspect' in args: return self.label
                self.steps.append('kubelet-' + args[-2])
            def kubectl(self, *args):
                if 'scale' in args: self.steps.append(args[-1])
            def stop_forward(self): pass
            def rollout(self): self.steps.append('api-rollout')
            def obj(self, *args):
                self.reads += 1
                if self.reads == 1: return {}
                self.steps.append('job-completed')
                return {'status': {'conditions': [{'type': 'Complete', 'status': 'True'}]}}
            def finish(self, *args, **kwargs):
                self.steps.append('terminal-read')
                if self.fail: raise campaign.CheckFailed('injected_failure')
                return {'state': 'succeeded', 'result': {'status': 'unavailable', 'exit_code': 0, 'output_complete': False}}
            def request(self, method, path, **kwargs):
                self.steps.append(path)
                return {'status': 'ready'}
        cluster = FakeCluster()
        result = campaign.finished_kubelet_unavailable(cluster)
        self.assertTrue(result['api_ready'])
        self.assertEqual(cluster.steps, ['--replicas=0', 'job-completed', 'kubelet-stop', '--replicas=1',
            'api-rollout', 'terminal-read', '/readyz', 'kubelet-start', '--replicas=1', 'api-rollout'])
        cluster = FakeCluster(fail=True)
        with self.assertRaisesRegex(campaign.CheckFailed, 'injected_failure'):
            campaign.finished_kubelet_unavailable(cluster)
        self.assertEqual(cluster.steps[-3:], ['kubelet-start', '--replicas=1', 'api-rollout'])
        cluster = FakeCluster(label='other-kind-cluster')
        with self.assertRaisesRegex(campaign.CheckFailed, 'refusing_stop_non_dedicated_kind_kubelet'):
            campaign.finished_kubelet_unavailable(cluster)
        self.assertEqual(cluster.steps, [])

    def test_load_uses_observed_overlap_and_preserves_signed_clock_skew(self):
        from unittest.mock import Mock
        observation = {name: {'started_at': '2026-01-01T00:00:00Z',
                             'finished_at': '2026-01-01T00:00:10Z'} for name in ('a', 'b')}
        records = [{'id': name, 'events': [{'state': 'succeeded', 'at': ended}]}
                   for name, ended in [('a', '2026-01-01T00:00:09Z'), ('b', '2026-01-01T00:00:12Z')]]
        watch = Mock()
        watch.load_observations.return_value = (observation, 2)
        result = campaign.load_measurements(watch, records, 2)
        self.assertEqual(result['peak_observed_running_containers'], 2)
        self.assertEqual(result['peak_container_timestamp_overlap'], 2)
        self.assertEqual(result['pod_exit_to_persisted_terminal_seconds']['min'], -1)
        self.assertEqual(result['pod_exit_to_persisted_terminal_seconds']['p95'], 2)
        self.assertEqual(result['negative_lag_samples'], 1)
        watch.load_observations.return_value = (observation, 1)
        with self.assertRaisesRegex(campaign.CheckFailed, 'target_running_concurrency_not_observed') as failed:
            campaign.load_measurements(watch, records, 2)
        self.assertEqual(failed.exception.evidence['peak_observed_running_containers'], 1)
        self.assertEqual(failed.exception.evidence['pod_exit_to_persisted_terminal_seconds']['count'], 2)
        watch.load_observations.return_value = ({'a': observation['a'], 'b': {}}, 2)
        with self.assertRaisesRegex(campaign.CheckFailed, 'missing_load_pod_timestamps'):
            campaign.load_measurements(watch, records, 2)

    def test_selected_load_records_configured_count_without_full_campaign_claims(self):
        import json
        from unittest.mock import Mock
        cluster = Mock(configuration={'max_active_total': 64}, worker_nodes={'worker'}, api_node='api', node_inventory=[])
        with tempfile.TemporaryDirectory() as temporary, patch.object(campaign, 'WORK', Path(temporary)), patch.object(campaign, 'Cluster', return_value=cluster) as construct, patch.object(campaign, 'run_campaign', return_value=[]) as execute, patch('sys.argv', ['tool', '--execute', '--only-case', 'load_64']):
            campaign.main()
            report = json.loads((Path(temporary) / 'result.json').read_text())
        self.assertEqual(construct.call_args.kwargs['load_sizes'], (64,))
        self.assertEqual(execute.call_args.kwargs['load_sizes'], (64,))
        self.assertEqual(report['load_concurrency_targets'], [64])
        self.assertEqual(report['sequential_runs'], 0)
        self.assertEqual(report['crash_submits'], 0)
        self.assertEqual(report['cluster_qualification'], 'not_qualified')

    def test_failed_case_retains_prior_cases_and_observed_failure_evidence(self):
        from unittest.mock import Mock
        cluster = Mock()
        cluster.finish.return_value = {'result': {'exit_code': 0, 'output_head': 'gVisor'}}
        evidence = {'peak_observed_running_containers': 12, 'target_concurrency': 16}
        results = []
        with patch.object(campaign, 'ObjectWatch'), patch.object(campaign, 'finished_kubelet_unavailable',
                side_effect=campaign.CheckFailed('injected_failure', evidence=evidence)), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(campaign.CheckFailed, 'injected_failure'):
                campaign.run_campaign(cluster, 200, 500, 'finished_kubelet_unavailable', results=results)
        self.assertEqual([item['case'] for item in results], ['actual_gvisor_canary', 'finished_kubelet_unavailable'])
        self.assertEqual(results[0]['status'], 'passed')
        self.assertEqual(results[1]['status'], 'failed')
        self.assertEqual(results[1]['observations'], evidence)

    def test_watch_uses_list_snapshot_in_raw_api_request_and_counts_uids(self):
        import json
        from unittest.mock import Mock
        from urllib.parse import urlsplit, parse_qs
        events = [
            {'type': 'BOOKMARK', 'object': {'metadata': {'resourceVersion': '124'}}},
            *[{'type': 'ADDED', 'object': {'metadata': {'uid': uid, 'labels': {campaign.RUN_LABEL: 'run'}}}}
              for uid in ('first', 'first', 'second')]]
        process = Mock(stdout=io.BytesIO(('\n'.join(json.dumps(value) for value in events)).encode()))
        cluster = Mock(k=['kubectl', '--context', campaign.CONTEXT])
        cluster.obj.return_value = {'metadata': {'resourceVersion': '123'}}
        for kind, prefix in [('jobs', '/apis/batch/v1'), ('pods', '/api/v1')]:
            process.stdout.seek(0)
            with patch.object(campaign.subprocess, 'Popen', return_value=process) as launch, patch.object(campaign.threading, 'Thread'):
                watch = campaign.ObjectWatch(cluster, kind)
            args = launch.call_args.args[0]
            url = urlsplit(args[args.index('--raw') + 1])
            self.assertEqual(url.path, prefix + '/namespaces/' + campaign.NAMESPACE + '/' + kind)
            self.assertEqual(parse_qs(url.query)['resourceVersion'], ['123'])
            self.assertEqual(parse_qs(url.query)['watch'], ['true'])
            watch.read()
            self.assertEqual(watch.seen['run'], {'first', 'second'})

    def test_pod_watch_retains_exit_timestamps_and_measures_running_overlap(self):
        import json
        from unittest.mock import Mock
        def event(run, event_type, state):
            return {'type': event_type, 'object': {'metadata': {'uid': run + '-pod', 'labels': {campaign.RUN_LABEL: run}},
                'status': {'containerStatuses': [{'name': 'agent', 'state': state}]}}}
        running = {'running': {'startedAt': '2026-01-01T00:00:00Z'}}
        ended = {'terminated': {'startedAt': '2026-01-01T00:00:00Z', 'finishedAt': '2026-01-01T00:00:10Z'}}
        events = [event('a', 'ADDED', {}), event('b', 'ADDED', {}),
                  event('a', 'MODIFIED', running), event('b', 'MODIFIED', running),
                  event('a', 'MODIFIED', ended), event('a', 'DELETED', ended), event('b', 'MODIFIED', ended)]
        process = Mock(stdout=io.BytesIO(('\n'.join(json.dumps(value) for value in events)).encode()))
        cluster = Mock(k=['kubectl'])
        cluster.obj.return_value = {'metadata': {'resourceVersion': '123'}}
        with patch.object(campaign.subprocess, 'Popen', return_value=process), patch.object(campaign.threading, 'Thread'):
            watch = campaign.ObjectWatch(cluster, 'pods')
        watch.read()
        observations, peak = watch.load_observations({'a', 'b'})
        self.assertEqual(peak, 2)
        self.assertEqual(observations['a']['finished_at'], '2026-01-01T00:00:10Z')
        self.assertEqual(watch.seen, {'a': {'a-pod'}, 'b': {'b-pod'}})
        self.assertEqual(watch.running, set())

    def test_runsc_patch_preserves_kind_v2_systemd_runtime_configuration(self):
        # kind v0.33's node image keeps a v2 config even with containerd 2.x.
        # A separate v3 runtimes table shadows the entire v2 runtime map during
        # containerd migration, losing runc's systemd setting before kubelet starts.
        image_config = '''version = 2
[plugins."io.containerd.grpc.v1.cri".containerd]
  default_runtime_name = "runc"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.test-handler]
  runtime_type = "io.containerd.runc.v2"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.test-handler.options]
  SystemdCgroup = true
'''
        config = bootstrap.cluster_config()
        merged = tomllib.loads(image_config + '\n' + '\n'.join(config['containerdConfigPatches']))
        self.assertEqual(merged['version'], 2)
        self.assertEqual(set(merged['plugins']), {'io.containerd.grpc.v1.cri'})
        containerd = merged['plugins']['io.containerd.grpc.v1.cri']['containerd']
        self.assertEqual(containerd['default_runtime_name'], 'runc')
        for runtime in ('runc', 'test-handler'):
            self.assertTrue(containerd['runtimes'][runtime]['options']['SystemdCgroup'])
        runsc = containerd['runtimes']['runsc']
        self.assertEqual(runsc['runtime_type'], 'io.containerd.runsc.v1')
        self.assertEqual(runsc['options'], {'TypeUrl': 'io.containerd.runsc.v1.options',
                                            'ConfigPath': '/etc/containerd/runsc.toml'})
        self.assertEqual(tomllib.loads(bootstrap.runsc_config()),
                         {'runsc_config': {'platform': 'systrap', 'systemd-cgroup': 'true'}})

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
        cluster.calls = []
        cluster.token = 'private-value'
        pod = {'metadata': {'namespace': 'test', 'name': 'pod', 'annotations': {'token': 'private-value'}},
               'spec': {'containers': [{'name': 'agent', 'env': [{'value': 'private-value'}]}]},
               'status': {'phase': 'Pending', 'message': 'private-value', 'containerStatuses': [
                   {'name': 'agent', 'state': {'waiting': {'reason': 'ImagePullBackOff', 'message': 'private-value'}}}]}}
        import json
        with patch.object(cluster, 'kubectl', return_value=json.dumps({'items': [pod]})), patch.object(cluster, 'obj', return_value={'items': [{'reason': 'Failed', 'message': 'runtime error private-value'}]}):
            value = cluster.diagnostics()
        self.assertNotIn('private-value', json.dumps(value))
        self.assertEqual(value['pods'][0]['containers'][0]['waiting_reason'], 'ImagePullBackOff')
        self.assertEqual(value['workload_events'][0]['message'], 'runtime error [redacted]')


if __name__ == '__main__':
    unittest.main()
