"""Host counter parsing and observer boundaries; no Docker or guest code runs."""
import importlib.util
import json
from pathlib import Path
import subprocess
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('process_probe', ROOT / 'scripts' / 'process_probe.py')
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
UID = '12345678-abcd-4321-9876-123456789abc'
CONTAINER_ID = 'a' * 64


def pod(qos='Guaranteed', uid=UID, node='selected-worker'):
    return {'metadata': {'uid': uid}, 'spec': {'nodeName': node}, 'status': {'qosClass': qos}}


class FakeCluster:
    dedicated_kind = True
    worker_nodes = {'selected-worker'}

    def __init__(self, samples=None, label='agentos-ci', mount_scope='hierarchical'):
        self.samples = iter(samples or ['128 40 42 5'])
        self.label, self.calls = label, []
        self.mount_scope = mount_scope
        self.sampled = threading.Event()

    def command(self, args, timeout):
        self.calls.append((args, timeout))
        if args[1] == 'inspect':
            return CONTAINER_ID + ' ' + self.label
        if args[5] == probe.VERIFY_COUNTER_SCOPE:
            return self.mount_scope
        try:
            value = next(self.samples)
        except StopIteration:
            value = 'gone'
        if isinstance(value, Exception):
            self.sampled.set()
            raise value
        if value in ('gone', 'unavailable'):
            self.sampled.set()
        return value


class ProcessProbeTests(unittest.TestCase):
    def test_static_mount_verifier_checks_both_option_sets_and_exact_cgroup2_mount(self):
        # Execute only the trusted read-only verifier, against synthetic stdin.
        # No workload code, Docker, host mount inventory or temporary file runs.
        script = probe.VERIFY_COUNTER_SCOPE.replace('/proc/self/mountinfo', '/dev/stdin')
        for inventory, expected in [
                ('29 23 0:26 / /sys/fs/cgroup rw,nosuid shared:3 - cgroup2 cgroup rw,nsdelegate\n', 'hierarchical'),
                ('29 23 0:26 / /sys/fs/cgroup rw,pids_localevents - cgroup2 cgroup rw\n', 'local_events'),
                ('29 23 0:26 / /sys/fs/cgroup ro - cgroup2 cgroup rw,pids_localevents\n', 'local_events'),
                ('29 23 0:26 / /sys/fs/cgroup/unified rw - cgroup2 cgroup rw\n', 'unknown'),
                ('29 23 0:26 / /sys/fs/cgroup rw - tmpfs tmpfs rw\n', 'unknown'),
                ('29 23 0:26 / /sys/fs/cgroup rw - cgroup2\n', 'unknown'),
                ('', 'unknown')]:
            with self.subTest(inventory=inventory):
                result = subprocess.run(['sh', '-c', script], input=inventory, text=True,
                                        capture_output=True, timeout=2)
                self.assertEqual(result.returncode, 0)
                self.assertEqual(result.stdout.strip(), expected)
                self.assertEqual(result.stderr, '')

    def test_strict_identity_and_selected_kind_gate_precede_every_host_command(self):
        for value, dedicated, reason in [
                (pod(uid='../../etc/passwd'), True, 'invalid_pod_uid'),
                (pod(uid=UID.upper()), True, 'invalid_pod_uid'),
                (pod(uid='urn:uuid:' + UID), True, 'invalid_pod_uid'),
                (pod(uid=UID + '; true'), True, 'invalid_pod_uid'),
                (pod(node='other-worker'), True, 'selected_worker'),
                (pod(node='--privileged'), True, 'selected_worker'),
                (pod(qos='BestEffort'), True, 'unsupported_qos'),
                (pod(), False, 'dedicated_kind')]:
            with self.subTest(value=value, dedicated=dedicated):
                cluster = FakeCluster()
                cluster.dedicated_kind = dedicated
                observer = probe.ProcessObserver(cluster, value)
                with self.assertRaisesRegex(probe.ProcessProbeError, reason):
                    observer.start()
                result = observer.close()
                self.assertEqual(cluster.calls, [])
                self.assertEqual(result['qualification'], 'not_qualified')
                self.assertFalse(result['pod_uid_verified'])
                self.assertIsNone(result['pod_uid'])

    def test_wrong_docker_ownership_never_reads_a_host_file(self):
        cluster = FakeCluster(label='other-kind-cluster')
        observer = probe.ProcessObserver(cluster, pod())
        with self.assertRaisesRegex(probe.ProcessProbeError, 'wrong_kind_container'):
            observer.start()
        result = observer.close()
        self.assertEqual(len(cluster.calls), 1)
        self.assertEqual(cluster.calls[0][0][1], 'inspect')
        self.assertFalse(result['pod_uid_verified'])

    def test_fixed_read_script_and_canonical_qos_paths_use_pinned_container_id(self):
        uid = UID.replace('-', '_')
        for qos, suffix in [('Guaranteed', 'kubepods-pod' + uid + '.slice'),
                            ('Burstable', 'kubepods-burstable.slice/kubepods-burstable-pod' + uid + '.slice')]:
            with self.subTest(qos=qos):
                cluster = FakeCluster()
                observer = probe.ProcessObserver(cluster, pod(qos=qos)).start()
                # The first counter read completed synchronously before start returned.
                self.assertEqual(len(cluster.calls), 3)
                result = observer.close()
                scope_args, _ = cluster.calls[1]
                self.assertEqual(scope_args, ['docker', 'exec', CONTAINER_ID, 'sh', '-c',
                                             probe.VERIFY_COUNTER_SCOPE, 'agentos-pids-scope'])
                args, timeout = cluster.calls[2]
                self.assertEqual(args[:5], ['docker', 'exec', CONTAINER_ID, 'sh', '-c'])
                self.assertEqual(args[5], probe.READ_COUNTERS)
                self.assertEqual(args[6:], ['agentos-pids-observer', '/sys/fs/cgroup/kubepods.slice/' + suffix])
                self.assertEqual(timeout, 1)
                self.assertFalse(observer._thread.is_alive())
                self.assertNotIn('/sys/', json.dumps(result))
                self.assertNotIn(CONTAINER_ID, json.dumps(result))

    def test_unknown_or_local_mount_scope_refuses_before_counter_reads(self):
        for scope in ('unknown', 'local_events', '', '/sys/fs/cgroup rw,pids_localevents'):
            with self.subTest(scope=scope):
                cluster = FakeCluster(mount_scope=scope)
                observer = probe.ProcessObserver(cluster, pod())
                with self.assertRaisesRegex(probe.ProcessProbeError, 'scope_not_hierarchical'):
                    observer.start()
                result = observer.close()
                self.assertEqual(len(cluster.calls), 2)
                self.assertEqual(result['counter_scope'], 'unknown')
                self.assertEqual(result['qualification'], 'not_qualified')
                self.assertEqual(result['sample_count'], 0)
                self.assertFalse(result['readings_complete'])

    def test_counter_increase_qualifies_same_pod_and_gone_retains_last_numbers(self):
        cluster = FakeCluster(['128 40 42 5', '128 90 128 8', 'gone'])
        observer = probe.ProcessObserver(cluster, pod(), interval_seconds=.1).start()
        self.assertTrue(cluster.sampled.wait(2))
        result = observer.close()
        self.assertEqual(result['qualification'], 'host_pid_limit_hit')
        self.assertEqual(result['counter_scope'], 'pod_subtree')
        self.assertEqual((result['pod_uid'], result['node_name'], result['qos_class']), (UID, 'selected-worker', 'Guaranteed'))
        self.assertTrue(result['pod_uid_verified'])
        self.assertEqual(result['pids_events_max_before'], 5)
        self.assertEqual(result['pids_events_max_after'], 8)
        self.assertEqual(result['pids_events_max_increase'], 3)
        self.assertEqual(result['pids_max'], 128)
        self.assertEqual(result['pids_current_peak'], 90)
        self.assertEqual(result['pids_peak'], 128)
        self.assertTrue(result['readings_complete'])
        self.assertTrue(result['cgroup_gone'])
        self.assertEqual(result['sample_count'], 2)
        self.assertFalse(observer._thread.is_alive())
        self.assertEqual(observer.close(), result)

    def test_existing_events_or_peak_without_counter_increase_do_not_qualify(self):
        cluster = FakeCluster(['128 40 128 25', '128 64 128 25', 'gone'])
        observer = probe.ProcessObserver(cluster, pod(), interval_seconds=.1).start()
        self.assertTrue(cluster.sampled.wait(2))
        result = observer.close()
        self.assertEqual(result['qualification'], 'not_qualified')
        self.assertEqual(result['pids_events_max_increase'], 0)
        self.assertTrue(result['readings_complete'])

    def test_counter_reset_and_read_failure_cannot_qualify(self):
        for samples, error in [(['128 40 80 5', '128 60 90 9', '128 4 5 1', 'gone'], 'reset'),
                               (['128 40 80 5', '128 60 90 9', RuntimeError('private host path')], 'read')]:
            with self.subTest(error=error):
                cluster = FakeCluster(samples)
                observer = probe.ProcessObserver(cluster, pod(), interval_seconds=.1).start()
                self.assertTrue(cluster.sampled.wait(2))
                result = observer.close()
                self.assertEqual(result['qualification'], 'not_qualified')
                self.assertFalse(result['readings_complete'])
                if error == 'reset':
                    self.assertEqual(result['pids_events_max_peak_increase'], 4)
                self.assertNotIn('private', json.dumps(result))
                self.assertFalse(observer._thread.is_alive())

    def test_missing_initial_cgroup_or_files_remain_explicitly_unqualified(self):
        for value in ('gone', 'unavailable', '128 10 unknown 0', '128 2 3 0 extra'):
            with self.subTest(value=value):
                observer = probe.ProcessObserver(FakeCluster([value]), pod())
                with self.assertRaises(probe.ProcessProbeError): observer.start()
                result = observer.close()
                self.assertEqual(result['qualification'], 'not_qualified')
                self.assertEqual(result['sample_count'], 0)
                self.assertFalse(result['readings_complete'])

    def test_observer_duration_and_interval_are_bounded_before_commands(self):
        for kwargs in ({'duration_seconds': 45}, {'duration_seconds': 0}, {'duration_seconds': float('inf')},
                       {'interval_seconds': .001}, {'interval_seconds': .5}):
            with self.subTest(kwargs=kwargs):
                cluster = FakeCluster()
                observer = probe.ProcessObserver(cluster, pod(), **kwargs)
                with self.assertRaises(probe.ProcessProbeError): observer.start()
                observer.close()
                self.assertEqual(cluster.calls, [])

    def test_thread_stops_at_its_deadline_and_close_survives_thread_start_failure(self):
        observer = probe.ProcessObserver(FakeCluster(), pod(), duration_seconds=.05, interval_seconds=.1).start()
        observer._thread.join(timeout=1)
        self.assertFalse(observer._thread.is_alive())
        self.assertEqual(observer.close()['sample_count'], 1)
        observer = probe.ProcessObserver(FakeCluster(), pod())
        with patch.object(probe.threading.Thread, 'start', side_effect=RuntimeError('private failure')):
            with self.assertRaisesRegex(probe.ProcessProbeError, 'start_failed'): observer.start()
        result = observer.close()
        self.assertEqual(result['qualification'], 'not_qualified')
        self.assertFalse(result['readings_complete'])
        self.assertNotIn('private failure', json.dumps(result))

    def test_counters_are_numeric_and_unlimited_is_explicit(self):
        self.assertEqual(probe.parse_counters('max 8 12 4')['pids_max'], None)
        for invalid in ('128 -1 2 0', '128 1 2 -5', '1 2 3 ' + str(2**64), '128 1.0 2 3', '128 5 4 0',
                        '128 1 2 infinity', '128 1 2 ３'):
            with self.subTest(invalid=invalid), self.assertRaises(probe.ProcessProbeError):
                probe.parse_counters(invalid)


if __name__ == '__main__':
    unittest.main()
