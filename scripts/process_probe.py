"""Read-only host PID accounting for one Pod on the dedicated kind cluster.

Guest output never supplies a host path or a counter. A positive change in the
same Pod cgroup's pids.events:max is evidence of a host PID-limit hit, not a
guest process ceiling or a diagnosis of the task's exit status.
"""
import math
import json
import re
import threading
import time


class ProcessProbeError(RuntimeError):
    pass


def require(condition, category):
    if not condition:
        raise ProcessProbeError(category)


# Only a fixed enum leaves the node. Both per-mount and superblock options are
# inspected, including when mountinfo contains optional fields before "-".
VERIFY_COUNTER_SCOPE = '''
awk '
    $5 == "/sys/fs/cgroup" {
        matches++
        separator = 0
        for (field = 7; field <= NF; field++) {
            if ($field == "-") { separator = field; break }
        }
        if (!separator || separator + 3 > NF || $(separator + 1) != "cgroup2") next
        option_count = split($6 "," $(separator + 3), options, ",")
        for (option_index = 1; option_index <= option_count; option_index++) {
            if (options[option_index] == "pids_localevents") local_events = 1
        }
        valid++
    }
    END {
        if (matches == 1 && valid == 1 && !local_events) print "hierarchical"
        else if (matches == 1 && valid == 1 && local_events) print "local_events"
        else print "unknown"
    }
' /proc/self/mountinfo 2>/dev/null
'''


# The sole path argument is constructed below from validated Kubernetes UID/QoS
# metadata. Nothing from workload output is executed or interpolated into sh.
READ_COUNTERS = '''
if [ ! -f /sys/fs/cgroup/cgroup.controllers ]; then printf 'unavailable\n'; exit 0; fi
if [ ! -d "$1" ]; then printf 'gone\n'; exit 0; fi
if {
    IFS= read -r probe_limit < "$1/pids.max" &&
    IFS= read -r probe_current < "$1/pids.current" &&
    IFS= read -r probe_peak < "$1/pids.peak" &&
    [ -r "$1/pids.events" ]; } 2>/dev/null; then
    probe_events_max=''
    while read -r probe_key probe_value probe_extra; do
        if [ "$probe_key" = max ] && [ -z "$probe_extra" ]; then
            probe_events_max=$probe_value
        fi
    done < "$1/pids.events" 2>/dev/null
    if [ -n "$probe_events_max" ]; then
        printf '%s %s %s %s\n' "$probe_limit" "$probe_current" "$probe_peak" "$probe_events_max"
        exit 0
    fi
fi
if [ ! -d "$1" ]; then printf 'gone\n'; else printf 'unavailable\n'; fi
'''


def parse_counters(output):
    fields = output.split()
    require(len(fields) == 4, 'invalid_host_pid_counters')
    require(fields[0] == 'max' or re.fullmatch(r'[0-9]+', fields[0]), 'invalid_host_pid_counters')
    require(all(re.fullmatch(r'[0-9]+', value) for value in fields[1:]), 'invalid_host_pid_counters')
    values = [None if value == 'max' else int(value) for value in fields]
    require(all(value is None or value <= 2**64 - 1 for value in values), 'invalid_host_pid_counters')
    require(values[2] >= values[1], 'invalid_host_pid_counters')
    return dict(zip(('pids_max', 'pids_current', 'pids_peak', 'pids_events_max'), values))


class ProcessObserver:
    """start() reads a baseline synchronously; close() always stops and joins.

    Call start() before the fixture's first fork. close() returns sanitized
    observations even if start() failed. External clusters are refused before
    any Docker command; no SSH, privileged Pod or host file write is used.
    """
    COMMAND_TIMEOUT_SECONDS = 1
    MAX_DURATION_SECONDS = 40

    def __init__(self, cluster, pod, duration_seconds=40, interval_seconds=.15):
        self.cluster = cluster
        self.pod_uid = pod.get('metadata', {}).get('uid')
        self.node_name = pod.get('spec', {}).get('nodeName')
        self.qos_class = pod.get('status', {}).get('qosClass')
        self.duration_seconds, self.interval_seconds = duration_seconds, interval_seconds
        self._stop, self._lock = threading.Event(), threading.Lock()
        self._thread = None
        self._started = self._closed = self._identity_verified = False
        self._counter_scope_verified = False
        self._baseline = self._last = None
        self._sample_count = self._max_current = self._max_peak = self._max_events = 0
        self._limits, self._errors = set(), set()
        self._unlimited = self._cgroup_gone = self._counter_reset = False

    def _validate(self):
        require(self.cluster.dedicated_kind is True, 'host_pid_observer_requires_dedicated_kind')
        require(isinstance(self.node_name, str) and self.node_name in self.cluster.worker_nodes
                and re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', self.node_name),
                'host_pid_observer_requires_selected_worker')
        require(isinstance(self.pod_uid, str) and re.fullmatch(
            r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', self.pod_uid),
            'host_pid_observer_invalid_pod_uid')
        require(self.qos_class in ('Guaranteed', 'Burstable'), 'host_pid_observer_unsupported_qos')
        require(type(self.duration_seconds) in (int, float) and math.isfinite(self.duration_seconds)
                and 0 < self.duration_seconds <= self.MAX_DURATION_SECONDS, 'invalid_host_pid_observer_duration')
        require(type(self.interval_seconds) in (int, float) and math.isfinite(self.interval_seconds)
                and .1 <= self.interval_seconds <= .25, 'invalid_host_pid_observer_interval')

    def _resolve_cgroup(self):
        try:
            response = self.cluster.kubectl('get', '--raw', '/api/v1/nodes/' + self.node_name + '/proxy/configz',
                                           '--request-timeout=1s', timeout=self.COMMAND_TIMEOUT_SECONDS + 1)
            config = json.loads(response)['kubeletconfig']
            require(isinstance(config, dict), 'host_pid_kubelet_config_unavailable')
        except Exception:
            raise ProcessProbeError('host_pid_kubelet_config_unavailable') from None
        require(config.get('cgroupDriver') == 'systemd', 'host_pid_observer_requires_systemd_driver')
        require(config.get('cgroupsPerQOS') is True, 'host_pid_observer_requires_qos_cgroups')
        root = config.get('cgroupRoot', '')
        require(isinstance(root, str) and (root == '' or root.startswith('/')),
                'host_pid_observer_invalid_cgroup_root')
        components = [] if root in ('', '/') else root[1:].split('/')
        require(all(re.fullmatch(r'[A-Za-z0-9_]+', component) for component in components),
                'host_pid_observer_invalid_cgroup_root')
        # Kubernetes CgroupName.ToSystemd expands each cumulative component
        # prefix into a slice. Restricted administrator root components need no
        # escaping; only the validated Pod UUID uses Kubernetes '-' -> '_' form.
        components.append('kubepods')
        if self.qos_class == 'Burstable':
            components.append('burstable')
        components.append('pod' + self.pod_uid.replace('-', '_'))
        self._cgroup = '/sys/fs/cgroup/' + '/'.join('-'.join(components[:index + 1]) + '.slice'
                                                  for index in range(len(components)))

    def _command(self, args):
        try:
            result = self.cluster.command(args, timeout=self.COMMAND_TIMEOUT_SECONDS)
        except Exception:
            raise ProcessProbeError('host_pid_observer_command_failed') from None
        require(isinstance(result, str), 'invalid_host_pid_counters')
        return result

    def _read(self):
        output = self._command(['docker', 'exec', self._container_id, 'sh', '-c', READ_COUNTERS,
                                'agentos-pids-observer', self._cgroup]).strip()
        if output == 'gone':
            self._cgroup_gone = True
            return False
        require(output != 'unavailable', 'host_pid_counters_unavailable')
        values = parse_counters(output)
        with self._lock:
            if self._last is not None and (values['pids_events_max'] < self._last['pids_events_max']
                                           or values['pids_peak'] < self._last['pids_peak']):
                self._counter_reset = True
            if self._baseline is None:
                self._baseline = dict(values)
            self._last = dict(values)
            self._sample_count += 1
            self._max_current = max(self._max_current, values['pids_current'])
            self._max_peak = max(self._max_peak, values['pids_peak'])
            self._max_events = max(self._max_events, values['pids_events_max'])
            if values['pids_max'] is None:
                self._unlimited = True
            else:
                self._limits.add(values['pids_max'])
        return True

    def start(self):
        require(not self._started and not self._closed, 'host_pid_observer_already_started_or_closed')
        self._started = True
        self._deadline = time.monotonic() + self.duration_seconds
        try:
            self._validate()
            self._resolve_cgroup()
            # Pin the immutable container ID after checking its kind ownership;
            # a later replacement with the same node name cannot retarget reads.
            result = self._command(['docker', 'inspect', '--format',
                '{{.Id}} {{ index .Config.Labels "io.x-k8s.kind.cluster" }}', self.node_name]).split()
            require(len(result) == 2 and re.fullmatch(r'[0-9a-f]{64}', result[0])
                    and result[1] == 'agentos-ci', 'host_pid_observer_wrong_kind_container')
            self._container_id = result[0]
            self._identity_verified = True
            scope = self._command(['docker', 'exec', self._container_id, 'sh', '-c',
                                   VERIFY_COUNTER_SCOPE, 'agentos-pids-scope']).strip()
            require(scope == 'hierarchical', 'host_pid_counter_scope_not_hierarchical')
            self._counter_scope_verified = True
            require(self._read(), 'host_pid_cgroup_absent_before_probe')
            self._thread = threading.Thread(target=self._sample, name='agentos-host-pids', daemon=True)
            self._thread.start()
            return self
        except ProcessProbeError as failure:
            self._errors.add(str(failure))
            raise
        except Exception:
            self._errors.add('host_pid_observer_start_failed')
            raise ProcessProbeError('host_pid_observer_start_failed') from None

    def _sample(self):
        while not self._stop.wait(self.interval_seconds):
            if time.monotonic() >= self._deadline:
                return
            try:
                if not self._read():
                    return
            except ProcessProbeError as failure:
                self._errors.add(str(failure))
                return
            except Exception:
                self._errors.add('host_pid_observer_sample_failed')
                return

    def close(self):
        self._stop.set()
        self._closed = True
        if self._thread is not None:
            try:
                self._thread.join(timeout=self.COMMAND_TIMEOUT_SECONDS + 1)
            except Exception:
                self._errors.add('host_pid_observer_join_failed')
            if self._thread.is_alive():
                self._errors.add('host_pid_observer_did_not_stop')
        with self._lock:
            baseline, last = self._baseline or {}, self._last or {}
            increase = max(0, last.get('pids_events_max', 0) - baseline.get('pids_events_max', 0))
            complete = (self._identity_verified and self._counter_scope_verified and self._sample_count >= 2 and not self._errors
                        and not self._counter_reset)
            hit = complete and increase > 0
            return {'qualification': 'host_pid_limit_hit' if hit else 'not_qualified',
                    'counter_scope': 'pod_subtree' if self._counter_scope_verified else 'unknown',
                    'pod_uid_verified': self._identity_verified and self._baseline is not None,
                    'pod_uid': self.pod_uid if self._identity_verified else None,
                    'node_name': self.node_name if self._identity_verified else None,
                    'qos_class': self.qos_class if self._identity_verified else None,
                    'pids_max': next(iter(self._limits)) if len(self._limits) == 1 and not self._unlimited else None,
                    'pids_limits_seen': sorted(self._limits), 'unlimited_limit_seen': self._unlimited,
                    'pids_current_before': baseline.get('pids_current'), 'pids_current_after': last.get('pids_current'),
                    'pids_current_peak': self._max_current if self._sample_count else None,
                    'pids_peak': self._max_peak if self._sample_count else None,
                    'pids_events_max_before': baseline.get('pids_events_max'),
                    'pids_events_max_after': last.get('pids_events_max'), 'pids_events_max_increase': increase,
                    'pids_events_max_peak_increase': max(0, self._max_events - baseline.get('pids_events_max', 0)),
                    'sample_count': self._sample_count, 'readings_complete': bool(complete),
                    'cgroup_gone': self._cgroup_gone, 'counter_reset': self._counter_reset,
                    'errors': sorted(self._errors)}
