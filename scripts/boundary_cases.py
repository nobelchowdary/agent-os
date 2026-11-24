"""Controlled, bounded probes for the disposable kind campaign.

These checks measure individual paths. Missing positive controls never qualify a
denial, and these checks cannot establish host escape resistance or a guest PID ceiling.
All fixture and submitted code runs through the required gVisor RuntimeClass.
"""
import importlib.util
import ipaddress
import json
import os
from pathlib import Path
import secrets

NAMESPACE = 'agentos-workloads'
PREFIX = 'AGENTOS_BOUNDARY_RESULT '
MAX_CHILDREN = 64
FIXTURE_PORT = 18081
CONTROL_LABEL = 'agentos.dev/boundary-control'
OBSERVER_ERRORS = frozenset({
    'invalid_host_pid_counters', 'host_pid_observer_requires_dedicated_kind',
    'host_pid_observer_requires_selected_worker', 'host_pid_observer_invalid_pod_uid',
    'host_pid_observer_unsupported_qos', 'invalid_host_pid_observer_duration',
    'invalid_host_pid_observer_interval', 'host_pid_observer_command_failed',
    'host_pid_counters_unavailable', 'host_pid_observer_already_started_or_closed',
    'host_pid_observer_wrong_kind_container', 'host_pid_counter_scope_not_hierarchical',
    'host_pid_cgroup_absent_before_probe', 'host_pid_observer_start_failed',
    'host_pid_observer_sample_failed', 'host_pid_observer_join_failed',
    'host_pid_observer_did_not_stop', 'observer_creation_failed', 'observer_close_failed',
    'host_pid_kubelet_config_unavailable', 'host_pid_observer_requires_systemd_driver',
    'host_pid_observer_requires_qos_cgroups', 'host_pid_observer_invalid_cgroup_root',
})


class BoundaryFailure(RuntimeError):
    def __init__(self, category, evidence=None):
        super().__init__(category)
        self.evidence = evidence


def require(condition, category):
    if not condition:
        raise BoundaryFailure(category)


def external_target(value):
    """An explicitly supplied controlled off-cluster IPv4 listener, never a URL."""
    if not value:
        return None
    try:
        address, port = value.rsplit(':', 1)
        ip = ipaddress.IPv4Address(address)
        number = int(port)
    except (ValueError, TypeError):
        raise BoundaryFailure('invalid_boundary_external_endpoint') from None
    require(not (ip.is_unspecified or ip.is_loopback or ip.is_multicast or ip.is_link_local)
            and 1 <= number <= 65535, 'invalid_boundary_external_endpoint')
    return {'kind': 'tcp', 'address': str(ip), 'port': number}


def probe_code(targets, filesystem=False):
    # TCP controls never send HTTP requests or credentials, including to the
    # Kubernetes API and metadata endpoint. DNS sends only a fixed test query.
    code = '''import errno,json,os,socket,time
targets = TARGETS
result = {"network": {}}
for name,target in targets.items():
    try:
        if target["kind"] == "dns":
            query = b"\\x61\\x6f\\x01\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00" + b"\\x0akubernetes\\x07default\\x03svc\\x07cluster\\x05local\\x00\\x00\\x01\\x00\\x01"
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
                connection.settimeout(1)
                connection.sendto(query, (target["address"], target["port"]))
                response,peer = connection.recvfrom(4096)
                reachable = peer == (target["address"], target["port"]) and len(response) >= 12 and response[:2] == query[:2] and bool(response[2] & 128) and not (response[3] & 15) and int.from_bytes(response[6:8], "big") > 0
        else:
            with socket.create_connection((target["address"], target["port"]), timeout=1):
                reachable = True
        result["network"][name] = {"reachable": reachable}
    except OSError:
        result["network"][name] = {"reachable": False}
'''.replace('TARGETS', repr(targets))
    if filesystem:
        code += '''
checks = {}
checks["uid_nonzero"] = os.geteuid() != 0
checks["service_account_token_absent"] = not os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token")
try:
    with open("/agentos-boundary-root-write", "w") as output:
        output.write("probe")
    checks["root_write_denied"] = False
except OSError as failure:
    checks["root_write_denied"] = failure.errno in (errno.EROFS, errno.EACCES, errno.EPERM)
checks["root_mount_read_only"] = bool(os.statvfs("/").f_flag & os.ST_RDONLY)
with open("/proc/1/cmdline", "rb") as source:
    checks["pid_one_is_python"] = b"python" in source.read(4096).split(b"\\0")[0]
result["process_and_filesystem"] = checks
'''
    code += '\nprint(' + repr(PREFIX) + '+json.dumps(result), flush=True)\n'
    if filesystem:
        # Keep the actual workload Pod available for inspecting its admitted
        # spec. Pod running readiness is observed before finish() is called.
        code += 'time.sleep(5)\n'
    return code


def fork_code():
    return '''import errno,json,os,resource,signal,time
children = []
result = {"stage": "complete", "attempt_limit": __LIMIT__, "spawned": 0, "resource_denied": False, "children_reaped": 0}
print(PREFIX+json.dumps({"stage": "fork_progress", "spawned": 0}), flush=True)
time.sleep(8)
resource.setrlimit(resource.RLIMIT_AS, (128*1024*1024, 128*1024*1024))
def deadline(*unused):
    raise TimeoutError("bounded_process_deadline")
signal.signal(signal.SIGALRM, deadline)
signal.alarm(15)
try:
    for unused in range(__LIMIT__):
        try:
            child = os.fork()
        except OSError as failure:
            if failure.errno not in (errno.EAGAIN, errno.ENOMEM):
                raise
            result["resource_denied"] = True
            break
        if child == 0:
            signal.alarm(0)
            time.sleep(20)
            os._exit(0)
        children.append(child)
        if len(children) % 8 == 0:
            print(PREFIX+json.dumps({"stage": "fork_progress", "spawned": len(children)}), flush=True)
    result["spawned"] = len(children)
finally:
    signal.alarm(0)
    for child in children:
        try:
            os.kill(child, signal.SIGKILL)
        except ProcessLookupError:
            pass
    for child in children:
        os.waitpid(child, 0)
        result["children_reaped"] += 1
print(PREFIX+json.dumps(result), flush=True)
'''.replace('__LIMIT__', str(MAX_CHILDREN)).replace('PREFIX', repr(PREFIX))


def result_from_output(value):
    attachment = value.get('result', {})
    output = attachment.get('output_head', '') + '\n' + attachment.get('output_tail', '')
    return result_from_text(output)


def process_diagnostics(record):
    """Only enum metadata, bounded numeric progress and fixed error flags escape."""
    attachment = record.get('result', {})
    parts = [attachment.get(field, '') for field in ('output_head', 'output_tail')]
    output = '\n'.join(part[:32768] for part in parts if isinstance(part, str))
    lower = output.lower()
    markers = {'resource_temporarily_unavailable': ('resource temporarily unavailable',),
               'cannot_allocate_memory': ('cannot allocate memory',),
               'traceback': ('traceback (most recent call last)',),
               'timeout': ('timeouterror', 'bounded_process_deadline'),
               'memory_error': ('memoryerror',),
               'failed_to_create_os_thread': ('failed to create new os thread',),
               'runtime_fatal': ('fatal error:',)}
    state = record.get('state')
    reason = record.get('reason_code')
    known_reasons = {'completed', 'task_error', 'deadline_exceeded', 'cancelled', 'oom_killed', 'evicted',
                     'image_pull_failed', 'policy_denied', 'budget_exceeded', 'infra_failure', 'unknown'}
    exit_code = attachment.get('exit_code')
    summary = {'state': state if state in ('succeeded', 'failed', 'cancelled') else 'unrecognized',
               'reason_code': reason if isinstance(reason, str) and reason in known_reasons else 'unrecognized',
               'exit_code': exit_code if type(exit_code) is int and -2147483648 <= exit_code <= 2147483647 else None,
               'last_progress_spawned': None, 'last_progress_stage': None,
               'error_markers': {name: any(marker in lower for marker in values) for name, values in markers.items()}}
    for line in output.splitlines():
        if not line.startswith(PREFIX):
            continue
        try:
            value = json.loads(line[len(PREFIX):])
        except ValueError:
            continue
        if not isinstance(value, dict):
            continue
        stage, spawned = value.get('stage'), value.get('spawned')
        if stage in ('fork_progress', 'complete') and type(spawned) is int and 0 <= spawned <= MAX_CHILDREN:
            summary['last_progress_spawned'], summary['last_progress_stage'] = spawned, stage
    return summary


def result_from_text(output):
    for line in reversed(output.splitlines()):
        if line.startswith(PREFIX):
            try:
                result = json.loads(line[len(PREFIX):])
            except ValueError:
                break
            require(isinstance(result, dict), 'invalid_boundary_probe_output')
            return result
    raise BoundaryFailure('missing_boundary_probe_output')


def evaluate_network(targets, before, restricted, after):
    result = {}
    for name in ('controlled_peer', 'kubernetes_api', 'dns', 'metadata', 'external_control'):
        if name not in targets:
            result[name] = {'qualification': 'not_qualified', 'reason': 'operator_control_not_supplied'}
            continue
        observations = [value.get('network', {}).get(name, {}).get('reachable') for value in (before, restricted, after)]
        require(all(type(item) is bool for item in observations), 'incomplete_boundary_network_observation')
        positive_before, workload_reachable, positive_after = observations
        require(not workload_reachable, 'workload_network_boundary_open_' + name)
        result[name] = {'qualification': 'measured_denial' if positive_before and positive_after else 'not_qualified',
                        'control_reachable_before': positive_before, 'control_reachable_after': positive_after,
                        'workload_reachable': workload_reachable}
        if not (positive_before and positive_after):
            result[name]['reason'] = 'positive_control_unreachable'
    return result


def inspect_pod(pod):
    spec = pod['spec']
    containers = spec.get('containers', [])
    require(spec.get('runtimeClassName') == 'gvisor', 'boundary_runtime_mismatch')
    require(not any(spec.get(field, False) for field in ('hostNetwork', 'hostPID', 'hostIPC')), 'boundary_host_namespace_enabled')
    require(spec.get('automountServiceAccountToken') is False, 'boundary_service_account_token_enabled')
    require(not spec.get('initContainers') and not spec.get('ephemeralContainers'), 'unexpected_boundary_containers')
    require(not any('hostPath' in volume or any('serviceAccountToken' in source for source in volume.get('projected', {}).get('sources', []))
                    for volume in spec.get('volumes', [])), 'boundary_host_or_token_volume')
    require(all(set(volume) == {'name', 'emptyDir'} for volume in spec.get('volumes', [])), 'unexpected_boundary_volume_source')
    require(spec.get('securityContext', {}).get('runAsNonRoot') is True, 'boundary_root_identity_allowed')
    require(len(containers) == 1 and containers[0]['name'] == 'agent', 'unexpected_boundary_containers')
    security = containers[0].get('securityContext', {})
    require(security.get('readOnlyRootFilesystem') is True and security.get('allowPrivilegeEscalation') is False
            and not security.get('privileged', False) and 'ALL' in security.get('capabilities', {}).get('drop', [])
            and not security.get('capabilities', {}).get('add', []), 'boundary_container_privileges')
    return {'required_runtime': True, 'host_namespaces_disabled': True, 'host_and_token_volumes_absent': True,
            'nonroot_no_capabilities_readonly_root': True}


def fixture_pod(namespace, name, image, code, node):
    return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': namespace,
        'labels': {CONTROL_LABEL: 'true'}}, 'spec': {
        'runtimeClassName': 'gvisor', 'restartPolicy': 'Never', 'automountServiceAccountToken': False,
        'serviceAccountName': 'boundary-control',
        'activeDeadlineSeconds': 300, 'terminationGracePeriodSeconds': 1,
        'nodeSelector': {'kubernetes.io/hostname': node},
        'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532, 'seccompProfile': {'type': 'RuntimeDefault'}},
        'containers': [{'name': 'control', 'image': image, 'imagePullPolicy': 'IfNotPresent', 'command': ['python', '-u', '-c', code],
            'resources': {'requests': {'cpu': '25m', 'memory': '32Mi'}, 'limits': {'cpu': '100m', 'memory': '128Mi'}},
            'securityContext': {'readOnlyRootFilesystem': True, 'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}}}]}}


def create_process_observer(cluster, pod):
    spec = importlib.util.spec_from_file_location('agentos_process_probe', Path(__file__).with_name('process_probe.py'))
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper.ProcessObserver(cluster, pod)


def safe_host_pid_evidence(value, pod, observer_started=False, stage_errors=()):
    """Bind numeric observations to this Pod without copying host output/paths."""
    if not isinstance(value, dict):
        value = {}
    reported_errors = value.get('errors', [])
    if not isinstance(reported_errors, (list, tuple, set)):
        reported_errors = []
    errors = sorted({item for item in [*reported_errors, *stage_errors] if type(item) is str and item in OBSERVER_ERRORS})
    complete = observer_started is True and value.get('readings_complete') is True and not errors
    qos = pod.get('status', {}).get('qosClass')
    result = {'qualification': 'host_pid_limit_hit' if complete and value.get('qualification') == 'host_pid_limit_hit' else 'not_qualified',
              'observer_started': observer_started is True, 'errors': errors,
              'pod_qos_class': qos if qos in ('Guaranteed', 'Burstable', 'BestEffort') else 'unrecognized',
              'pod_uid_present': isinstance(pod.get('metadata', {}).get('uid'), str) and bool(pod['metadata']['uid']),
              'counter_scope': 'pod_subtree' if value.get('counter_scope') == 'pod_subtree' else 'unverified',
              'pod_uid_verified': value.get('pod_uid_verified') is True,
              'same_pod_uid': bool(pod.get('metadata', {}).get('uid')) and value.get('pod_uid') == pod['metadata']['uid'],
              'same_selected_worker': bool(pod.get('spec', {}).get('nodeName')) and value.get('node_name') == pod['spec']['nodeName'],
              'same_qos_class': bool(pod.get('status', {}).get('qosClass')) and value.get('qos_class') == pod['status']['qosClass'],
              'readings_complete': complete, 'cgroup_gone': value.get('cgroup_gone') is True}
    for field in ('pids_max', 'pids_current_peak', 'pids_peak', 'pids_events_max_before', 'pids_events_max_after',
                  'pids_events_max_increase', 'sample_count'):
        number = value.get(field)
        result[field] = number if type(number) is int and 0 <= number <= 2**63 - 1 else None
    return result


def host_pid_limit_observed(value):
    if value.get('qualification') != 'host_pid_limit_hit' or value.get('counter_scope') != 'pod_subtree' or not all(value.get(field) is True for field in (
            'observer_started', 'pod_uid_verified', 'same_pod_uid', 'same_selected_worker', 'same_qos_class', 'readings_complete')):
        return False
    numbers = ('pids_max', 'pids_current_peak', 'pids_peak', 'pids_events_max_before', 'pids_events_max_after',
               'pids_events_max_increase', 'sample_count')
    if not all(type(value.get(field)) is int for field in numbers):
        return False
    return (value['pids_max'] == 128 and 0 < value['pids_current_peak'] <= value['pids_peak'] <= 128
            and value['pids_events_max_after'] > value['pids_events_max_before'] >= 0
            and value['pids_events_max_increase'] == value['pids_events_max_after'] - value['pids_events_max_before']
            and value['sample_count'] >= 2)


def process_exercise(cluster):
    # External clusters retain their administrator's node settings. A readable
    # finite kubelet limit is required before deliberately creating children.
    try:
        for node in cluster.worker_nodes:
            effective = json.loads(cluster.kubectl('get', '--raw', '/api/v1/nodes/' + node + '/proxy/configz'))
            limit = effective.get('kubeletconfig', {}).get('podPidsLimit')
            if type(limit) is not int or not 1 <= limit <= 128:
                raise ValueError('missing finite PID prerequisite')
    except Exception:
        return {'qualification': 'not_qualified', 'reason': 'node_pid_prerequisite_unavailable',
                'required_max_node_pod_pids_limit': 128}
    fork = cluster.submit(fork_code(), 'boundary-process-' + secrets.token_hex(8), timeout=45)
    pod = cluster.running_pod(fork)
    inspect_pod(pod)
    require(pod['spec'].get('nodeName') in cluster.worker_nodes, 'boundary_process_outside_selected_workers')
    observer, observer_started, host, stage_errors = None, False, {}, []
    try:
        if getattr(cluster, 'dedicated_kind', False):
            try:
                observer = create_process_observer(cluster, pod)
            except Exception:
                observer = None
            if observer is not None:
                try:
                    observer.start()
                    observer_started = True
                except Exception as failure:
                    category = str(failure)
                    stage_errors.append(category if category in OBSERVER_ERRORS else 'host_pid_observer_start_failed')
            else:
                stage_errors.append('observer_creation_failed')
        done = cluster.finish(fork, state=None, timeout=120)
    finally:
        if observer is not None:
            try:
                observed_host = observer.close()
                if isinstance(observed_host, dict):
                    host = observed_host
                else:
                    stage_errors.append('observer_close_failed')
            except Exception:
                stage_errors.append('observer_close_failed')
    host = safe_host_pid_evidence(host, pod, observer_started=observer_started, stage_errors=stage_errors)
    diagnostics = {'qualification': 'not_qualified', 'node_pid_configuration_readback': True,
                   'fixture': process_diagnostics(done), 'host_pid_observation': host}
    exit_code = done.get('result', {}).get('exit_code')
    pressure_termination = (done.get('state') == 'failed' and done.get('reason_code') == 'task_error'
                            and type(exit_code) is int and exit_code == 128 and host_pid_limit_observed(host))
    if not pressure_termination and (done.get('state') != 'succeeded' or type(exit_code) is not int or exit_code != 0):
        raise BoundaryFailure('unexpected_bounded_process_outcome', evidence=diagnostics)
    if pressure_termination:
        result = dict(diagnostics, qualification='host_limit_observed', outcome='failed_execution_with_host_pid_limit_event')
    else:
        try:
            observed = result_from_output(done)
            require(observed.get('stage') == 'complete' and observed.get('attempt_limit') == MAX_CHILDREN
                    and type(observed.get('resource_denied')) is bool
                    and type(observed.get('spawned')) is int and 0 < observed['spawned'] <= MAX_CHILDREN
                    and type(observed.get('children_reaped')) is int and observed['children_reaped'] == observed['spawned'],
                    'bounded_process_cleanup_failed')
        except BoundaryFailure as failure:
            raise BoundaryFailure(str(failure), evidence=diagnostics) from None
        result = dict({field: observed[field] for field in ('stage', 'attempt_limit', 'spawned', 'children_reaped', 'resource_denied')},
                      qualification='exercise_only', node_pid_configuration_readback=True, host_pid_observation=host)
    try:
        # Resource pressure may terminate this sandbox. Both outcomes still
        # require its cleanup and useful work through the actual runtime.
        selected = sorted(cluster.worker_nodes | {cluster.api_node})
        cluster.kubectl('wait', '--for=condition=Ready', 'nodes', *selected, '--timeout=60s')
        cluster.drain()
        health_code = ('import json,subprocess; print(' + repr(PREFIX) + '+json.dumps({"gvisor": '
                       '"gvisor" in subprocess.check_output(["dmesg"]).decode().lower()}), flush=True)')
        healthy = cluster.submit(health_code, 'boundary-health-' + secrets.token_hex(8), timeout=30)
        healthy_done = cluster.finish(healthy, 'succeeded', timeout=120)
        require(healthy_done.get('result', {}).get('exit_code') == 0
                and result_from_output(healthy_done).get('gvisor') is True, 'boundary_followup_not_gvisor')
        cluster.drain()
        require(cluster.request('GET', '/readyz', retry=True).get('status') == 'ready', 'boundary_api_not_ready_after_process')
    except Exception:
        raise BoundaryFailure('boundary_process_recovery_failed', evidence=diagnostics) from None
    return dict(result, selected_nodes_ready=True, process_and_followup_cleaned_up=True,
                api_ready=True, followup_actual_gvisor_succeeded=True)


def boundary_checks(cluster):
    """Return measured partial results; raise if any expected boundary is open."""
    namespace = 'agentos-boundary-' + secrets.token_hex(8)
    external = external_target(os.environ.get('AGENTOS_BOUNDARY_EXTERNAL_ENDPOINT', ''))
    config = json.loads(cluster.obj('configmap', 'agentos-config', namespace='agentos-system')['data']['config.json'])
    image = config['templates']['python']['image']
    require('@sha256:' in image, 'boundary_fixture_image_not_pinned')
    inventory = {item['metadata']['name']: item for item in cluster.obj('nodes', namespace='default')['items']}
    require(cluster.worker_nodes and cluster.worker_nodes <= inventory.keys(), 'boundary_selected_workers_not_found')
    workers = [inventory[name]['metadata'].get('labels', {}).get('kubernetes.io/hostname')
               for name in sorted(cluster.worker_nodes)]
    require(all(workers) and len(set(workers)) == len(workers), 'boundary_worker_hostname_not_unique')
    api = cluster.obj('service', 'kubernetes', namespace='default')['spec']['clusterIP']
    dns = cluster.obj('service', 'kube-dns', namespace='kube-system')['spec']['clusterIP']
    # The disposable namespace supplies independent positive controls, and its
    # Pods still require the real sandbox runtime.
    # create refuses a name collision. Cleanup is authorized only after its
    # success; an ambiguous create can leave an empty namespace for review.
    cluster.kubectl('create', '-f', '-', data=json.dumps({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': namespace,
        'labels': {'pod-security.kubernetes.io/enforce': 'restricted'}}}))
    try:
        cluster.apply({'apiVersion': 'v1', 'kind': 'ServiceAccount', 'metadata': {
            'name': 'boundary-control', 'namespace': namespace}, 'automountServiceAccountToken': False})
        # Strict CNI modes deny new endpoints until an explicit policy applies.
        # Only fixture-labeled Pods in this separate namespace get this allowance.
        cluster.apply({'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
            'metadata': {'name': 'allow-control-fixtures', 'namespace': namespace},
            'spec': {'podSelector': {'matchLabels': {CONTROL_LABEL: 'true'}},
                     'policyTypes': ['Ingress', 'Egress'], 'ingress': [{}], 'egress': [{}]}})
        server = fixture_pod(namespace, 'peer', image,
            'import socketserver\nsocketserver.TCPServer.allow_reuse_address=True\n'
            'socketserver.TCPServer(("0.0.0.0", 18081), socketserver.BaseRequestHandler).serve_forever()\n', workers[-1])
        server['spec']['containers'][0]['readinessProbe'] = {'tcpSocket': {'port': FIXTURE_PORT},
            'periodSeconds': 1, 'timeoutSeconds': 1, 'failureThreshold': 30}
        cluster.apply(server)
        controls = ['control-' + str(index) for index in range(len(workers))]
        for name, worker in zip(controls, workers):
            cluster.apply(fixture_pod(namespace, name, image, 'import time; time.sleep(290)', worker))
        cluster.kubectl('-n', namespace, 'wait', '--for=condition=Ready', 'pod', '--all', '--timeout=120s')
        peer = cluster.obj('pod', 'peer', namespace=namespace)['status']['podIP']
        targets = {'controlled_peer': {'kind': 'tcp', 'address': peer, 'port': FIXTURE_PORT},
                   'kubernetes_api': {'kind': 'tcp', 'address': api, 'port': 443},
                   'dns': {'kind': 'dns', 'address': dns, 'port': 53},
                   'metadata': {'kind': 'tcp', 'address': '169.254.169.254', 'port': 80}}
        if external:
            targets['external_control'] = external
        def control():
            readings = [result_from_text(cluster.kubectl('-n', namespace, 'exec', name, '--', 'python', '-u', '-c', probe_code(targets)))
                        for name in controls]
            # Check every eligible worker so a broken route on the workload's
            # chosen node cannot masquerade as NetworkPolicy enforcement.
            return {'network': {target: {'reachable': all(
                item.get('network', {}).get(target, {}).get('reachable') is True for item in readings)} for target in targets}}
        before = control()
        run = cluster.submit(probe_code(targets, filesystem=True), 'boundary-network-' + secrets.token_hex(8), timeout=60)
        pod = cluster.running_pod(run)
        require(pod['spec'].get('nodeName') in cluster.worker_nodes, 'boundary_workload_outside_selected_workers')
        admitted = inspect_pod(pod)
        observed = result_from_output(cluster.finish(run, 'succeeded', timeout=120))
        after = control()
        network = evaluate_network(targets, before, observed, after)
        # The controlled in-cluster endpoints are mandatory; a failed positive
        # control fails the campaign instead of silently reducing its coverage.
        require(all(network[name]['qualification'] == 'measured_denial' for name in ('controlled_peer', 'kubernetes_api', 'dns')),
                'required_boundary_positive_control_unavailable')
        process = observed.get('process_and_filesystem', {})
        expected = {'uid_nonzero', 'service_account_token_absent', 'root_write_denied', 'root_mount_read_only', 'pid_one_is_python'}
        require(set(process) == expected and all(value is True for value in process.values()), 'boundary_process_or_filesystem_check_failed')
        evidence = {'qualification': 'partial', 'network': network, 'control_node_count': len(controls), 'admitted_pod_checks': admitted,
                'process_and_filesystem': process,
                'host_escape_resistance': 'not_qualified', 'guest_pid_ceiling': 'not_qualified',
                'limits': ['No controlled host marker or escape exploit is exercised.',
                           'A bounded fork loop and node PID configuration do not establish a guest PID ceiling.',
                           'Network results apply only to the named paths and observed control windows.']}
        try:
            evidence['bounded_process_exercise'] = process_exercise(cluster)
        except BoundaryFailure as failure:
            raise BoundaryFailure(str(failure), evidence=dict(evidence, bounded_process_exercise=failure.evidence)) from None
        return evidence
    finally:
        cluster.kubectl('delete', 'namespace', namespace, '--wait=true', '--timeout=60s', timeout=90)
