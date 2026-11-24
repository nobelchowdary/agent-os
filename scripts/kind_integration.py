#!/usr/bin/env python3
"""Opt-in gVisor lifecycle checks; defaults target the disposable agentos-ci cluster."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import importlib.util
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / '.local' / 'kind-ci'
CONTEXT = 'kind-agentos-ci'
NAMESPACE = 'agentos-workloads'
RUN_LABEL = 'agentos.dev/run-id'
PYTHON_IMAGE = 'python@sha256:9ef1a599f863231c3d91e941389bfdc4a2d6070557243d54df1a4e57015b960e'
TERMINAL = {'succeeded', 'failed', 'cancelled'}


class CheckFailed(Exception):
    def __init__(self, category, evidence=None):
        super().__init__(category)
        self.evidence = evidence


def require(condition, category):
    if not condition:
        raise CheckFailed(category)


def rendered_resources(text):
    """kubectl create prints one JSON document per input YAML resource."""
    decoder, items = json.JSONDecoder(), []
    pending = text.strip()
    while pending:
        value, used = decoder.raw_decode(pending)
        require(isinstance(value, dict), 'invalid_rendered_resource')
        if value.get('kind') == 'List':
            require(isinstance(value.get('items'), list), 'invalid_rendered_list')
            items.extend(value['items'])
        else:
            items.append(value)
        pending = pending[used:].lstrip()
    require(items and all(isinstance(item, dict) and item.get('kind') for item in items), 'empty_rendered_resources')
    return {'apiVersion': 'v1', 'kind': 'List', 'items': items}


def eventually(operation, predicate=bool, timeout=180, category='condition_timeout'):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = operation()
        if predicate(result):
            return result
        time.sleep(0.25)
    raise CheckFailed(category)


def api_egress_destinations(values, port):
    require(isinstance(port, int) and not isinstance(port, bool) and 1 <= port <= 65535,
            'invalid_api_egress_port')
    destinations = set()
    for value in values:
        try:
            network = ipaddress.ip_network(value, strict=True)
        except (ValueError, TypeError):
            raise CheckFailed('invalid_api_egress_host_cidr') from None
        require(isinstance(value, str) and '/' in value and network.prefixlen == network.max_prefixlen,
                'invalid_api_egress_host_cidr')
        destinations.add(str(network))
    return tuple(sorted(destinations))


class Cluster:
    def __init__(self, kubectl=None, kubeconfig=None, context=CONTEXT, worker_nodes=(), api_node=None,
                 api_image=None, workload_image=None, cluster_profile=None, allow_destructive_external=False,
                 load_sizes=(), api_egress_cidrs=(), api_egress_port=443):
        kubeconfig = Path(kubeconfig) if kubeconfig else WORK / 'kubeconfig'
        self.dedicated_kind = context == CONTEXT and kubeconfig.resolve() == (WORK / 'kubeconfig').resolve()
        require(self.dedicated_kind or allow_destructive_external, 'external_cluster_requires_destructive_opt_in')
        require(self.dedicated_kind or (worker_nodes and api_node and api_image and workload_image), 'external_cluster_requires_nodes_and_images')
        self.context = context
        self.profile = cluster_profile or ('kind' if self.dedicated_kind else 'external')
        require(self.dedicated_kind or self.profile != 'kind', 'external_cluster_cannot_claim_dedicated_kind')
        require(not self.dedicated_kind or self.profile == 'kind', 'dedicated_kind_requires_kind_profile')
        self.api_egress_cidrs = api_egress_destinations(api_egress_cidrs, api_egress_port)
        self.api_egress_port = api_egress_port
        require(self.dedicated_kind or self.api_egress_cidrs, 'external_cluster_requires_api_egress_destinations')
        self.worker_nodes, self.api_node = set(worker_nodes), api_node
        self.api_image = api_image or 'agent-os:kind-ci'
        self.workload_image = workload_image or PYTHON_IMAGE
        require(re.fullmatch(r'[^\s@]+@sha256:[0-9a-f]{64}', self.workload_image), 'workload_image_requires_sha256_digest')
        self.load_sizes = tuple(load_sizes)
        self.configuration = {}
        self.node_inventory = []
        self.k = [str(kubectl or WORK / 'bin' / 'kubectl'), '--kubeconfig', str(kubeconfig), '--context', context]
        self.token = secrets.token_urlsafe(48)
        self.forward = None
        self.forward_ready = None
        self.forward_lock = threading.Lock()
        self.url = 'http://127.0.0.1:18080'
        self.calls = []

    def command(self, args, data=None, timeout=180):
        result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
        if result.returncode:
            verbs = {'get', 'apply', 'create', 'delete', 'rollout', 'wait', 'scale', 'drain', 'uncordon', 'config'}
            verb = next((item for item in args[1:] if item in verbs), 'unknown')
            raise CheckFailed('cluster_command_failed_' + verb)
        return result.stdout

    def kubectl(self, *args, data=None, timeout=180):
        return self.command(self.k + list(args), data, timeout)

    def obj(self, kind, name=None, namespace=NAMESPACE):
        args = ['get', kind]
        if name:
            args.append(name)
        args += ['-n', namespace, '-o', 'json']
        return json.loads(self.kubectl(*args))

    def apply(self, value):
        self.kubectl('apply', '-f', '-', data=json.dumps(value))

    def start_forward(self):
        with self.forward_lock:
            if self.forward is None or self.forward.poll() is not None:
                if self.forward is not None:
                    self.forward.wait()
                self.forward = subprocess.Popen(self.k + ['-n', 'agentos-system', 'port-forward', 'service/agentos-api', '18080:8080'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.forward_ready = None
            process = self.forward
            if self.forward_ready is process:
                return process
            # Only one client establishes a forward. Other concurrent submitters
            # must not kill a replacement that has not bound its socket yet.
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and process.poll() is None:
                try:
                    with socket.create_connection(('127.0.0.1', 18080), timeout=0.2):
                        self.forward_ready = process
                        return process
                except OSError:
                    time.sleep(0.1)
            raise OSError('port_forward_not_ready')

    def stop_forward(self, expected=None):
        with self.forward_lock:
            if expected is not None and self.forward is not expected:
                return  # A late failure from an old request cannot kill a new forward.
            if self.forward is not None:
                self.forward.terminate()
                try:
                    self.forward.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.forward.kill()
                    self.forward.wait(timeout=10)
                self.forward = None
                self.forward_ready = None

    def request(self, method, path, body=None, key=None, retry=False, timeout=180):
        end = time.monotonic() + timeout
        payload = None if body is None else json.dumps(body).encode()
        while True:
            forward = None
            headers = {'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'}
            if key is not None:
                headers['Idempotency-Key'] = key
            request = urllib.request.Request(self.url + path, data=payload, method=method, headers=headers)
            try:
                forward = self.start_forward()
                with urllib.request.urlopen(request, timeout=5) as response:
                    return json.load(response)
            except urllib.error.HTTPError as failure:
                # No response body or credential is printed in test failures.
                if not retry or failure.code not in (409, 429, 500, 502, 503, 504):
                    raise CheckFailed('unexpected_api_status_' + str(failure.code)) from None
            except (OSError, ValueError):
                if not retry:
                    raise CheckFailed('api_transport_failed') from None
                if forward is not None:
                    self.stop_forward(expected=forward)
            if time.monotonic() >= end:
                raise CheckFailed('api_retry_deadline')
            time.sleep(0.3)

    def submit(self, code, key, template='python', timeout=120):
        body = {'template': template, 'command': ['python', '-u', '-c', code], 'timeout_seconds': timeout}
        return self.request('POST', '/v1/runs', body, key=key, retry=True, timeout=300)

    def get(self, run):
        return self.request('GET', '/v1/runs/' + run['id'], retry=True)

    def finish(self, run, state=None, reason=None, timeout=240):
        result = eventually(lambda: self.get(run), lambda item: item['state'] in TERMINAL,
                            timeout, 'run_never_terminal')
        attachment = result.get('result', {})
        output = attachment.get('output_head', '') + attachment.get('output_tail', '')
        self.calls.append({'state': result['state'], 'reason_code': result.get('reason_code'),
                           'result_status': attachment.get('status'), 'exit_code': attachment.get('exit_code'),
                           'output_complete': attachment.get('output_complete'),
                           'missing_dmesg_command': 'dmesg' in output and 'FileNotFoundError' in output,
                           'denied_dmesg': 'dmesg' in output and 'Operation not permitted' in output})
        self.calls = self.calls[-20:]
        require(state is None or result['state'] == state, 'wrong_terminal_state')
        require(reason is None or result.get('reason_code') == reason, 'wrong_terminal_reason')
        require(sum(event['state'] in TERMINAL for event in result['events']) == 1, 'nonunique_terminal_history')
        return result

    def pods(self, run):
        return [item for item in self.obj('pods')['items'] if item['metadata'].get('labels', {}).get(RUN_LABEL) == run['id']]

    def running_pod(self, run):
        return eventually(lambda: self.pods(run), lambda items: len(items) == 1 and any(
            value.get('state', {}).get('running') is not None for value in items[0].get('status', {}).get('containerStatuses', [])
            if value['name'] == 'agent'), category='agent_never_started')[0]

    def no_jobs(self):
        return not self.obj('jobs')['items'] and not self.obj('pods')['items']

    def drain(self):
        eventually(self.no_jobs, timeout=180, category='finished_jobs_or_pods_leaked')

    def rollout(self):
        self.kubectl('-n', 'agentos-system', 'rollout', 'status', 'deployment/agentos-api', '--timeout=180s')
        self.stop_forward()
        self.request('GET', '/v1/status', retry=True)

    def diagnostics(self):
        """Status summaries and redacted workload events; never logs or Secrets."""
        try:
            pods = json.loads(self.kubectl('get', 'pods', '--all-namespaces', '-o', 'json', timeout=15))
            result = []
            for pod in pods['items']:
                status = pod.get('status', {})
                result.append({'namespace': pod['metadata']['namespace'], 'name': pod['metadata']['name'],
                    'phase': status.get('phase'), 'reason': status.get('reason'),
                    'containers': [{'name': item['name'],
                        'waiting_reason': item.get('state', {}).get('waiting', {}).get('reason'),
                        'terminated_reason': item.get('state', {}).get('terminated', {}).get('reason'),
                        'exit_code': item.get('state', {}).get('terminated', {}).get('exitCode')}
                        for item in status.get('containerStatuses', [])]})
            diagnostics = {'pods': result, 'recent_run_outcomes': self.calls}
            try:
                events = self.obj('events')['items']
                diagnostics['workload_events'] = [{'reason': event.get('reason'),
                    'message': event.get('message', '').replace(self.token, '[redacted]')[:1024]}
                    for event in events[-30:]]
            except Exception:
                diagnostics['events_unavailable'] = True
            return diagnostics
        except Exception:
            return {'unavailable': True}

    def install(self):
        # Selection is explicit on every invocation, independent of current-context.
        nodes = self.obj('nodes')['items']
        by_name = {node['metadata']['name']: node for node in nodes}
        if not self.worker_nodes and self.dedicated_kind:
            self.worker_nodes = {name for name, node in by_name.items()
                                 if node['metadata'].get('labels', {}).get('agentos.dev/test-worker') == 'true'}
        require(self.worker_nodes and self.worker_nodes <= by_name.keys(), 'selected_workers_not_found')
        if self.api_node is None and self.dedicated_kind:
            controls = [name for name, node in by_name.items()
                        if 'node-role.kubernetes.io/control-plane' in node['metadata'].get('labels', {})]
            require(len(controls) == 1, 'dedicated_api_node_not_unique')
            self.api_node = controls[0]
        require(self.api_node in by_name and self.api_node not in self.worker_nodes, 'api_node_must_be_separate')
        self.node_inventory = [{'name': name, 'role': 'api' if name == self.api_node else 'workload',
            'allocatable': by_name[name].get('status', {}).get('allocatable', {}),
            'reported_node_info': by_name[name].get('status', {}).get('nodeInfo', {})}
            for name in sorted(self.worker_nodes | {self.api_node})]
        if not self.dedicated_kind:
            # Do not install/replace an external node runtime or silently widen its placement.
            runtime = self.obj('runtimeclass', 'gvisor')
            require(runtime.get('handler') == 'runsc', 'external_runtime_not_runsc')
            selector = runtime.get('scheduling', {}).get('nodeSelector', {})
            eligible = {name for name, node in by_name.items() if all(
                node['metadata'].get('labels', {}).get(key) == value for key, value in selector.items())}
            require(selector and eligible == self.worker_nodes, 'external_runtime_nodes_must_match_selection')
            existing = {item['metadata']['name'] for item in self.obj('namespaces')['items']}
            require(not existing.intersection({'agentos-system', NAMESPACE}), 'external_campaign_requires_fresh_namespaces')
            # create, rather than apply, also fails closed if another actor
            # creates either namespace between inspection and this operation.
            self.kubectl('create', '-f', str(ROOT / 'deploy' / 'namespaces.yaml'))
        else:
            self.kubectl('apply', '-f', str(ROOT / 'deploy' / 'namespaces.yaml'))
        for leaf in ('rbac.yaml', 'workload-policy.yaml', 'workload-limits.yaml'):
            self.kubectl('apply', '-f', str(ROOT / 'deploy' / leaf))
        if not self.dedicated_kind:
            self.kubectl('apply', '-f', str(ROOT / 'deploy' / 'api-policy.yaml'))
            self.apply({'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
                'metadata': {'name': 'agentos-api-kubernetes-egress', 'namespace': 'agentos-system'},
                'spec': {'podSelector': {'matchLabels': {'app.kubernetes.io/name': 'agent-os',
                                                        'app.kubernetes.io/component': 'api'}},
                         'policyTypes': ['Egress'], 'egress': [{
                             'to': [{'ipBlock': {'cidr': cidr}} for cidr in self.api_egress_cidrs],
                             'ports': [{'protocol': 'TCP', 'port': self.api_egress_port}]}]}})
        if self.dedicated_kind:
            self.apply({'apiVersion': 'node.k8s.io/v1', 'kind': 'RuntimeClass', 'metadata': {'name': 'gvisor'}, 'handler': 'runsc',
                        'scheduling': {'nodeSelector': {'agentos.dev/test-worker': 'true'}}})
        config = json.loads((ROOT / 'config' / 'gvisor.example.json').read_text())
        config.update(max_active_per_tenant=4, max_active_total=4, startup_timeout_seconds=20,
                      result_retention_seconds=86400, kubernetes_qps=50, kubernetes_burst=100)
        python = config['templates']['python']
        python['deadline_scope'] = 'pod'
        python['termination_grace_period_seconds'] = 10
        python['image'] = self.workload_image
        python['resources'] = {'cpu_milli': 100, 'memory_mib': 256, 'disk_mib': 32}
        python['default_timeout_seconds'], python['max_timeout_seconds'] = 120, 300
        config['templates'] = {'python': python, 'bad-image': dict(python, image='registry.invalid/agentos-ci@sha256:' + '0' * 64)}
        if self.load_sizes:
            config['templates']['load'] = dict(python, resources={'cpu_milli': 25, 'memory_mib': 64, 'disk_mib': 16})
        self.administrator_config = config
        self.configuration = self.configuration_snapshot(config)
        self.apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'agentos-config', 'namespace': 'agentos-system'}, 'data': {'config.json': json.dumps(config)}})
        # API token travels only through stdin to kubectl, never command arguments/logs.
        self.apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'agentos-api-tokens', 'namespace': 'agentos-system'}, 'stringData': {'local-token': self.token}})
        objects = rendered_resources(self.kubectl('create', '--dry-run=client', '-f', str(ROOT / 'deploy' / 'api.yaml'), '-o', 'json'))
        for item in objects['items']:
            if item['kind'] == 'Deployment':
                spec = item['spec']['template']['spec']
                spec['terminationGracePeriodSeconds'] = 5
                spec['nodeSelector'] = {'kubernetes.io/hostname': by_name[self.api_node]['metadata']['labels']['kubernetes.io/hostname']}
                spec['tolerations'] = [{'key': 'node-role.kubernetes.io/control-plane', 'operator': 'Exists', 'effect': 'NoSchedule'}]
                spec['containers'][0]['image'] = self.api_image
        self.apply(objects)
        self.rollout()

    def configuration_snapshot(self, config):
        snapshot = {'max_active_per_tenant': config['max_active_per_tenant'],
                              'max_active_total': config['max_active_total'],
                              'kubernetes_qps': config['kubernetes_qps'], 'kubernetes_burst': config['kubernetes_burst'],
                              'startup_timeout_seconds': config['startup_timeout_seconds'],
                              'templates': {name: {'resources': template['resources'],
                                  'image': template['image'],
                                  'termination_grace_period_seconds': template['termination_grace_period_seconds'],
                                  'deadline_scope': template.get('deadline_scope', 'pod'),
                                  'max_timeout_seconds': template['max_timeout_seconds']}
                                  for name, template in config['templates'].items()},
                              'resource_quota': self.obj('resourcequota', 'agentos-workloads')['spec']['hard']}
        snapshot['api_kubernetes_egress'] = None if self.dedicated_kind else {
            'host_cidrs': list(self.api_egress_cidrs), 'tcp_port': self.api_egress_port}
        return json.loads(json.dumps(snapshot))

    def configure_load(self, capacity):
        require(capacity in self.load_sizes, 'unselected_load_capacity')
        # Keep the full 200/500 lifecycle baseline at its original 4/20 policy.
        # Only the subsequent selected load case changes admission/startup limits.
        self.drain()
        config = json.loads(json.dumps(self.administrator_config))
        config.update(max_active_per_tenant=capacity, max_active_total=capacity, startup_timeout_seconds=240)
        quota = self.obj('resourcequota', 'agentos-workloads')
        quota['spec']['hard']['pods'] = str(capacity)
        quota['spec']['hard']['count/jobs.batch'] = str(max(64, capacity))
        self.apply(quota)
        self.apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'agentos-config', 'namespace': 'agentos-system'}, 'data': {'config.json': json.dumps(config)}})
        self.kubectl('-n', 'agentos-system', 'rollout', 'restart', 'deployment/agentos-api')
        self.rollout()
        self.administrator_config = config
        return self.configuration_snapshot(config)


class ObjectWatch:
    """Loss-intolerant watches count actual Job/Pod UIDs, including deleted objects."""
    def __init__(self, cluster, kind):
        self.cluster, self.kind = cluster, kind
        self.seen, self.errors, self.lock = {}, [], threading.Lock()
        self.pod_times, self.running, self.running_history = {}, set(), []
        listing = cluster.obj(kind)
        version = listing['metadata']['resourceVersion']
        # kubectl get has no --resource-version flag. Its raw transport can
        # request the API watch directly from the LIST snapshot without a gap.
        prefix = '/apis/batch/v1' if kind == 'jobs' else '/api/v1'
        path = prefix + '/namespaces/' + NAMESPACE + '/' + kind + '?' + urlencode({
            'watch': 'true', 'resourceVersion': version, 'allowWatchBookmarks': 'true', 'timeoutSeconds': 3600})
        self.process = subprocess.Popen(cluster.k + ['get', '--raw', path, '--request-timeout=0'],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()

    def read(self):
        try:
            self.read_events()
        except Exception:
            self.errors.append('invalid_watch_stream')

    def read_events(self):
        decoder, pending = json.JSONDecoder(), ''
        while True:
            block = self.process.stdout.read1(8192)
            if not block:
                return
            pending += block.decode('utf-8')
            require(len(pending) < 4 * 1024 * 1024, 'watch_buffer_exceeded')
            while pending.strip():
                pending = pending.lstrip()
                try:
                    event, used = decoder.raw_decode(pending)
                except ValueError:
                    break
                pending = pending[used:]
                if event['type'] == 'ERROR':
                    self.errors.append('watch_error')
                    return
                value = event['object']
                metadata = value.get('metadata', {})
                run_id = metadata.get('labels', {}).get(RUN_LABEL)
                if run_id:
                    with self.lock:
                        if event['type'] == 'ADDED':
                            self.seen.setdefault(run_id, set()).add(metadata['uid'])
                        if self.kind == 'pods':
                            for container in value.get('status', {}).get('containerStatuses', []):
                                if container['name'] != 'agent':
                                    continue
                                state = container.get('state', {})
                                active = state.get('running', {})
                                ended = state.get('terminated', {})
                                times = self.pod_times.setdefault(run_id, {})
                                if active.get('startedAt') or ended.get('startedAt'):
                                    times['started_at'] = active.get('startedAt') or ended['startedAt']
                                if ended.get('finishedAt'):
                                    times['finished_at'] = ended['finishedAt']
                                if active and event['type'] != 'DELETED':
                                    self.running.add(run_id)
                                else:
                                    self.running.discard(run_id)
                                self.running_history.append(frozenset(self.running))

    def load_observations(self, ids):
        with self.lock:
            return ({run_id: dict(self.pod_times.get(run_id, {})) for run_id in ids},
                    max((len(snapshot.intersection(ids)) for snapshot in self.running_history), default=0))

    def unique(self, ids):
        require(self.process.poll() is None and not self.errors and self.thread.is_alive(), 'watch_continuity_lost')
        with self.lock:
            require(all(len(self.seen.get(run_id, ())) == 1 for run_id in ids), 'missing_or_duplicate_' + self.kind)

    def close(self):
        self.process.terminate()
        self.process.wait(timeout=10)
        self.thread.join(timeout=10)


def cancel_ignoring_term(cluster):
    run = cluster.submit("import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ignoring-term',flush=True); time.sleep(180)", 'cancel-ignore-term', timeout=240)
    pod = cluster.running_pod(run)
    require(pod.get('spec', {}).get('terminationGracePeriodSeconds') == 10, 'configured_ten_second_grace_not_applied')
    # Running only proves the container started, not that Python installed its
    # handler. Observe the line emitted after signal.signal before stopping it.
    eventually(lambda: cluster.request('GET', '/v1/runs/' + run['id'] + '/logs', retry=True),
               lambda value: 'ignoring-term' in value.get('logs', ''), timeout=30,
               category='sigterm_handler_not_ready')
    started = time.monotonic()
    cluster.request('POST', '/v1/runs/' + run['id'] + '/cancel', retry=True)
    deletion_grace = set()
    end = time.monotonic() + 120
    while time.monotonic() < end:
        current = cluster.get(run)
        pods = cluster.pods(run)
        for value in pods:
            grace = value.get('metadata', {}).get('deletionGracePeriodSeconds')
            if grace is not None:
                require(0 <= grace <= 10, 'delete_grace_exceeds_template_grace')
                deletion_grace.add(grace)
        if current['state'] == 'cancelled':
            require(not pods, 'cancelled_before_pod_disappeared')
            return {'handler_ready': True,
                    'configured_grace_seconds': pod.get('spec', {}).get('terminationGracePeriodSeconds'),
                    'observed_delete_grace_seconds': sorted(deletion_grace),
                    'stop_seconds': round(time.monotonic() - started, 3)}
        time.sleep(.2)
    raise CheckFailed('cancellation_did_not_finish')


def deadline_with_api_stopped(cluster):
    """Observe the persisted native deadline while no API process can enforce it."""
    run = cluster.submit("import time; print('deadline-started',flush=True); time.sleep(120)",
                         'offline-deadline', timeout=60)
    scope = run.get('spec', {}).get('deadline_scope', 'job')
    require(scope in ('job', 'pod'), 'unsupported_deadline_scope')
    pod = cluster.running_pod(run)
    job_name = 'agentos-' + run['id']
    job = cluster.obj('job', job_name)
    job_uid, pod_uid = job.get('metadata', {}).get('uid'), pod.get('metadata', {}).get('uid')
    require(job_uid and pod_uid and any(owner.get('uid') == job_uid and owner.get('controller') is True
            for owner in pod.get('metadata', {}).get('ownerReferences', [])), 'deadline_pod_ownership_changed')
    job_deadline = 60 if scope == 'job' else None
    pod_deadline = 60 if scope == 'pod' else None
    require(job.get('spec', {}).get('activeDeadlineSeconds') == job_deadline
            and job.get('spec', {}).get('template', {}).get('spec', {}).get('activeDeadlineSeconds') == pod_deadline
            and pod.get('spec', {}).get('activeDeadlineSeconds') == pod_deadline, 'deadline_scope_changed')

    def current_job():
        item = cluster.obj('job', job_name)
        require(item.get('metadata', {}).get('uid') == job_uid, 'deadline_job_identity_changed')
        return item

    def current_pod():
        item = cluster.obj('pod', pod['metadata']['name'])
        require(item.get('metadata', {}).get('uid') == pod_uid and any(
            owner.get('uid') == job_uid and owner.get('controller') is True
            for owner in item.get('metadata', {}).get('ownerReferences', [])), 'deadline_pod_ownership_changed')
        return item

    def job_failed(item):
        return any(condition.get('type') == 'Failed' and condition.get('status') == 'True'
                   for condition in item.get('status', {}).get('conditions', []))

    def job_deadline_reached(item):
        return any(condition.get('reason') == 'DeadlineExceeded' and condition.get('status') == 'True'
                   for condition in item.get('status', {}).get('conditions', []))

    def pod_deadline_reached(item):
        status = item.get('status', {})
        return status.get('phase') == 'Failed' and status.get('reason') == 'DeadlineExceeded'

    try:
        # Restore even if scaling or waiting has an ambiguous failure outcome.
        cluster.kubectl('-n', 'agentos-system', 'scale', 'deployment/agentos-api', '--replicas=0')
        cluster.kubectl('-n', 'agentos-system', 'wait', '--for=delete', 'pod', '-l',
                        'app.kubernetes.io/component=api', '--timeout=60s')
        cluster.stop_forward()
        reached = pod_deadline_reached(current_pod()) if scope == 'pod' else job_deadline_reached(current_job())
        require(not reached, 'deadline_fired_before_api_stopped')
        if scope == 'pod':
            eventually(current_pod, pod_deadline_reached, timeout=90, category='deadline_depended_on_api')
            # A Pod failure is not the terminal Job observation required by the API.
            eventually(current_job, job_failed, timeout=60, category='deadline_job_never_terminal')
        else:
            eventually(current_job, lambda item: job_failed(item) and job_deadline_reached(item),
                       timeout=90, category='deadline_depended_on_api')
    finally:
        cluster.kubectl('-n', 'agentos-system', 'scale', 'deployment/agentos-api', '--replicas=1')
        cluster.rollout()
    cluster.finish(run, 'failed', 'deadline_exceeded')
    return {'deadline_scope': scope, 'deadline_boundary': 'pod_start_time' if scope == 'pod' else 'job_activation',
            'api_replicas_during_deadline': 0, 'terminal_job_observed': True}


def finished_kubelet_unavailable(cluster):
    """The Job's completed status must survive an unreachable worker log API."""
    if not cluster.dedicated_kind:
        return {'status': 'skipped', 'reason': 'kubelet_disruption_requires_dedicated_kind_docker_node'}
    run = cluster.submit("import time; print('completed-offline',flush=True); time.sleep(45)",
                         'finished-kubelet-unavailable', timeout=120)
    pod = cluster.running_pod(run)
    node = pod['spec']['nodeName']
    require(node in cluster.worker_nodes and node != cluster.api_node, 'refusing_stop_unselected_kubelet')
    label = cluster.command(['docker', 'inspect', '--format', '{{ index .Config.Labels "io.x-k8s.kind.cluster" }}', node]).strip()
    require(label == 'agentos-ci', 'refusing_stop_non_dedicated_kind_kubelet')
    stopped = False
    try:
        cluster.kubectl('-n', 'agentos-system', 'scale', 'deployment/agentos-api', '--replicas=0')
        cluster.kubectl('-n', 'agentos-system', 'wait', '--for=delete', 'pod', '-l', 'app.kubernetes.io/component=api', '--timeout=60s')
        cluster.stop_forward()
        require(not any(c.get('type') == 'Complete' and c.get('status') == 'True' for c in
                        cluster.obj('job', 'agentos-' + run['id']).get('status', {}).get('conditions', [])),
                'task_completed_before_api_stopped')
        eventually(lambda: cluster.obj('job', 'agentos-' + run['id']), lambda job: any(
            c.get('type') == 'Complete' and c.get('status') == 'True' for c in job.get('status', {}).get('conditions', [])),
            timeout=120, category='offline_task_never_completed')
        # Mark the restoration obligation before issuing a potentially ambiguous command.
        stopped = True
        cluster.command(['docker', 'exec', node, 'systemctl', 'stop', 'kubelet'])
        cluster.kubectl('-n', 'agentos-system', 'scale', 'deployment/agentos-api', '--replicas=1')
        cluster.rollout()
        done = cluster.finish(run, 'succeeded', 'completed', timeout=120)
        require(done.get('result', {}).get('status') == 'unavailable', 'unreachable_logs_not_marked_unavailable')
        require(done['result'].get('output_complete') is False, 'unavailable_output_claimed_complete')
        require(done['result'].get('exit_code') == 0, 'unavailable_logs_lost_exit_status')
        require(cluster.request('GET', '/readyz', retry=True).get('status') == 'ready', 'api_not_ready_after_unavailable_logs')
        return {'status': 'passed', 'terminal_state': done['state'], 'result_status': done['result']['status'],
                'exit_code': done['result']['exit_code'], 'api_ready': True}
    finally:
        try:
            if stopped:
                cluster.command(['docker', 'exec', node, 'systemctl', 'start', 'kubelet'])
                cluster.kubectl('wait', '--for=condition=Ready', 'node', node, '--timeout=180s')
        finally:
            cluster.kubectl('-n', 'agentos-system', 'scale', 'deployment/agentos-api', '--replicas=1')
            cluster.rollout()


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def distribution(values):
    require(bool(values) and all(math.isfinite(value) for value in values), 'missing_timing_samples')
    ordered = sorted(values)
    return {'count': len(ordered), 'min': round(ordered[0], 6),
            **{name: round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)], 6)
               for name, fraction in [('p50', .5), ('p95', .95), ('p99', .99)]},
            'max': round(ordered[-1], 6)}


def load_measurements(watch, records, target):
    ids = {record['id'] for record in records}
    require(len(ids) == target, 'load_receipt_count_mismatch')
    observations, observed_peak = watch.load_observations(ids)
    intervals, lags = [], []
    result_capture = {name: 0 for name in ('captured', 'unavailable', 'expired', 'missing', 'unknown',
                                         'output_complete', 'output_incomplete', 'output_completeness_missing')}
    for record in records:
        attachment = record.get('result')
        if not isinstance(attachment, dict) or 'status' not in attachment:
            result_capture['missing'] += 1
        else:
            status = attachment['status']
            result_capture[status if status in ('captured', 'unavailable', 'expired') else 'unknown'] += 1
        complete = attachment.get('output_complete') if isinstance(attachment, dict) else None
        result_capture['output_complete' if complete is True else
                       'output_incomplete' if complete is False else 'output_completeness_missing'] += 1
        times = observations[record['id']]
        require('started_at' in times and 'finished_at' in times, 'missing_load_pod_timestamps')
        started, finished = timestamp(times['started_at']), timestamp(times['finished_at'])
        require(finished > started, 'invalid_load_pod_interval')
        terminal = [event for event in record['events'] if event['state'] in TERMINAL]
        require(len(terminal) == 1, 'nonunique_terminal_history')
        lags.append(timestamp(terminal[0]['at']) - finished)
        intervals.extend([(started, 1), (finished, -1)])
    active = interval_peak = 0
    for _, delta in sorted(intervals):
        active += delta
        interval_peak = max(interval_peak, active)
    measurements = {'target_concurrency': target, 'peak_observed_running_containers': observed_peak,
            'peak_container_timestamp_overlap': interval_peak,
            'pod_exit_to_persisted_terminal_seconds': distribution(lags),
            'result_capture': result_capture,
            'clock_basis': 'node agent-container finishedAt to API terminal event at; clock skew unqualified',
            'negative_lag_samples': sum(value < 0 for value in lags)}
    if observed_peak < target or interval_peak < target:
        raise CheckFailed('target_running_concurrency_not_observed', evidence=measurements)
    return measurements


def load_case(cluster, watch, count):
    # Hold tasks to a common wall-clock gate so admission count cannot stand in
    # for measured running concurrency. Insufficient resources fail this case.
    configuration = cluster.configure_load(count)
    gate = time.time() + 180
    code = "import time; time.sleep(max(0, " + repr(gate) + " - time.time())); print('load-complete')"
    with ThreadPoolExecutor(max_workers=count) as pool:
        runs = list(pool.map(lambda index: cluster.submit(code, 'load-' + str(count) + '-' + str(index),
                                                         template='load', timeout=300), range(count)))
    records = []
    try:
        for run in runs:
            done = cluster.finish(run, 'succeeded', 'completed', timeout=360)
            require(done.get('result', {}).get('exit_code') == 0, 'load_missing_success_exit_status')
            records.append(done)
        eventually(lambda: watch.load_observations({run['id'] for run in runs})[0],
                   lambda observations: all('finished_at' in item for item in observations.values()),
                   timeout=15, category='load_watch_missing_exit')
        watch.unique({run['id'] for run in runs})
        return {**load_measurements(watch, records, count), 'configuration': configuration}
    except CheckFailed as failure:
        if failure.evidence is None:
            observations, peak = watch.load_observations({run['id'] for run in runs})
            failure.evidence = {'target_concurrency': count, 'peak_observed_running_containers': peak,
                                'receipts': len(runs), 'validated_terminal_records': len(records),
                                'pods_with_exit_timestamp': sum('finished_at' in item for item in observations.values())}
        failure.evidence['configuration'] = configuration
        raise


def run_boundary_checks(cluster):
    spec = importlib.util.spec_from_file_location('agentos_boundary_cases', ROOT / 'scripts' / 'boundary_cases.py')
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    try:
        return helper.boundary_checks(cluster)
    except helper.BoundaryFailure as failure:
        raise CheckFailed(str(failure), evidence=failure.evidence) from None


def run_campaign(cluster, sequential, crash_submits, only_case='all', load_sizes=(), results=None):
    if results is None:
        results = []
    watchers = [ObjectWatch(cluster, kind) for kind in ('jobs', 'pods')]
    try:
        def case(name, operation):
            if only_case != 'all' and name not in ('actual_gvisor_canary', only_case):
                return
            start = time.monotonic()
            try:
                observation = operation()
                cluster.drain()
            except Exception as failure:
                safe_category = isinstance(failure, CheckFailed) or type(failure).__name__ == 'BoundaryFailure'
                failed = {'case': name, 'status': 'failed', 'seconds': round(time.monotonic() - start, 3),
                          'failure': str(failure) if safe_category else type(failure).__name__}
                evidence = getattr(failure, 'evidence', None) if safe_category else None
                if isinstance(evidence, dict):
                    failed['observations'] = evidence
                results.append(failed)
                raise
            result = {'case': name, 'status': 'passed', 'seconds': round(time.monotonic() - start, 3)}
            if isinstance(observation, dict):
                result['observations'] = observation
                result['status'] = observation.get('status', 'passed')
            results.append(result)
            print(json.dumps(results[-1]), flush=True)

        def standard(code, key, state, reason, exit_code=None):
            run = cluster.submit(code, key)
            done = cluster.finish(run, state, reason)
            if exit_code is not None:
                require(done.get('result', {}).get('exit_code') == exit_code, 'missing_exit_status')
            return run, done

        def canary():
            _, done = standard("import subprocess; print(subprocess.check_output(['dmesg']).decode())", 'actual-gvisor', 'succeeded', 'completed', 0)
            text = done['result'].get('output_head', '') + done['result'].get('output_tail', '')
            require('gvisor' in text.lower(), 'runtime_canary_not_gvisor')
        case('actual_gvisor_canary', canary)
        case('boundary_checks', lambda: run_boundary_checks(cluster))

        def orphan():
            plan = cluster.request('POST', '/v1/plan', {'template': 'python'}, retry=True)
            job = plan['job']
            name = job['metadata']['name']
            require(job['spec']['suspend'] is True, 'orphan_probe_not_suspended')
            cluster.apply(job)
            try:
                eventually(lambda: cluster.request('GET', '/v1/status', retry=True),
                    lambda value: value.get('controller', {}).get('orphan_jobs') == 1,
                    timeout=30, category='unknown_job_not_reported')
                retained = cluster.obj('job', name)
                require(retained['spec']['suspend'] is True, 'unknown_job_adopted')
                require(not cluster.obj('pods')['items'], 'unknown_job_executed')
            finally:
                cluster.kubectl('-n', NAMESPACE, 'delete', 'job', name, '--wait=true')
        case('unknown_suspended_job_reported_not_adopted', orphan)
        case('exit_zero', lambda: standard("print('exit-zero-marker')", 'exit-zero', 'succeeded', 'completed', 0))

        def exit_three():
            run, done = standard("print('exit-three-marker'); raise SystemExit(3)", 'exit-three', 'failed', 'task_error', 3)
            cluster.drain()
            cluster.kubectl('-n', 'agentos-system', 'rollout', 'restart', 'deployment/agentos-api')
            cluster.rollout()
            reread = cluster.get(run)
            require(reread['result'] == done['result'], 'result_changed_after_restart')
            logs = cluster.request('GET', '/v1/runs/' + run['id'] + '/logs', retry=True)
            require('exit-three-marker' in logs['logs'], 'output_missing_after_cleanup_restart')
            for watch in watchers:
                watch.unique([run['id']])
        case('exit_three_retained_once_after_restart', exit_three)

        def head_tail():
            _, done = standard("print('head-marker'); print('x'*100000); print('tail-traceback-marker'); raise SystemExit(3)", 'bounded-output', 'failed', 'task_error', 3)
            result = done['result']
            require('head-marker' in result.get('output_head', ''), 'output_head_lost')
            require('tail-traceback-marker' in result.get('output_tail', ''), 'output_tail_lost')
            require(result.get('output_truncated') is True and result.get('output_complete') is True, 'incorrect_output_completeness')
        case('bounded_head_and_tail', head_tail)
        case('memory_limit', lambda: standard("import time; x=[]\nwhile True: x.append(bytearray(4*1024*1024)); time.sleep(.01)", 'memory-limit', 'failed', 'oom_killed'))
        case('image_pull_start_bound', lambda: cluster.finish(cluster.submit('pass', 'bad-image', 'bad-image', 180), 'failed', 'image_pull_failed'))

        def disk():
            run = cluster.submit("import time; f=open('/workspace/fill','wb'); f.write(b'x'*(48*1024*1024)); f.flush(); print('disk-limit-written',flush=True); time.sleep(180)", 'disk-limit', timeout=240)
            done = cluster.finish(run, 'failed', 'evicted', timeout=300)
            # One local observation cannot establish portable task-limit attribution.
            results.append({'case': 'disk_reason_observed', 'reason_code': done['reason_code'], 'status': 'observed'})
        case('disk_limit', disk)

        def drain_node():
            run = cluster.submit("import time; print('drain-started',flush=True); time.sleep(180)", 'node-drain', timeout=240)
            pod = cluster.running_pod(run)
            node = pod['spec']['nodeName']
            require(node in cluster.worker_nodes and node != cluster.api_node, 'refusing_drain_unselected_worker')
            try:
                cluster.kubectl('drain', node, '--ignore-daemonsets', '--delete-emptydir-data', '--timeout=150s')
                cluster.finish(run, 'failed', 'infra_failure')
            finally:
                cluster.kubectl('uncordon', node)
        case('node_drain', drain_node)

        case('deadline_with_api_scaled_zero', lambda: deadline_with_api_stopped(cluster))
        case('finished_kubelet_unavailable', lambda: finished_kubelet_unavailable(cluster))

        def cancelled():
            results.append({'case': 'cancellation_observation', 'status': 'observed', **cancel_ignoring_term(cluster)})
        case('cancel_ignores_sigterm_until_gone', cancelled)

        def delete_job():
            run = cluster.submit("import time; print('delete-started',flush=True); time.sleep(120)", 'deleted-job')
            cluster.running_pod(run)
            cluster.kubectl('-n', NAMESPACE, 'delete', 'job', 'agentos-' + run['id'], '--wait=false')
            cluster.finish(run, 'failed', 'infra_failure')
            cluster.drain()
            for watch in watchers:
                watch.unique([run['id']])
        case('deleted_job_no_replay', delete_job)

        def sequential_cleanup():
            for index in range(sequential):
                standard("print('sequential-ok')", 'sequence-' + str(index), 'succeeded', 'completed', 0)
            cluster.drain()
        case('sequential_' + str(sequential) + '_cleanup', sequential_cleanup)

        def crash_campaign():
            def all_run_ids():
                ids, before = set(), ''
                while True:
                    page = cluster.request('GET', '/v1/runs?limit=100' + ('&before=' + before if before else ''), retry=True)
                    ids.update(item['id'] for item in page['runs'])
                    before = page['next_cursor']
                    if not before:
                        return ids
            prior = all_run_ids()
            stopped, crashes = threading.Event(), []
            def disrupt():
                while not stopped.wait(10):
                    try:
                        cluster.kubectl('-n', 'agentos-system', 'delete', 'pod', '-l', 'app.kubernetes.io/component=api', '--wait=false', '--grace-period=0', '--force', timeout=20)
                        crashes.append(1)
                    except CheckFailed:
                        pass
            disruptor = threading.Thread(target=disrupt, daemon=True)
            disruptor.start()
            def submission(index):
                key = 'crash-idem-' + str(index)
                code = "print('one-attempt'); raise SystemExit(3)"
                first = cluster.submit(code, key)
                again = cluster.submit(code, key)
                require(first['id'] == again['id'], 'idempotency_admitted_twice')
                return first
            try:
                with ThreadPoolExecutor(max_workers=6) as pool:
                    runs = list(pool.map(submission, range(crash_submits)))
            finally:
                stopped.set()
                disruptor.join(timeout=30)
            require(len(crashes) >= 2, 'insufficient_api_disruptions')
            cluster.rollout()
            require(len({run['id'] for run in runs}) == crash_submits, 'separate_keys_shared_run')
            require(all_run_ids() - prior == {run['id'] for run in runs}, 'lost_receipt_or_extra_admission')
            for run in runs:
                done = cluster.finish(run, 'failed', 'task_error')
                require(done.get('result', {}).get('exit_code') == 3, 'crash_lost_exit_status')
            cluster.drain()
            for watch in watchers:
                eventually(lambda: all(len(watch.seen.get(run['id'], ())) == 1 for run in runs), timeout=10, category='watch_missing_execution')
                watch.unique([run['id'] for run in runs])
            results.append({'case': 'crash_count', 'api_pod_deletions': len(crashes), 'status': 'observed'})
        case('crash_during_' + str(crash_submits) + '_idempotent_submits', crash_campaign)
        for size in load_sizes:
            case('load_' + str(size), lambda size=size: load_case(cluster, watchers[1], size))
        for watch in watchers:
            require(not watch.errors and watch.thread.is_alive(), 'watch_continuity_lost')
        return results
    finally:
        for watch in watchers:
            watch.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--sequential-runs', type=int, default=200)
    parser.add_argument('--crash-submits', type=int, default=500)
    parser.add_argument('--only-case', choices=('all', 'cancel_ignores_sigterm_until_gone',
                        'finished_kubelet_unavailable', 'boundary_checks', 'load_16', 'load_64'), default='all')
    parser.add_argument('--load-cases', action='store_true', help='Add measured 16/64 running-task cases to the full campaign.')
    parser.add_argument('--context', default=CONTEXT, help='Explicit Kubernetes context; defaults to dedicated kind.')
    parser.add_argument('--kubeconfig', type=Path, help='Private kubeconfig file; defaults to .local/kind-ci/kubeconfig.')
    parser.add_argument('--kubectl', type=Path, help='kubectl executable; defaults to the pinned kind-ci binary.')
    parser.add_argument('--worker-node', action='append', default=[], help='Selected eligible worker; repeat for each node.')
    parser.add_argument('--api-node', help='Separate node for the API Deployment.')
    parser.add_argument('--api-image', help='Operator-prepared API image available to the selected cluster.')
    parser.add_argument('--workload-image', help='Digest-pinned Python workload image, including a private registry mirror.')
    parser.add_argument('--api-egress-cidr', action='append', default=[],
                        help='External cluster Kubernetes API destination /32 or /128; repeat and account for CNI service/DNAT handling.')
    parser.add_argument('--api-egress-port', type=int, default=443,
                        help='External cluster Kubernetes API destination TCP port (default: 443).')
    parser.add_argument('--cluster-profile', choices=('kind', 'eks', 'external'))
    parser.add_argument('--allow-destructive-external-cluster', action='store_true',
                        help='Allow creating fresh Agent OS namespaces, API disruptions and draining selected external workers.')
    args = parser.parse_args()
    if not args.execute:
        print('NOT_EXECUTED: --execute required; no API, Docker or Kubernetes call was made.')
        return 0
    require(1 <= args.sequential_runs <= 1000 and 1 <= args.crash_submits <= 1000, 'invalid_campaign_counts')
    require(not args.load_cases or args.only_case == 'all', 'load_cases_requires_full_selection')
    load_sizes = (int(args.only_case[5:]),) if args.only_case.startswith('load_') else ((16, 64) if args.load_cases else ())
    dedicated = args.context == CONTEXT and (args.kubeconfig is None or args.kubeconfig.resolve() == (WORK / 'kubeconfig').resolve())
    require(dedicated or (args.kubeconfig is not None and args.context != CONTEXT), 'external_cluster_requires_explicit_context_and_kubeconfig')
    cluster = Cluster(kubectl=args.kubectl, kubeconfig=args.kubeconfig, context=args.context,
                      worker_nodes=args.worker_node, api_node=args.api_node, api_image=args.api_image,
                      workload_image=args.workload_image,
                      cluster_profile=args.cluster_profile, allow_destructive_external=args.allow_destructive_external_cluster,
                      load_sizes=load_sizes, api_egress_cidrs=args.api_egress_cidr, api_egress_port=args.api_egress_port)
    profile = args.cluster_profile or ('kind' if dedicated else 'external')
    report_path = WORK / 'result.json' if dedicated else ROOT / '.local' / 'cluster-ci' / profile / 'result.json'
    report = {'scope': 'real_' + profile + '_gvisor_lifecycle', 'network_isolation': 'not_qualified',
              'cluster_profile': profile, 'cluster_qualification': 'not_qualified',
              'cluster_profile_source': 'dedicated_kind_configuration' if dedicated else 'operator_declared',
              'eks': 'selected_cluster_only' if profile == 'eks' else 'not_run',
              'selection': args.only_case,
              'load_concurrency_targets': list(load_sizes), 'cases': [],
              'sequential_runs': args.sequential_runs if args.only_case == 'all' else 0,
              'crash_submits': args.crash_submits if args.only_case == 'all' else 0, 'status': 'failed'}
    try:
        cluster.install()
        report.update(configuration=cluster.configuration,
                      selected_worker_nodes=sorted(cluster.worker_nodes), api_node=cluster.api_node,
                      nodes=cluster.node_inventory)
        report['cases'] = run_campaign(cluster, args.sequential_runs, args.crash_submits, args.only_case,
                                       load_sizes=load_sizes, results=report['cases'])
        report['status'] = 'passed_with_skips' if any(item['status'] == 'skipped' for item in report['cases']) else 'passed'
    except Exception as failure:
        report['failure'] = str(failure) if isinstance(failure, CheckFailed) else type(failure).__name__
        report['diagnostics'] = cluster.diagnostics()
        raise
    finally:
        cluster.stop_forward()
        report_path.parent.mkdir(exist_ok=True, parents=True)
        report_path.parent.chmod(0o700)
        descriptor = os.open(report_path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w') as handle:
            handle.write(json.dumps(report, indent=2) + '\n')
        report_path.chmod(0o600)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as failure:
        print('kind_campaign_failed: ' + (str(failure) if isinstance(failure, CheckFailed) else type(failure).__name__), file=sys.stderr)
        sys.exit(1)
