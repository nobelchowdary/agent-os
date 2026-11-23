#!/usr/bin/env python3
"""Real kubelet/gVisor lifecycle campaign on the dedicated agentos-ci cluster."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
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
    pass


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


class Cluster:
    def __init__(self):
        self.k = [str(WORK / 'bin' / 'kubectl'), '--kubeconfig', str(WORK / 'kubeconfig'), '--context', CONTEXT]
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
        # Every command uses the private kubeconfig/context; never the user's default.
        require(self.kubectl('config', 'current-context').strip() == CONTEXT, 'wrong_context')
        for leaf in ('namespaces.yaml', 'rbac.yaml', 'workload-policy.yaml', 'workload-limits.yaml'):
            self.kubectl('apply', '-f', str(ROOT / 'deploy' / leaf))
        self.apply({'apiVersion': 'node.k8s.io/v1', 'kind': 'RuntimeClass', 'metadata': {'name': 'gvisor'}, 'handler': 'runsc',
                    'scheduling': {'nodeSelector': {'agentos.dev/test-worker': 'true'}}})
        config = json.loads((ROOT / 'config' / 'gvisor.example.json').read_text())
        config.update(max_active_per_tenant=4, max_active_total=4, startup_timeout_seconds=20,
                      result_retention_seconds=86400, kubernetes_qps=50, kubernetes_burst=100)
        python = config['templates']['python']
        python['resources'] = {'cpu_milli': 100, 'memory_mib': 256, 'disk_mib': 32}
        python['default_timeout_seconds'], python['max_timeout_seconds'] = 120, 300
        config['templates'] = {'python': python, 'bad-image': dict(python, image='registry.invalid/agentos-ci@sha256:' + '0' * 64)}
        self.apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'agentos-config', 'namespace': 'agentos-system'}, 'data': {'config.json': json.dumps(config)}})
        # API token travels only through stdin to kubectl, never command arguments/logs.
        self.apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'agentos-api-tokens', 'namespace': 'agentos-system'}, 'stringData': {'local-token': self.token}})
        objects = rendered_resources(self.kubectl('create', '--dry-run=client', '-f', str(ROOT / 'deploy' / 'api.yaml'), '-o', 'json'))
        for item in objects['items']:
            if item['kind'] == 'Deployment':
                spec = item['spec']['template']['spec']
                spec['terminationGracePeriodSeconds'] = 5
                spec['nodeSelector'] = {'kubernetes.io/hostname': 'agentos-ci-control-plane'}
                spec['tolerations'] = [{'key': 'node-role.kubernetes.io/control-plane', 'operator': 'Exists', 'effect': 'NoSchedule'}]
                spec['containers'][0]['image'] = 'agent-os:kind-ci'
        self.apply(objects)
        self.rollout()


class ObjectWatch:
    """Loss-intolerant watches count actual Job/Pod UIDs, including deleted objects."""
    def __init__(self, cluster, kind):
        self.cluster, self.kind = cluster, kind
        self.seen, self.errors, self.lock = {}, [], threading.Lock()
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
                if run_id and event['type'] == 'ADDED':
                    with self.lock:
                        self.seen.setdefault(run_id, set()).add(metadata['uid'])

    def unique(self, ids):
        require(self.process.poll() is None and not self.errors and self.thread.is_alive(), 'watch_continuity_lost')
        with self.lock:
            require(all(len(self.seen.get(run_id, ())) == 1 for run_id in ids), 'missing_or_duplicate_' + self.kind)

    def close(self):
        self.process.terminate()
        self.process.wait(timeout=10)
        self.thread.join(timeout=10)


def run_campaign(cluster, sequential, crash_submits):
    results = []
    watchers = [ObjectWatch(cluster, kind) for kind in ('jobs', 'pods')]
    try:
        def case(name, operation):
            start = time.monotonic()
            operation()
            cluster.drain()
            results.append({'case': name, 'status': 'passed', 'seconds': round(time.monotonic() - start, 3)})
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
            done = cluster.finish(run, 'failed', timeout=300)
            require(done.get('reason_code') in ('unknown', 'infra_failure', 'evicted'), 'disk_not_observed_as_eviction')
            # One local observation cannot establish portable task-limit attribution.
            results.append({'case': 'disk_reason_observed', 'reason_code': done['reason_code'], 'status': 'observed'})
        case('disk_limit', disk)

        def drain_node():
            run = cluster.submit("import time; print('drain-started',flush=True); time.sleep(180)", 'node-drain', timeout=240)
            pod = cluster.running_pod(run)
            node = pod['spec']['nodeName']
            require(node in ('agentos-ci-worker', 'agentos-ci-worker2'), 'refusing_drain_nonworker')
            try:
                cluster.kubectl('drain', node, '--ignore-daemonsets', '--delete-emptydir-data', '--timeout=150s')
                cluster.finish(run, 'failed', 'infra_failure')
            finally:
                cluster.kubectl('uncordon', node)
        case('node_drain', drain_node)

        def offline_deadline():
            run = cluster.submit("import time; print('deadline-started',flush=True); time.sleep(120)", 'offline-deadline', timeout=60)
            cluster.running_pod(run)
            cluster.kubectl('-n', 'agentos-system', 'scale', 'deployment/agentos-api', '--replicas=0')
            cluster.kubectl('-n', 'agentos-system', 'wait', '--for=delete', 'pod', '-l', 'app.kubernetes.io/component=api', '--timeout=60s')
            cluster.stop_forward()
            try:
                require(not any(c.get('reason') == 'DeadlineExceeded' and c.get('status') == 'True' for c in cluster.obj('job', 'agentos-' + run['id']).get('status', {}).get('conditions', [])), 'deadline_fired_before_api_stopped')
                job = eventually(lambda: cluster.obj('job', 'agentos-' + run['id']),
                    lambda item: any(c.get('reason') == 'DeadlineExceeded' and c.get('status') == 'True' for c in item.get('status', {}).get('conditions', [])),
                    timeout=90, category='deadline_depended_on_api')
                require(job['spec']['activeDeadlineSeconds'] == 60, 'job_deadline_changed')
            finally:
                cluster.kubectl('-n', 'agentos-system', 'scale', 'deployment/agentos-api', '--replicas=1')
                cluster.rollout()
            cluster.finish(run, 'failed', 'deadline_exceeded')
        case('deadline_with_api_scaled_zero', offline_deadline)

        def cancelled():
            run = cluster.submit("import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ignoring-term',flush=True); time.sleep(180)", 'cancel-ignore-term', timeout=240)
            cluster.running_pod(run)
            cluster.request('POST', '/v1/runs/' + run['id'] + '/cancel', retry=True)
            end = time.monotonic() + 120
            while time.monotonic() < end:
                current = cluster.get(run)
                pods = cluster.pods(run)
                if current['state'] == 'cancelled':
                    require(not pods, 'cancelled_before_pod_disappeared')
                    return
                time.sleep(.2)
            raise CheckFailed('cancellation_did_not_finish')
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
    args = parser.parse_args()
    if not args.execute:
        print('NOT_EXECUTED: --execute required; no API, Docker or Kubernetes call was made.')
        return 0
    require(1 <= args.sequential_runs <= 1000 and 1 <= args.crash_submits <= 1000, 'invalid_campaign_counts')
    cluster = Cluster()
    report = {'scope': 'real_kind_gvisor_lifecycle', 'network_isolation': 'not_qualified', 'eks': 'not_run',
              'sequential_runs': args.sequential_runs, 'crash_submits': args.crash_submits, 'status': 'failed'}
    try:
        cluster.install()
        report['cases'] = run_campaign(cluster, args.sequential_runs, args.crash_submits)
        report['status'] = 'passed'
    except Exception as failure:
        report['failure'] = str(failure) if isinstance(failure, CheckFailed) else type(failure).__name__
        report['diagnostics'] = cluster.diagnostics()
        raise
    finally:
        cluster.stop_forward()
        WORK.mkdir(exist_ok=True, parents=True)
        (WORK / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
        (WORK / 'result.json').chmod(0o600)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as failure:
        print('kind_campaign_failed: ' + (str(failure) if isinstance(failure, CheckFailed) else type(failure).__name__), file=sys.stderr)
        sys.exit(1)
