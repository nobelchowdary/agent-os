#!/usr/bin/env python3
"""Create only the disposable agentos-ci kind cluster with real pinned gVisor."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tarfile
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / '.local' / 'kind-ci'
NAME = 'agentos-ci'
KIND_VERSION = 'v0.33.0'
NODE_IMAGE = 'kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5'
GVISOR_VERSION = 'release-20261116.0'
POD_PIDS_LIMIT = 128
PINS = {
    'kind': ('https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-amd64', 'sha256', 'aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d'),
    'kubectl': ('https://dl.k8s.io/release/v1.37.0/bin/linux/amd64/kubectl', 'sha256', '6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f'),
    'gvisor.tar.bz2': ('https://github.com/gvisor/gvisor/releases/download/release-20261116.0/gvisor-x86_64.tar.bz2', 'sha512', 'c87d792d5842efb70c38ee822124cfe0821a547715e7abcf3f2b14997cd02784edaa0b91e238ec5c27a25a1b9e00bf98113398eb62a0778d8d20d4e222f8b0e0'),
}


def command(args, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, timeout=300, **kwargs)
    if result.returncode:
        # Do not echo arbitrary command output into public CI logs.
        raise RuntimeError('command_failed_' + Path(args[0]).name + '_' + args[1])
    return result.stdout


def fetch_verified(name, destination):
    url, algorithm, expected = PINS[name]
    if not destination.exists() or hashlib.new(algorithm, destination.read_bytes()).hexdigest() != expected:
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read(160 * 1024 * 1024)
        if hashlib.new(algorithm, data).hexdigest() != expected:
            raise RuntimeError('download_checksum_mismatch')
        destination.write_bytes(data)
    if name in ('kind', 'kubectl'):
        destination.chmod(0o700)


def cluster_config():
    # kind v0.33 ships containerd 2.x with a version 2 config. Keep its plugin
    # namespace: mixing in v3 runtimes prevents migration of kind's existing
    # runc systemd options and makes kubelet select the cgroupfs driver.
    # The runsc handler invokes the actual gVisor shim; no fallback is allowed.
    patch = '''[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc.options]
  TypeUrl = "io.containerd.runsc.v1.options"
  ConfigPath = "/etc/containerd/runsc.toml"
'''
    return {'kind': 'Cluster', 'apiVersion': 'kind.x-k8s.io/v1alpha4',
            'containerdConfigPatches': [patch],
            # kubeadm distributes the first node's KubeletConfiguration to all
            # nodes. Read back the running kubelets below before any abuse test.
            'kubeadmConfigPatches': ['apiVersion: kubelet.config.k8s.io/v1beta1\n'
                                    'kind: KubeletConfiguration\n'
                                    f'podPidsLimit: {POD_PIDS_LIMIT}\n'],
            'nodes': [{'role': 'control-plane', 'image': NODE_IMAGE},
                      {'role': 'worker', 'image': NODE_IMAGE},
                      {'role': 'worker', 'image': NODE_IMAGE}]}


def runsc_config():
    return '[runsc_config]\n  platform = "systrap"\n  systemd-cgroup = "true"\n'


def verify_pid_configuration(kubectl, nodes):
    """Read effective configuration; this alone does not qualify enforcement."""
    if not nodes:
        raise RuntimeError('no_disposable_nodes')
    for node in nodes:
        value = json.loads(command(kubectl + ['get', '--raw', '/api/v1/nodes/' + node + '/proxy/configz']))
        limit = value.get('kubeletconfig', {}).get('podPidsLimit')
        if type(limit) is not int or limit != POD_PIDS_LIMIT:
            raise RuntimeError('pod_pids_limit_not_configured')
    return len(nodes)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if not args.execute:
        print('NOT_EXECUTED: pass --execute to create the disposable agentos-ci cluster. No existing kubeconfig is changed.')
        return 0
    if platform.system() != 'Linux' or platform.machine() != 'x86_64':
        raise RuntimeError('requires_linux_amd64_runner')
    memory, cpus = command(['docker', 'info', '--format', '{{.MemTotal}} {{.NCPU}}']).split()
    if int(memory) < 6 * 1024**3 or int(cpus) < 4:
        raise RuntimeError('requires_docker_at_least_6GiB_and_4CPUs')
    WORK.mkdir(parents=True, exist_ok=True)
    WORK.chmod(0o700)
    bindir = WORK / 'bin'
    bindir.mkdir(exist_ok=True)
    for name in PINS:
        fetch_verified(name, bindir / name)
    kind = str(bindir / 'kind')
    if NAME in command([kind, 'get', 'clusters']).splitlines():
        raise RuntimeError('disposable_cluster_already_exists_refusing_replacement')
    config = WORK / 'cluster.json'
    config.write_text(json.dumps(cluster_config()))
    # The workflow may clean up even if kind fails midway. Never authorize
    # cleanup until we have proved this dedicated name was absent beforehand.
    (WORK / 'created-cluster').write_text(NAME + '\n')
    command([kind, 'create', 'cluster', '--name', NAME, '--config', str(config), '--kubeconfig', str(WORK / 'kubeconfig'), '--wait', '180s'])
    # Extract verified archive using the safe data filter (Python >=3.12).
    runtime = WORK / 'runtime'
    runtime.mkdir(exist_ok=True)
    with tarfile.open(bindir / 'gvisor.tar.bz2') as archive:
        archive.extractall(runtime, filter='data')
    shim_config = WORK / 'runsc.toml'
    shim_config.write_text(runsc_config())
    nodes = command([kind, 'get', 'nodes', '--name', NAME]).splitlines()
    for node in nodes:
        for leaf in ('runsc', 'containerd-shim-runsc-v1', 'gvisor-bin'):
            command(['docker', 'cp', str(runtime / leaf), node + ':/usr/local/bin/' + leaf])
        command(['docker', 'cp', str(shim_config), node + ':/etc/containerd/runsc.toml'])
        version = command(['docker', 'exec', node, '/usr/local/bin/runsc', '--version'])
        if 'runsc version' not in version or '20261116' not in version:
            raise RuntimeError('unexpected_runsc_version')
        command(['docker', 'exec', node, 'systemctl', 'restart', 'containerd'])
    kubectl = [str(bindir / 'kubectl'), '--kubeconfig', str(WORK / 'kubeconfig'), '--context', 'kind-' + NAME]
    command(kubectl + ['wait', '--for=condition=Ready', 'node', '--all', '--timeout=180s'])
    verified_nodes = verify_pid_configuration(kubectl, nodes)
    # kind v0.33 includes kube-network-policies in kindnet. Readiness is only a
    # prerequisite; the boundary campaign must prove each tested denial with a
    # reachable positive control before reporting measured enforcement.
    command(kubectl + ['-n', 'kube-system', 'rollout', 'status', 'daemonset/kindnet', '--timeout=180s'])
    command(kubectl + ['label', 'node', NAME + '-worker', NAME + '-worker2', 'agentos.dev/test-worker=true'])
    print(json.dumps({'cluster': NAME, 'node_image': NODE_IMAGE, 'gvisor': GVISOR_VERSION,
                      'runtime': 'actual_runsc_systrap', 'kubeconfig': '.local/kind-ci/kubeconfig',
                      'pod_pids_limit_configured': POD_PIDS_LIMIT, 'pid_config_readback_nodes': verified_nodes,
                      'network_policy_implementation': 'kindnet_kube_network_policies',
                      'network_policy_enforcement': 'not_qualified'}))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as failure:
        # Commands can involve credentials later; keep failures categorized.
        category = str(failure) if isinstance(failure, RuntimeError) else type(failure).__name__
        WORK.mkdir(parents=True, exist_ok=True)
        WORK.chmod(0o700)
        report = WORK / 'result.json'
        report.write_text(json.dumps({'scope': 'kind_gvisor_bootstrap', 'status': 'failed', 'failure': category,
                                      'gvisor': GVISOR_VERSION, 'node_image': NODE_IMAGE}, indent=2) + '\n')
        report.chmod(0o600)
        print('kind_bootstrap_failed: ' + category, file=sys.stderr)
        sys.exit(1)
