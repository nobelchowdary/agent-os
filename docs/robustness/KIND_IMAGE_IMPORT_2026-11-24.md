# Draft: intermittent missing image reference after kind preload

November 24, 2026. **Not filed upstream.** This is an observed compatibility failure with an incomplete reproduction, not an attributed containerd, kind or gVisor regression.

## Observed behavior

Two independent disposable-cluster runs failed their first Python canary before an agent container started: [`8f5699d`, targeted attempt 1](https://github.com/nobelchowdary/agent-os/actions/runs/45459175429/attempts/1) and [`bffa26f`, full attempt 1](https://github.com/nobelchowdary/agent-os/actions/runs/45459345437/attempts/1). Bootstrap and image-preload steps passed. Kubernetes events reported a successful registry pull, then a container-creation error with these identifiers:

```text
failed to check if this is a checkpoint image
failed to get image from containerd "sha256:67fd5f7b4b120eaa3308775fe87aff98a2492287576bd6656ef785d54a1b5605"
image "docker.io/library/import-2026-11-24@sha256:a8e2e52bf60d23f8d3ee9e26f39f6d7e9cae6f6df2ed073f9c6c1c8bc8179d4f": not found
```

Later events said the workload image was already present. Agent OS's configured 20-second startup bound ended each canary with `failed/infra_failure`, unavailable output and no observed exit code. Neither failed attempt collected load measurements. An unchanged-source retry at [`8f5699d`, attempt 2](https://github.com/nobelchowdary/agent-os/actions/runs/45459175429/attempts/2) passed the canary and all 64 load executions. The unchanged-source full retry at [`bffa26f`, attempt 2](https://github.com/nobelchowdary/agent-os/actions/runs/45459345437/attempts/2) also passed, including 200 sequential tasks, 500 idempotency keys and both load sizes. Earlier campaigns with the same pins also passed. These observations establish intermittency, not its frequency or cause.

The word “checkpoint” does not establish that the Python image is a checkpoint. In containerd v2.3.4, ordinary `CreateContainer` reaches this check for a non-file image reference; `checkIfCheckpointOCIImage` resolves the reference and calls `toContainerdImage` before inspecting checkpoint annotations. See [container creation](https://github.com/containerd/containerd/blob/v2.3.4/internal/cri/server/container_create.go#L122-L140) and [image inspection](https://github.com/containerd/containerd/blob/v2.3.4/internal/cri/server/container_checkpoint_linux.go#L180-L218).

## Environment and reproduction path

GitHub-hosted Ubuntu 24.04, Linux amd64; kind v0.33.0; Kubernetes v1.37.0; reported containerd 2.3.4; kernel `7.0.0-1014-aws`; gVisor `release-20261116.0` with systrap. The cluster has one control-plane/API node and two workload nodes. The canary uses RuntimeClass `gvisor`/handler `runsc`, `IfNotPresent`, 100 millicores, 256 MiB memory and 32 MiB scratch. [Bootstrap pins/configuration](https://github.com/nobelchowdary/agent-os/blob/8f5699d2c4614863cfd69defa0e6fb1f61d45cb6/scripts/kind_cluster.py) and [exact workflow](https://github.com/nobelchowdary/agent-os/blob/8f5699d2c4614863cfd69defa0e6fb1f61d45cb6/.github/workflows/kind.yml) are retained.

On a fresh disposable Linux amd64 checkout at `8f5699d2c4614863cfd69defa0e6fb1f61d45cb6`, the relevant workflow commands are:

```sh
docker build --tag agent-os:kind-ci .
python3 scripts/kind_cluster.py --execute
docker pull python@sha256:9ef1a599f863231c3d91e941389bfdc4a2d6070557243d54df1a4e57015b960e
.local/kind-ci/bin/kind load docker-image --name agentos-ci agent-os:kind-ci python@sha256:9ef1a599f863231c3d91e941389bfdc4a2d6070557243d54df1a4e57015b960e
python3 scripts/kind_integration.py --execute --sequential-runs 200 --crash-submits 500 --only-case load_64
```

The linked workflow removes only its newly created disposable cluster afterward. The failure is not deterministic; the successful retry is part of the record. A minimal direct-Pod/CRI reproduction has not been captured.

## Expected behavior and investigation needed

A successfully pulled, digest-pinned ordinary image should be usable for container creation without depending on an absent temporary import name. An import/reference synchronization or stale metadata issue is a hypothesis. The available events do not isolate image import, cache/event ordering, garbage collection or another component, and no workaround is verified.

Before filing, reproduce with failure-time `crictl inspecti` and `ctr -n k8s.io images ls`/image metadata, timestamped containerd/kubelet logs and image events, effective containerd configuration, and host Docker version/storage backend. Capture the original import name's creation/deletion sequence before cleanup. Compare native CRI pulling without `kind load`, then a minimal Pod, in fresh clusters while preserving the image digest and runtime pins. The sanitized campaign artifacts lack those image-store snapshots and node logs; the clusters have been deleted.
