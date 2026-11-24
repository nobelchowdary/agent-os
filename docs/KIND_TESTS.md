# Real kind and gVisor lifecycle checks

The `kind gVisor lifecycle` workflow creates an ephemeral Linux/amd64 cluster with real kubelets, containerd and **gVisor runsc**. It runs the API Deployment with a PVC, then uses the public API and independent Kubernetes Job/Pod watches. A RuntimeClass alias to runc is forbidden. Missing or broken gVisor fails the job; the first workload must return the gVisor `dmesg` marker.

This adds a local Kubernetes lifecycle acceptance track. It does not qualify EKS, Nitro, CNI network enforcement, host escape resistance, production performance or exactly-once external effects. The default kind network plugin is not accepted as NetworkPolicy enforcement. Do not run sensitive or network-connected agents based on this job.

## Run

Use a disposable Linux/amd64 Docker host with at least **6 GiB RAM and 4 CPUs** available to Docker; Python 3.12+ and internet access are required. The current developer Mac's Docker VM has only 2 GiB/2 CPUs, so the full campaign is not run there and this tooling does not resize it. Bootstrap installs pinned CLI tools inside ignored `.local/kind-ci/bin`; it leaves the user's default kubeconfig unchanged. Every Kubernetes command uses the private `kind-agentos-ci` context. Bootstrap refuses to replace an existing cluster named `agentos-ci`.

```sh
docker build --tag agent-os:kind-ci .
python3 scripts/kind_cluster.py --execute
docker pull python@sha256:9ef1a599f863231c3d91e941389bfdc4a2d6070557243d54df1a4e57015b960e
.local/kind-ci/bin/kind load docker-image --name agentos-ci agent-os:kind-ci python@sha256:9ef1a599f863231c3d91e941389bfdc4a2d6070557243d54df1a4e57015b960e
python3 scripts/kind_integration.py --execute --sequential-runs 200 --crash-submits 500
.local/kind-ci/bin/kind delete cluster --name agentos-ci --kubeconfig .local/kind-ci/kubeconfig
```

Without `--execute`, both scripts print a dry-run notice and contact nothing. No AWS resources are created. The campaign creates a random token, passes it to Kubernetes through stdin, and excludes it, raw logs, Secrets and kubeconfigs from the uploaded JSON summary. It never executes submitted task commands on the API or developer host.

For a manual cancellation-only rerun, select `cancel_ignores_sigterm_until_gone` in the workflow's `only_case` input, or run:

```sh
gh workflow run kind.yml --ref main -f only_case=cancel_ignores_sigterm_until_gone
```

Locally, add `--only-case cancel_ignores_sigterm_until_gone` to the integration command after creating the cluster. This still verifies real gVisor before running the selected case. The summary records the selected scope and zero sequential/idempotent campaign counts. It does not satisfy full acceptance. Push and pull-request events, and manual runs with the default `all`, retain the full 200/500 campaign. Existing concurrency is per ref, so a manual targeted run replaces an in-progress campaign on the same ref.

## Acceptance cases

- Actual gVisor runtime canary, exit 0, and exit 3; the failing task has one observed Job UID and one Pod UID. Exit code and output remain available after Job deletion and API restart.
- Unknown suspended Job appears in operator status and remains unadopted/unexecuted.
- Memory exhaustion, image-pull startup expiry, disk-limit eviction and a worker drain. The disk outcome remains evidence from this cluster; a single test does not justify portable `evicted` attribution. Unknown/infrastructure classification is permitted where structured evidence cannot distinguish the cause.
- Job deadline becomes authoritative while the API has zero Pods. The timeout still means the Job's activation-to-deadline clock; no process-CPU-time guarantee is introduced.
- SIGTERM-ignoring cancellation reaches `cancelled` only after the workload Pod disappears. Default workload termination grace remains in effect.
- Deletion during execution produces infrastructure failure without recreation.
- 200 sequential successful tasks finish and leave zero Jobs and Pods, detecting the default 64-Job quota leak.
- 500 unique idempotency keys, each submitted twice with identical input while the API Pod is repeatedly deleted. A full receipt/history check and continuous Job/Pod UID watches require one admission, one terminal history event and one observed execution per key in this campaign. Watch loss, errors or missing events fail rather than silently claiming no duplicates. The test does not make a universal exactly-once guarantee about Kubernetes or external writes.

Shorter counts are available for explicit smoke runs and are recorded in the summary; they do not pass the 200/500 acceptance target. Results are under ignored `.local/kind-ci/result.json` and uploaded as a short-lived CI artifact. On failure it includes Pod phase and container-state categories, excluding command output, environment values, annotations and Secret contents. Bootstrap writes a failure category too; the workflow only deletes a cluster after bootstrap recorded that its dedicated name was initially absent. A checked-in workflow is not evidence of a passed campaign; use the actual run conclusion and artifact.

The full baseline at commit `ebeb66f` passed in [workflow run 45289823857](https://github.com/nobelchowdary/agent-os/actions/runs/45289823857): 200 sequential runs and 500 idempotency keys, including 62 API Pod deletions. That remains the full-campaign evidence for the same unchanged runtime. A manual cancellation-only rerun supplements it with evidence for the tightened cancellation test; it does not establish a new full baseline or qualify runtime changes.

## Reproducible dependencies

The bootstrap pins kind **v0.33.0**, Kubernetes node **v1.37.0** by image digest, kubectl **v1.37.0** by SHA-256, and gVisor **release-20261116.0** by SHA-512. The verified gVisor bundle includes runsc, its containerd shim and sidecar binaries, kept together. The handler uses the actual shim with systrap and the systemd cgroup driver. Do not remove checksum checks or substitute an ordinary runtime to get a green result.

kind's node image uses a [version 2 containerd configuration](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/images/base/files/etc/containerd/config.toml), including systemd settings for its existing runtimes. The runsc patch uses the same `io.containerd.grpc.v1.cri` namespace. Mixing in a version 3 runtime table prevents containerd from migrating the existing runtime map, causing kubelet to select cgroupfs while runsc expects systemd paths.

Pins were checked against [kind's release](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0), [gVisor's release/checksums](https://github.com/gvisor/gvisor/releases/tag/release-20261116.0), [gVisor installation](https://gvisor.dev/docs/user_guide/install/), [containerd configuration](https://gvisor.dev/docs/user_guide/containerd/configuration/) and [systemd cgroup guidance](https://gvisor.dev/docs/user_guide/systemd/). Review and deliberately update all matching version/checksum values together.
