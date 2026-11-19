# Compatibility evidence before benchmarking

These helpers do not install a runtime or provision a cluster. Only run them against an explicitly selected pilot installation. Keep reports in ignored `evidence/` or another private directory. JSON reports use mode `0600`, refuse existing files/symlinks, and separate observations from unverified properties. Context names and node names can still identify internal infrastructure: review reports before sharing.

## Read-only declarations inventory

With a trusted kubeconfig and `kubectl` available, the following reads the selected API; it never runs workload code. `kubectl` can invoke the credential plugin configured in that trusted context.

```sh
mkdir -p evidence
python3 scripts/qualify_cluster.py \
  --context YOUR_EXPLICIT_CONTEXT \
  --profile eks-standard-gvisor \
  --runtime-class gvisor --runtime-handler runsc --architecture amd64 \
  --node-selector agent-sandbox/runtime=gvisor \
  --namespace agentos-workloads \
  --cni cilium \
  --output evidence/cluster-declarations.json
```

The selector must be enforced by the RuntimeClass itself. Replace it with the actual qualified pool label for a separately prepared installation; a CLI-only selector cannot constrain Agent OS Jobs. Every currently eligible node is inspected, including nodes that would be hidden by an extra CLI filter. A pool scaled to zero is inconclusive. Auto Mode, Fargate, another architecture and an ordinary-container handler fail this initial gVisor profile. Nitro is a separate backend and conformance profile.

Use `--cni aws-vpc` only when native VPC CNI is the intended enforcement implementation; a chained Cilium installation should select `cilium`. Native VPC CNI requires explicitly observed strict startup mode, network-policy node-agent and controller declarations, kernel >=5.10, identifiable CNI >=1.21, and a Ready CNI Pod on each eligible node. This conservative check can block managed configurations that do not expose the expected ConfigMap: review them separately rather than infer enforcement. Cilium requires a recognized policy mode and target-node coverage. No unrelated environment values, Secrets, kubeconfig contents, pod logs, addresses or raw API objects are written.

Exit `0` means only `DECLARATIONS_ONLY`; the report remains `NOT_QUALIFIED`. Exit `1` means missing/incompatible declarations or incomplete required inventory; `2` is invalid CLI input. Optional CRD inventory gaps do not block the alpha profile. CRD presence does not establish a working controller, installed release, runtime support or an integration.

The helper intentionally has no dynamic probe switch. A timeout by itself is not evidence of network isolation. A separate live network test must use controlled, reachable endpoints, positive controls before and after the negative test, the same image/runtime/nodes/labels, and bounded timeouts. Check same-node and cross-node paths; DNS and direct IPv4/IPv6; metadata, node and cluster API paths; ingress and egress; and connections attempted immediately at startup as well as after policy convergence. Record test Pod ownership, actual node/runtime, CNI policy status, endpoint availability and all selected additive policies. A denied DNS answer does not prove raw-IP blocking. Do not contact customer databases, public targets, model providers or production credentials for this test. These live tests have not run here.

## AWS baseline provenance

The [AWS AI on EKS Agent Sandbox infrastructure guide](https://awslabs.github.io/ai-on-eks/docs/infra/agents/agent-sandbox) has distinct Standard EKS/gVisor and Auto Mode paths. Source reviewed: [`cfccca1a82071826f38a6308d298010be0a45baa`](https://github.com/awslabs/ai-on-eks/tree/cfccca1a82071826f38a6308d298010be0a45baa/infra/agent-sandbox), with the [reference workload](https://github.com/awslabs/ai-on-eks/tree/cfccca1a82071826f38a6308d298010be0a45baa/blueprints/agent-sandbox). The baseline uses the actual upstream controller, templates, pools and claims; an ordinary Job should be labeled a direct-Job baseline. Keep installed upstream release, node image, runsc version, policy implementation and warm capacity in the experiment record. The blueprint itself is a starting point, not a security or performance certification.

AWS announced Nitro Sandbox support for Auto Mode on [November 9, 2026](https://docs.aws.amazon.com/eks/latest/userguide/auto-change.html); [isolated/confidential pods](https://docs.aws.amazon.com/eks/latest/userguide/sandboxing.html) use different runtime handlers and supported instance requirements. Do not substitute Nitro for gVisor while retaining a gVisor experiment label. [Kueue already integrates with Sandbox](https://kueue.sigs.k8s.io/docs/tasks/run/external_workloads/sandbox/); admission queues and quotas should be evaluated against that existing capability.

## Dedicated Substrate continuity smoke

The opt-in `scripts/probe_substrate.py` tests one **pre-created, dedicated** actor. It does not install Substrate, build/publish an image, create/delete an actor, configure credentials, inject arbitrary commands or change customer workload state. It increments only this fixture's counters twice, synchronously suspends that actor, observes a new durable memory snapshot and released **control-plane worker assignment**, resumes it, and checks RAM/file continuity. It attempts a bounded resume after an ambiguous suspend failure. The operator owns the actor and must verify/clean it up afterwards.

Reviewed source is pinned to Substrate [`1117caed19f0cdee42e5f045bad2043ea08c207d`](https://github.com/agent-substrate/substrate/tree/1117caed19f0cdee42e5f045bad2043ea08c207d). Build `cmd/kubectl-ate` from that checkout. Its [flags](https://github.com/agent-substrate/substrate/blob/1117caed19f0cdee42e5f045bad2043ea08c207d/cmd/kubectl-ate/internal/cmd/root.go), [get/suspend/resume commands](https://github.com/agent-substrate/substrate/blob/1117caed19f0cdee42e5f045bad2043ea08c207d/cmd/kubectl-ate/internal/cmd/actor.go), [protojson printer](https://github.com/agent-substrate/substrate/blob/1117caed19f0cdee42e5f045bad2043ea08c207d/cmd/kubectl-ate/internal/printer/printer.go) and [snapshot/status API](https://github.com/agent-substrate/substrate/blob/1117caed19f0cdee42e5f045bad2043ea08c207d/api/ateapi/v1/ateapi.proto) define the adapter contract. The helper records the CLI binary hash; source correspondence and server revision still need independent provenance.

An operator must first review the pinned [installation guide](https://github.com/agent-substrate/substrate/blob/1117caed19f0cdee42e5f045bad2043ea08c207d/docs/guides/operator-install.md) and [quickstart](https://github.com/agent-substrate/substrate/blob/1117caed19f0cdee42e5f045bad2043ea08c207d/docs/get-started/quickstart.md), prepare a separate EKS staging installation with gVisor, PostgreSQL and snapshot storage, and confirm the required node/security privileges are acceptable. The quickstart's KinD setup is not an EKS installation recipe.

Build `examples/conformance/Dockerfile` with this directory as context and pin the resulting image by digest in an administrator-owned ActorTemplate. Its default Python base is the Linux/amd64 child digest of the repository's pinned index; an arm64 pilot needs a separately verified `PYTHON_IMAGE` build argument and matching recorded architecture. Adapt the pinned Counter template through Substrate's actual template API; no speculative template YAML is supplied here. Use a dedicated gVisor WorkerPool, memory snapshot fidelity (`SNAPSHOT_FIDELITY_MEMORY`), port `8080`, a writable private scratch filesystem, and no customer mounts, credentials or external egress. Set `AGENTOS_PROBE_ID` to a dedicated random identifier of 16–128 characters; it is an identity check, not an authorization secret. The fixture stores a file nonce/counter in `STATE_DIR` and deliberately never writes its RAM nonce/counter to disk.

Create the actor yourself, obtain a private Substrate bearer-token file (`0600`), and establish separate loopback forwards for its HTTP proxy and the Substrate gRPC API. Confirm both forwards use the explicit EKS context and the HTTP proxy targets this dedicated actor, not another workload. An explicit API endpoint overrides the CLI's automatic connection selection; its association with the context is operator-attested, not independently proved by the helper, and is marked unverified in the report. Invoke the actual pinned `kubectl-ate` binary directly; `--execute` is required:

```sh
python3 scripts/probe_substrate.py --execute \
  --context YOUR_EXPLICIT_CONTEXT \
  --api-endpoint 127.0.0.1:18080 \
  --http-endpoint http://127.0.0.1:18081 \
  --actor dedicated-probe --atespace probe-only \
  --probe-id YOUR_DEDICATED_PROBE_ID \
  --cli /absolute/path/to/kubectl-ate \
  --cli-source-revision 1117caed19f0cdee42e5f045bad2043ea08c207d \
  --token-file /absolute/private/substrate-token \
  --transition-timeout 30 \
  --output evidence/substrate-continuity.json
```

No fixture HTTP requests are made while suspended: they could wake it. Each CLI call has a timeout, transition polling is bounded, redirects/proxy environment forwarding are disabled, and raw CLI/API output is not reported. Without `--execute`, no endpoint is contacted. Unsupported schema, a rootfs-only snapshot, a retained worker assignment, changed actor identity or lost RAM state fails the smoke rather than producing synthetic success.

`OBSERVED_CONTINUITY` with exit `0` is one installation's functional smoke, while `qualification` remains `NOT_QUALIFIED`. Worker Pod deletion is not expected: Substrate deliberately reuses warm worker Pods. Physical memory/CPU reclamation, automated idle detection, node-loss or cross-node recovery, token/connection behavior, useful task quality, and total-cost savings require separate experiments. Failure exits `1`; a missing execution opt-in exits `2`. No EKS/Substrate execution or runtime enforcement is claimed by the local fixture/unit tests.
