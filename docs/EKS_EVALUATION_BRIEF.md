# First EKS evaluation

Date: November 19, 2026; comparison sequence updated November 23. Status: executable helpers and runtime profiles exist; the founder can set up EKS. No EKS cluster has been connected, qualified or benchmarked yet. Use the [evaluation runbook](EVALUATION_RUNBOOK.md) for commands and evidence contracts.

November 21 update: a [Python harness client](../sdk/python/README.md), resumable lifecycle events and controller health are implemented. Use the [tool-launch example](../examples/agent-harness/README.md) during Phase 1 after the runtime/CNI prerequisites are satisfied. Local client/server interoperability tests use a nonexecuting backend fixture; they do not add a live EKS result.

## Question to answer

Can an existing sandbox stack meet a platform team's agent workload, isolation, recovery and cost requirements on EKS? Establish that baseline before implementing another runtime or claiming an execution advantage.

The first milestone is a reproducible, qualified execution environment. Memory-preserving idle recovery is a later experiment, separate from the alpha's initial suspended Job creation.

## Environment to prepare

The requirements below define the Standard EKS/gVisor track. The implementation also provides a separate offline Nitro isolated profile and RBAC overlay. Prepare that arm on AWS-supported infrastructure; retain its different isolation/lifecycle label. AWS's [Agent Sandbox blueprint](https://awslabs.github.io/ai-on-eks/docs/infra/agents/agent-sandbox) is the upstream reference deployment, including preparation, controller and egress effort/cost.

- A disposable EKS cluster with an operator-controlled Linux **EC2 managed or self-managed node group**, with a customizable runtime/node image. Auto Mode workers are not the gVisor target for this experiment. Start with one instance type/CPU feature profile, x86_64 architecture and a fixed, small node pool to reduce the compatibility matrix. Record instance type, node count, AMI, kernel, Kubernetes and containerd versions. Final capacity must accommodate the selected upstream stack as well as workload resources; do not size a fleet from the alpha's API admission limit.
- Administrator control of the worker runtime installation, with `runsc` configured in containerd and a `gvisor` RuntimeClass selecting only qualified nodes. A stock EKS node image or RuntimeClass declaration alone is insufficient. Use the version-matched [gVisor Kubernetes](https://gvisor.dev/docs/user_guide/quick_start/kubernetes/) and [containerd](https://gvisor.dev/docs/user_guide/containerd/quick_start/) instructions when preparing the node image.
- A CNI configuration that actually enforces ingress and egress NetworkPolicy from workload startup, with its version and enforcement mode recorded. For Amazon VPC CNI, enable NetworkPolicy and use `NETWORK_POLICY_ENFORCING_MODE=strict`; standard mode initially allows traffic until policies load. Supply required system-service/CoreDNS policies and test denied traffic immediately at workload startup as well as after policy convergence. Qualify DNS, metadata, node services and peer paths. [AWS startup enforcement](https://docs.aws.amazon.com/eks/latest/userguide/cni-network-policy-configure.html)
- Persistent storage for the alpha's 1 GiB ReadWriteOnce API claim. Record the storage class and CSI driver. Snapshot experiments additionally need the selected runtime's documented state store and access path; an EBS filesystem snapshot is not a process-memory checkpoint.
- Image registry access and API images built for the worker architecture. Workload image pulls by the node are separate from sandbox outbound traffic.
- Access through a local AWS profile and kubeconfig, plus a time/spend bound for the experiment. Account for worker, EKS control-plane, storage, address, network/NAT and registry costs, including resources retained while tasks are idle.

EC2 workers are selected because this experiment needs runtime/node configuration. Fargate does not provide custom node AMIs or node access and does not run DaemonSets. [AWS Fargate constraints](https://docs.aws.amazon.com/eks/latest/userguide/fargate.html)

Auto Mode also does not support custom AMIs. AWS documents managed Nitro Sandboxes there as a separate isolation option. Use the explicit Nitro profile for that comparison; changing a RuntimeClass string alone is insufficient. [Auto Mode customization](https://docs.aws.amazon.com/eks/latest/best-practices/automode.html), [November 9 sandbox release](https://docs.aws.amazon.com/eks/latest/userguide/auto-change.html)

Use the existing [installation guide](GETTING_STARTED.md) after prerequisites are satisfied. Cluster setup does not establish that this alpha or an upstream stack is compatible with that environment.

## Phase 0: capture the environment

Before installing Agent OS, inspect the intended context and collect a private environment record:

```sh
kubectl config current-context
kubectl version -o yaml
kubectl get nodes -o wide
kubectl get runtimeclass -o yaml
kubectl get storageclass
kubectl -n kube-system get daemonsets
```

Record worker runtime configuration and CNI policy settings through the cluster operator's tooling. Keep endpoint/account details in the private experiment record and publish a sanitized manifest with results.

## Phase 1: qualify execution

Run known bounded test commands first. Verify actual gVisor execution independently, required runtime failure without fallback, Python/Node completion, deadline behavior, cancellation, API restart and retained run metadata. Exercise direct egress, DNS, metadata and peer restrictions on the actual CNI. Required API-to-Kubernetes access is cluster-specific; the base manifests require an explicit control-plane egress rule.

Any failed boundary check blocks untrusted workloads. Unsupported configurations are reported as failures or exclusions, not silently changed to ordinary containers.

## Phase 1b: establish Substrate compatibility

Before comparative performance, run the dedicated [Substrate continuity smoke](../examples/conformance/README.md#dedicated-substrate-continuity-smoke). Its snapshot/assignment observations and memory/file continuity result determine which recovery experiments are supported. Record unsupported configurations instead of inferring a missing runtime feature. The awkward recovery cases remain in Phase 3.

## Phase 2: measure overhead and useful execution

Compare the same deterministic commands, digest-pinned images, limits, nodes and isolation, using the AWS blueprint's upstream controller as reference:

1. Plain gVisor Jobs.
2. The current Agent OS Job backend.
3. Upstream Agent Sandbox on the AWS blueprint deployment. The implemented collector measures cold claims; warm resident execution requires a separate dispatch protocol and idle-capacity accounting.

Keep the node pool fixed for the initial comparison and separate cold images, cached images and provisioned warm capacity. Record admission-to-first-useful-result, p50/p95/p99 with sample counts, completion/error rate, throughput, CPU/RAM, idle resources and control-plane overhead. Capture timestamps at equivalent boundaries and include observer/polling overhead. The alpha's reconciliation interval is part of its measured overhead.

Do not call the collector's end-to-end differences isolated control-plane overhead: HTTP and kubectl arms use different observation paths. Time installation, upgrades and failure recovery with the operator journal, including privileges, customization and failed steps. Apply the registered decision gates before interpreting an advantage.

This compares current execution paths; it does not test the alpha for a hibernation feature it lacks.

A fixed node pool measures resource use and capacity headroom; the EC2 bill does not decrease while the same nodes remain provisioned. A later cost experiment must actually remove or avoid nodes while meeting the workload SLO and counting snapshot, warm-capacity and scaling overhead.

The [next comparison campaign](EVALUATION_PLAN.md#next-comparison-campaign) sequences this deterministic baseline, matched failure/operating exercises, real coding/research tasks and fleet economics. Its later stages require additional harness and usage instrumentation. Keep real-agent correctness and token measurements separate from the current deterministic collector's decision report.

## Phase 3: assess state preservation and integration

Evaluate Agent Substrate's existing gVisor checkpoint/restore path on the qualified EKS environment. Compare an always-resident stateful session with its idle-release mode at declared active/idle ratios, including snapshot storage/I/O and warm-worker cost. Record any installation or compatibility failure as a result.

Use one deterministic stateful task that proves both in-memory and filesystem continuity. Measure time to quiesce, durable-state commitment, actual resource release, readiness after resume and completed-task correctness. Exercise worker loss and expired credentials. Recheck policy on resumed work. Open connections, kernel/runtime compatibility and external side effects need explicit contracts; a checkpoint alone does not solve them.

Install OpenShell on Kubernetes as a separate policy/credential integration baseline when its prerequisites fit. Pin the release/driver and check supported operations: v0.1.9's Kubernetes suspension implementation is not provided merely by the existence of suspend/resume APIs. Keep APF development builds separate from released baselines. A proposed OpenShell/Substrate integration is not assumed operational until tested.

GKE can be a second environment for provider-specific snapshot comparison after the EKS result. Passing EKS does not establish AKS, OpenShift or cross-cloud snapshot portability.

## Deliverables and product gate

Retain pinned versions/configuration, commands, raw measurements, failed cases, actual billing evidence and a sanitized reproducibility record. Run real-model task/quality trials separately from deterministic infrastructure measurements.

Ask the platform team whether it has evaluated OpenShell and Agent Substrate since KubeCon, which workload still fails its requirements, and what paid-pilot acceptance criteria it would commit to. If upstream meets the requirements at an acceptable operating cost, investigate integration/support value or a different problem before adding custom lifecycle code.

See the full [evaluation plan](EVALUATION_PLAN.md), [alpha capability map](ALPHA_CAPABILITY_MAP.md) and [product-direction proposal](PRODUCT_DIRECTION_REVIEW.md).
