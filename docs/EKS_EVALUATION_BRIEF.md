# First EKS evaluation

Date: November 19, 2026; blueprint and decision notes updated November 24. Status: executable helpers and runtime profiles exist; the founder can set up EKS. No EKS cluster has been connected, qualified or benchmarked yet. Use the [evaluation runbook](EVALUATION_RUNBOOK.md) for commands and evidence contracts.

November 21 update: a [Python harness client](../sdk/python/README.md), resumable lifecycle events and controller health are implemented. Use the [tool-launch example](../examples/agent-harness/README.md) during Phase 1 after the runtime/CNI prerequisites are satisfied. Local client/server interoperability tests use a nonexecuting backend fixture; they do not add a live EKS result.

## Question to answer

Can an existing sandbox stack meet a platform team's agent workload, isolation, recovery and cost requirements on EKS? Establish that baseline before implementing another runtime or claiming an execution advantage.

The first milestone is a reproducible, qualified execution environment. Memory-preserving idle recovery is a later experiment, separate from the alpha's initial suspended Job creation.

## Environment to prepare

Start with AWS's [Agent Sandbox blueprint](https://awslabs.github.io/ai-on-eks/docs/infra/agents/agent-sandbox). Its reviewed source at [`cfccca1a82071826f38a6308d298010be0a45baa`](https://github.com/awslabs/ai-on-eks/tree/cfccca1a82071826f38a6308d298010be0a45baa/infra/agent-sandbox) specifies Standard EKS 1.37, Karpenter/gVisor workers and Cilium chained onto the VPC CNI. The upstream default is v1.0.1; keep that pinned blueprint baseline unless deliberately registering and qualifying another release. AWS's approximately 20–30 minute estimate is infrastructure setup guidance, not an observed end-to-end pilot time.

Resolve and record the blueprint's moving AMI/runtime references before registration. Pin the worker instance type, architecture, node count, AMI, runsc and CNI/controller images. Keep the selected worker/API nodes ready and stable for the campaign; address Karpenter consolidation, expiry and replacement explicitly. A changing or cold provisioned fleet is a separate experiment from the fixed-node execution comparison.

The implementation also provides a separate offline Nitro isolated profile and RBAC overlay. Prepare that arm on AWS-supported infrastructure; retain its different isolation/lifecycle label. The requirements below cover prepared EC2/gVisor workers, including the blueprint's dedicated pool.

- A disposable EKS cluster with operator-controlled Linux **EC2 workers**, using the blueprint's dedicated Karpenter pool or a prepared managed/self-managed node group. Auto Mode workers are not the gVisor target for this experiment. Start with one instance type/CPU feature profile, x86_64 architecture and a fixed, small node pool to reduce the compatibility matrix. Record instance type, node count, AMI, kernel, Kubernetes and containerd versions. Final capacity must accommodate the selected upstream stack as well as workload resources; do not size a fleet from the alpha's API admission limit.
- Administrator control of the worker runtime installation, with `runsc` configured in containerd and a `gvisor` RuntimeClass selecting only qualified nodes. A stock EKS node image or RuntimeClass declaration alone is insufficient. Use the version-matched [gVisor Kubernetes](https://gvisor.dev/docs/user_guide/quick_start/kubernetes/) and [containerd](https://gvisor.dev/docs/user_guide/containerd/quick_start/) instructions when preparing the node image.
- A CNI configuration that actually enforces ingress and egress NetworkPolicy from workload startup, with its version and enforcement mode recorded. Use `--cni cilium` for declaration inventory on the blueprint's chained Cilium track, then run actual denial/positive-control probes. A native VPC CNI policy track is a different configuration: enable its NetworkPolicy support and `NETWORK_POLICY_ENFORCING_MODE=strict` to avoid standard mode's initial allow window. Do not treat these profiles as interchangeable or assume both engines must be enabled. Supply required system-service/CoreDNS policies and qualify DNS, metadata, node services and peer paths from startup through convergence. [AWS native startup enforcement](https://docs.aws.amazon.com/eks/latest/userguide/cni-network-policy-configure.html)
- Persistent storage for the alpha's 1 GiB ReadWriteOnce API claim. Record the storage class and CSI driver. Snapshot experiments additionally need the selected runtime's documented state store and access path; an EBS filesystem snapshot is not a process-memory checkpoint.
- Image registry access and API images built for the worker architecture. Mirror the pinned workload images and API image into the pilot's ECR repositories and record the actual references/digests in the administrator configuration and experiment. Workload image pulls by the node are separate from sandbox outbound traffic.
- Access through a local AWS profile and kubeconfig, plus a time/spend bound for the experiment. Account for worker, EKS control-plane, storage, address, network/NAT and registry costs, including resources retained while tasks are idle.

EC2 workers are selected because this experiment needs runtime/node configuration. Fargate does not provide custom node AMIs or node access and does not run DaemonSets. [AWS Fargate constraints](https://docs.aws.amazon.com/eks/latest/userguide/fargate.html)

Auto Mode also does not support custom AMIs. AWS documents managed Nitro Sandboxes there as a separate isolation option. Use the explicit Nitro profile for that comparison; changing a RuntimeClass string alone is insufficient. [Auto Mode customization](https://docs.aws.amazon.com/eks/latest/best-practices/automode.html), [November 9 sandbox release](https://docs.aws.amazon.com/eks/latest/userguide/auto-change.html)

Use the existing [installation guide](GETTING_STARTED.md) after prerequisites are satisfied. Cluster setup does not establish that this alpha or an upstream stack is compatible with that environment.

Record the first provisioning/install session in a private timestamped exploratory log, including failed steps and waiting. The existing operator journal requires a real experiment registration, and its decision route requires at least three matched sessions per arm. After qualifying and freezing the environment, register and repeat the same install/upgrade/recovery procedure from a documented reset state. Never attach later registration hashes to an earlier exploratory installation. State whether common EKS/runtime/CNI provisioning is shared infrastructure or part of both measured tasks; comparing a full blueprint installation with an Agent OS overlay on an already prepared cluster would not measure equivalent operating effort.

Agree on an AWS time/spend bound before provisioning, configure the owner's budget alerts, and use the pinned blueprint's cleanup procedure after the session. Retain billing and resource inventory through teardown; no cost figure or completed cleanup is implied by this plan. These repository tools do not provision or delete the AWS cluster.

## Billing evidence before the cost experiment

Prepare billing collection before the pilot. For shared EC2 workers, AWS split cost allocation data (SCAD) provides Pod allocations in legacy CUR or CUR 2.0 through Data Exports; it is absent from Cost Explorer. The management account, or a standalone account, must opt in to EKS under Cost Management preferences. Enable split cost allocation data and resource IDs in the selected report/export, choose hourly data, and retain its configuration and delivered manifests. Record the EKS measurement basis: resource requests allocates by requested CPU/memory; an actual-utilization option needs its documented telemetry setup and its operating cost. [AWS enablement](https://docs.aws.amazon.com/cur/latest/userguide/enabling-split-cost-allocation-data.html), [CUR 2.0 export settings](https://docs.aws.amazon.com/cur/latest/userguide/dataexports-create-standard.html)

AWS documents up to 24 hours before SCAD appears in CUR. Kubernetes labels can become cost allocation tags, but their keys require activation by the payer/management account; discovery can take up to 24 hours and activation another 24 hours. Start with a small canary, wait for actual rows with the expected Pod identity and tags, and verify joins before the registered cost window. A day elapsed is not evidence that every short-lived Pod was captured. [Kubernetes label requirements and limits](https://docs.aws.amazon.com/cur/latest/userguide/split-cost-allocation-data-kubernetes-labels.html)

Use the [runbook's billing preparation](EVALUATION_RUNBOOK.md#billing-preparation-before-the-pilot) to map both arms to the registered experiment/trials and exact Pod identities. The current collector does not attach one common arm label to every Pod; a label on an Agent OS Job is not inherited by an already created Pod. Preserve its recorded Pod UID/name and run-to-arm mapping instead of assuming automatic attribution. Keep unrelated work out of the accounting window or allocate it explicitly.

The 20% gate concerns **fully accounted cost per correct task**, not just a Pod's allocated EC2 share. SCAD reallocates shared charges; it does not demonstrate a smaller bill. Include unused capacity and shared services, reconcile allocations to parent instance charges without counting both, and account separately for EKS, EBS/snapshots, networking/NAT, registry and observability/export costs. Missing billing rows or unresolved attribution keep `billing_complete=false`. A complete alternative billing ledger can support the gate without SCAD; it must establish the same lineage and coverage. [Split/unused cost fields](https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-split-line-item.html)

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

The [portable lifecycle campaign](KIND_TESTS.md#prepared-external-clusters) now accepts explicit context/kubeconfig, selected worker/API nodes and operator-prepared images on a disposable cluster. Its local kubelet-stop fault is Docker-specific and skipped externally; selected boundary probes remain partial unless their positive controls are reachable. Review its destructive-operation opt-in and namespace restrictions before use. Capture the 16/64 running-task terminal-lag distributions before replacing polling with watches. No EKS result is implied by this tooling.

## Phase 1b: establish Substrate compatibility

Before comparative performance, run the dedicated [Substrate continuity smoke](../examples/conformance/README.md#dedicated-substrate-continuity-smoke). Its snapshot/assignment observations and memory/file continuity result determine which recovery experiments are supported. Record unsupported configurations instead of inferring a missing runtime feature. The awkward recovery cases remain in Phase 3.

## Phase 2: measure overhead and useful execution

Compare the same deterministic commands, digest-pinned images, limits, nodes and isolation, using the AWS blueprint's upstream controller as reference:

1. Plain gVisor Jobs.
2. The current Agent OS Job backend.
3. Upstream Agent Sandbox on the AWS blueprint deployment. The implemented collector measures cold claims; warm resident execution requires a separate dispatch protocol and idle-capacity accounting.

Keep the node pool fixed for the initial comparison and separate cold images, cached images and provisioned warm capacity. Record admission-to-first-useful-result, p50/p95/p99 with sample counts, completion/error rate, throughput, CPU/RAM, idle resources and control-plane overhead. Capture timestamps at equivalent boundaries and include observer/polling overhead. The alpha's reconciliation interval is part of its measured overhead.

Register the selected one-second and sixty-second elapsed profiles separately, before collecting either arm. First run the short direct-Job versus Agent OS instrument check. New format-3 protocols retain the caller-outcome allowance `candidate p95 <= baseline p95 * 1.10 + 2 seconds`, with shared authenticated transport, an independent fixed-cadence sampler and recorded nonterminal-to-terminal request brackets. A missing or wider-than-0.5-second bracket makes latency `not_measurable`; sufficiently narrow p95 intervals must pass the rule conservatively, and straddling intervals stay undecided. Inspect unexpected instrument-check deltas rather than assuming a known one-second reference. Record terminal-Pod, native caller-outcome and validated-completion observations for every arm, preserve the written policy/rationale and keep both durations separate. Format-1/format-2 registrations retain their original metrics, rules and collection paths. These are synthetic duration probes, not measured customer/CPU workloads. Keep Job authority and stop reporting-delay tuning at this stage; sessions/reuse are a later lifecycle experiment. See [decision 0009](decisions/0009-comparison-boundaries-and-latency-budget.md) and [decision 0010](decisions/0010-comparison-observation-precision.md).

Do not call the collector's end-to-end differences isolated control-plane overhead: the native APIs and service guarantees still differ even when per-call process/login setup is removed. Time installation, upgrades and failure recovery with the operator journal, including privileges, customization and failed steps. Apply the registered decision gates before interpreting an advantage.

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
