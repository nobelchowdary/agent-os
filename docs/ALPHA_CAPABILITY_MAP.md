# Alpha capability map and research handoff

Reviewed November 19, 2026. Base implementation reference: `30a8e33135ed6051dc569537dfdc3e5afcc3ffeb`; the current runtime-profile/evidence iteration extends that base under [decision 0002](decisions/0002-runtime-profiles-and-evidence.md).

Updated November 21 for the [live lifecycle/client iteration](decisions/0003-live-lifecycle-and-harness-client.md). The November 19 evidence below remains historical. New local HTTP/2/SSE, storage, controller and Python-client tests verify contracts; no EKS workload has run. [Launch and live API](LIVE_API.md) describes what an external harness can use and what still requires integration.

This document can be shared with a researcher who cannot access the private repository. It distinguishes inspected local code, documented upstream capabilities and results that still need measurement. No upstream runtime was installed or benchmarked for this review.

## What the alpha actually does

Agent OS is a small authenticated Kubernetes Job execution service. An administrator defines digest-pinned Python/Node environments, fixed resources, maximum deadlines and an explicit gVisor or offline AWS Nitro isolated profile. Tenants submit bounded commands, inspect status/logs and request cancellation. The service checks the selected RuntimeClass contract and a named default-deny workload NetworkPolicy; declarations do not prove runtime or network enforcement.

| Area | Implemented | Important limit | Local evidence |
| --- | --- | --- | --- |
| Admission and identity | Tenant bearer tokens, transactional active-run limits, persistent tenant-scoped idempotency | No per-workload capability identity, delegation or enterprise SSO | [API](../internal/api/api.go), [store](../internal/store/store.go) |
| Execution policy | Administrator-owned images/resources/profile; non-root, read-only root, dropped capabilities, bounded scratch and deadline; Nitro additionally has NIC-free placement and memory scratch | No arbitrary caller PodSpecs; both profiles remain unqualified on actual nodes | [configuration](../internal/config/config.go), [backend](../internal/backend/kubernetes/kubernetes.go) |
| Lifecycle | Durable run state; suspended initial creation; persisted activation intent; version-checked activation | Initial Job suspension prevents premature launch. It is **not** memory-preserving hibernation | [controller](../internal/controller/controller.go), [accepted decision](decisions/0001-initial-execution-slice.md) |
| Failure handling | Ambiguous activation cannot recreate missing work; cancellation retains admission capacity until owned objects disappear | API object disappearance does not prove a partitioned worker stopped; no external-effect fencing | [backend](../internal/backend/kubernetes/kubernetes.go), [tests](../internal/controller/controller_test.go) |
| Persistence | Synchronous bbolt run metadata/events, one API process, PVC deployment | No durable workspace/log archive, checkpoint, distributed controller or automatic application retry | [store](../internal/store/store.go), [deployment](../deploy/api.yaml) |
| Access to tools/models | Workload egress denied by default | No controlled provider access, credential injection, tool policy or action approval | [installation guide](GETTING_STARTED.md) |
| Economics | Fixed CPU/RAM/storage bounds and execution deadline | No token/dollar budget, root/child accounting, warm pool, density optimization or measured savings | [evaluation plan](EVALUATION_PLAN.md) |
| Evidence tooling | Explicit-context inventory; preregistered cold execution collector; Substrate continuity probe; node CPU/memory observations; operator journal and decision report | Fake-client tests do not establish isolation; upstream/controller and AWS installation are operator prerequisites | [runbook](EVALUATION_RUNBOOK.md) |
| Live operation and client | Durable per-run SSE replay/resume; commit-driven controller wakeups; bounded streams; tenant history pagination; controller status; stdlib Python submit/wait/cancel/log client | Lifecycle events are not stdout or a hard real-time guarantee; the example invokes an offline tool from an external harness and is not a complete in-sandbox AI agent | [live API](LIVE_API.md), [client](../sdk/python/README.md) |

## Evidence already available

The [initial implementation CI run](https://github.com/nobelchowdary/agent-os/actions/runs/44431891574) passed formatting, vet, race-enabled tests, binary build, compiled HTTP preview smoke and a Docker build for commit `c2469769ea65195559311803f465d6d0990e8ce6`. This historical run does not validate the current iteration. The expanded [Makefile](../Makefile) and [workflow](../.github/workflows/ci.yml) also run Python evaluation-tool tests; local checks and CI are separate from live qualification.

For the November 19 runtime-profile/evidence iteration, local `make check` passed Go vet/race tests, 72 Python tests, a binary build and the compiled preview smoke for legacy/explicit gVisor and Nitro configurations. The API image built and served authenticated preview requests as a non-root, read-only container; both deployment variants rendered. The dedicated continuity fixture also built for Linux/amd64 and passed its local container smoke. These are local packaging/contract checks, not an EKS execution result.

For the November 21 live-lifecycle iteration, local `make check` passed vet, Go race tests, 88 Python tests, a binary build and all three profile preview smokes. A Python client also exercised the real HTTP API, durable store and controller with a nonexecuting backend fixture; a real HTTP/2 test covered an idle event stream. The API image built and passed authenticated status/plan previews in non-root, read-only containers for gVisor and Nitro configurations. No workload or model call ran on a cluster.

Backend tests use fake Kubernetes clients or a local HTTP test server. They cover lifecycle and API contracts, not physical isolation or cloud performance. There are **no published live-cluster acceptance results, raw performance benchmarks, real-agent quality trials or token-savings results**. Numerical targets in the evaluation plan are hypotheses.

## Comparison with upstream projects

Upstream columns describe documentation/source inspected on November 19, 2026. They are not independent security or performance validation, nor a claim that every capability composes correctly in one deployment.

| Capability | Agent OS alpha | Agent Sandbox / OpenShell | Agent Substrate (Ate) |
| --- | --- | --- | --- |
| Execution foundation | Native Kubernetes Jobs with gVisor or offline Nitro isolated profile | OpenShell's Kubernetes driver provisions through upstream Agent Sandbox | Stateful Actors run on reusable isolated Workers |
| Warm capacity | Not implemented | Agent Sandbox warm pools are an explicit baseline to evaluate | Worker pooling is part of its design |
| Durable workspace | Pod scratch only | OpenShell documents a workspace PVC; memory recovery is a separate capability | Snapshot/restore of actor state is part of the implementation |
| Memory-preserving suspension | Not implemented | OpenShell v0.1.6 added APIs; its v0.1.9 Kubernetes driver returns unimplemented for suspend. Generic Agent Sandbox suspension terminates/recreates a Pod; preserving RAM needs an additional mechanism | gVisor worker code implements checkpoint/restore; non-GKE deployment is already an upstream capability to test |
| Network and credentials | Default-deny declarations; no provider gateway | OpenShell documents a separate trusted supervisor, mediated egress and credential handling | Identity-aware gateway/credential handling is part of the documented design |
| Operational/economic advantage | Unmeasured | Matched tests required | Published vendor performance claims require reproduction under the customer's conditions |

AWS's [EKS Agent Sandbox blueprint](https://awslabs.github.io/ai-on-eks/docs/infra/agents/agent-sandbox) is the upstream reference deployment. Node preparation, controller and egress integrations count toward operating effort. Our collector's cold offline fixture exercises its controller contract; it does not reproduce the entire connected reference workflow. Nitro is an explicit comparison profile, not an automatic gVisor replacement.

References: [OpenShell Kubernetes setup](https://docs.nvidia.com/openshell/v0.1.9/kubernetes/setup), [suspend/resume release](https://github.com/NVIDIA/OpenShell/releases/tag/v0.1.6), [v0.1.9 Kubernetes driver](https://github.com/NVIDIA/OpenShell/blob/v0.1.9/crates/openshell-driver-kubernetes/src/grpc.rs#L170-L177), [Agent Sandbox suspension semantics](https://agent-sandbox.sigs.k8s.io/docs/go-client/), [warm pools](https://agent-sandbox.sigs.k8s.io/docs/examples/warmpool-quickstart/), [GKE snapshot integration](https://agent-sandbox.sigs.k8s.io/docs/sandbox/snapshots/), [Substrate README at inspected commit](https://github.com/agent-substrate/substrate/blob/1117caed19f0cdee42e5f045bad2043ea08c207d/README.md), [gVisor worker checkpoint code](https://github.com/agent-substrate/substrate/blob/1117caed19f0cdee42e5f045bad2043ea08c207d/cmd/ateworker-gvisor/runsc.go#L173).

Do not interpret unsupported OpenShell suspension as an empty non-GKE market: Substrate is a separate existing implementation. Conversely, the existence of an API or a generic snapshot extension does not establish working checkpoint recovery on EKS.

## Corrections to the latest pasted research

1. **OpenShell's CNCF contribution is confirmed.** CNCF's November 10 welcome also describes Cedar adoption in progress. A Sandbox project designation is not production qualification. [CNCF announcement](https://www.cncf.io/blog/2026/11/10/welcoming-openshell-to-the-cncf-open-governance-to-secure-agentic-ai/)
2. **Agent Substrate's maturity description is outdated.** Google's November 5 announcement reports version 1.0, GKE general availability and open-source deployment on other Kubernetes clusters. Those statements do not qualify our intended EKS configuration. [Google announcement](https://cloud.google.com/blog/products/containers-kubernetes/open-infrastructure-for-autonomous-agents-comes-to-cncf)
3. **AWS user budgets are not categorically absent.** Its session documentation explicitly describes stable per-user sessions for cross-conversation daily limits. The temporal mechanism remains session-scoped, has a 24-hour condition-window limit, and a new session resets history; policy changes invalidate active sessions. This is not by itself an invoice-backed cross-provider budget. [Session scope](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/policy-session-based-temporal.html), [temporal limits](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/policy-temporal.html)
4. **OpenShell approvals need a precise definition.** Approval of proposed network-policy changes differs from approval of each external business action. Neither a documentation-index omission nor a broad feature label settles the latter contract. [Policy advisor](https://docs.nvidia.com/openshell/v0.1.9/how-it-works/policies/advisor)
5. **Agent Policy Fabric is a development integration.** Its current documentation distinguishes implemented features from planned ones, has no tagged release, and limits supported service topology. Native human-review workflows are reserved, but existing information-provider integration can enforce external approval facts and their withdrawal. Assess these contracts rather than assuming either a production fleet service or absent approval/revocation controls. [Support matrix](https://docs.nvidia.com/agent-policy-fabric/about/support-and-compatibility), [release status](https://docs.nvidia.com/agent-policy-fabric/releases/release-notes), [external facts](https://docs.nvidia.com/agent-policy-fabric/integrations/policy-information-providers)

## Next decision

The founder can provide EKS for a pilot. Follow the [EKS evaluation brief](EKS_EVALUATION_BRIEF.md): qualify one environment, measure the existing execution path, then assess upstream warm-pool and memory-recovery implementations. Use OpenShell as a required policy/credential integration comparison before writing a new gateway.

Continue exploring execution economics and cross-runtime controls. Select a product only after a reachable platform team supplies a reproducible unmet need and agrees what result would justify a paid pilot. No speed, cost, recovery or acquisition advantage is established by this review.
