# Capability roadmap

The direction is an open-source environment for coding agents, research agents, short tasks, and long-lived agent workflows on Kubernetes. Different workloads should share identity, execution, policy, resource, state, and operational contracts. A complete agent framework, model, browser stack, or operating-system distribution is not required to start using the execution layer.

The first alpha deliberately covers bounded commands. Its checks exercise code and mocked Kubernetes behavior; live runtime/CNI qualification and production operation remain separate gates. The dates and cost assumptions in [PROJECT_PLAN.md](PROJECT_PLAN.md) remain planning estimates, not promised releases.

The November 19 [capability comparison](ALPHA_CAPABILITY_MAP.md) keeps execution economics and cross-runtime controls under investigation. The founder can provide EKS; the [first evaluation brief](EKS_EVALUATION_BRIEF.md) sequences cluster qualification, matched overhead measurements and assessment of upstream recovery. Explore OpenShell and Agent Substrate before expanding custom lifecycle or gateway code. No pivot or efficiency result has been established.

Implemented for that milestone: administrator-owned gVisor/offline Nitro profiles, declaration inventory, Substrate continuity smoke, registered cold execution collection, node resource sampling, operator-effort journals and decision gates. [Runbook](EVALUATION_RUNBOOK.md). Live qualification, resident warm-pool performance and disruptive/cross-instance recovery remain unmeasured. Upstream compatibility notes and concrete reproductions are required outputs even if no product advantage appears.

The November 21 iteration adds resumable committed lifecycle events, immediate controller wakeups for admission/cancellation, controller health, tenant-scoped history pagination and a Python harness client. [Live API](LIVE_API.md), [decision 0003](decisions/0003-live-lifecycle-and-harness-client.md). Its acceptance tests cover lost-wakeup/replay, tenant/cursor separation, stream bounds/disconnection, health recovery and pending cleanup. Actual agent execution on EKS and controlled provider access remain the next integration gates; this iteration does not establish runtime performance or a full in-sandbox AI agent.

| Stage | Capability | Evidence required before expanding |
| --- | --- | --- |
| First alpha | Authenticated tenant API; administrator-owned Python/Node templates; bounded Jobs requiring gVisor; default-deny workload policy prerequisite; admission limits; idempotent submission; persisted run metadata; status/cancel/log access; local Job preview | Automated contract tests, source checks, reproducible build instructions; no claim of live-cluster qualification |
| Cluster qualification | One documented Kubernetes/node/runtime/CNI/storage combination; actual task execution; network-isolation exercises; timeout/cancellation behavior; restart and resource-pressure tests | Published environment versions and commands; observed results, failures, and compatibility limits |
| Agent integration | One coding-agent harness and one research harness; controlled model/tool gateway; short-lived operation capabilities; provider-accounting adapters | Real workloads complete with attributable traces; denied actions stay denied; credentials stay within their declared boundary |
| Cost and fairness | Tenant/root-child budget reservations; bounded fan-out; fair queueing; measured cache or warm-capacity experiments | Lower total cost per successful policy-compliant task against equal-isolation baselines; include idle capacity, storage, network, and model charges |
| Durable work | Versioned workspace artifacts; application/framework checkpoints; external-event waits; safe recovery and uncertain-effect handling | Crash/partition exercises; no silent replay of ambiguous side effects; reproducible recovery for each advertised integration |
| Fleet operations | Multi-cluster visibility, tested backups/upgrades, enterprise identity integration, release/support ownership | Design partners operate routinely without founder intervention; paid pilots meet agreed acceptance criteria |
| Additional backends | Evaluate upstream Agent Sandbox or Ate integration, VM isolation, selected memory-snapshot support | A concrete customer need and compatibility/operational evidence that justify the added backend |

The initial native Kubernetes Job backend makes a small execution contract testable. It does not settle the long-term build-versus-extend decision. Compare the same tasks and policies on Agent Sandbox and Ate before building a scheduler, lifecycle substrate, or transparent hibernation layer.

Broad workload support means compatible execution contracts and documented integration profiles. It does not mean every framework can automatically recover arbitrary program memory or preserve open network connections. The first alpha offers neither process suspension nor automatic durable workflow recovery.

Before a public production release, publish a tested support matrix, vulnerability-reporting channel, release policy, tenant-boundary review, operational runbooks, and reproducible evaluations. Select a distinct public project name: “Agent OS” remains a working title, and [Agno already uses AgentOS](https://docs.agno.com/agent-os/introduction).
