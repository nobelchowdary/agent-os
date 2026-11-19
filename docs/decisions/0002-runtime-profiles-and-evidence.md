# 0002: Explicit runtime profiles and evidence before product expansion

Date: November 19, 2026. Status: accepted engineering scope; product direction remains under evaluation.

## Context

EKS is available for a pilot. AWS provides an Agent Sandbox blueprint and Nitro isolated pods, while Agent Substrate implements pooling, actor access controls, secret injection and checkpoint/restore. These are baselines to evaluate before adding custom scheduling, hibernation or governance. The alpha has no demonstrated advantage over them.

## Decision

Keep the staged Job lifecycle, synchronous admission and immutable run contract. Add administrator-owned runtime profiles with exact kind/class/handler combinations. Persist explicit profiles in new resolved run specifications and idempotency fingerprints. Legacy `runtime_class` configuration and nil-profile records retain their gVisor meaning. Caller request fields do not change.

Support gVisor and AWS Nitro **isolated**, offline bounded-command execution. Nitro requires its declared class/handler, sandbox-capable placement and guest/slot overhead. Render the network-isolated annotation and bounded memory-backed scratch; reject profile/annotation drift. Confidential mode, attestation, online provider access and deployment of AWS infrastructure are outside this implementation. A readiness check inspects declarations; live qualification remains mandatory.

Preserve the default gVisor installation. Provide a separate Nitro configuration and least-privilege RBAC overlay at `overlays/nitro`; do not add fallback behavior or change the selected runtime after failure. Finish outstanding work and preserve its database before changing an installation's profile.

Build an opt-in evidence pipeline: explicit kube context, immutable-by-convention input registration with hashes, bounded trial collection, retained failed/unknown outcomes, guarded cleanup and private files. Comparison tools execute only registered deterministic workloads on explicitly selected clusters; they never provision a cluster or execute submitted workloads on the API host.

Use AWS's blueprint-compatible upstream Agent Sandbox path as the reference arm. Plain Jobs remain a diagnostic floor. A cold claim test is not a warm-pool result, and an offline fixture does not qualify AWS's complete connected Bedrock/FQDN example. Examine Substrate's existing recovery before introducing competing lifecycle code. Do not equate Kueue admission/fairness, transactional API idempotency and tenant authorization; assess upstream Kueue rather than building another scheduler.

## Decision gates

The checked-in protocol proposes 30 trials per arm, 100% correctness on the deterministic fixture, no more than 10% p95 latency regression, and either 20% lower fully accounted cost per correct task or 25% less median operator time. These are initial engineering gates, not customer-approved commercial targets. Replace them with partner-agreed values **before registration**, then retain failures and finish the planned trial set.

The decision tool requires matching task/environment/protocol hashes and referenced live qualification. Declarations-only preflight cannot pass. A failed qualification stops the experiment; missing evidence leaves it undecided. Passing a route continues customer validation rather than establishing production readiness or acquisition value. Operator observations are human-reported; local hashes and timestamps are not an independently anchored audit log.

## Consequences

Local tests can establish configuration, lifecycle and evidence-processing behavior. Only actual supported nodes can establish runtime compatibility. Publish qualified support notes, reproducible upstream failures and proposed fixes as experiment outputs, including when Agent OS offers no advantage. Do not open speculative upstream issues or submit changes without a concrete reproduction.

Primary references: [AWS blueprint](https://awslabs.github.io/ai-on-eks/docs/infra/agents/agent-sandbox), [Nitro isolation](https://docs.aws.amazon.com/eks/latest/userguide/sandboxing.html), [network-isolated behavior](https://docs.aws.amazon.com/eks/latest/userguide/sandboxing-network-isolation.html), [Nitro limitations](https://docs.aws.amazon.com/eks/latest/userguide/sandboxing-limitations.html), [Kueue Sandbox integration](https://kueue.sigs.k8s.io/docs/tasks/run/external_workloads/sandbox/).
