# Security

This is an experimental alpha with no supported production release, independent security assessment, or guaranteed response SLA. The code implements a bounded execution boundary for evaluation; actual isolation also depends on the cluster administrator, worker runtime, kernel, CNI, and admission configuration.

## Reporting

When this repository is published, use its **Security → Report a vulnerability** feature if private vulnerability reporting is enabled. A private reporting address has not been established. If private reporting is unavailable, open a public issue asking only for a private contact channel; omit exploit details, credentials, customer data, and sensitive logs. Do not assume that an unconfigured GitHub feature is monitored.

## Current boundary

- API callers authenticate using administrator-provisioned bearer tokens scoped to tenant IDs. These are shared tenant credentials in this alpha, not workload identity or per-user enterprise authentication. Keep the API behind loopback port-forwarding or a separately configured authenticated TLS endpoint.
- Administrators, the API process, its bbolt database, template configuration, Kubernetes control plane, and qualified worker infrastructure are trusted. An API compromise can use its scoped Job/Pod privileges; the controller is outside the untrusted workload sandbox.
- Callers can choose commands within allowed templates. Templates control digest-pinned images, resources and deadline bounds; the administrator selects a gVisor or AWS Nitro isolated profile for the API/backend. Workloads use non-root users, read-only root filesystems, dropped capabilities and no mounted API credentials. The Nitro profile requests a NIC-free guest and memory-backed scratch; trusted-parent/vsock paths remain part of its separate, unqualified boundary. Neither profile has live EKS qualification results.
- Workload ingress and egress are denied by a named NetworkPolicy prerequisite. Readiness verifies the declared policy shape and RuntimeClass handler name, not actual CNI enforcement or node runtime installation. Kubernetes NetworkPolicies are additive and have node-local limitations. Independently qualify the actual traffic paths and exclude conflicting policies.
- CPU and memory limits, bounded scratch requests/limits, deadlines, and concurrency admission reduce resource exposure. Kubernetes ephemeral-storage enforcement and deadline/termination behavior depend on node behavior; they are not instantaneous physical cutoffs. Per-Pod PID limits require node configuration. No model-spend or external-tool budget is enforced yet.
- Submission idempotency prevents duplicate API admission for the same tenant and request key. A Kubernetes Job can still execute application code more than once in some failure scenarios. Do not infer exactly-once external actions from the API or from disabled application retries.
- Run metadata is persisted locally on one API replica. Workload files are ephemeral, and logs depend on retained Kubernetes Pods. Cancellation requests deletion and waits for observed cleanup; an unreachable node or an already completed action cannot be undone by the API.

The runtime does not solve prompt injection, decide whether an allowed tool action is appropriate, validate model output, or stop misuse of data already made available to workload code. Outbound credential brokering, approval enforcement, workload identity, durable workspaces, and checkpoint-aware recovery are roadmap items. Do not supply reusable provider credentials or sensitive datasets on the assumption that these features already exist.

Use a dedicated evaluation namespace and qualified nodes. Treat template changes, new admission controllers, runtime upgrades, CNI changes, and broader API exposure as changes to the trust boundary and repeat the relevant live checks.
