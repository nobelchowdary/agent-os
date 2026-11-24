# 0001: A bounded execution slice before broader orchestration

Date: November 18, 2026. Status: accepted for the first experimental alpha; revisit after workload evaluation. The new-admission deadline default is superseded by [ADR 0007](0007-persisted-execution-deadline-scope.md); the original Job clock remains the legacy persisted contract.

## Context

The product centers on an agent sandbox/environment. Its future users may run short tools, coding workspaces, research tasks and durable workflows. Existing projects already implement sophisticated sandbox lifecycles. We need a small executable contract and meaningful evaluation evidence before adopting a broader substrate or building scheduling infrastructure.

## Decision

Start with a Go HTTP API, tenant-scoped authentication, administrator-owned environment templates and durable run metadata. The first backend uses ordinary Kubernetes Jobs with the explicit `runsc` runtime handler exposed as RuntimeClass `gvisor`. It delegates placement and execution to Kubernetes and does not implement a new scheduler or VMM.

Create Jobs suspended, durably record an `activating` phase, then activate the acknowledged matching Job using a noncreating, version-checked operation. A retry after activation intent can never recreate a missing Job. This prevents a delayed first Create request from launching a task after cancellation. A late suspended Job may still require operator cleanup. A Job that has started can still execute work after an asynchronous cancellation request until Kubernetes stops it; Kubernetes object deletion cannot fence a partitioned physical worker.

Every run is durably admitted before acknowledgment. Active-run reservations include queued and cancelling records. Idempotency keys are tenant-scoped and fingerprint the resolved environment and command. The database is bbolt with synchronous transactions and an exclusive process lock; one API replica is supported. PostgreSQL/distributed ownership from the original proposal is deferred until scaling requirements justify it.

Caller inputs are a template name, optional command and execution timeout within the template maximum. They cannot provide arbitrary PodSpecs, images, mounts, privileges or credentials. Workload traffic is denied by default. Node/runtime and network enforcement need independent cluster qualification.

The initial API's `timeout_seconds` controls the Job execution deadline. [ADR 0007](0007-persisted-execution-deadline-scope.md) changes new admissions to the Pod clock while retaining the original clock on legacy records. Neither is a strict admission-to-completion deadline. Unstarted work has a separate five-minute startup wait. Completed Job status remains authoritative when the API reconnects after downtime. Reconciliation uses at most eight workers and prioritizes already-requested cancellations; the alpha accepts at most 64 active reservations (default 16). No fixed cancellation-latency SLO is promised.

## Consequences

This gives us runnable code, state/lifecycle tests and concrete environment manifests without requiring a new upstream CRD installation. It does not deliver persistent workspaces, general workflow recovery, warm pools, snapshots, a credential gateway or model-spend budgets. The default network policy deliberately prevents real provider access until the next controlled-egress layer exists.

SIG Agent Sandbox and Ate remain candidate foundations/backends. Evaluate them with the same workload and failure contracts before implementing their adapters. Keep the backend interface small and change it when real requirements establish a need; avoid promising compatibility the current interface does not provide.

The first implementation has no measured hardware, startup, density or token-efficiency advantage. Optimization work starts with the published evaluation protocol and includes control-plane overhead, idle capacity, failure rates and task output quality.
