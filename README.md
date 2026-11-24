# Agent OS

**An experimental sandbox environment for running agent work on Kubernetes.**

Agent OS is being built to give coding agents, research agents, and workflow harnesses a simple, controlled execution environment with predictable resource use and explicit failure handling. The long-term measure is useful work completed reliably per unit of compute and model spend.

This first alpha provides a runnable execution API and constrained Python/Node environments. It is not a Linux distribution, a complete agent framework, or a production-qualified isolation service. “Agent OS” remains a working name; [Agno also uses AgentOS](https://docs.agno.com/agent-os/introduction).

## Available in this version

- Administrator-owned environments with digest-pinned images and fixed CPU, RAM, scratch-space and execution-time settings. New runs default to a persisted Pod clock; legacy records retain their Job clock.
- Kubernetes Jobs with an administrator-selected gVisor or AWS Nitro isolated profile, non-root processes, read-only root filesystems, dropped capabilities and no workload Kubernetes token. Real kind/gVisor lifecycle checks have passed; EKS and Nitro remain unqualified.
- Bearer-token authentication with tenant-scoped create, list, status, bounded logs and cancellation.
- Durable run records, lifecycle events and bounded exit/output results using transactional embedded storage.
- Structured outcome/cleanup codes, versioned metadata storage and drained offline check/backup/restore commands.
- Restore-aware live lifecycle events, epoch-bound tenant-scoped history pagination and operational status.
- Controller wakeups on committed admission/cancellation, failure/recovery health and graceful stream shutdown.
- A standard-library Python client and an external harness example for submitting and monitoring tools.
- Atomic tenant/global admission limits and persistent idempotency keys for duplicate submissions.
- Suspended Job creation followed by separate activation; ambiguous initial creation does not immediately launch code.
- Offline environment preview, deployment manifests, a small workload example set and CI checks.
- Opt-in cluster preflight, registered comparison tooling, node resource sampling, operator-effort journaling and decision reports. Cloud resources are never provisioned by these tools.

The node runtime and enforcing CNI must be installed and independently qualified. A declared RuntimeClass or NetworkPolicy does not prove isolation. Disk limits depend on kubelet accounting/eviction; PID limits require node configuration. Kubernetes execution can duplicate work under failures, so external effects still need idempotency.

## Try the environment preview

Use Go 1.27.4, or allow the Go toolchain download selected by `go.mod`. From the repository root:

```sh
export AGENTOS_LOCAL_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
make check
make preview
```

Preview binds to `127.0.0.1:8080`. It renders the exact initial Job through `POST /v1/plan`; it never executes submitted code or cancels existing runs. See the [getting-started guide](docs/GETTING_STARTED.md) for authenticated requests and installation on a prepared cluster.

```json
{"template":"python","command":["python","-c","print(sum(range(1000)))"],"timeout_seconds":30}
```

Submit this shape to `POST /v1/runs` in cluster execution mode. Image, resource limits, mounts, credentials and Pod specifications are not caller-controlled. The optional command executes within the selected environment's restrictions.

## Current boundaries

The API runs as one process/replica with a private bbolt database. Workload files are ephemeral; bounded output snapshots have administrator-controlled retention. Failed application work is not automatically retried. Cancellation is asynchronous; disappearing Kubernetes objects cannot prove a partitioned worker physically stopped. Finished Jobs and late creations for known terminal runs are cleaned up after result persistence; unknown labeled Jobs are reported for operator review.

Workload networking is denied by default; the Nitro profile also requests a NIC-free guest and uses memory-backed scratch. Its trusted-parent/vsock and memory limits require separate qualification. A model/tool gateway, provider-spend enforcement, durable workspaces, integrated checkpoint recovery and Agent OS warm pools are future work. The [historical Job-clock acceptance report](docs/robustness/LOCAL_ACCEPTANCE_7d4df23.md) covers real lifecycle/failure execution, selected controlled network denials and measured 16/64 concurrency. The [November 24 review](docs/robustness/REPORTING_REVIEW_2026-11-24.md) records staged reporting measurements, the Pod-clock change and its separate validation status. These results do not qualify EKS, comprehensive isolation or production operation. No token savings, cost or density advantage is claimed.

## Direction and contribution

- [Getting started](docs/GETTING_STARTED.md): preview, cluster prerequisites, API examples and operational limits.
- [Launch and live API](docs/LIVE_API.md): agent-harness integration, event replay, health and pagination; [Python client](sdk/python/README.md).
- [API contract](docs/API_CONTRACT.md): embedded OpenAPI, stable error codes and request compatibility.
- [Results and cleanup](docs/RESULTS_AND_CLEANUP.md): retained exit/output data, retention limits, terminal sweeps and administrator request/startup settings.
- [Outcome codes](docs/OUTCOME_CODES.md) and [offline storage recovery](docs/STORAGE_RECOVERY.md): cause evidence, legacy compatibility and maintenance limits.
- [Real kind/gVisor checks](docs/KIND_TESTS.md): repeatable kubelet lifecycle and failure campaign; inspect the actual CI outcome.
- [Robustness work](docs/robustness/README.md): the supplied plan, audit of all 70 requirements and phase 0 gates.
- [Architecture decision](docs/decisions/0001-initial-execution-slice.md): the first backend and its scope.
- [Roadmap](docs/ROADMAP.md): capability gates driven by workload evidence and customer needs.
- [Alpha capability map](docs/ALPHA_CAPABILITY_MAP.md), [EKS evaluation brief](docs/EKS_EVALUATION_BRIEF.md), and [product-direction review](docs/PRODUCT_DIRECTION_REVIEW.md): current evidence and experiments before choosing a broader product.
- [Evaluation runbook](docs/EVALUATION_RUNBOOK.md): executable preflight, comparison, operator records and preregistered decision gates; [runtime profile decision](docs/decisions/0002-runtime-profiles-and-evidence.md).
- [Original research and product proposal](docs/PROJECT_PLAN.md) and [evaluation plan](docs/EVALUATION_PLAN.md).
- [Contributing](CONTRIBUTING.md) and [security reporting](SECURITY.md).

Feature proposals should identify a real workload, the current failure or cost, and a measurable acceptance condition. Build a shared execution contract while validating each workload profile independently.

Licensed under [Apache-2.0](LICENSE).
