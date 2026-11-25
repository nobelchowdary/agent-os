# Agent OS: evaluation plan

Status: performance experiments with founder decisions updated November 24, 2026. Execution, comparison and compatibility-smoke tooling exists. The [real kind/gVisor lifecycle campaign](KIND_TESTS.md) has passed; comparative performance, EKS qualification and cost savings remain unmeasured. The executable workflow is in [EVALUATION_RUNBOOK.md](EVALUATION_RUNBOOK.md).

Before the EKS comparison, the founder selected the existing executable benefit gate: **20% lower fully accounted cost per correct task OR 25% less median operator time**. Operator steps remain a reported diagnostic, not a 50% acceptance gate. Keep correctness, trial-count and latency requirements in the registered protocol; do not reinterpret the exploratory targets below as alternative pass rules. Future registrations use the agreed Pod deadline scope, while historical admitted runs retain their stored Job clock; [decision 0007](decisions/0007-persisted-execution-deadline-scope.md) defines the compatibility boundary. No existing registration is rewritten by these decisions.

For the next pilot the founder selected **two separate nominal elapsed profiles: one second and sixty seconds**, with both retained in the result. Caller-visible native outcome latency keeps the allowance `candidate p95 <= baseline p95 * 1.10 + 2 seconds`. New format-3 protocols add a shared authenticated transport, fixed 0.1-second observation cadence and 0.5-second maximum terminal bracket before applying that rule conservatively to p95 intervals. Missing, coarse or gate-straddling measurements are `not_measurable`. This precision policy and the additive allowance are fixed before collection; previous diagnostics and format-1/format-2 registrations retain their original rules. The collector also reports terminal-Pod and validated-completion boundaries with their differing native guarantees. [Decision 0009](decisions/0009-comparison-boundaries-and-latency-budget.md) defines the boundaries; [decision 0010](decisions/0010-comparison-observation-precision.md) defines precision.

November 19 update: EKS is the first available pilot environment. Follow the [EKS evaluation brief](EKS_EVALUATION_BRIEF.md) and [current capability map](ALPHA_CAPABILITY_MAP.md). First qualify and measure existing execution; compare upstream implementations before building idle-memory recovery or another policy gateway.

## 1. Unit of value

Optimize **cost per successfully completed, policy-compliant task at an agreed latency and recovery target**. Also report failure rate, recovery behavior and operational overhead. A large count of registered or sleeping agents is not evidence of equivalent concurrent execution capacity.

Hold task inputs, artifacts, model configuration, output checks and isolation requirements constant. Reject a claimed efficiency improvement if it comes from omitting work, weakening isolation, or moving costs outside the accounting boundary.

## 2. Baselines

Evaluate the customer's existing deployment plus these reproducible configurations where supported:

| Baseline | Purpose |
| --- | --- |
| Ordinary Kubernetes containers | Establish a performance floor; label the different isolation boundary explicitly |
| Plain gVisor on Kubernetes | Quantify sandbox overhead separately from product overhead |
| AWS EKS Agent Sandbox blueprint, with upstream gVisor controller and separately declared cold/warm configurations | Primary upstream reference; include preparation and idle warm-capacity cost. The current collector implements cold claims only |
| AWS Nitro isolated sandbox profile | Explicit hardware-isolation arm with its own placement, scratch and network contract; never silently relabel it gVisor |
| Agent Substrate (Ate), using its documented gVisor worker configuration | Test existing actor/worker pooling and checkpoint/restore on EKS; do not assume non-GKE memory recovery is absent |
| OpenShell on Kubernetes / Agent Sandbox | Compare policy, credential and operational behavior; distinguish lifecycle APIs from operations implemented by the selected driver |
| Proposed Agent OS configuration | Measure the selected product design and every enabled control |

Managed services can be a separate operational/cost comparison when equivalent workloads and billing evidence are available. Do not pretend different isolation technologies or service responsibilities are identical.

Pin source commits/releases, OCI image digests, Kubernetes, kernel, CNI, runtime and configuration versions. Record node type/count, architecture, storage, region, limits, security policies, active ratio, dataset size, cache state and warm capacity. Document features unavailable on a baseline rather than silently approximating them.

Start on EKS with one qualified configuration. Add GKE later to compare provider-specific snapshot facilities; report provider support separately from portable runtime mechanisms. Reproduce any claimed Substrate recovery or efficiency result on the selected EKS stack. OpenShell is also a proposed integration for the control experiment, not an integration already implemented by this repository.

## 3. Workload matrix

1. **Short tools:** deterministic Python and Node commands, small files and many independent tasks.
2. **Coding workspace:** repository checkout, dependency installation from controlled sources, edits and tests; measure a cached and uncached environment.
3. **Research:** concurrent retrieval, bounded fan-out and retained evidence; add browser execution only after qualifying its sandbox and network behavior.
4. **Durable workflow:** multiple steps separated by human/external-event waits, then a worker failure and restart from a declared checkpoint.
5. **Adverse mix:** CPU-heavy compilation, syscall/file-heavy work, large artifacts and one tenant submitting a burst while another has a small steady workload.

Use a deterministic mock model to isolate infrastructure effects. Run separate real-model evaluations with pinned configurations, repeated trials and output checks. Report provider latency/rate limiting separately from execution overhead. Mark non-deterministic quality differences instead of hiding them in averages.

### Next comparison campaign

First run the registered **direct Job versus Agent OS instrument check**, using the short fixture on the same qualified gVisor configuration. Inspect request intervals, missed cadence and the Pod/outcome decomposition; an unexpectedly negative or large delta needs investigation, not an assumed exact one-second correction. Then run **Agent OS versus upstream Agent Sandbox on the AWS reference deployment**. This compares our execution and operating contracts while keeping the underlying sandbox comparable. Evaluate Substrate, OpenShell and Nitro in later, separately qualified arms where their execution and policy contracts fit; the existing collectors do not automate those comparisons.

| Step | Work to compare | Evidence to collect |
| --- | --- | --- |
| 0. Instrument check | Direct Job versus Agent OS, short fixture, shared transport and the same registered cadence | Native-outcome request brackets, maximum/p95 gap, p95 interval bounds, missed cadence, submission/Pod/outcome decomposition and retained anomalies |
| 1. Execution baseline | Separate one-second and sixty-second elapsed deterministic cold-claim fixtures, with cold-image and cached-image trials registered separately | Correct completion rate, admission-to-readiness, terminal-Pod/native caller-outcome/validated-completion latency, resource observations and all failed trials |
| 2. Failure and operations | Equivalent task outcomes under API restart, worker loss, deadline and cancellation; installation, upgrade and cleanup from comparable starting states | Lost or duplicated work, recovery/stop time, retained results, leaked resources, operator time and steps |
| 3. Real agent work | One coding task set with test-based grading, then one research task set with a frozen evidence/citation rubric | Task quality, total time and cost per correct task, model usage and retry waste; extend the harness before collecting these results |
| 4. Fleet economics | The measured task mix at declared concurrency and active/idle ratios, followed by a capacity-reduction experiment | Correct tasks per node-hour, tail latency, fairness, idle cost and actual fully accounted billing |

Steps 1 and 2 begin only after EKS runtime and network-policy qualification. Reuse the [registration, resource sampler and operator journal](EVALUATION_RUNBOOK.md); the kind lifecycle suite supplies functional cases, not a competitor performance result. The current collector automates only the deterministic cold execution comparison. Fault adapters for other products, resident warm-pool dispatch, real-agent grading and provider-usage collection still need implementation. Record unsupported cases explicitly.

For real-agent trials, keep the agent harness/version, prompts, tools, repository/data snapshot, model/version, sampling settings and context strategy fixed across arms. Use repeated paired tasks and counterbalance arm order to limit cache, provider-load and learning effects. Record provider request IDs privately to reconcile retries and usage without exposing credentials or sensitive prompts.

Report input, output and cached-token usage separately, including failed and repeated attempts in the numerator for **tokens per correctly completed task**. Report the fraction spent on work discarded after infrastructure failures, with an explicit attribution rule. A runtime might reduce that waste; fewer reasoning tokens or better answers require separate evidence. The existing decision tool does not ingest token or task-rubric scores: retain them in a versioned supplemental report until a new protocol supports them.

Use the named short/minute format-3 measured protocols for the selected next comparison. Keep existing registrations and legacy examples unchanged: format 1 keeps its strict ratio and original metric, while format 2 keeps its registered point-observation rule. Neither legacy collection path is the selected instrument for the new pilot. Synthetic wait durations probe elapsed overhead, not CPU efficiency or real-agent task quality. Publish each profile, regressions and ties separately, with sample counts, bracket coverage and uncertainty. The exploratory targets below are not substitutes for a registered decision rule.

## 4. Initial engineering gates

| Measurement | Proposed first gate | Required reporting |
| --- | --- | --- |
| Warm task readiness | p95 ≤2 seconds for a declared cached image and ready capacity | Authenticated admission to first successful application command; p50/p95/p99, samples, errors and pool size |
| Cold task readiness | Establish baseline before setting a target | Queueing, image pull, scheduling, sandbox boot, application initialization and first useful result separately |
| Useful infrastructure cost | Seek ≥30% improvement over the partner's existing idle-heavy deployment | Same workload/SLO/isolation; include warm/idle nodes, control plane, gateways, storage, network and failures; report model bills separately and in total |
| Mixed fleet | 1,000 registered sessions, 100 active at the declared resource size, for 24 hours | Queue age, throughput, latency, resource pressure, accepted-work accounting and artifacts |
| Tenant controls | No unauthorized access in the defined conformance cases | Exact cases and environment; passing tests do not prove absence of vulnerabilities |
| Budget admission | Concurrent requests and child runs cannot exceed the reserved allowance under the declared adapter contract | Reservations, release/reconciliation, retries, provider uncertainty and outage behavior |
| Recovery | Every injected failure yields a documented terminal or recoverable state | Last durable checkpoint, lost progress, uncertain external actions and measured recovery time |

The cost gate may be unattainable against an already optimized baseline. Report that result and reconsider the efficiency pitch. A product can create operational value without winning every microbenchmark.

Use repeated trials, retain raw measurements and publish scripts/configuration. Keep warm and cold distributions separate. Choose sample sizes sufficient for tail estimates, disclose them, and avoid presenting a p99 from a tiny sample as stable. Never turn one successful run into a scale claim.

## 5. Fault and lifecycle experiments

For each exercise, define expected behavior before running it. Inject failures at important transitions: admission acknowledgment, budget reservation, tool submission, external success before acknowledgment, checkpoint commit and compute release.

| Fault | Expected property |
| --- | --- |
| Worker disappears | Accepted work remains accounted for; recovery uses only a committed checkpoint; stale effects are denied |
| Controller restarts or two reconcilers race | No duplicate authoritative owner; state transitions and budget reservations remain consistent |
| Network partition | Lease expiry alone is not treated as proof the old executor stopped; the gateway rejects stale actions, and writer fencing or isolated per-attempt storage prevents concurrent workspace corruption |
| Database, policy or gateway outage | New privileged actions fail closed; bounded local deadlines continue; cancellation and status degrade explicitly |
| Object store unavailable / partial checkpoint | Incomplete state is never advertised as recoverable; compute is released only after durable commitment |
| Disk, log or artifact quota exhausted | Controlled failure and cleanup; no node-wide exhaustion caused by unbounded output |
| Provider 429 / timeout / lost acknowledgment | Bounded retries, no blind repetition of non-idempotent actions, visible uncertain outcome and reconciled budget |
| Cancellation / permission revocation | New operations are denied; execution stops within the declared bound; already submitted actions are reported |
| Backup restore / version upgrade | Supported state remains readable; incompatible state is rejected with an actionable status |

Observe CPU/RAM/disk on nodes and trusted components during these exercises. Warm pools and caches must not retain a previous tenant's files, credentials or process state for reassignment.

## 6. Boundary and compatibility checks

Verify the installed boundary rather than the presence of YAML resources:

- Runtime handler selected correctly; unavailable gVisor never falls back to an ordinary container.
- Workload inputs cannot bypass administrator-owned templates, request privileged execution or host namespaces, mount host paths/runtime sockets, attach arbitrary devices, or obtain an automatic Kubernetes service-account token.
- Tenant cannot read another tenant's API objects, logs, artifacts, workspace, checkpoint or broker capability.
- Direct egress, DNS bypass, IPv4/IPv6 paths, metadata, host services, cluster API and peer access match the declared policy on the actual CNI.
- Unsupported protocols, redirects, destination changes and broker outages cannot bypass the gateway contract.
- Agents cannot supply trusted lineage, enlarge delegated permissions, reuse approvals for different arguments or replay stale attempt tokens.
- Concurrent model calls and nested agents share the same applicable budget; accounting failures cannot authorize unlimited new spend.
- Restoring a checkpoint rechecks current identity/policy, handles expired capabilities and rejects unauthorized or incompatible state.
- Python/Node tools, package managers and chosen agent integrations work under the required filesystem and network restrictions.

These checks supplement review of trusted gateways and runtime dependencies. An independent security review is a beta gate. Never publish claims such as “no sandbox escapes” based on a conformance suite.

## 7. Evidence required before a public claim

Each published result includes the task, baseline, complete accounting boundary, versions, hardware, security configuration, active/idle ratio, latency distribution, completion rate and a reproducible command or script. Distinguish observed values, estimates and future goals.

Before a pilot, agree with the customer on the failure/recovery contract and target metrics. Before a production SLO, obtain sustained operational data and assign incident response ownership. Before marketing portable memory snapshots, qualify each supported runtime/CPU/network/storage combination.

Relevant primary references: [Ate architecture](https://ate.dev/docs/concepts/architecture/), [SIG Agent Sandbox](https://github.com/kubernetes-sigs/agent-sandbox), [gVisor performance](https://gvisor.dev/docs/architecture_guide/performance/), [Kubernetes RuntimeClass](https://kubernetes.io/docs/concepts/containers/runtime-class/), [NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/).
