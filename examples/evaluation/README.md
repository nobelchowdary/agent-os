# Opt-in execution comparison

This stdlib Python collector measures a bounded deterministic task on an **already prepared** cluster. It installs no runtime, controller, node group or cloud resource. Register the experiment before collecting any arm. Keep the evidence directory outside the checkout; it contains private cluster/context identifiers, hashes and run/object IDs, with directory mode `0700` and files `0600` (`0400` for registration). API tokens are read only from the named environment variable. Raw workload logs, API response bodies, kubectl diagnostics, kubeconfigs and tokens are not retained.

The three implemented arms are:

| Label | What executes | Scope |
| --- | --- | --- |
| `direct-job` | A single active Job | Diagnostic orchestration baseline using the alpha's security/resource profile |
| `agentos` | The registered command submitted to the execution API | Includes durable admission, initial staging, activation and reconciliation |
| `agent-sandbox` | A `SandboxClaim` through a `SandboxWarmPool` with **zero replicas**, using an experiment-owned `SandboxTemplate` | A **cold claim** comparison using the upstream API contract used by the AWS blueprint |

The upstream arm is not a warm-capacity benchmark or the complete AWS Bedrock/FQDN blueprint. It uses the [v1.0.1 claim contract](https://github.com/kubernetes-sigs/agent-sandbox/blob/v1.0.1/extensions/api/v1beta1/sandboxclaim_types.go) and [template contract](https://github.com/kubernetes-sigs/agent-sandbox/blob/v1.0.1/extensions/api/v1beta1/sandboxtemplate_types.go), matching the [pinned AWS blueprint's cold-claim structure](https://github.com/awslabs/ai-on-eks/blob/cfccca1a82071826f38a6308d298010be0a45baa/blueprints/agent-sandbox/basic/sandbox-claim-basic.yaml). The installed controller image must be declared with a digest; this is recorded as an operator declaration, not independently proven by the collector. Prestarting this one-shot task in a nonzero warm pool would finish work before admission and invalidate its timings. A future warm comparison needs a resident execution harness and idle-capacity accounting.

## Prepare administrator inputs

1. Qualify the chosen runtime, nodes and CNI separately. Install the workload ServiceAccount and the `agentos-deny-all` NetworkPolicy in the selected namespace. The collector checks declarations but always reports `qualification.status=not_verified`; dynamic conformance evidence is a separate gate.
2. Prepare the Agent OS API with matching administrator-owned image, resources and runtime profile. The supplied `python-workload.json` matches `config/gvisor.example.json`; `python-nitro-workload.json` matches `config/nitro-isolated.example.json`. Review and replace the digest-pinned image for your architecture as needed **before registration**, updating the API template too. The API command override is used for the exact registered deterministic command.
3. For the upstream arm, install the pinned Agent Sandbox controllers and CRDs separately. The collector creates only its labelled template, zero-capacity pool and sequential claims. Template networking is `Unmanaged` so the same administrator-installed default-deny policy selects every task.
4. Copy `environment.example.json` to a private location and record the actual stack, nodes, versions, cache condition and dynamic qualification reference. Its path and canonical JSON hash are retained; its contents are not copied. Keep the original file with the evidence. Prepare the selected short/minute format-3 measured protocols below, agreeing their criteria before trials. Keep legacy registrations and example files unchanged.

Nitro is an explicit alternative profile: `isolated-sandbox` class/handler, provider network annotation, explicit placement and memory-backed scratch. The tool never substitutes ordinary containers, and does not equate Nitro and gVisor measurements. Use a separate registration per runtime/environment.

## Selected pilot contract

| Profile | Protocol | Workload |
| --- | --- | --- |
| First cluster direct-Job/Agent OS instrument check | `evaluation/decision.job-instrument-check.example.json` | `examples/evaluation/python-short-workload.json` |
| Nominal one-second elapsed tool | `evaluation/decision.short-measured.example.json` | `examples/evaluation/python-short-workload.json` |
| Nominal sixty-second elapsed task | `evaluation/decision.minute-measured.example.json` | `examples/evaluation/python-minute-workload.json` |

Register and report both profiles separately, with at least 30 trials per arm per profile. The caller-outcome allowance remains `candidate p95 <= baseline p95 * 1.10 + 2 seconds`, with the selected boundary, additive allowance and rationale hashed before collection. Format 3 adds a fixed 0.1-second sampling period and a 0.5-second maximum terminal bracket, then applies that rule conservatively to p95 intervals. Missing, coarse or gate-straddling evidence makes latency `not_measurable`. Correctness, dynamic qualification and the 20% cost / 25% median operator-time benefit routes remain required. A latency pass alone proves no value route.

The fixtures use the same pinned image/resources/runtime, deterministic file/hash checks and a bounded wait for their nominal application elapsed duration. They are not CPU-throughput or real-agent benchmarks. Keep the short result visible alongside the minute result; do not pool distributions. [Decision 0009](../../docs/decisions/0009-comparison-boundaries-and-latency-budget.md) records the precollection choice, and [decision 0010](../../docs/decisions/0010-comparison-observation-precision.md) adds precision.

Run the separate [cluster instrument check](../../docs/EVALUATION_RUNBOOK.md#3a-check-the-instrument-on-the-cluster) first. It compares `direct-job` and `agentos` with the short workload. Its `instrument_check` diagnostic reports signed p95-difference bounds and flags a wholly negative interval or an additional-delay lower bound above the registered two-second allowance for review. Inspect bracket coverage, cadence and Pod/outcome decomposition rather than assuming an exact one-second difference. Unexpected ordering or a large delta needs retained investigation; it is not a correction to subtract, proof of an instrument fault or a reason to tune a gate after collection.

## Register, inspect and collect

Use Python 3.10+ on Linux/macOS and a compatible `kubectl` with Unix-socket proxy support on `PATH`. Supply the explicit kubeconfig context on every command. In this example, `/private/agentos-evidence-short-v3` is a new directory under an existing private parent; replace the controller image with the actual digest. Repeat the block for the minute protocol/workload and a different evidence directory from the declared reset/cache state.

```sh
python3 scripts/evaluate.py register \
  --context "$CTX" --namespace agentos-workloads \
  --protocol evaluation/decision.short-measured.example.json \
  --workload examples/evaluation/python-short-workload.json \
  --environment /private/environment.json \
  --evidence-dir /private/agentos-evidence-short-v3 \
  --upstream-controller-ref 'registry.example/controller@sha256:REPLACE_WITH_ACTUAL_DIGEST'

python3 scripts/evaluate.py render --context "$CTX" \
  --evidence-dir /private/agentos-evidence-short-v3 --baseline agent-sandbox

python3 scripts/evaluate.py collect --context "$CTX" \
  --evidence-dir /private/agentos-evidence-short-v3 --baseline agent-sandbox --execute

python3 scripts/evaluate.py collect --context "$CTX" \
  --evidence-dir /private/agentos-evidence-short-v3 --baseline agentos --execute \
  --api-url http://127.0.0.1:8080 --token-env AGENTOS_EVAL_TOKEN

python3 scripts/evaluate.py collect --context "$CTX" \
  --evidence-dir /private/agentos-evidence-short-v3 --baseline direct-job --execute

python3 scripts/evaluate.py cleanup --context "$CTX" \
  --evidence-dir /private/agentos-evidence-short-v3 --execute \
  --api-url http://127.0.0.1:8080 --token-env AGENTOS_EVAL_TOKEN
```

Set `AGENTOS_EVAL_TOKEN` privately before the API arm; do not pass its value on the command line. HTTPS is required for remote API hosts; loopback HTTP supports a local authenticated port-forward. Redirects and proxy environment settings are not used for bearer requests. For the actual initial Agent OS staged Job, call its authenticated `/v1/plan`; offline `render --baseline agentos` is refused.

For the format-3 pilot, use a URL such as `http://127.0.0.1:8080` for that port-forward. Network socket/TLS operations have request deadlines, but the synchronous system hostname resolver is not separately bounded or qualified. Remote HTTPS certificate/hostname validation is preserved.

Registration fixes the requested count, observer timeout, polling interval, cache declaration, protocol, workload and environment hashes. `--trials` can increase the count at registration, never reduce the protocol minimum. The collector refuses to overwrite an existing arm, including failed evidence. Run all requested trials sequentially; trial failures remain in the denominator. Cleanup failures and uncertain submissions stop an arm and leave `completed=false`.

Private evidence is bounded at 2 MiB per file to retain intervals/diagnostics for up to 200 registered trials. Individual Kubernetes/API JSON responses remain capped at 512 KiB; workload logs use their separate 64 KiB bound. Increasing the evidence envelope does not rewrite old registration bytes or relax a latency rule.

The format-3 collection path starts one authenticated `kubectl proxy` on a private Unix socket and prepares persistent Kubernetes/API channels before timing. It uses successful baseline POST responses directly and a separate fixed-cadence sampler for the native outcome. Auxiliary ownership/profile checks and logs remain required but no longer set that sampler's cadence. No credential diagnostics are retained and no public proxy listener is opened. The collector closes its owned channels, proxy and socket after collection. See [official proxy options](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_proxy/). Native submission/admission work remains timed, and reconnect/request delay remains measurement uncertainty.

## Interpret the evidence

Readiness means the client first observed the exact readiness line from the workload. Completion requires successful execution status and **exactly** the two registered output lines in order; the final line includes the expected digest from file readback and deterministic computation. Logs are hashed, not stored. Timings include client calls, polling and the alpha's reconciliation delay. A fast task can become ready and finish between polls, so readiness is an observed upper bound, not an internal process-start timestamp. The current tool does not collect CPU/RAM time series or infer isolated hardware execution.

Every arm also records three distinct events: first observed terminal Pod, native outcome visible to the caller, and validated completion. They carry harness-monotonic elapsed offsets, UTC receipt times, source labels and explicit missing status. Agent OS uses its authenticated terminal API response; direct Jobs use their Job condition; the upstream cold-claim collector uses its selected Pod phase. The underlying guarantees differ, and a Pod phase never substitutes for an Agent OS API outcome. A previously verified execution can already have been cleaned up when its API outcome arrives; its missing Pod observation stays missing. UID/owner/profile drift still fails the trial.

For the selected format-3 outcome, the interval begins at the last nonterminal request's start and ends at the first terminal response's receipt. Both requests' start/receipt pairs are retained. If no nonterminal observation exists, it stays missing and the lower bound explicitly uses the trial origin. Per-arm `latency_observation` reports interval coverage, maximum/p95 gap and p95 lower/upper bounds. Any missing interval or gap above 0.5 seconds prevents a latency decision. With complete narrow intervals, candidate upper p95 at most `baseline lower p95 * 1.10 + 2 seconds` passes; candidate lower p95 above `baseline upper p95 * 1.10 + 2 seconds` fails; overlap remains `not_measurable`. These are conservative observation brackets, not statistical confidence intervals.

Original `elapsed_seconds` retains its validated-output, pre-cleanup meaning. Format-1 protocols continue using that field and the strict ratio; format-2 `*-confirmed.example.json` protocols keep their explicit point-observation rule. Both legacy collection paths remain available for compatibility and are not selected for the new pilot's latency evidence. Another timing field or a supplemental cost/operator record cannot fill a format-3 gap. Use new registration/evidence directories for changed rules, workloads, rationale or observation settings. The old examples remain unchanged.

Format 3 currently gates caller-visible outcome only; other boundary selections are rejected before registration. Pod-terminal and validated-completion observations remain visible diagnostics. Format 2 keeps its three original point-observation choices.

New registrations require explicit `deadline_scope`; an upstream comparison requires `pod` so all three arms set Pod deadlines and no Job deadline. The Pod clock starts at kubelet `Pod.status.startTime`, including image pulls, and excludes scheduling before node acknowledgement. Already registered workloads that omit the field keep their original JSON/hashes and native behavior: Agent OS/direct Jobs use Job clocks, while upstream uses its native Pod clock. Reports disclose this mismatch; it is not matched deadline/failure proof. The current deterministic collector labels fault qualification as not collected, and all arms also have the separately registered client observation timeout. Deadline compatibility, network enforcement and recovery still need dynamic qualification. Cache states are operator declarations; `uncontrolled` is the default and cold/cached samples must not be pooled as if equivalent.

Each trial is saved before submission and after its outcome, including errors. Ambiguous API submissions retry only the **same** persisted idempotency key. An unresolved submission or lost Kubernetes creation response can commit late; the tool stops, records uncertainty and never treats a currently absent object as proof the submission failed. Cleanup cancels only recorded API run IDs, or deletes the exact experiment-labelled object with UID/resourceVersion preconditions. It checks observed descendant UIDs disappear and never force-deletes Pods. API records remain durably retained. Object disappearance cannot prove physical termination of a partitioned worker. Unknown submissions and orphaned descendants need operator reconciliation before another experiment on that capacity.

If the collector is killed without cleanup, its private `.collect.lock` prevents concurrent mutation. Confirm that process has stopped before manually removing the stale lock and invoking cleanup. A cleanup invocation does not make an incomplete collection complete, or resolve an unknown submission merely because nothing is currently visible.

Costs are deliberately absent: `billing_complete=false` and `node_reduction_demonstrated=false`. Supply complete actual billing evidence separately to the decision report. A fixed provisioned node pool does not save EC2 dollars when task resource use falls. Offline fake/HTTP tests exercise the tooling contract and produce no benchmark, conformance or cost claims.

## Observe node CPU and memory

Run the read-only sampler in another terminal across each collection window, using the actual runtime-eligible node selector and a new private report path for each arm:

```sh
python3 scripts/sample_resources.py --context "$CTX" \
  --node-selector 'agent-sandbox/runtime=gvisor' \
  --experiment-id eks-short-measured-v3 \
  --duration-seconds 120 --interval-seconds 5 \
  --output /private/agentos-evidence-short-v3/agentos-node-samples.json --execute
```

For the Nitro profile, select `eks.amazonaws.com/nitro-sandbox=true`. The selector must be explicit and supports comma-separated `key=value` terms. This script runs only Kubernetes `get` requests. It requires permission to read Node objects and the `metrics.k8s.io/v1beta1` NodeMetrics API; it does not install Metrics Server or modify the cluster. Missing, stale, invalid or unavailable metrics produce failed evidence and a nonzero exit, never substituted zero usage. Output files are private and cannot overwrite a prior report.

The report preserves selected node UIDs, allocatable CPU/memory, observation times and the metrics server's timestamp/window for every value. A selected node replacement or allocatable change fails the affected sample. CPU is a window observation, and memory is working set, as defined by the [Metrics API](https://kubernetes.io/docs/reference/external-api/metrics.v1beta1/). Repeated server timestamps are flagged: frequent client polls do not create independent measurements. Match the recorded server windows to the trial interval before interpreting overlap. The sampler includes system services, warm capacity and unrelated work on the selected nodes; it does not attribute usage to individual tasks, integrate CPU time, aggregate Pod requests or infer billing savings. Keep control-plane/gateway nodes and other accounting components as separate declared scopes when they do not match the workload node selector.

The observation window is bounded to 1–600 seconds, with at most 120 scheduled samples and 512 node/sample combinations. Requests and output are bounded; the initial inventory can add up to ten seconds before that window. Missed cadence is recorded as incomplete rather than filled with a burst of late samples. Use longer runs or a dedicated monitoring system for sustained fleet tests.
