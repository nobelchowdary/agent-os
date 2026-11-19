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
4. Copy `environment.example.json` to a private location and record the actual stack, nodes, versions, cache condition and dynamic qualification reference. Its path and canonical JSON hash are retained; its contents are not copied. Keep the original file with the evidence. Prepare the decision protocol at `evaluation/decision.example.json`, agreeing its criteria before trials.

Nitro is an explicit alternative profile: `isolated-sandbox` class/handler, provider network annotation, explicit placement and memory-backed scratch. The tool never substitutes ordinary containers, and does not equate Nitro and gVisor measurements. Use a separate registration per runtime/environment.

## Register, inspect and collect

Use Python 3.10+ on Linux/macOS and a compatible `kubectl` on `PATH`. Supply the explicit kubeconfig context on every command. In this example, `/private/agentos-evidence` is a new directory under an existing private parent; replace the controller image with the actual digest.

```sh
python3 scripts/evaluate.py register \
  --context "$CTX" --namespace agentos-workloads \
  --protocol evaluation/decision.example.json \
  --workload examples/evaluation/python-workload.json \
  --environment /private/environment.json \
  --evidence-dir /private/agentos-evidence \
  --upstream-controller-ref 'registry.example/controller@sha256:REPLACE_WITH_ACTUAL_DIGEST'

python3 scripts/evaluate.py render --context "$CTX" \
  --evidence-dir /private/agentos-evidence --baseline agent-sandbox

python3 scripts/evaluate.py collect --context "$CTX" \
  --evidence-dir /private/agentos-evidence --baseline agent-sandbox --execute

python3 scripts/evaluate.py collect --context "$CTX" \
  --evidence-dir /private/agentos-evidence --baseline agentos --execute \
  --api-url http://127.0.0.1:8080 --token-env AGENTOS_EVAL_TOKEN

python3 scripts/evaluate.py collect --context "$CTX" \
  --evidence-dir /private/agentos-evidence --baseline direct-job --execute

python3 scripts/evaluate.py cleanup --context "$CTX" \
  --evidence-dir /private/agentos-evidence --execute \
  --api-url http://127.0.0.1:8080 --token-env AGENTOS_EVAL_TOKEN
```

Set `AGENTOS_EVAL_TOKEN` privately before the API arm; do not pass its value on the command line. HTTPS is required for remote API hosts; loopback HTTP supports a local authenticated port-forward. Redirects and proxy environment settings are not used for bearer requests. For the actual initial Agent OS staged Job, call its authenticated `/v1/plan`; offline `render --baseline agentos` is refused.

Registration fixes the requested count, observer timeout, polling interval, cache declaration, protocol, workload and environment hashes. `--trials` can increase the count at registration, never reduce the protocol minimum. The collector refuses to overwrite an existing arm, including failed evidence. Run all requested trials sequentially; trial failures remain in the denominator. Cleanup failures and uncertain submissions stop an arm and leave `completed=false`.

## Interpret the evidence

Readiness means the client first observed the exact readiness line from the workload. Completion requires successful execution status and **exactly** the two registered output lines in order; the final line includes the expected digest from file readback and deterministic computation. Logs are hashed, not stored. Timings include client calls, polling and the alpha's reconciliation delay. A fast task can become ready and finish between polls, so readiness is an observed upper bound, not an internal process-start timestamp. The current tool does not collect CPU/RAM time series or infer isolated hardware execution.

The direct/API arms use Job deadlines; the upstream arm additionally sets a Pod execution deadline. Kubernetes controllers apply these at different lifecycle boundaries. All arms also have the separately registered client observation timeout. Deadline compatibility, network enforcement and recovery still need dynamic qualification. Cache states are operator declarations; `uncontrolled` is the default and cold/cached samples must not be pooled as if equivalent.

Each trial is saved before submission and after its outcome, including errors. Ambiguous API submissions retry only the **same** persisted idempotency key. An unresolved submission or lost Kubernetes creation response can commit late; the tool stops, records uncertainty and never treats a currently absent object as proof the submission failed. Cleanup cancels only recorded API run IDs, or deletes the exact experiment-labelled object with UID/resourceVersion preconditions. It checks observed descendant UIDs disappear and never force-deletes Pods. API records remain durably retained. Object disappearance cannot prove physical termination of a partitioned worker. Unknown submissions and orphaned descendants need operator reconciliation before another experiment on that capacity.

If the collector is killed without cleanup, its private `.collect.lock` prevents concurrent mutation. Confirm that process has stopped before manually removing the stale lock and invoking cleanup. A cleanup invocation does not make an incomplete collection complete, or resolve an unknown submission merely because nothing is currently visible.

Costs are deliberately absent: `billing_complete=false` and `node_reduction_demonstrated=false`. Supply complete actual billing evidence separately to the decision report. A fixed provisioned node pool does not save EC2 dollars when task resource use falls. Offline fake/HTTP tests exercise the tooling contract and produce no benchmark, conformance or cost claims.

## Observe node CPU and memory

Run the read-only sampler in another terminal across each collection window, using the actual runtime-eligible node selector and a new private report path for each arm:

```sh
python3 scripts/sample_resources.py --context "$CTX" \
  --node-selector 'agent-sandbox/runtime=gvisor' \
  --experiment-id eks-gvisor-001 \
  --duration-seconds 120 --interval-seconds 5 \
  --output /private/agentos-evidence/agentos-node-samples.json --execute
```

For the Nitro profile, select `eks.amazonaws.com/nitro-sandbox=true`. The selector must be explicit and supports comma-separated `key=value` terms. This script runs only Kubernetes `get` requests. It requires permission to read Node objects and the `metrics.k8s.io/v1beta1` NodeMetrics API; it does not install Metrics Server or modify the cluster. Missing, stale, invalid or unavailable metrics produce failed evidence and a nonzero exit, never substituted zero usage. Output files are private and cannot overwrite a prior report.

The report preserves selected node UIDs, allocatable CPU/memory, observation times and the metrics server's timestamp/window for every value. A selected node replacement or allocatable change fails the affected sample. CPU is a window observation, and memory is working set, as defined by the [Metrics API](https://kubernetes.io/docs/reference/external-api/metrics.v1beta1/). Repeated server timestamps are flagged: frequent client polls do not create independent measurements. Match the recorded server windows to the trial interval before interpreting overlap. The sampler includes system services, warm capacity and unrelated work on the selected nodes; it does not attribute usage to individual tasks, integrate CPU time, aggregate Pod requests or infer billing savings. Keep control-plane/gateway nodes and other accounting components as separate declared scopes when they do not match the workload node selector.

The observation window is bounded to 1–600 seconds, with at most 120 scheduled samples and 512 node/sample combinations. Requests and output are bounded; the initial inventory can add up to ten seconds before that window. Missed cadence is recorded as incomplete rather than filled with a burst of late samples. Use longer runs or a dedicated monitoring system for sustained fleet tests.
