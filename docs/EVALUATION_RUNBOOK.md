# Run the EKS evidence milestone

These tools are implemented; live EKS compatibility and comparative performance results do not exist yet. `make check` covers code contracts and fake-client evidence flows. Every cluster-mutating collector requires explicit context and `--execute`; no tool provisions AWS infrastructure. Keep raw reports private under ignored `evidence/`.

## Billing preparation before the pilot

Set up cost evidence before collecting either arm. The selected gate remains 20% lower fully accounted cost per correct task or 25% less median operator time. SCAD is the AWS route for attributing shared EC2 compute to Pods; it does not replace full experiment accounting or the separate operating-effort route.

1. Have the management/standalone account owner opt in to EKS split cost allocation data in Cost Management preferences, then include it and resource IDs in a legacy CUR or CUR 2.0 Data Export. Use hourly granularity and retain the export definition, report manifests and source objects/query version. SCAD is not in Cost Explorer. Select one measurement basis before registration: CPU/memory requests, or a supported utilization source configured to AWS's requirements. The collector's node sampler does not supply SCAD telemetry. [AWS opt-in](https://docs.aws.amazon.com/cur/latest/userguide/enabling-split-cost-allocation-data.html), [export settings](https://docs.aws.amazon.com/cur/latest/userguide/dataexports-create-standard.html)
2. Declare the attribution plan before the measured window. Kubernetes Pod labels can be imported as user-defined cost allocation tags, but require payer/management-account activation. AWS imports at most the first 50 labels alphabetically; verify the required keys, including `agentos.dev/run-id` when used, are present and active. AWS allows up to 24 hours for label-key discovery and another 24 hours after activation, so label readiness can exceed one day. Keep secrets and personal data out of labels. [AWS label behavior](https://docs.aws.amazon.com/cur/latest/userguide/split-cost-allocation-data-kubernetes-labels.html)
3. Verify the **actual Pods**, not only their parent labels. Direct-Job Pods carry `agentos.dev/evaluation` and `agentos.dev/evaluation-trial`. The upstream template carries `agentos.dev/evaluation`; verify any trial-label propagation from SandboxClaims instead of assuming it. Agent OS Pods carry `agentos.dev/run-id`, while `tag_api_job` adds the evaluation label only to Job metadata. No common arm label is currently emitted. Retain each arm's report, `run_id`, `resources`, Pod `descendants` UID/name and owner chain, the registration's namespace, and a private record of the accounting window. Prove how delivered billing resource IDs/tags join to them. If an operator adds cost labels, record and qualify that setup before registration; do not change execution/security fields or add labels only after short tasks finish.
4. Run a short canary for each intended attribution path. AWS documents up to 24 hours for SCAD data to become visible; Data Exports can also take up to 24 hours to start delivery. Wait for delivered rows covering the canary's actual resource identities and usage window, with usable tags and parent EC2 IDs. Preserve the row-to-Pod-to-arm mapping. Missing or ambiguous short-task rows are an accounting gap, not zero cost. [SCAD delivery](https://docs.aws.amazon.com/cur/latest/userguide/enabling-split-cost-allocation-data.html), [Data Exports delivery](https://docs.aws.amazon.com/cur/latest/userguide/dataexports-create.html)

Before registration, freeze the currency, cost basis/discount treatment, accounting interval, correct-task denominator and allocation of idle/shared resources. The billing ledger must include failed work and time before/after execution while resources remain provisioned, control-plane/API/controller overhead, warm/unused nodes, EBS/snapshots, NAT/data transfer/address costs, registry and telemetry/export/query costs. Resource-request allocations are not measurements of actual CPU efficiency. If using utilization-based SCAD, include the configured telemetry service's charges. [CloudWatch telemetry requirements](https://docs.aws.amazon.com/cur/latest/userguide/split-cost-allocation-data-cloudwatch.html)

Reconcile Pod `split` and `unused` costs with their parent EC2 charges using a consistent net/amortized basis. Do not add the parent EC2 charge again on top of its allocated children, and do not discard unattributed costs. Reducing a Pod's assigned share on the same fixed node fleet is not a reduction in total provisioned cost. A reviewed non-SCAD ledger is acceptable if it provides equally complete attribution and billing coverage; the decision tool does not mandate a particular AWS export. [SCAD field definitions](https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-split-line-item.html)

No billing preference, export, label or telemetry service is configured by these tools. Keep raw account/resource IDs private. Leave `billing_complete=false` until the complete cost window and all required allocations are reconciled; only then supply the reviewed ledger through `cost_accounting_ref` and `full_cost_usd` in the existing supplemental sidecar.

## 1. Choose and qualify the runtime

Use [AWS's Agent Sandbox blueprint](https://awslabs.github.io/ai-on-eks/docs/infra/agents/agent-sandbox) as the upstream deployment reference. Its infrastructure, controller, warm capacity and egress components count toward setup effort and cost. The [conformance guide](../examples/conformance/README.md) records pinned source references and commands for the declaration inventory and Substrate smoke.

The current `qualify_cluster.py` inventory supports Standard EKS/gVisor only. It reports `NOT_QUALIFIED` even when all declarations match. It does not run packet-level isolation tests. Native VPC CNI startup enforcement and a chained Cilium configuration are distinct profiles. Match the runtime, node labels, policies and resources to the installation being tested.

The application additionally implements an offline Nitro isolated profile through `config/nitro-isolated.example.json` and `overlays/nitro`. It is not a one-field gVisor substitution: network-device removal, scratch backing, node placement, guest overhead and RBAC differ. Nitro needs its own live qualification, supported AWS infrastructure and experiment registration. See [installation](GETTING_STARTED.md#aws-nitro-isolated-profile).

## 2. Test Substrate continuity before comparative performance

Use the [dedicated actor fixture and probe](../examples/conformance/README.md#dedicated-substrate-continuity-smoke) on an operator-prepared Substrate installation. It checks memory/file continuity through synchronous suspend/resume and observes durable snapshot state and released control-plane assignment. It does not establish physical memory reclamation, automatic idling or node-cost savings.

Then extend the live experiment to restore onto a different eligible worker/instance type, retain open connections at checkpoint, expire credentials and observe a controlled external write made before suspension. Report unsupported CPU/runtime combinations and broken connections. External actions remain external; neither the fixture nor a snapshot rolls them back. The initial helper does not automate disruptive worker replacement or external writes.

## 3. Register the comparison before collection

Copy `examples/evaluation/environment.example.json` outside Git and replace its placeholders with the actual private environment record. The selected next pilot uses separate `short` and `minute` profiles and new format-3 protocols: 30 trials per arm per profile, 100% deterministic correctness, caller-visible outcome p95 bounded by `baseline p95 * 1.10 + 2 seconds`, and either 20% lower fully accounted cost or 25% less median operator time. Format 3 also freezes the 0.1-second sampling cadence and 0.5-second maximum terminal-observation bracket, then applies the latency rule conservatively to p95 intervals. The additive allowance is a precollection founder decision for a confirmed native outcome, with its rationale in the hashed policy. These are pilot gates, not achieved results or commercial commitments. Review them with the partner before registration; retain them after collection begins. [Decision 0009](decisions/0009-comparison-boundaries-and-latency-budget.md) explains the boundaries; [decision 0010](decisions/0010-comparison-observation-precision.md) adds the precision contract.

```sh
mkdir -p evidence
for profile in short minute; do
  python3 scripts/evaluate.py register \
    --context YOUR_EXPLICIT_CONTEXT --namespace agentos-workloads \
    --protocol "evaluation/decision.${profile}-measured.example.json" \
    --workload "examples/evaluation/python-${profile}-workload.json" \
    --environment /absolute/private/environment.json \
    --evidence-dir "evidence/eks-${profile}-measured-v3" \
    --upstream-controller-ref 'YOUR_REGISTRY/controller@sha256:REPLACE_WITH_ACTUAL_64_HEX_DIGEST'
done
```

The new commands include deterministic file/hash work and a bounded wait establishing one or sixty seconds of minimum application elapsed time. They share the pinned image, resources and sandbox policy. They do not model one/sixty seconds of CPU demand or a real agent. Run both profile/arm combinations from the declared reset/cache state, sequentially or under an explicitly registered schedule; do not accidentally run the independent evidence directories against competing capacity. Never pool the two profiles or use a long task's result to suppress a short-tool regression.

`evaluation/decision.example.json` and its workload remain legacy format-1 inputs with the original strict 1.10 p95 ratio on validated completion. The `*-confirmed.example.json` format-2 protocols also remain unchanged, with their registered point-observation rules. Their original collection paths are retained for compatibility and are not selected for the new pilot. Do not rewrite them or reinterpret earlier data. Format 3 requires complete selected-boundary intervals; another timing field cannot replace missing evidence.

The format-3 protocol's `observation_policy.period_seconds` fixes sampling cadence; use a matching registration polling interval if supplying `--poll-seconds`. A later collector flag cannot change the frozen policy.

Format 3 currently gates `outcome_visible_to_caller` only. Terminal-Pod and validated-completion observations remain diagnostics; other format-3 boundary selections fail before registration. The three boundary choices in existing format-2 protocols remain supported by their original path.

The new private registration stores canonical JSON hashes, workload, context, trial count, cache declaration and polling interval before resource creation. Inputs cannot be switched through collector flags. New registrations require an explicit workload `deadline_scope`; comparisons with the upstream arm require `pod`, matching new Agent OS and direct-Job Pod deadlines. Loading an existing registration never adds the field or changes its hash: omitted legacy scopes retain Job deadlines for Agent OS/direct Jobs and the upstream native Pod deadline. That mismatch is reported and is not matched deadline/failure evidence. The deterministic collector does not collect fault qualification; use a separately registered failure protocol for that claim. Local hashes detect accidental input drift; an operator can still rewrite local files, so this is not an immutable or independently timestamped audit system.

To rerun an omitted-scope legacy registration against the new API binary, its administrator template must explicitly select `deadline_scope: job`; the collector rejects a resolved Pod-clock record as a profile mismatch. For the new matched Pod-clock comparison, create a new registration instead of editing the old one.

Use `python-nitro-workload.json` with the Nitro administrator config for a **separate** experiment. Adjust the registered workload's resources to match its administrator template exactly. RuntimeClass, image, arguments, resource or annotation drift stops collection. Reports from different registered workloads/environments cannot automatically pass a matched-comparison gate.

New example workloads also register `termination_grace_period_seconds: 10`, matching the administrator template. The collector compares grace in the resolved API specification and actual execution. Older registrations without the field retain their implicit Kubernetes 30-second contract; changing an example does not rewrite their hashes or inputs. Use a new registration for changed policy.

## 3a. Check the instrument on the cluster

Before the upstream profiles, register a separate direct-Job versus Agent OS check with the short fixture. Use the same qualified workers, cached images and resources as the comparison, and preserve its registration and failed/uncertain trials:

```sh
python3 scripts/evaluate.py register \
  --context YOUR_EXPLICIT_CONTEXT --namespace agentos-workloads \
  --protocol evaluation/decision.job-instrument-check.example.json \
  --workload examples/evaluation/python-short-workload.json \
  --environment /absolute/private/environment.json \
  --evidence-dir evidence/eks-job-instrument-v3

python3 scripts/evaluate.py collect --context YOUR_EXPLICIT_CONTEXT \
  --evidence-dir evidence/eks-job-instrument-v3 --baseline direct-job --execute
python3 scripts/evaluate.py collect --context YOUR_EXPLICIT_CONTEXT \
  --evidence-dir evidence/eks-job-instrument-v3 --baseline agentos --execute \
  --api-url http://127.0.0.1:8080 --token-env AGENTOS_EVAL_TOKEN

python3 scripts/decision_report.py \
  --protocol evaluation/decision.job-instrument-check.example.json \
  --registration evidence/eks-job-instrument-v3/registration.json \
  --baseline evidence/eks-job-instrument-v3/direct-job.json \
  --candidate evidence/eks-job-instrument-v3/agentos.json \
  --output evidence/eks-job-instrument-v3/decision.json
```

Set `AGENTOS_EVAL_TOKEN` privately before the API arm. Review the reported `latency_observation` coverage, maximum/p95 gap and p95 bounds for both arms, alongside submission, Pod and native-outcome observations. The `instrument_check` diagnostic is emitted before qualification routing: it reports the signed observed p95 difference, conservative `p95_difference_bounds_seconds` and review flags, with `expected_difference_seconds=null`. A wholly negative difference interval or an additional-delay lower bound above the registered two-second allowance prompts review. The unaugmented decision remains insufficient without independently referenced qualification and benefit evidence; this exercise checks the instrument rather than requiring a cost/operator win against direct Jobs. Use the actual qualification sidecar described in section 6 when applying the full gates.

Do not assume a known one-second delta. Agent OS adds durable admission, staging, activation and reconciliation; the two executions also encounter scheduler, Job-controller and transport variation. An unexpectedly faster Agent OS result or a large positive delta needs retained investigation of cadence, request durations, arm order and execution boundaries. Repeat only under a new registration and retain the original. Do not adjust a gate or subtract a guessed correction until the expected ordering appears. The cluster check has not run yet.

## 4. Collect and retain all outcomes

With the upstream controller installed and Agent OS authenticated through a local port-forward, set `AGENTOS_EVAL_TOKEN` in the shell. Use the literal `127.0.0.1` address shown below: socket/TLS I/O is bounded, while the system hostname resolver is synchronous and unqualified as a bounded part of this pilot instrument. The token is read from the environment, not CLI arguments or report output.

```sh
python3 scripts/evaluate.py collect --context YOUR_EXPLICIT_CONTEXT \
  --evidence-dir evidence/eks-short-measured-v3 --baseline agent-sandbox --execute
python3 scripts/evaluate.py collect --context YOUR_EXPLICIT_CONTEXT \
  --evidence-dir evidence/eks-short-measured-v3 --baseline agentos --execute \
  --api-url http://127.0.0.1:8080 --token-env AGENTOS_EVAL_TOKEN
```

The upstream arm creates experiment-owned v1beta1 templates, a pool with **zero warm replicas**, and claims. This is a cold-claim execution measurement compatible with the blueprint's controller contract. It does not execute the complete AWS Bedrock/IRSA/FQDN reference workflow and is not a warm-pool performance result. The optional `direct-job` arm is a diagnostic floor, not the AWS blueprint.

The fixed fixture writes/reads scratch, performs deterministic work and emits an exact readiness line and completion digest. Only validated output counts as correct. Each arm records first observed terminal Pod, native caller-visible outcome and validated completion as separate monotonic offsets and UTC receipt times. Agent OS uses its authenticated terminal API response, direct Jobs use their terminal Job condition, and the upstream collector uses its selected Pod phase; their native guarantees are explicitly different. Missing or already-cleaned-up Pod evidence remains missing, never zero or an inferred timestamp from API state. Original `elapsed_seconds` remains the validated-completion measurement for compatibility.

The format-3 path starts one authenticated `kubectl proxy` on a Unix socket in a private temporary directory and prepares persistent Kubernetes/API channels before timing. Gated outcomes use a separate fixed-cadence sampler, so auxiliary ownership/profile and log checks do not determine polling frequency. Successful Kubernetes submissions use the returned POST object rather than paying an unconditional follow-up GET. Proxy process startup/authentication is outside each trial; native submission/admission work remains inside. No public proxy port is opened, credential diagnostics are suppressed, and the owned proxy/socket are closed after collection. See the official [proxy options](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_proxy/).

For each selected native outcome, inspect the last nonterminal request start/receipt, first terminal request start/receipt and resulting conservative bracket. A missing prior nonterminal observation stays missing; the lower bound then uses the trial origin explicitly. Missed cadence and request/transport delays remain evidence, not a correction subtracted from latency. Any correct trial with a missing interval or a bracket wider than 0.5 seconds makes latency `not_measurable`. Narrow intervals must also agree on the comparison: candidate p95 upper bound must be at most `baseline p95 lower bound * 1.10 + 2 seconds` to pass; candidate lower bound above `baseline upper bound * 1.10 + 2 seconds` fails; overlap remains undecided.

Readiness is first observed output, not a kernel timestamp. Failures and uncertain outcomes are retained. Existing arm files are not replaced, collections are serialized, and unresolved executions stop the arm. Retry a failed experiment only with a new registration and retain the original. The commands below illustrate the short profile; repeat them with the separate minute evidence directory and keep both decisions.

Cleanup is explicit and limited to recorded, owned resource identities:

```sh
python3 scripts/evaluate.py cleanup --context YOUR_EXPLICIT_CONTEXT \
  --evidence-dir evidence/eks-short-measured-v3 --execute \
  --api-url http://127.0.0.1:8080 --token-env AGENTOS_EVAL_TOKEN
```

API disappearance cannot prove a partitioned worker physically stopped, and an ambiguous creation can commit late. Inspect unresolved records before considering an experiment cleaned up. Collect retained logs/evidence before manual housekeeping. No namespace-wide or selector-wide deletion is performed.

Use the read-only node sampler in a separate terminal during an arm, with a fresh output path for each observation window:

```sh
python3 scripts/sample_resources.py --context YOUR_EXPLICIT_CONTEXT \
  --node-selector 'agent-sandbox/runtime=gvisor' --experiment-id eks-short-measured-v3 \
  --duration-seconds 120 --interval-seconds 5 \
  --output evidence/eks-short-measured-v3/upstream-node-samples.json --execute
```

Replace the selector with the labels of the actual eligible nodes and record the matching arm/window. Adjust the experiment ID and duration for the minute profile; a 120-second sample is not coverage of thirty minute-long tasks. The sampler records CPU-window usage, memory working set and allocatable capacity through Metrics API. Missing/stale metrics or changing node identities fail the observation; repeated scrapes are marked and do not count as independent measurements. Observations include system and unrelated work and can cover time before a task started. They cannot establish per-task attribution, CPU time, physical snapshot reclamation or billing savings. See the [resource-sampling details](../examples/evaluation/README.md).

Warm-pool performance requires a separately registered resident workload/dispatch protocol: prestarting this one-shot fixture would complete the work before measurement. Record warm pool size, idle resources and safe reuse behavior. No warm-pool result is claimed by this collector.

## 5. Measure operator effort

Time the same installation/upgrade/failure-recovery task in reset environments, keeping the starting state and operator experience comparable. Start each journal from the comparison's actual private `registration.json`; the journal derives its experiment, protocol, workload, environment and registration hashes from that file. Use the same concrete `--task-ref` in both arms (for example, a versioned installation procedure with a defined starting state and outcome). Record failed steps as well as successful ones; rotate task order to reduce learning bias. Never put commands containing credentials into labels. Keep the first infrastructure/setup discovery in a private timestamped exploratory log if registration and environment freeze have not happened. Do not attach later matching hashes to earlier observations. Register before the measured repetitions, and define whether the common reset starting state is a prepared qualified cluster or greenfield infrastructure.

```sh
python3 scripts/operator_journal.py start --directory evidence/ops-upstream-1 \
  --registration evidence/eks-short-measured-v3/registration.json --arm agent-sandbox \
  --task-ref install-standard-gvisor-v1 --operator operator-a --phase install
python3 scripts/operator_journal.py step --directory evidence/ops-upstream-1 \
  --name runtime-install --runbook-ref installation-step-2 \
  --privilege node-admin --node-customization --outcome succeeded
python3 scripts/operator_journal.py finish --directory evidence/ops-upstream-1 \
  --qualified --evidence-ref dynamic-conformance-record
```

Repeat with at least three completed sessions per arm, then use `summary --sessions DIR1 DIR2 DIR3 --output NEW_FILE` to produce a private sidecar. Every session in a summary must share its registration, arm, concrete task and phase. The decision tool checks those bindings against the trial reports and requires matching task references between arms. The sidecar records wall time, step count, required privileges, node customization and failed steps. These are human-reported observations, not proof of causation or independent verification; a matching label alone does not establish comparable starting conditions.

## 6. Apply the preregistered decision

```sh
python3 scripts/decision_report.py --protocol evaluation/decision.short-measured.example.json \
  --registration evidence/eks-short-measured-v3/registration.json \
  --baseline evidence/eks-short-measured-v3/agent-sandbox.json \
  --candidate evidence/eks-short-measured-v3/agentos.json \
  --output evidence/eks-short-measured-v3/decision.json
```

The decision reads the actual registration and binds both arms to its full trial count and canonical inputs; two equally truncated reports cannot pass by repeating a registration hash. Unaugmented collector output must yield `insufficient_evidence`: it cannot qualify isolation or invent AWS billing. Use optional `--supplemental` for a private sidecar with matching experiment/protocol/registration/workload/environment hashes and `arms` keyed by arm name. Allowed arm fields are `qualification` (`status` and `evidence_ref`), `billing_complete`, `full_cost_usd`, `node_reduction_demonstrated` and `cost_accounting_ref`. It cannot replace measured trials. Supply optional `--baseline-operations` / `--candidate-operations` journal summaries for the operating-effort route. Raw trial files remain intact.

The cost route requires complete referenced billing and demonstrated candidate capacity reduction; fixed-node headroom alone cannot pass it. Include control plane, nodes, idle/warm capacity, snapshot storage/I/O, network and failed work. Referenced qualification and billing sidecars are operator assertions that need review; the tool does not authenticate their truth.

`stop_experiment` means a qualification failed; `change_direction` means the candidate missed a measured contract/value gate; `continue_validation` means a route passed and needs customer validation. Missing or incomplete measurements remain undecided. In format 3, inspect `latency_gate.status`: `not_measurable` means absent/oversized intervals or interval bounds that straddle the allowed comparison, and leaves `decision=insufficient_evidence`. The per-arm `latency_observation` reports coverage and p95 bounds; it is an observation bracket, not a statistical confidence interval. None of these outputs certifies production safety, proves token savings or values the company.

## 7. Publish useful upstream outputs

Produce EKS compatibility notes, sanitized reproductions, conformance fixtures and candidate upstream fixes even if Agent OS has no advantage. Use the [finding template](templates/UPSTREAM_COMPATIBILITY_REPORT.md). File issues or propose patches only after a concrete reproduction and contribution-policy review. No upstream messages or changes are submitted by these tools.
