# Run the EKS evidence milestone

These tools are implemented; live compatibility and performance results do not exist yet. `make check` covers code contracts and fake-client evidence flows. Every cluster-mutating collector requires explicit context and `--execute`; no tool provisions AWS infrastructure. Keep raw reports private under ignored `evidence/`.

## 1. Choose and qualify the runtime

Use [AWS's Agent Sandbox blueprint](https://awslabs.github.io/ai-on-eks/docs/infra/agents/agent-sandbox) as the upstream deployment reference. Its infrastructure, controller, warm capacity and egress components count toward setup effort and cost. The [conformance guide](../examples/conformance/README.md) records pinned source references and commands for the declaration inventory and Substrate smoke.

The current `qualify_cluster.py` inventory supports Standard EKS/gVisor only. It reports `NOT_QUALIFIED` even when all declarations match. It does not run packet-level isolation tests. Native VPC CNI startup enforcement and a chained Cilium configuration are distinct profiles. Match the runtime, node labels, policies and resources to the installation being tested.

The application additionally implements an offline Nitro isolated profile through `config/nitro-isolated.example.json` and `overlays/nitro`. It is not a one-field gVisor substitution: network-device removal, scratch backing, node placement, guest overhead and RBAC differ. Nitro needs its own live qualification, supported AWS infrastructure and experiment registration. See [installation](GETTING_STARTED.md#aws-nitro-isolated-profile).

## 2. Test Substrate continuity before comparative performance

Use the [dedicated actor fixture and probe](../examples/conformance/README.md#dedicated-substrate-continuity-smoke) on an operator-prepared Substrate installation. It checks memory/file continuity through synchronous suspend/resume and observes durable snapshot state and released control-plane assignment. It does not establish physical memory reclamation, automatic idling or node-cost savings.

Then extend the live experiment to restore onto a different eligible worker/instance type, retain open connections at checkpoint, expire credentials and observe a controlled external write made before suspension. Report unsupported CPU/runtime combinations and broken connections. External actions remain external; neither the fixture nor a snapshot rolls them back. The initial helper does not automate disruptive worker replacement or external writes.

## 3. Register the comparison before collection

Copy `examples/evaluation/environment.example.json` outside Git and replace its placeholders with the actual private environment record. Review `evaluation/decision.example.json` with a partner before registering: its proposed gates are 30 trials, 100% deterministic correctness, at most 10% p95 latency regression, and either 20% lower fully accounted cost or 25% less median operator time. These are engineering defaults, not achieved results or commercial commitments.

```sh
mkdir -p evidence
python3 scripts/evaluate.py register \
  --context YOUR_EXPLICIT_CONTEXT --namespace agentos-workloads \
  --protocol evaluation/decision.example.json \
  --workload examples/evaluation/python-workload.json \
  --environment /absolute/private/environment.json \
  --evidence-dir evidence/eks-comparison-01 \
  --upstream-controller-ref 'YOUR_REGISTRY/controller@sha256:REPLACE_WITH_ACTUAL_64_HEX_DIGEST'
```

The new private registration stores canonical JSON hashes, workload, context, trial count, cache declaration and polling interval before resource creation. Inputs cannot be switched through collector flags. New registrations require an explicit workload `deadline_scope`; comparisons with the upstream arm require `pod`, matching new Agent OS and direct-Job Pod deadlines. Loading an existing registration never adds the field or changes its hash: omitted legacy scopes retain Job deadlines for Agent OS/direct Jobs and the upstream native Pod deadline. That mismatch is reported and is not matched deadline/failure evidence. The deterministic collector does not collect fault qualification; use a separately registered failure protocol for that claim. Local hashes detect accidental input drift; an operator can still rewrite local files, so this is not an immutable or independently timestamped audit system.

To rerun an omitted-scope legacy registration against the new API binary, its administrator template must explicitly select `deadline_scope: job`; the collector rejects a resolved Pod-clock record as a profile mismatch. For the new matched Pod-clock comparison, create a new registration instead of editing the old one.

Use `python-nitro-workload.json` with the Nitro administrator config for a **separate** experiment. Adjust the registered workload's resources to match its administrator template exactly. RuntimeClass, image, arguments, resource or annotation drift stops collection. Reports from different registered workloads/environments cannot automatically pass a matched-comparison gate.

New example workloads also register `termination_grace_period_seconds: 10`, matching the administrator template. The collector compares grace in the resolved API specification and actual execution. Older registrations without the field retain their implicit Kubernetes 30-second contract; changing an example does not rewrite their hashes or inputs. Use a new registration for changed policy.

## 4. Collect and retain all outcomes

With the upstream controller installed and Agent OS authenticated through a local port-forward, set `AGENTOS_EVAL_TOKEN` in the shell. The token is read from the environment, not CLI arguments or report output.

```sh
python3 scripts/evaluate.py collect --context YOUR_EXPLICIT_CONTEXT \
  --evidence-dir evidence/eks-comparison-01 --baseline agent-sandbox --execute
python3 scripts/evaluate.py collect --context YOUR_EXPLICIT_CONTEXT \
  --evidence-dir evidence/eks-comparison-01 --baseline agentos --execute \
  --api-url http://127.0.0.1:8080 --token-env AGENTOS_EVAL_TOKEN
```

The upstream arm creates experiment-owned v1beta1 templates, a pool with **zero warm replicas**, and claims. This is a cold-claim execution measurement compatible with the blueprint's controller contract. It does not execute the complete AWS Bedrock/IRSA/FQDN reference workflow and is not a warm-pool performance result. The optional `direct-job` arm is a diagnostic floor, not the AWS blueprint.

The fixed fixture writes/reads scratch, performs deterministic work and emits an exact readiness line and completion digest. Only validated output counts as correct. Timings include client observation and polling overhead; readiness is first observed output, not a kernel timestamp. Failures and uncertain outcomes are retained. Existing arm files are not replaced, collections are serialized, and unresolved executions stop the arm. Retry a failed experiment only with a new registration and retain the original.

Cleanup is explicit and limited to recorded, owned resource identities:

```sh
python3 scripts/evaluate.py cleanup --context YOUR_EXPLICIT_CONTEXT \
  --evidence-dir evidence/eks-comparison-01 --execute \
  --api-url http://127.0.0.1:8080 --token-env AGENTOS_EVAL_TOKEN
```

API disappearance cannot prove a partitioned worker physically stopped, and an ambiguous creation can commit late. Inspect unresolved records before considering an experiment cleaned up. Collect retained logs/evidence before manual housekeeping. No namespace-wide or selector-wide deletion is performed.

Use the read-only node sampler in a separate terminal during an arm, with a fresh output path for each observation window:

```sh
python3 scripts/sample_resources.py --context YOUR_EXPLICIT_CONTEXT \
  --node-selector 'agent-sandbox/runtime=gvisor' --experiment-id eks-first-comparison \
  --duration-seconds 120 --interval-seconds 5 \
  --output evidence/eks-comparison-01/upstream-node-samples.json --execute
```

Replace the selector with the labels of the actual eligible nodes and record the matching arm/window. The sampler records CPU-window usage, memory working set and allocatable capacity through Metrics API. Missing/stale metrics or changing node identities fail the observation; repeated scrapes are marked and do not count as independent measurements. Observations include system and unrelated work and can cover time before a task started. They cannot establish per-task attribution, CPU time, physical snapshot reclamation or billing savings. See the [resource-sampling details](../examples/evaluation/README.md).

Warm-pool performance requires a separately registered resident workload/dispatch protocol: prestarting this one-shot fixture would complete the work before measurement. Record warm pool size, idle resources and safe reuse behavior. No warm-pool result is claimed by this collector.

## 5. Measure operator effort

Time the same installation/upgrade/failure-recovery task in reset environments, keeping the starting state and operator experience comparable. Start each journal from the comparison's actual private `registration.json`; the journal derives its experiment, protocol, workload, environment and registration hashes from that file. Use the same concrete `--task-ref` in both arms (for example, a versioned installation procedure with a defined starting state and outcome). Record failed steps as well as successful ones; rotate task order to reduce learning bias. Never put commands containing credentials into labels. Keep the first infrastructure/setup discovery in a private timestamped exploratory log if registration and environment freeze have not happened. Do not attach later matching hashes to earlier observations. Register before the measured repetitions, and define whether the common reset starting state is a prepared qualified cluster or greenfield infrastructure.

```sh
python3 scripts/operator_journal.py start --directory evidence/ops-upstream-1 \
  --registration evidence/eks-comparison-01/registration.json --arm agent-sandbox \
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
python3 scripts/decision_report.py --protocol evaluation/decision.example.json \
  --registration evidence/eks-comparison-01/registration.json \
  --baseline evidence/eks-comparison-01/agent-sandbox.json \
  --candidate evidence/eks-comparison-01/agentos.json \
  --output evidence/eks-comparison-01/decision.json
```

The decision reads the actual registration and binds both arms to its full trial count and canonical inputs; two equally truncated reports cannot pass by repeating a registration hash. Unaugmented collector output must yield `insufficient_evidence`: it cannot qualify isolation or invent AWS billing. Use optional `--supplemental` for a private sidecar with matching experiment/protocol/registration/workload/environment hashes and `arms` keyed by arm name. Allowed arm fields are `qualification` (`status` and `evidence_ref`), `billing_complete`, `full_cost_usd`, `node_reduction_demonstrated` and `cost_accounting_ref`. It cannot replace measured trials. Supply optional `--baseline-operations` / `--candidate-operations` journal summaries for the operating-effort route. Raw trial files remain intact.

The cost route requires complete referenced billing and demonstrated candidate capacity reduction; fixed-node headroom alone cannot pass it. Include control plane, nodes, idle/warm capacity, snapshot storage/I/O, network and failed work. Referenced qualification and billing sidecars are operator assertions that need review; the tool does not authenticate their truth.

`stop_experiment` means a qualification failed; `change_direction` means the candidate missed a measured contract/value gate; `continue_validation` means a route passed and needs customer validation. Missing or incomplete measurements remain undecided. None of these outputs certifies production safety, proves token savings or values the company.

## 7. Publish useful upstream outputs

Produce EKS compatibility notes, sanitized reproductions, conformance fixtures and candidate upstream fixes even if Agent OS has no advantage. Use the [finding template](templates/UPSTREAM_COMPATIBILITY_REPORT.md). File issues or propose patches only after a concrete reproduction and contribution-policy review. No upstream messages or changes are submitted by these tools.
