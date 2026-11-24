# Separate execution and confirmed-outcome comparison boundaries

Date: November 24, 2026. Scope: comparison collector, decision tooling, fixtures and documentation. Production Go, deployment policy and the runtime failure-campaign harness are unchanged. [Decision 0009](../decisions/0009-comparison-boundaries-and-latency-budget.md) records the founder's choices before any new comparison collection.

## Selected next pilot

Register both profiles independently before running either arm:

| Profile | Protocol | Workload |
| --- | --- | --- |
| Nominal one-second elapsed tool | [Short protocol](../../evaluation/decision.short-confirmed.example.json) | [Short workload](../../examples/evaluation/python-short-workload.json) |
| Nominal sixty-second elapsed task | [Minute protocol](../../evaluation/decision.minute-confirmed.example.json) | [Minute workload](../../examples/evaluation/python-minute-workload.json) |

Both retain 30 trials per arm, exact deterministic output, 100% correctness and separate dynamic qualification. Their format-2 latency gate is `candidate caller-outcome p95 <= baseline caller-outcome p95 * 1.10 + 2.0 seconds`. The absolute allowance, selected boundary and rationale are part of the hashed protocol and report. Passing latency still needs either 20% lower fully accounted cost per correct task or 25% less median operator time to reach the benefit route. Do not pool the two durations or revise a registration after seeing its results.

The fixtures perform the same deterministic file/hash work, then wait for their declared minimum elapsed application duration. They probe the effect of fixed overhead at two durations; they do not represent sustained CPU work, token efficiency or a complete coding/research agent.

## Recorded contract

Each arm now preserves three first observations with harness-monotonic elapsed offsets and UTC receipt times:

| Boundary | Agent OS | Direct Job diagnostic | Upstream cold claim |
| --- | --- | --- | --- |
| `pod_terminal` | Verified workload Pod phase | Verified workload Pod phase | Verified workload Pod phase |
| `outcome_visible_to_caller` | Authenticated terminal run API response | Owned terminal Job condition | Owned Sandbox Pod terminal phase |
| `validated_completion` | Successful native outcome plus exact stdout validation | Same validation requirement | Same validation requirement |

These native outcomes have different guarantees. Job authority remains mandatory for Agent OS; a terminal Pod alone never completes its run. Reporting-delay tuning stops at this milestone. Session reuse remains later Phase 4 work.

Observed object identities remain bound to their owners and workload profile. The first verified terminal response is retained even when submission already returns a finished run. A previously verified execution may have been cleaned up before the next API poll: absent Pod timing remains explicitly missing. Never-seen execution context, contradictory terminal Job conditions and observed Pod replacement fail the trial. Signed receipt gaps are retained because polling order can differ from execution order.

Original `elapsed_seconds` still ends after successful native status and output validation, before cleanup. Existing format-1 protocols keep their strict 1.10 ratio and original JSON/hashes. Format-2 decisions check the registered arm-specific timing contract and policy, report coverage/p95 for each boundary and refuse to gate on an incomplete subset of correct trials. Supplemental qualification, billing and operator files cannot replace measured trial timing.

## Evidence and limits

The earlier [full kind/gVisor campaign at `6389b17`](JOB_CONTROLLER_TIMING_2026-11-24.md#verified-full-instrumented-regression) is now verified successful, including 200 sequential tasks, 500 duplicate-key/restart submissions and actual 16/64 concurrency. Its [source/artifact evidence](evidence/full-regression-6389b17.json) also retains the diagnostic denominator correction. Those measured results concern the unchanged runtime; they are not runs of these new comparison profiles.

Local tests cover delayed caller outcomes, first terminal submission responses, failed outcomes, explicit missing Pod evidence, UID replacement, wall-clock adjustment, legacy compatibility and the additive formula. Offline integration exercised each new profile through registration, operator-journal sessions and decision reporting with synthetic reports; altered policies invalidated existing bindings. No cluster or AWS account was contacted by these comparison tests.

A final compatibility check fed actual collector-emitted reports from fake Kubernetes/API clients into the tightened decision validator: both profiles, all three arms, 30 trials each (**180 simulated trials**). All state/source/UID and phase-coverage checks passed. Unaugmented reports correctly remained `insufficient_evidence`; explicitly synthetic qualification plus twelve offline journal sessions exercised the operator route. Registration bytes stayed unchanged and network sockets were disabled. These simulated results are not benchmark measurements.

`make check` passed: Go formatting/vet/race tests, **203 Python tests**, build and authenticated preview smokes for legacy, gVisor and Nitro profiles. The preview did not execute agent code. In particular, `test_evaluate.py` covers all three collector arms and `test_decision_report.py` covers formula boundaries, missing selected observations, native-state/identity contradictions and compatibility. This validation is local contract evidence; the new comparison profiles have not yet run on a real cluster.

The [runbook](../EVALUATION_RUNBOOK.md#3-register-the-comparison-before-collection) provides the next commands. EKS qualification, both real comparison profiles, delivered billing allocations and matched operator sessions remain pending. No new cost, latency, density, isolation, production-readiness or token-saving advantage is claimed.
