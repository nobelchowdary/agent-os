# Robustness work index

The November 21 proposal is preserved in [source-plan-2026-11-21.txt](source-plan-2026-11-21.txt). Start implementation planning with the [70-requirement gap audit and phase report](GAP_AUDIT_2026-11-21.md), which checks the actual repository instead of relying on earlier status summaries. It includes test references, cluster facts, measurement status, the T1–T16 matrix, deviations and open decisions.

The [November 22 continuation report](PHASE_ZERO_2026-11-22.md) adds structured outcome codes (C2) and stopped, drained metadata check/backup/restore with schema compatibility (C14). Both are local progress against Partial requirements; EKS results and the frozen audit counts do not change.

At audited commit `e2ed1a4`, 41 whole requirements were Partial and 29 Missing. No whole requirement was accepted as Implemented against its full requested acceptance test. These labels include the EKS evidence gate and do not erase narrower implemented features. All EKS outcomes remain Not run. Machine-readable records are in `audit-core.json`, `audit-security.json` and `audit-platform.json`; do not silently update this frozen baseline after code changes.

| Phase | Status | Next acceptance work |
| --- | --- | --- |
| 0: core invariants | In progress | State graph, API contract, structured outcome codes and drained metadata recovery have local tests. Full cause/fault campaigns, active disaster recovery, C11 HA design and other unmet core criteria remain. |
| 1: EKS qualification | Not run | Connect a selected disposable environment; qualify runtime/CNI/node resources and first offline task. |
| 2: complete connected agent | Gated | Spike existing gateways against N4–N9 only after qualification. No direct provider egress or credentials in task commands. |
| 3: upstream comparison | Gated | Pin the reference, select metrics/thresholds before collection, include real billing and operator effort; publish all outcomes. |
| 4: sessions/recovery | Gated | Adopt upstream warm sessions and evaluate Substrate checkpoints; define external-effect safety before retry/recovery. |
| 5: budgets/policy | Gated | Define reserved units, price versions, settlement/unknowns and enforcement under concurrency/partitions. |
| 6: tenancy/availability/packaging | Gated | Resolve its dependency overlap with C11 before claiming availability or zero-downtime upgrades. |

Use [decision 0004](../decisions/0004-robustness-phase-zero.md) for the current change boundaries. Do not claim exactly-once remote effects, fixed physical-stop times, snapshots that undo external actions, or savings from unused fixed-node capacity. Do not change old experiment registrations to match new targets. The proposal's 50% fewer operator steps is a different metric from the existing example protocol's 25% lower median operator time.

The API, gateway and execution integrations should share explicit contracts; Agent OS should not build a hypervisor, checkpoint engine, egress proxy or policy language. The gateway and new drivers are still choices to validate, not installed components. Existing [evaluation tooling](../EVALUATION_RUNBOOK.md) and [live client](../LIVE_API.md) provide preparation and narrower local evidence.

The [November 22 review bundle](../reviews/2026-11-22/README.md) ties the outside review to exact source and test names, preserves the historical diff/report, and documents the continuation's cause precedence, pending-start deadline and schema-2 restore epochs. [Decision 0006](../decisions/0006-restore-epochs-and-cause-evidence.md) adopts C1–C10/C12 as the smaller local qualification target, retaining later production gates and current tenant limits. No whole-requirement or live-cluster acceptance is newly claimed. The store is confirmed as single-owner bbolt; shared gateway/HA storage must be decided before Phase 2.
