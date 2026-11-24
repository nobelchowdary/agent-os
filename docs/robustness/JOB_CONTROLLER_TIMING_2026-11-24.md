# Job completion timing investigation

Date: November 24, 2026. Instrumentation source: [`6389b173db6afeae10b3022e6df8bbfa99f39e69`](https://github.com/nobelchowdary/agent-os/tree/6389b173db6afeae10b3022e6df8bbfa99f39e69). [Standard CI](https://github.com/nobelchowdary/agent-os/actions/runs/45474381368) and the [targeted real kind/gVisor 64-task diagnostic](https://github.com/nobelchowdary/agent-os/actions/runs/45474391178) passed on attempt 1. Local `make check` passed Go race tests, vet, build, three profile preview smokes and 179 Python tests.

The change adds measurement and its tests; production Go/configuration/deployment sources are unchanged from `5b6e451`. The [previous full runtime acceptance](REPORTING_REVIEW_2026-11-24.md) remains its execution baseline. This targeted diagnostic is not a new 200/500 acceptance campaign. The automatically triggered [full harness regression](https://github.com/nobelchowdary/agent-os/actions/runs/45474381467) is still in progress when this report is prepared; no result from it is claimed here. Documentation-only follow-ups do not change the instrumented/tested source.

## Coverage and recorded boundaries

All 64 tasks reached actual running-container concurrency and timestamp overlap of 64. All outputs were captured completely; unavailable results and negative exit-to-persisted samples were zero. Every Pod owner UID matched its watched Job UID. All ten timing boundaries were present and valid for all 64, with no missing, invalid or conflicting facts. Every selected Pod was `Succeeded` and every Job `Complete`; Pod/Job/success-decision observations came from watch receipts. Both bounded watch drains completed for all 64 without deletion censoring.

Pod phase has no authoritative API-server transition timestamp. `pod_terminal_observed_at` is the harness's first receipt, and may lag the status write. Job `completionTime` and condition timestamps use controller clock/semantics; they are not API-server write timestamps. Container `finishedAt`, API persisted-event time and harness observations retain their separate clock sources. Clock skew and watch delivery remain unqualified. API response receipts come from the existing sequential polling loop and include ordering delay. No extra API calls were added by this instrumentation.

Each per-run record retains its source timestamps, receive timestamps, Pod/Job UIDs, observation sources, qualification flags and signed intervals. Missing, malformed or conflicting boundaries cannot silently become zeros. The checked-in [evidence and analysis](evidence/job-controller-timing-2026-11-24.json) retain these records, the paired fractions, exact artifact identity and raw JSON checksum beyond Actions artifact retention.

## Measured distributions

All rows have 64 paired samples and zero unavailable samples. Quantiles use the harness's nearest-rank convention; medians used by the decision rule are calculated separately below.

| Signed interval, seconds | Minimum | p50 | p95 | Negative samples |
| --- | --- | --- | --- | --- |
| Container exit to terminal Pod receipt | 1.570 | 3.625 | 4.532 | 0 |
| Terminal Pod receipt to Job completion time | -0.912 | 4.325 | 9.102 | 1 |
| Job completion time to persisted API outcome | 1.295 | 1.508 | 2.310 | 0 |
| Container exit to Job completion time | 2.000 | 8.000 | 13.000 | 0 |
| Terminal Pod receipt to terminal Job receipt | 0.915 | 5.752 | 10.729 | 0 |
| Terminal Job receipt to terminal API response | 0.799 | 6.134 | 11.996 | 0 |
| Job completion time to terminal Job receipt | 0.877 | 1.386 | 1.827 | 0 |
| Terminal Pod receipt to success-decision receipt | 0.369 | 5.152 | 10.230 | 0 |
| Success-decision receipt to terminal Job receipt | 0.270 | 0.600 | 0.601 | 0 |
| Success-decision condition time to Job completion time | 0 | 0 | 0 | 0 |
| Success-decision condition time to persisted API outcome | 1.295 | 1.508 | 2.310 | 0 |

The unchanged container-exit-to-persisted-outcome metric measured **p50 9.508 seconds, p95 15.295 seconds**, minimum 3.388 seconds. These quantiles describe one batch; they are not stable tail SLOs, independent component service times or competitive performance results. Quantiles for separate intervals do not add. The negative mixed-source sample is retained, not clipped or assigned to a component.

The 6.134-second API-response interval above includes sequential polling after other tasks; it must not be called API processing time. Likewise, the Job completion timestamp can precede first observed terminal Job status, so the 1.508-second completion-to-persist interval includes more than Agent OS work. The evidence supports substantial waiting before the Job's success decision, with additional container-status/watch delay; it does not calibrate those components precisely.

## Conditional optimization decision

[ADR 0008](../decisions/0008-earlier-success-observation.md) was committed before this diagnostic. Its observation rule required a median per-run Job fraction above one half and at least one second of median success-decision-receipt to terminal-Job-receipt headroom.

- Conventional median paired fraction: **0.485273128**; 30/64 exceeded one half. The fraction is conservative and affected by the sequential-response denominator; its failure is not evidence that the Job controller is fast or that Agent OS consumed the remainder.
- Conventional median success-decision receipt to terminal Job receipt: **0.5998715 seconds**; maximum 0.606047 seconds.
- The recorded success-decision condition time and Job completion time are equal in all 64 at their timestamp precision. Equality does not mean the controller performed zero work or persisted both facts simultaneously.

Both numerical conditions failed. Keep the existing runtime behavior. The prepared earlier-success candidate was incomplete, remained uncommitted and was removed from the working tree after preserving a private local patch; it is not a supported feature or validated implementation. The selected cost/operator gates were not changed to reinterpret this result.

The review's source model is plausible but needs precise limits: v1.37 batches relevant events by one second, and normal tracking can require two Job status writes plus a Pod finalizer patch. The controller-manager defaults are not a measurement of effective kind/EKS limits. A previously recorded `SuccessCriteriaMet=True` is considered before later deadline/failure evaluation; the late-Job-deadline hazard therefore concerns the initial success decision, not every subsequent sync. [Job controller source](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/job/job_controller.go), [client defaults](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/apis/config/v1alpha1/defaults.go).

A terminal Pod alone does not replace the aggregate Job contract merely because parallelism/completions are one and backoff is zero. Kubernetes documents possible duplicate program starts, and replacement/failed execution history matters. A new Pod-based outcome contract needs explicit ownership/history/fencing reasoning rather than treating a fresh status snapshot as a global guarantee. [Job execution and tracking](https://kubernetes.io/docs/concepts/workloads/controllers/job/).

A source-supported possible race illustrates the limit; it was **not observed in this campaign**. A Pod create can commit while its response is lost, causing the controller to decrement its expectation without receiving the successful-write callback. A retry from a stale informer can have a second create in flight while our Pod list sees only the first successful Pod. A later genuine `active=1` can represent that second execution, and its failure can still affect the first Job outcome decision. A fresh GET does not fence that in-flight create. Preserving Job-outcome semantics therefore needs the recorded controller decision; choosing one Pod's result instead would be a broader lifecycle contract requiring new fencing/history and fault tests. [Pod create/error callback](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/controller_utils.go#L592-L606), [creation expectations](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/job/job_controller.go#L1916-L1931).

## EKS next

Keep nodes warm and images available, then qualify the selected runtime/CNI and repeat the matched workload measurements. Use the [billing preparation checklist](../EVALUATION_RUNBOOK.md#billing-preparation-before-the-pilot) before a registered cost window: validate actual delivered Pod allocations/tags and resource-to-arm joins, account for shared/idle and ancillary charges, and keep missing data explicit. No AWS preferences, exports, tags or telemetry were configured during this work.

EKS, Nitro, connected model access, real-agent quality, comprehensive isolation and a competitive cost/token advantage remain unmeasured. The next architecture choice should use those workload/operator results and the upstream comparison, rather than infer a universal latency fix from a single local batch.
