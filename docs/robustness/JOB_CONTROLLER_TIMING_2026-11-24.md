# Job completion timing investigation

Date: November 24, 2026. Instrumentation source: [`6389b173db6afeae10b3022e6df8bbfa99f39e69`](https://github.com/nobelchowdary/agent-os/tree/6389b173db6afeae10b3022e6df8bbfa99f39e69). [Standard CI](https://github.com/nobelchowdary/agent-os/actions/runs/45474381368) and the [targeted real kind/gVisor 64-task diagnostic](https://github.com/nobelchowdary/agent-os/actions/runs/45474391178) passed on attempt 1. Local `make check` passed Go race tests, vet, build, three profile preview smokes and 179 Python tests.

The change adds measurement and its tests; production Go/configuration/deployment sources are unchanged from `5b6e451`. The [previous full runtime acceptance](REPORTING_REVIEW_2026-11-24.md) remains its earlier execution baseline. The targeted diagnostic is separate from the subsequently verified [full harness regression](https://github.com/nobelchowdary/agent-os/actions/runs/45474381467), which also passed on attempt 1 at `6389b17`. The full result is recorded below without pooling its timings with the diagnostic batch. Documentation-only follow-ups do not change the instrumented/tested source.

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

## Follow-up: use the persisted outcome denominator

Recomputing from the same 64 raw records separates the first observed terminal Job from the persisted Agent OS outcome, without the harness's later sequential response polling. The [follow-up evidence](evidence/full-regression-6389b17.json) retains the calculation for every run, preserving fractional timestamp precision. These are still observation boundaries: a Job watch receipt can lag its API-server update, and process/receipt clocks remain unqualified.

| Diagnostic interval, seconds | Conventional median | Nearest-rank p50 | Nearest-rank p95 |
| --- | --- | --- | --- |
| Terminal Job receipt to persisted API outcome | 0.347433 | 0.332023 | 0.776148 |
| Terminal Pod receipt to terminal Job receipt | 5.757833 | 5.752151 | 10.729421 |
| Container exit to terminal Pod receipt | 3.650394 | 3.625377 | 4.532166 |

All three rows have 64 samples and no negative values. The first row ranges from 0.021575 to 0.876526 seconds. Thus the review's approximately 0.33-second Agent OS figure matches the harness's nearest-rank p50; the usual even-sample median is about 0.35 seconds. This difference in convention does not change the conclusion.

For each run, divide terminal-Pod-receipt to terminal-Job-receipt time by terminal-Pod-receipt to **persisted** API-outcome time. The median paired fraction is **0.951689498**, compared with the historical response-based fraction of 0.485273128 below. The Job interval exceeds the remaining Job-receipt-to-persist interval in all 64 runs. The approximate 95% figure describes the central fraction, not every run: individual fractions range from 65.283% to 99.689%, with 32/64 at or above 95%. The earlier fraction's sequential-polling denominator should not be used to apportion runtime service delay.

The first-to-last terminal Job receipt span is 11.751252 seconds. Dividing 64 observed outcomes by that span gives about 5.45 per second (the 63 interarrival gaps give 5.36 per second). This is compatible with the source-based write/rate model, but the diagnostic does not measure controller request counts or prove which effective client/server limit caused the rate.

Keep Job outcome authority and stop reporting-delay tuning for this milestone. The result supports moving to EKS and matched comparison boundaries. It does not turn watch receipts into exact server-transition timestamps, establish a universal latency floor or change the outcome safety argument.

## Historical conditional optimization decision

This section preserves the decision rule and calculation recorded before the persisted-denominator correction above. Its response-based fraction is historical experiment bookkeeping, not the corrected attribution of runtime delay.

[ADR 0008](../decisions/0008-earlier-success-observation.md) was committed before this diagnostic. Its observation rule required a median per-run Job fraction above one half and at least one second of median success-decision-receipt to terminal-Job-receipt headroom.

- Conventional median paired fraction: **0.485273128**; 30/64 exceeded one half. The fraction is conservative and affected by the sequential-response denominator; its failure is not evidence that the Job controller is fast or that Agent OS consumed the remainder.
- Conventional median success-decision receipt to terminal Job receipt: **0.5998715 seconds**; maximum 0.606047 seconds.
- The recorded success-decision condition time and Job completion time are equal in all 64 at their timestamp precision. Equality does not mean the controller performed zero work or persisted both facts simultaneously.

Both numerical conditions failed. Keep the existing runtime behavior. The prepared earlier-success candidate was incomplete, remained uncommitted and was removed from the working tree after preserving a private local patch; it is not a supported feature or validated implementation. The selected cost/operator gates were not changed to reinterpret this result.

The review's source model is plausible but needs precise limits: v1.37 batches relevant events by one second, and normal tracking can require two Job status writes plus a Pod finalizer patch. The controller-manager defaults are not a measurement of effective kind/EKS limits. A previously recorded `SuccessCriteriaMet=True` is considered before later deadline/failure evaluation; the late-Job-deadline hazard therefore concerns the initial success decision, not every subsequent sync. [Job controller source](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/job/job_controller.go), [client defaults](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/apis/config/v1alpha1/defaults.go).

A terminal Pod alone does not replace the aggregate Job contract merely because parallelism/completions are one and backoff is zero. Kubernetes documents possible duplicate program starts, and replacement/failed execution history matters. A new Pod-based outcome contract needs explicit ownership/history/fencing reasoning rather than treating a fresh status snapshot as a global guarantee. [Job execution and tracking](https://kubernetes.io/docs/concepts/workloads/controllers/job/).

A source-supported possible race illustrates the limit; it was **not observed in this campaign**. A Pod create can commit while its response is lost, causing the controller to decrement its expectation without receiving the successful-write callback. A retry from a stale informer can have a second create in flight while our Pod list sees only the first successful Pod. A later genuine `active=1` can represent that second execution, and its failure can still affect the first Job outcome decision. A fresh GET does not fence that in-flight create. Preserving Job-outcome semantics therefore needs the recorded controller decision; choosing one Pod's result instead would be a broader lifecycle contract requiring new fencing/history and fault tests. [Pod create/error callback](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/controller_utils.go#L592-L606), [creation expectations](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/job/job_controller.go#L1916-L1931).

## Verified full instrumented regression

[Run 45474381467](https://github.com/nobelchowdary/agent-os/actions/runs/45474381467) passed at exact source `6389b173db6afeae10b3022e6df8bbfa99f39e69`, attempt 1. Artifact **14962466369** was created at `2026-11-24T20:38:40Z`, after the attempt started at `19:50:05Z`; its downloaded JSON checksum is `01d1b5745299225281eb527e50e30cc41c88f09e2b71ce0d74ae7e1f6a25ac7c`. All 18 executable cases passed; three supplemental observation records captured disk cause, cancellation timing and API Pod deletion count. There were no failed or skipped cases. The workflow's disposable-cluster cleanup also passed. The [compact full-regression evidence](evidence/full-regression-6389b17.json) preserves each case and its qualification limits.

| Full campaign check | Observed result |
| --- | --- |
| Sustained cleanup | 200 sequential successes with the harness's zero-remaining-Job/Pod checks |
| Admission/restart stress | 500 unique keys, each submitted twice, during **74 API Pod deletions**; receipt/history and observed execution identity checks passed |
| Deadline without API | Explicit Pod scope, `pod_start_time` boundary, API replicas zero and terminal Job observed; final deadline outcome check passed |
| Exit/output and fault contracts | Canary, retained exit 3 after restart, bounded head/tail, orphan non-adoption, memory limit, image-pull startup bound, disk `evicted`, node drain and deleted-Job no-replay checks passed |
| Unavailable kubelet | `succeeded`, unavailable output, observed exit 0 and ready API |
| Cancellation | Handler ready, configured/observed grace 10 seconds, observed stop **11.504 seconds**; no stop-latency SLO is inferred |

| Full campaign load | Actual peak/overlap | Exit to persisted p50 | p95 | Complete captured output |
| --- | --- | --- | --- | --- |
| 16 | 16/16 | 3.505 s | 4.499 s | 16/16 |
| 64 | 64/64 | 10.245 s | 16.226 s | 64/64 |

All 80 load outcomes had complete captured output, no unavailable results and no negative exit-to-persist samples. Both load cases retained matched identities, all ten valid timing boundaries and complete Pod/Job watch drains. These are separate full-campaign batches, not repetitions to be pooled into the 64-task diagnostic's quantiles.

The pressure fixture remained a failed task: exit 128 after last progress at 40 children. Its trusted same-Pod observer recorded PID limit/peaks 128, events 0 to 1, **121 samples** and cgroup removal, followed by cleanup, ready selected nodes/API and a fresh successful gVisor task in the selected pool. This passes the documented host-limit-event/recovery alternative, not a precise guest PID ceiling or a uniquely established cause of exit 128.

Boundary qualification remains partial: peer, Kubernetes API TCP, CoreDNS UDP and metadata TCP denials were measured against reachable before/after controls on two workers. No controlled external listener was supplied; general internet/protocol/startup-race coverage, host escape resistance, EKS and Nitro remain unqualified. Passing the full local regression changes none of those limits or the frozen requirement scorecard.

## EKS next

Keep nodes warm and images available, then qualify the selected runtime/CNI and repeat the matched workload measurements. Use the [billing preparation checklist](../EVALUATION_RUNBOOK.md#billing-preparation-before-the-pilot) before a registered cost window: validate actual delivered Pod allocations/tags and resource-to-arm joins, account for shared/idle and ancillary charges, and keep missing data explicit. No AWS preferences, exports, tags or telemetry were configured during this work.

EKS, Nitro, connected model access, real-agent quality, comprehensive isolation and a competitive cost/token advantage remain unmeasured. The next architecture choice should use those workload/operator results and the upstream comparison, rather than infer a universal latency fix from a single local batch.
