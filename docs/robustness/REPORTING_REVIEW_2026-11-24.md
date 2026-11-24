# Reporting delay and persisted Pod deadlines

Date: November 24, 2026. This report records the staged response to the reporting-delay review and the founder's selected deadline/benefit contract. It supplements, rather than rewrites, [the Job-clock acceptance at 7d4df23](LOCAL_ACCEPTANCE_7d4df23.md). No EKS or competitor result is claimed.

Implementation [`bffa26f`](https://github.com/nobelchowdary/agent-os/tree/bffa26fd855fb80cb0b26ee5ed7a177b05c80212) passed [standard CI](https://github.com/nobelchowdary/agent-os/actions/runs/45459345445), including container packaging, and the [full Pod-clock campaign, attempt 2](https://github.com/nobelchowdary/agent-os/actions/runs/45459345437/attempts/2). Local `make check` passed Go race tests, vet, build, three profile preview smokes and 171 Python tests. Documentation-only commits recording these results leave the tested implementation unchanged. The staged Job-clock measurements and full Pod-clock acceptance are reported separately below.

## Staged reporting measurements

Each row is a separate targeted `load_64` campaign on a disposable Kubernetes 1.37 kind cluster with actual gVisor execution. Every row reached 64 observed running containers and 64 overlapping container intervals, and retained complete captured output for all 64 tasks with no unavailable results or negative lag samples.

| Implementation | Exact source | Campaign | Pod exit to terminal p50 | p95 | p99 |
| --- | --- | --- | --- | --- | --- |
| Fresh old-implementation baseline | [`be04b71`](https://github.com/nobelchowdary/agent-os/tree/be04b710080365e0523ba90c36592931b8996529) | [45454253248](https://github.com/nobelchowdary/agent-os/actions/runs/45454253248) | 15.987 s | 23.128 s | 24.087 s |
| Shared bounded RuntimeClass cache | [`96e2565`](https://github.com/nobelchowdary/agent-os/tree/96e25656137785471801b6f9c2f8e058767d8c70) | [45452728380](https://github.com/nobelchowdary/agent-os/actions/runs/45452728380) | 14.328 s | 19.547 s | 20.448 s |
| Reuse observed Job/Pods for capture | [`39afaa0`](https://github.com/nobelchowdary/agent-os/tree/39afaa0e749173189a33e69b9363be2ad839e256) | [45454246627](https://github.com/nobelchowdary/agent-os/actions/runs/45454246627) | 13.543 s | 18.963 s | 19.022 s |
| Commit terminal cohort before cleanup | [`67cf56a`](https://github.com/nobelchowdary/agent-os/tree/67cf56a4d8e80ee154bbc91684bd42de9ffb708f) | [45456345462](https://github.com/nobelchowdary/agent-os/actions/runs/45456345462) | 13.235 s | 19.615 s | 19.675 s |
| Bounded Job/Pod lists per pass | [`8f5699d`](https://github.com/nobelchowdary/agent-os/tree/8f5699d2c4614863cfd69defa0e6fb1f61d45cb6) | [45459175429, attempt 2](https://github.com/nobelchowdary/agent-os/actions/runs/45459175429/attempts/2) | 9.757 s | 14.849 s | 15.055 s |

The staged rows above retain the original Job deadline so the separately approved clock change does not enter those measurements. Each load task requests/limits 25 millicores, 64 MiB memory and 16 MiB scratch and waits for a common release time. Shared Kubernetes client limits remain 50 QPS/100 burst. Two kind workers share one Docker runner; separate runs can use different hosted runners and experience different load. These are empirical quantiles from one batch per stage, not replicated causal estimates or stable tail SLOs. Cleanup's p95 did not improve over the preceding batch; that result is retained.

Lag spans the node's container `finishedAt` to the API's committed terminal-event timestamp. Clock skew is unqualified. It includes Kubernetes/runtime/controller observation effects and does not isolate API processing time. It measures neither real-agent throughput nor model-token efficiency, cost savings, hardware efficiency or a competitive advantage. The raw sanitized `kind-gvisor-result/result.json` artifact is attached to each linked campaign; Actions artifact retention is limited.

The checked-in [evidence summary](evidence/reporting-2026-11-24.json) retains the staged observations, exact source/run identities and raw-artifact SHA-256 values beyond that retention window. It is a repository record, not an independent signed attestation.

The first attempts at [`8f5699d`](https://github.com/nobelchowdary/agent-os/tree/8f5699d2c4614863cfd69defa0e6fb1f61d45cb6) ([targeted run, attempt 1](https://github.com/nobelchowdary/agent-os/actions/runs/45459175429/attempts/1)) and [`bffa26f`](https://github.com/nobelchowdary/agent-os/tree/bffa26fd855fb80cb0b26ee5ed7a177b05c80212) ([full run, attempt 1](https://github.com/nobelchowdary/agent-os/actions/runs/45459345437/attempts/1)) stopped at the initial canary. Both report containerd's checkpoint-image check referring to a missing temporary `import-2026-11-24` image name. No agent container started; the admission-age startup bound produced `infra_failure` with unavailable output. No load/deadline/recovery result can be drawn from either attempt. The implementation and pinned cluster/runtime/workload inputs were retained for the retry, and the failed attempt artifacts were preserved. The [upstream report draft](KIND_IMAGE_IMPORT_2026-11-24.md) distinguishes observations from hypotheses; it has not been filed and no workaround is verified.

## Full Pod-clock acceptance

The separate full campaign at `bffa26f` passed its 200/500 targets with administrator templates explicitly recording `pod`. Its default lifecycle limits remained four active runs, a 20-second startup wait and 50 QPS/100 burst; only the selected load cases raised active capacity and startup waiting. It ran the following cases against real kubelets and gVisor:

| Check | Observed result |
| --- | --- |
| Deadline independent of API | Persisted `pod` scope; Pod deadline present and Job deadline absent; structured Pod `DeadlineExceeded` and terminal Job failure observed with API replicas zero; restored API recorded `failed/deadline_exceeded` |
| Sustained cleanup | 200 sequential successes; the harness confirmed zero remaining workload Jobs/Pods |
| Admission/restart stress | 500 unique keys, each submitted twice, during 76 API Pod deletions; watched execution identities and terminal-history checks passed |
| Exit/output retention | Exit 0, retained exit 3 across restart, bounded head/tail output and orphan non-adoption passed |
| Structured faults | Memory limit, image-pull startup bound, own-limit disk eviction, worker drain and deleted-Job no-replay checks passed |
| Missing kubelet logs | `succeeded`, unavailable output, observed exit 0 and ready API |
| Cancellation | SIGTERM handler confirmed ready, configured/observed deletion grace 10 seconds, observed stop 12.029 seconds; no physical-stop SLO is inferred |

| Actual concurrent tasks | Samples | Pod exit to terminal p50 | p95 | p99 | Complete captured output |
| --- | --- | --- | --- | --- | --- |
| 16 | 16 | 4.113 s | 5.087 s | 5.087 s | 16/16 |
| 64 | 64 | 10.638 s | 16.621 s | 16.624 s | 64/64 |

Both cases reached the required observed peak and timestamp overlap; all 80 results were captured and complete, with no unavailable or negative-lag samples. The single-batch, hosted-runner and unqualified-clock limits above also apply here. Do not pool these Pod-clock results with the staged Job-clock rows or treat the two different batches at `8f5699d` and `bffa26f` as an isolated estimate of changing the clock.

Boundary checks measured denial of the named peer, Kubernetes API TCP, CoreDNS UDP and metadata TCP paths with reachable before/after controls on two workers. No controlled external listener was supplied. General network/isolation qualification remains incomplete, including internet/IPv6/redirect/protocol and startup-race coverage, host escape resistance and a precise guest PID ceiling.

The process-pressure fixture remained a **failed execution**: exit 128 after its last progress report at 40 children. Its same-Pod trusted cgroup observer recorded configured PID limit/peaks 128, 130 samples, event count 0 to 1 and subsequent cgroup removal. Selected nodes/API were ready, cleanup completed and a fresh gVisor task succeeded in the selected pool. This passes the documented host-limit-event/recovery alternative; it neither identifies a unique cause for exit 128 nor proves a guest process ceiling or fresh execution on the pressure worker.

Field mismatch diagnostics have unit/fake-client and durable controller/reopen coverage; no live mutating-webhook campaign was run. EKS, Nitro, connected model access, real-agent quality, comprehensive isolation and competitive cost/operator benefits remain unqualified.

## Runtime contracts

- RuntimeClass declarations share a three-second positive cache with concurrent reads coalesced and independent copies. Invalid/error results are not cached. Readiness cannot extend this interval by stacking its own cache. Node isolation is not inferred from this declaration cache.
- Normal terminal capture accepts a private backend/run/Job-UID-bound observation receipt. It reuses deep-copied validated Pod evidence and reads logs without refetching the Job/Pods. Foreign or modified receipts fail closed; retries observe again. Cancellation retains its fresh path.
- Normal finished-execution deletion begins after that pass's observation/capture workers have committed results and terminal states. All fresh terminals are drained by at most eight cleanup workers, with durable inventory recovery after a crash or ambiguous deletion. Physical stop is not established by a terminal event.
- Multiple running executions share a pass-local, bounded Job list and Pod list. Normal 64-run cohorts fit one page per collection; pagination is capped at four pages/256 items. Partial Job lists fail the read, partial Pod lists are discarded, and missing Jobs receive a fresh lookup before disappearance can be reported. Lists do not authorize creation, activation, cancellation or deletion. No view persists between passes, and polling remains in place.
- Execution-contract mismatch reasons retain only the first differing schema path, including numeric indexes. Values, object names, map keys and wrapper text are excluded. Results from the changed workload remain quarantined while ownership-fenced cleanup proceeds; the safe reason survives database reopen.

## Approved deadline and comparison decisions

The founder selected **20% lower fully accounted cost per correct task OR 25% less median operator time**, retaining the executable protocol's correctness, trial-count and latency gates. Operator steps remain a diagnostic; 50% fewer steps is not the selected benefit gate. No existing registration is rewritten.

New admissions persist an administrator-owned deadline scope, defaulting to `pod`. The Pod clock includes node-side pulling/initialization relative to kubelet `StartTime`, excludes pre-acknowledgement scheduling, and is not process CPU time. Legacy stored omission retains the Job clock. Only one deadline location is rendered. Caller requests cannot choose the clock. The separate startup wait remains admission-age based.

Schema 4 is a metadata-only downgrade fence: previous records, fingerprints, keys, history and epoch remain unchanged. A narrow legacy idempotency alias returns the original omitted-scope admission only when clearing the newly resolved scope reproduces its original fingerprint; other policy drift and explicit clock changes still conflict. A schema-3 binary cannot safely operate on new scope-bearing records.

New evaluation registrations require an explicit scope and require `pod` for upstream comparisons. Old registrations retain original hashes/native clocks; reports disclose any mismatch and do not qualify deadline faults. See [ADR 0007](../decisions/0007-persisted-execution-deadline-scope.md), the [evaluation plan](../EVALUATION_PLAN.md) and the [EKS brief](../EKS_EVALUATION_BRIEF.md).

## Requirement to check references

| Contract | Focused checks |
| --- | --- |
| Bounded declaration cache | `runtime_cache_test.go`: coalescing, independent copies, invalid/error freshness, waiter cancellation and readiness expiry |
| Reused terminal evidence | `observed_results_test.go`: HTTP request counts, deep copies, foreign/changed receipts, optional Pod error and finish-time capture budget |
| Result before deferred deletion | `cleanup_phase_test.go`: all 64 persisted before first cleanup, bounded cleanup workers, 200 sequential one-slot runs, pending/lost-response/restart recovery and cancelled context |
| Bounded per-pass reads | `observation_batch_test.go`, `observation_pass_test.go`: 64-run HTTP counts, paginated/expired/partial/error lists, independent views, ownership/mutation checks, fresh mutation operations, shared context, cancellation priority, quotas and restart |
| Safe mismatch diagnostic | `pod_mismatch_test.go`, `pod_contract_test.go`, `pod_mismatch_persistence_test.go`: mutation/permission matrix, redaction, cleanup, quota retention and database reopen |
| Persisted clock and unchanged old keys | Config/API/backend/store `deadline*_test.go`: default/explicit/legacy clocks, caller rejection, activation fencing, structured cause collisions, exact data preservation and idempotency conflicts |
| Comparison clock boundary | `scripts/tests/test_evaluate.py`: new registration requirements, one manifest deadline, unchanged legacy registration hashes and explicit mismatch reporting |
| Controller-independent deadline | `scripts/tests/test_kind_integration.py` plus the live API-stopped case: native deadline location, stable owner UIDs, terminal Job and restored API observation |

The Go unit/fake-client and Python fixture checks validate software contracts. Actual node execution is claimed only for the linked kind campaigns. They do not change the frozen full-requirement audit counts or establish EKS compatibility.

## Next external gate

Use the pinned AWS blueprint and [EKS brief](../EKS_EVALUATION_BRIEF.md) to qualify prepared, fixed workers before comparison. Record the first exploratory installation honestly, then preregister matched repetitions from a defined reset state. Include actual billing or matched operator journals before evaluating the selected benefit gate. No cluster, AWS account or billing evidence was connected during this work.

Retain polling until remaining reporting time is profiled on that environment: container finish, Pod/Job terminal evidence and API commit are distinct boundaries. The single-batch observations do not isolate those phases or justify a stable tail SLO. Postgres/shared ownership, sessions/checkpoints and the provider gateway remain later decisions; this change does not implement them.
