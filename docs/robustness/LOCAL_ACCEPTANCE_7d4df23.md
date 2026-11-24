# Local acceptance before EKS

Tested implementation: [`7d4df23450a999d7dfbbe54e444e4d9e359fdccd`](https://github.com/nobelchowdary/agent-os/tree/7d4df23450a999d7dfbbe54e444e4d9e359fdccd). [Standard CI](https://github.com/nobelchowdary/agent-os/actions/runs/45342334181) and the [full real kind/gVisor campaign](https://github.com/nobelchowdary/agent-os/actions/runs/45342334153) both passed. Local `make check` passed Go race tests, vet, build, preview HTTP checks and 167 Python tests. Later documentation-only commits record this evidence without changing runtime or test sources.

This is local Kubernetes acceptance for bounded commands. It does not close the full robustness audit or qualify EKS, Nitro, comprehensive isolation, connected model access, production operation or a competitive efficiency claim. Earlier failed campaigns remain in Actions history; their configured 200/500 targets are not completed-run evidence.

## Review to evidence

| Review item | Implemented behavior | Verification |
| --- | --- | --- |
| Finished run blocked by log failures | Retry window is 15 seconds from stable Job finish time; HTTP 400/404 is final immediately; unavailable output retains an observed exit status | Permanent-error/restart/HTTP/stream tests; `finished_kubelet_unavailable` passed with `succeeded`, unavailable output, exit 0 and ready API |
| Per-run error removes the API from its Service | Readiness checks loop progress, storage and sandbox declarations; run/maintenance failures remain diagnostic status | API/controller tests preserve reads and cancellation; unavailable-kubelet live case remained ready |
| Own-limit eviction has no producer | Pinned kubelet disruption and started-container evidence distinguishes task-limit from infrastructure eviction | Table tests and live `disk_limit` produced `evicted`; node drain produced infrastructure failure |
| Cancellation uses default 30-second grace | Administrator templates can persist an explicit grace; examples select 10 seconds and legacy omission preserves 30 | SIGTERM handler confirmed ready; watched deletion grace 10 seconds; observed stop 11.629 seconds |
| Targeted CI cancels the full campaign | Concurrency group includes selected case | Workflow lint and overlapping full/targeted runs; current full campaign completed |
| Check the actual admitted Pod | Compare execution/security fields with the approved Job; allow only verified defaults/runtime additions; mutation requests cleanup and refuses output | Fake-client mutation matrix and real canonical Pod execution; no live mutating-webhook campaign was run |
| Portable cluster tests | Explicit context/kubeconfig/nodes/images and API egress; fresh external namespaces required; unsupported Docker fault is skipped externally | Safety/ordering tests and full dedicated kind execution; external/EKS execution remains not run |
| Boundaries and pressure | Controlled before/after network probes, filesystem/token checks and bounded process pressure with trusted PID observations | Peer/API/DNS/metadata denials measured on two worker controls; partial scope described below |
| Sustained cleanup and restart behavior | Durable results, terminal cleanup, immutable outcomes and no automatic replay | 200 sequential successes with zero remaining Jobs/Pods; 500 unique keys submitted twice during 64 API Pod deletions, with one observed Job/Pod UID and terminal event per key |
| Measure 16/64 active work | Require actual running-container concurrency and timestamp overlap; retain reporting lag and output-availability counts | Both load cases passed with the requested peaks and complete captured output |

`timeout_seconds` retains the activated Job deadline, including scheduling/image pulls. The separate startup timeout remains configurable. No scheduling-based timeout contract was introduced.

## Measured reporting delay

| Actual concurrent tasks | Samples | Pod exit to terminal p50 | p95 | Complete captured output |
| --- | --- | --- | --- | --- |
| 16 | 16 | 3.438 s | 5.258 s | 16/16 |
| 64 | 64 | 15.225 s | 22.505 s | 64/64 |

These are empirical quantiles from one batch per size, on two kind workers sharing one Docker runner. Each load task requested/limited 25 millicores, 64 MiB memory and 16 MiB scratch, and waited for a common release time. API client limits were 50 QPS/100 burst. The workload holds concurrency; it is not a real-agent throughput or hardware-efficiency benchmark.

Lag uses the node's container `finishedAt` and the API's committed terminal-event timestamp. It includes Kubernetes/runtime/controller observation effects, not solely Agent OS processing. Clock skew is unqualified; neither batch had negative samples. The small sample sizes do not establish stable tail SLOs. These observations motivate profiling and EKS measurement before choosing a watch implementation.

## What the process-pressure case established

The pressure fixture failed with container exit 128 after its last structured progress report at 40 children. It is retained as a failed execution. The trusted observer sampled the same Pod's cgroup hierarchy 107 times: configured host PID limit 128, sampled/kernel peaks 128, PID-limit event count 0 to 1. The cgroup was subsequently removed. Selected nodes and API were ready after the exercise, cleanup completed, and a fresh gVisor workload succeeded in the selected pool.

This satisfies the declared pressure-test alternative: a failed execution with a correlated host-limit event and successful recovery. It does not prove that 40 is a guest process ceiling, identify a unique nested limiting cgroup or establish the exact cause of exit 128. The follow-up may use another selected worker. Pinned gVisor does not enforce guest `RLIMIT_NPROC`; the [campaign guide](../KIND_TESTS.md) links the primary sources and observer contract.

## Remaining qualification

The network observations cover the named peer, Kubernetes API TCP, CoreDNS UDP and metadata TCP paths against reachable controls. No controlled external listener was supplied. General internet egress, IPv6, redirects, alternate protocols, startup races, host escape resistance, a controlled host-filesystem marker and a precise guest PID ceiling remain unqualified. Overall network/isolation qualification remains incomplete.

Next run the documented [EKS qualification](../EKS_EVALUATION_BRIEF.md) on prepared fixed nodes and then compare matched fault/workload cases with the upstream stack. EKS access and billing evidence have not been supplied. Gateway/provider accounting, durable workspaces and the shared-store decision remain separate work.
