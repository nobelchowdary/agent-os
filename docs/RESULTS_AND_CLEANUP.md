# Results and finished execution cleanup

The controller captures a bounded result before recording a normal terminal outcome and deleting its Kubernetes execution. The terminal state/history stays immutable. A five-second inventory sweep retries cleanup after a crash or uncertain delete response, including a late suspended creation for a known terminal run. Cleanup validates the recorded execution shape and uses UID preconditions. Unknown managed-label Jobs are counted in `/v1/status` under `controller.orphan_jobs`; they are never adopted, executed or deleted by the sweep. Counts are global operational summaries; unknown run IDs and output are not exposed.

The backend also checks each observed, owned Pod against the approved Job template. It permits documented API defaults, scheduler binding, exact standard tolerations and scheduling/overhead additions authorized by the current RuntimeClass. Images, commands, credentials, volumes, sidecars, security settings and grace cannot be changed by admission. A positively detected mutation requests cleanup as `infra_failure` and its output is refused; ownership-fenced deletion still proceeds. Optional Pod observation failure does not erase an authoritative terminal Job outcome. Custom admission defaults outside these allowances need explicit compatibility work and tests; this check is detection after Pod admission, not a replacement for admission controls or isolation qualification.

Normal terminal capture reuses the Job/validated-Pod evidence from that observation through an opaque, backend/run/Job-UID-bound receipt. It retains independent Pod copies only for that observation; it is not a global Pod cache and is never serialized into the API. The log read still occurs, but a second Job/Pod lookup is avoided. A retry obtains a fresh observation and retains the existing finish-time capture budget. Cancellation and legacy backends keep their fresh capture path. Kubernetes' log subresource has no Pod-UID precondition; reusing observed evidence does not remove that existing endpoint limitation.

`GET /v1/runs/{id}` includes an optional `result` attachment:

```json
{
  "exit_code": 3,
  "output_head": "first line\n",
  "output_tail": "last traceback line\n",
  "output_truncated": true,
  "output_complete": true,
  "status": "captured",
  "captured_at": "2026-11-23T00:00:00Z",
  "expires_at": "2026-11-24T00:00:00Z"
}
```

`exit_code` is present only when observed on the owned Pod's agent container; absence never means zero. Output combines stdout/stderr from Kubernetes logs. Capture retains up to 16 KiB at each end, with no overlap for short output. It scans at most 16 MiB under the backend operation deadline. When that scan cannot reach EOF, `output_complete` is false and the tail is the end of the scanned prefix, not a claimed end of the entire log. Truncation and UTF-8 replacement are explicit. Even a complete readable log can omit older kubelet-rotated data.

`GET /v1/runs/{id}/logs` serves a retained result after its Job/Pod is gone, with the original bounded `logs` text plus capture flags, source and optional exit code. A truncation separator joins the head/tail. History list pages omit result payloads; fetch the individual run. The Python client validates the optional attachment without deriving an outcome from output prose.

Cancellation takes its snapshot before requesting deletion. The process may still be running, so its snapshot can lack a final exit code or shutdown output. Log transport failure must not block an authorized cancellation: an unavailable result is retained when possible and cleanup proceeds. If result persistence itself fails, cleanup is still attempted; the existing cancellation intent/reservation remains until state can be committed. Output may be lost in that storage-failure case.

For a normal terminal observation, transient capture errors are retried for 15 seconds from the Job's completion/terminal-condition timestamp. Controller restart does not restart that clock. Missing or future Job timestamps use the last durable lifecycle-change timestamp. At the first failed attempt after this window, persist `status: unavailable`, preserve any independently observed exit code, commit the authoritative outcome and clean up. Each attempt still has a five-second operation budget; polling and queued operations add delay, so 15 seconds is a retry window rather than a terminal-latency SLO. A Kubernetes log HTTP 400 (including a container that never started) or 404 is final immediately. A storage failure still prevents a normal terminal commit/deletion until the result can be persisted.

A Pod/log already removed by Kubernetes also produces `status: unavailable`; no exit code or missing text is invented. In particular, a Job deadline while the API is stopped can remove its Pods before capture. Guaranteed complete output through those cases requires an independent durable log pipeline, which is not implemented here.

## Retention and storage

Administrator configuration `result_retention_seconds` defaults to 86400 (24 hours), with a maximum of 30 days. The retention clock starts at capture. Reads suppress expired output immediately; periodic pruning removes its persisted payload while retaining exit/capture metadata with `status: expired`. `/logs` then returns 410 `result_expired` without falling back to an old live Pod. Unavailable output returns 404. A repeated capture cannot resurrect or overwrite the first attachment, including after expiry.

Schema 3 stores these attachments separately from immutable run/spec/history/idempotency bytes. Upgrading schema 2 creates the result bucket while preserving its epoch and cursors. Earlier supported schemas still migrate. Drained maintenance validates result references and bounds along with other metadata. Database files and backups now include captured workload output and must remain private. Expiry makes bbolt pages reusable; it does not shrink the file or cap retained run/idempotency metadata. Monitor the PVC; the default 1 GiB volume is not an unlimited archive.

## Controller limits

`startup_timeout_seconds` defaults to 300 and can be set from 1 to 86400. It bounds unstarted work using admission age, including dispatched Jobs whose agent has not started. It does not replace `timeout_seconds`: that remains the activated Job's Kubernetes `activeDeadlineSeconds`, including scheduling/image-pull time after activation. Moving the clock to the Pod would change the API contract and still would not measure process CPU time; this iteration preserves the documented clock.

Each administrator-owned template can set `termination_grace_period_seconds` from 1 to 30. The examples select 10 seconds. Omitted/zero preserves the legacy Kubernetes 30-second default and omitted persisted specification, including old idempotency fingerprints. Grace is persisted with each run; changing a template does not rewrite already admitted runs and can cause an old idempotency key to conflict with a newly resolved policy. Callers cannot set it. Grace is Kubernetes' process shutdown allowance, not a complete cancellation-latency guarantee.

Readiness uses controller progress, storage access and sandbox prerequisite declarations. A per-run reconciliation or log error is reported through `/v1/status` (`run_errors`, `consecutive_failures`); maintenance errors have `maintenance_failed`. They do not remove the API from its Service and prevent other tenants from reading or cancelling. A stopped or stalled controller still fails readiness; bounded-operation progress refreshes its heartbeat during a long pass.

`kubernetes_qps` and `kubernetes_burst` default to 50 and 100, with supported ranges 1–1000 and 1–2000. They set the shared clientset limiter explicitly. Successful declaration readiness checks are cached for three seconds with concurrent lookups coalesced; failures are not cached. A removed RuntimeClass or deny policy can therefore take up to that cache interval to fail a later operation. These checks validate declarations, not actual isolation or network enforcement. Polling remains in place; measured terminal latency at 16 and 64 active runs will inform whether a watch-based replacement is needed.

RuntimeClass declarations use a shared three-second positive cache for readiness and actual-Pod validation, with coalesced concurrent reads and independent copies. Invalid handlers/profiles and failed reads are not cached. RuntimeClass scheduling/overhead changes can take up to that interval to affect validation; this is administrator declaration staleness, not fresh node-isolation evidence. Reusing the declaration avoids reading the same cluster-scoped object once per finished run.

Local acceptance tests cover 200 sequential fixture successes with zero executions left, result-before-delete ordering, exit 3/output survival through reopen, ambiguous delete and crash sweeps, orphan non-adoption, retention, and client compatibility. They do not execute workload code. The separate [kind/gVisor campaign](KIND_TESTS.md) exercises actual kubelets; its result must be read from CI rather than inferred from these tests or its workflow file.
