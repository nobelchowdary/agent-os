# Launch and observe work

Agent OS exposes a bounded execution service for an external agent harness. The harness can call a model using its own credentials, choose a tool action, submit that action to Agent OS and consume its result. Model credentials remain outside the workload. Treat returned tool output as untrusted input when passing it back to a model or another program.

A prepared, independently qualified Kubernetes runtime/CNI is required for execution. Local preview renders plans and does not execute them. No EKS execution result or full in-sandbox AI-agent integration has been established. Workload outbound traffic remains denied; running an entire network-connected agent inside a sandbox requires the future controlled provider/tool integration.

## Launch from a harness

The [Python client](../sdk/python/README.md) uses only the standard library. With the API installed and port-forwarded as described in [getting started](GETTING_STARTED.md), run the [tool example](../examples/agent-harness/README.md). It submits a deterministic Python task, follows its lifecycle and reads bounded logs; it makes no model calls.

All run endpoints require an `Authorization: Bearer` header. Do not put credentials in URLs, commands, idempotency keys or logs. The API accepts an administrator-defined template, command and bounded timeout; callers cannot choose an image, runtime, PodSpec, credentials, mounts or resources. An idempotency key identifies one submission. Retain the original body and key if a POST response is lost; use that same pair to resolve the ambiguous result rather than submitting another task identity.

## Resumable lifecycle events

```sh
curl --no-buffer --fail-with-body \
  -H "Authorization: Bearer ${AGENTOS_LOCAL_TOKEN}" \
  "http://127.0.0.1:8080/v1/runs/${AGENTOS_RUN_ID}/events"
```

`GET /v1/runs/{id}/events` returns `text/event-stream`, starting with committed history. A lifecycle frame is:

```text
id: 2
event: run
data: {"sequence":2,"run_id":"RUN_ID","at":"2026-11-21T12:00:00Z","state":"starting"}

```

Sequence IDs are one-based positions in the run's append-only durable event history. Resume with **either** `?after=2` or `Last-Event-ID: 2`; the next event has ID 3. The server rejects ambiguous, malformed or future cursors. IDs are scoped to one run; authorization always uses the authenticated tenant. Foreign and absent runs both return 404. Restart/reconnect replay comes from the database; no separate memory-only event queue is authoritative. Keep the database when restarting the API.

Default stream version 1 preserves the original fields for older strict clients. Request header `AgentOS-Events-Version: 2` to receive optional `reason_code` alongside `reason`; the new Python client sends it automatically and still accepts older servers without codes. The response echoes the selected version. Sequence IDs, resume cursors and terminal framing are identical across formats. Unsupported/ambiguous versions return 400. The [outcome guide](OUTCOME_CODES.md) defines cause evidence and legacy omissions. A terminal `end` frame still has only `run_id`, `state` and `last_event_id`; consume the last version-2 lifecycle frame or fetch the record for its code.

Comments provide a connection acknowledgment and ten-second idle heartbeats. Terminal streams end with an `event: end` frame containing `run_id`, `state` and `last_event_id`, without a new event ID. A request already caught up with a terminal run returns 204. If a connection closes without a terminal frame, reconnect from the last fully validated event ID and recheck run status. JSON encoding preserves reason strings without allowing them to inject SSE fields.

Connections rotate after 60 seconds or 256 lifecycle frames. Reconnects may redeliver an event if the client did not persist its cursor before disconnecting; consumers must tolerate that. Each API process allows four streams per tenant and 32 total; excess streams receive 429 with `Retry-After`. Writes/flushes have five-second deadlines; slow clients are disconnected without blocking reconciliation or retaining a database transaction. Request cancellation and process shutdown release stream capacity. Disable proxy buffering and allow idle periods longer than the heartbeat interval when deploying behind a proxy. Browser clients need a client that supports bearer headers; native EventSource cannot set that header.

Events are observed lifecycle changes, not a hard real-time guarantee or a live stdout stream. `running` currently acknowledges dispatch; it does not prove the sandbox process is ready. Kubernetes progress is observed by periodic reconciliation, normally at one-second intervals when passes finish promptly. Store commits wake the loop immediately for new admission and cancellation. Backend call latency, contention, proxies and reconnects affect delivery. A cancellation event records intent; physical termination remains asynchronous, especially on partitioned workers. Logs remain bounded, on-demand Kubernetes reads and are not durably archived.

## Operational status and history

Authenticated `GET /v1/status` reports the selected runtime profile, the caller's active reservations/limit, counts by state and controller health. Counts exclude other tenants. Controller status contains only loop state, timestamps and consecutive failed-pass count; backend error text and workload credentials are not exposed. Failed workload results and normal pending deletion do not count as controller failures.

In the executable, `/readyz` checks storage, backend declarations and the running controller. It becomes unavailable before the first completed pass, after a failed pass, when a completed pass is older than 30 seconds, or when the loop stops. A successful pass clears failure status. Liveness remains a process check. These checks do not qualify node isolation. Bounded durable admission can still accept requests during an outage when directly reachable; readiness is not a transactional admission gate. Long passes at the alpha's maximum concurrency can exceed the staleness threshold and need investigation.

`GET /v1/runs?limit=25` returns `runs` and `next_cursor`. Request older records with `GET /v1/runs?limit=25&before=OPAQUE_CURSOR`; use a URL encoder rather than editing the cursor. Limits range from 1 to 100 and default to 100. A blank `next_cursor` means the end. Pagination uses the authenticated tenant's creation-time/ID index, so newer admissions do not shift the boundary. It is not a snapshot across requests. Cursors are versioned navigation values, not authorization tokens. Unknown/duplicate parameters and malformed or foreign-tenant cursors return 400.

## Remaining integration gates

The service still runs as one process with one bbolt database owner. Model/tool gateway enforcement, provider spend budgets, persistent workspaces, checkpoint recovery and warm agent sessions remain separate milestones. The [EKS evaluation runbook](EVALUATION_RUNBOOK.md) covers real-node qualification, Substrate continuity before comparisons, the AWS blueprint baseline and preregistered decision gates. Runtime-profile names, local tests and successful container builds cannot establish those live results.
