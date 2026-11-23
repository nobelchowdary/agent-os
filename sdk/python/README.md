# Python execution client

`agentos_client.py` uses only the Python 3.10+ standard library. Add `sdk/python` to your import path or vendor this module into an external harness. No package installation, model SDK or Kubernetes credentials are needed by the client. It connects to an already running Agent OS API; server preview mode cannot execute runs.

```python
from agentos_client import AgentOSClient

client = AgentOSClient("http://127.0.0.1:8080", token_env="AGENTOS_TOKEN")
run = client.create_run(
    "python", ["python", "-c", "print(sum(range(1000)))"], 30,
    idempotency_key="my-persisted-tool-call-001",
)
finished = client.wait(run["id"], timeout=360)
print(finished["state"])
print(client.logs(run["id"])["logs"])
```

Set `AGENTOS_TOKEN` securely to a tenant token configured by the API operator. Alternatively pass `token=...` from your application's secret store. Authentication is captured at construction and remains fixed for that client. HTTP is accepted only for literal loopback addresses or `localhost` (pinned to `127.0.0.1`); use HTTPS elsewhere. HTTPS uses the system's verified trust roots. Connections bypass environment proxy settings, and redirects are refused. Exceptions contain fixed error codes and HTTP status numbers, excluding tokens, endpoint URLs and response bodies. Workload logs remain untrusted application output.

`create_run` requires a keyword `idempotency_key` and makes exactly one POST attempt. Persist the key and exact input **before** submitting. An interrupted connection can leave an admitted run even when no response was received. Recover by repeating the identical input with the same key, then use the returned ID. A different key can admit another run, and the same key with different input produces HTTP 409. The key protects admission; Kubernetes execution can still duplicate external effects. The client never runs submitted code locally.

`get_run(id)` and `cancel(id)` return the API run record. Cancellation is a request; inspect status afterwards. `logs(id)` returns `{run_id, logs, limit_bytes}` and enforces the server's 64 KiB log bound. Templates, images, resources, runtime policy and network permissions are administrator-owned; the client cannot supply a PodSpec or credentials to a workload.

Run records and lifecycle events may include a machine-readable `reason_code`. Legacy responses that omit it remain accepted; the client preserves fields and never derives a code from the untrusted human-readable `reason`. When present, the code must be a nonempty known string consistent with its state:

| State | Accepted `reason_code` |
| --- | --- |
| `succeeded` | `completed` |
| `cancelled` | `cancelled` |
| `failed` | `task_error`, `deadline_exceeded`, `oom_killed`, `evicted`, `image_pull_failed`, `policy_denied`, `budget_exceeded`, `infra_failure`, `unknown` |
| `cancelling` | `cancelled` or one of the failure codes above |
| Other nonterminal states | Field must be omitted |

`policy_denied` and `budget_exceeded` are reserved codes; accepting them does not mean a policy engine or model-spend budget producer is implemented. The existing state names are unchanged. A code on `cancelling` describes the cancellation intent or failure context and does not prove the workload has stopped.

The client requires `AgentOS-Events-Version: 3` and a valid `AgentOS-Store-Epoch` response header. It does not downgrade to numeric-only events on an older server. Deploy a format-3-capable API before upgrading clients. Servers retain formats 1(default)/2 for existing consumers, but those formats cannot detect database restore.

`iter_events(id, after=0, timeout=300)` starts fresh and yields lifecycle `run` events. Each contains `sequence`, `run_id`, `store_epoch`, `at`, `state`, and optional `reason`/`reason_code`. It validates contiguous IDs, tenant-owned run identity, epoch continuity and reason codes before advancing its cursor. It skips heartbeat comments, consumes terminal `end` internally, and reconnects from the last validated `epoch:sequence` ID. A terminal HTTP 204 is confirmed with an authenticated terminal record carrying the same epoch header. `wait` also checks that header on its final GET.

Persist `event["store_epoch"] + ":" + str(event["sequence"])` together with the run ID and tenant identity after processing an event. Pass that complete string as `after` to resume; positive integer cursors are rejected before networking. HTTP 409 `store_epoch_mismatch`, a changed successful response epoch, or invalid framing stops consumption without resetting or retrying. Reconcile acknowledged tasks and external effects before deliberately starting fresh. A restore cannot recover post-backup submissions or idempotency keys. See the [restore contract](../../docs/STORAGE_RECOVERY.md#client-recovery-contract).

```python
from contextlib import closing

with closing(client.iter_events(run["id"], timeout=360)) as events:
    for event in events:
        print(event["sequence"], event["state"])
```

Close the iterator when stopping early so its socket is released. The timeout budget includes reconnects, connection establishment and consumer time; server streams may end after 60 seconds and be reconnected. Socket inactivity bounds default to 15 seconds for JSON requests and 20 seconds for streams. After connection establishment, a watchdog interrupts the exchange at its deadline, including a drip-fed partial stream. **OS DNS resolution cannot be interrupted by that socket watchdog and can exceed the caller's deadline before a connection exists.** Its elapsed time still consumes the budget; subsequent work fails if the budget is exhausted. Literal loopback URLs avoid DNS resolution for the local example. `max_reconnects` (default 20) bounds retries. Only event GETs reconnect automatically; POST creation and cancellation have no automatic retry. `wait(id, timeout=...)` consumes events and then fetches a terminal record. `AgentOSError` exposes `code`, `status_code` and `retryable`; `AgentOSTimeout` and `AgentOSProtocolError` specialize it.

This client has local HTTP/SSE stub tests. Those tests establish neither live Kubernetes isolation nor compatibility/performance on an actual cluster.

Run records may include a validated `result` attachment with an observed `exit_code`, bounded `output_head`/`output_tail`, capture/completeness flags and retention timestamps. `logs(id)` serves retained text after cleanup, with additional source/flag/exit metadata. Expired output returns HTTP 410; unavailable output returns 404. List pages omit payloads. A cancelled or deadline-terminated task can have partial or unavailable output and no observed exit code. See [results and cleanup](../../docs/RESULTS_AND_CLEANUP.md) for the exact limits.
