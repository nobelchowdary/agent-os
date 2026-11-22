# Structured outcome and cleanup codes

New run records and stored lifecycle events may contain `reason_code` alongside the human-readable `reason`. Use the code for decisions; messages are descriptive. SSE format 1 remains the default and omits the new field for older strict clients. Request `AgentOS-Events-Version: 2` to receive it; the new Python client negotiates this automatically. The terminal `end` frame and all sequence IDs remain unchanged. Codes do not change public run state names: a deadline still produces `failed`, not a new `timed_out` state. `completed` describes execution outcome, not proof that the business task was solved correctly.

| Code | Current evidence / producer |
| --- | --- |
| `completed` | An authoritative completed Job or an explicitly successful terminal transition. |
| `task_error` | A nonzero exit from the owned Pod's declared agent container. Exit status alone does not establish OOM. |
| `deadline_exceeded` | A structured Kubernetes Job deadline condition. A failure target requests cleanup first; terminal Job failure can record the final outcome. |
| `cancelled` | Explicit API cancellation intent, carried through observed cleanup to `cancelled`. |
| `oom_killed` | The owned agent container's current terminated status explicitly names `OOMKilled`; log/message text and exit 137 alone are insufficient. |
| `evicted` | An owned failed Pod has the structured `Evicted` reason. |
| `image_pull_failed` | An unrecoverable invalid image name, or an image-pull hint when an existing pre-dispatch startup bound expires. Transient pull/backoff status alone does not trigger immediate failure. |
| `infra_failure` | Startup wait exhaustion without a more specific established cause, disappeared execution, or externally requested execution deletion. |
| `unknown` | A failed outcome lacks trustworthy evidence for a more specific cause. This is an explicit uncertainty category. |
| `policy_denied`, `budget_exceeded` | Reserved vocabulary only. No current gateway/policy/budget producer or enforcement is implied. |

Failure evidence before a terminal Job condition is not proof of cleanup. The controller persists `cancelling` with the observed cause and retains the active reservation until the backend confirms Job/Pod disappearance; only then does it record the failed/cancelled result. Physical stop on an unreachable worker remains outside this metadata guarantee. Existing completed Job outcomes remain authoritative.

The store validates state/code combinations in the same CAS transaction as the state event and quota change. Success permits only `completed`; cancellation permits only `cancelled`; failure permits failure/unknown codes. A cleanup intent may carry cancellation or failure codes. Other nonterminal states do not persist a cause. Same-state transitions cannot rewrite the reason/code or append history, and terminal records remain immutable.

Older records/events may omit `reason_code`. They are returned unchanged: neither startup migration nor read access rewrites historical causes. Legacy terminal failures must not be inferred from free-form prose. The controller retains a narrow compatibility path for the old exact cancellation-intent marker when finishing an already cancelling legacy record; new API cancellation persists its code explicitly.

The Python client accepts legacy omissions and validates present codes and their state association before advancing an SSE cursor. The OpenAPI schema documents the optional field. Submitted JSON cannot supply a reason code, and workload logs/termination messages are never parsed to manufacture one.

This advances C2 locally. It does not complete the requested EKS cause-production matrix; policy/budget producers, real OOM/eviction/resource canaries and the full fault campaign still need their phase gates.
