# Structured outcome and cleanup codes

New run records and stored lifecycle events may contain `reason_code` alongside the human-readable `reason`. Use the code for decisions; messages are descriptive. SSE format 1 remains the default and omits the field for older strict clients. Formats 2/3 include it. The updated Python client requires format 3 with restore-aware cursors; see [live events](LIVE_API.md). Codes do not change public run state names: a deadline still produces `failed`. `completed` describes execution outcome, not proof that the business task was solved correctly.

| Code | Current evidence / producer |
| --- | --- |
| `completed` | An authoritative completed Job or an explicitly successful terminal transition. |
| `task_error` | A nonzero exit from the owned Pod's declared agent container. Exit status alone does not establish OOM. |
| `deadline_exceeded` | A structured Kubernetes Job deadline condition. A failure target requests cleanup first; terminal Job failure can record the final outcome. |
| `cancelled` | Explicit API cancellation intent, carried through observed cleanup to `cancelled`. |
| `oom_killed` | The owned agent container's current terminated status explicitly names `OOMKilled`; log/message text and exit 137 alone are insufficient. |
| `evicted` | Reserved for positively attributed task-limit eviction. Generic Pod `Evicted` is insufficient; there is currently no producer. |
| `image_pull_failed` | An unrecoverable invalid image name, or an image-pull hint when the five-minute startup bound expires, including a dispatched Job whose Pod never starts. Transient pull/backoff status alone does not trigger immediate failure. |
| `infra_failure` | Startup wait exhaustion without a more specific established cause, disappeared execution, externally requested execution deletion, or a failed/deleting owned Pod with a recognized true `DisruptionTarget` platform reason. |
| `unknown` | A failed outcome lacks trustworthy evidence for a more specific cause. This is an explicit uncertainty category. |
| `policy_denied`, `budget_exceeded` | Reserved vocabulary only. No current gateway/policy/budget producer or enforcement is implied. |

Failure evidence before a terminal Job condition is not proof of cleanup. The controller persists `cancelling` with the observed cause and retains the active reservation until the backend confirms Job/Pod disappearance; only then does it record the failed/cancelled result. Physical stop on an unreachable worker remains outside this metadata guarantee. Existing completed Job outcomes remain authoritative.

Cause precedence is explicit: a committed terminal record cannot change. A persisted cancellation/cleanup intent keeps its recorded cause through cleanup and controller restart. Otherwise authoritative terminal Job conditions take precedence over Pod details, and Job deadline conditions take precedence over an OOM hint. Contradictory terminal Job conditions fail observation without committing an outcome or releasing quota. Conflicting Pod causes resolve to `unknown`. For cancellation racing completion, the successful store CAS determines which intent/outcome commits first.

`DisruptionTarget` alone is provisional: a healthy Pod can remain running if disruption is interrupted. The backend requires failed/deleting status and a recognized structured reason. Absence of the condition does not prove task fault. In the pinned Kubernetes v1.37.2 source, node-pressure eviction adds it, while own ephemeral/emptyDir-limit eviction uses the same `Evicted` reason without a structured limit subtype ([Pod conditions](https://kubernetes.io/docs/concepts/workloads/pods/pod-condition/#disruptiontarget), [kubelet source](https://github.com/kubernetes/kubernetes/blob/v1.37.2/pkg/kubelet/eviction/eviction_manager.go)). The alpha therefore reports generic eviction as `unknown` and does not parse message prose. Actual EKS-version behavior still requires cluster canaries. Infrastructure failure never automatically replays an agent task; safe retry remains gated on external-effect and fencing contracts.

The store validates state/code combinations in the same CAS transaction as the state event and quota change. Success permits only `completed`; cancellation permits only `cancelled`; failure permits failure/unknown codes. A cleanup intent may carry cancellation or failure codes. Other nonterminal states do not persist a cause. Same-state transitions cannot rewrite the reason/code or append history, and terminal records remain immutable.

Older records/events may omit `reason_code`. They are returned unchanged: neither startup migration nor read access rewrites historical causes. Legacy terminal failures must not be inferred from free-form prose. The controller retains a narrow compatibility path for the old exact cancellation-intent marker when finishing an already cancelling legacy record; new API cancellation persists its code explicitly.

The Python client accepts legacy omissions and validates present codes and their state association before advancing an SSE cursor. The OpenAPI schema documents the optional field. Submitted JSON cannot supply a reason code, and workload logs/termination messages are never parsed to manufacture one.

This advances C2 locally. It does not complete the requested EKS cause-production matrix; policy/budget producers, real OOM/eviction/resource canaries and the full fault campaign still need their phase gates.
