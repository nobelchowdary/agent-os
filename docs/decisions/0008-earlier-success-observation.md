# 0008: Measure terminal observation before accelerating success

Date: November 24, 2026. Status: proposed; the profiling change alone does not change runtime outcomes.

## Context

The reporting work through `5b6e451` reduced API traffic but still measured substantial container-exit-to-terminal delay. Kubernetes Job accounting and finalization are possible contributors. Source defaults and an estimated write count are a model, not measured effective controller limits or per-component service times. The next experiment must distinguish observation boundaries before changing the outcome contract.

This serves short bounded agent tools and synchronized bursts of successful tasks. It does not claim model-token efficiency, hardware optimization or a customer cost benefit. The selected external comparison gate remains 20% lower fully accounted cost per correct task or 25% less median operator time.

## Measurement and decision rule

Before reading the new 64-task diagnostic result, use these conditions to decide whether to continue:

1. Reach the existing actual-concurrency and complete-output gates. Require all 64 Pod/Job identities and the timing boundaries used for the decision to be present, consistent and from watch receipts rather than initial terminal snapshots. Preserve missing, conflicting and negative samples; incomplete coverage makes the attribution inconclusive.
2. For each run, compare the same-harness-clock interval from first observed terminal Pod to first observed terminal Job with the interval from that Pod receipt to the first terminal API response. Treat Job observation as the dominant portion only when its median per-run fraction exceeds one half. Sequential API polling and watch delivery still affect those observations; no exact service-time attribution is claimed.
3. Consider the narrower success-decision optimization only if the median interval from first observed `SuccessCriteriaMet=True` to first observed terminal Job is at least one second. Otherwise retain the current behavior and publish the measurement rather than adding an unproven path.

Keep the existing container `finishedAt` to persisted API-terminal-event metric. Also retain source Job completion/condition timestamps and harness receipts separately. Pod phase has no authoritative API-server transition timestamp. Job `completionTime` is controller-generated, not the timestamp of an API-server write. Do not assign clock skew, watch transport delay or sequential polling delay to a particular component.

## Candidate contract

A terminal Pod alone is not a general proof of the Job outcome: replacement executions, earlier failures and aggregate accounting can matter even with one completion and no retries. If the measurement supports acceleration, prefer an already recorded Kubernetes `SuccessCriteriaMet=True` decision plus a single approved successful Pod. Kubernetes v1.37 [checks an existing success decision before later failure evaluation and delays final completion for accounting](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/job/job_controller.go).

Select any accelerated path from the durable record's explicit Pod scope. Verify the live Job against that durable execution and reject unsupported policies, conflicting conditions, active/terminating work, failed history, other success UIDs, Pod mutation, deletion, disruption, nonzero exits or restarts. Preserve result persistence before terminal state, cancellation CAS precedence, quotas and terminal immutability. Legacy/explicit Job clocks keep waiting for final Job conditions.

Cleanup must make fresh ownership, policy and outcome checks before UID-fenced deletion. If early deletion prevents final Job completion evidence, label it censored/missing; never fabricate zero delay. A later contradiction remains a cleanup/operational diagnostic and cannot rewrite a terminal result. The final implementation and its acceptance evidence must be recorded before accepting this proposal.
