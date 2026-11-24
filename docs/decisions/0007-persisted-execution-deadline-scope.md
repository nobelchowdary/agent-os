# 0007: Persist the execution deadline clock

Date: November 24, 2026. Status: accepted for new admissions. Live verification of this change must be reported separately from earlier Job-clock campaigns.

## Context

Agent OS originally budgets `timeout_seconds` through Job `activeDeadlineSeconds`, starting when the suspended Job resumes. The upstream Agent Sandbox cold collector sets a Pod deadline. Identical timeout values therefore measure different windows during scheduling and provisioning. The user selected Pod timing for new runs while preserving already admitted behavior, and retained the existing evaluation gates of 20% lower fully accounted cost or 25% less median operator time. Those gates are proposals for registered measurement, not achieved benefits.

## Decision

Add administrator template `deadline_scope` with `job` and `pod` values. An omitted template value defaults to `pod` for new resolutions, and every new resolved specification persists the explicit selected value. Callers still submit only template, command and timeout; they cannot select a clock or supply Pod specifications.

For a persisted `pod` scope, render only `Pod.spec.activeDeadlineSeconds`. Its budget is relative to kubelet `Pod.status.startTime`: image pulling and initialization on the node consume it, but scheduling before node acknowledgement does not. This is not process CPU time, application-only execution time or an admission-to-completion deadline. For persisted `job`, render only Job `activeDeadlineSeconds`, relative to Job start/resume. An omitted scope on a stored specification always means that original Job contract; reads and migration never fill it from current defaults. See the Kubernetes [Pod API](https://kubernetes.io/docs/reference/kubernetes-api/core/pod-v1/) and [Job deadline and suspension semantics](https://kubernetes.io/docs/concepts/workloads/controllers/job/#job-termination-and-cleanup).

Keep the separate admission-age startup wait and asynchronous cancellation/cleanup contract. Node enforcement of a Pod deadline does not depend on the API process being available; the startup wait still depends on reconciliation. Neither Kubernetes object status nor deletion fences an unreachable physical worker. Preserve the existing lifecycle, quota, ambiguous creation/activation and terminal-state rules.

Classify an ownership/spec-verified failed Pod with a configured Pod deadline and structured `DeadlineExceeded` reason as `deadline_exceeded`. Generic exit status or messages cannot create this cause. Competing specific causes remain unknown. Before terminal Job evidence, retain the active reservation and request cleanup; existing authoritative Job outcomes and persisted cleanup intents retain their precedence.

Keep canonical resolved-specification fingerprints unchanged. A narrow idempotency alias permits a retry of an omitted-scope legacy admission to return its original record when clearing only the new incoming scope reproduces the original fingerprint. It changes no stored record, key, event or reservation. Other policy changes and clock changes on explicit-scope admissions still conflict.

Raise the store schema from 3 to 4 as a metadata-only upgrade, preserving existing data and epoch. This fences schema-3 binaries, which could otherwise discard an unknown scope while rewriting a record and change queued work to the old clock. A downgrade requires the matched pre-upgrade backup and binary; no downgrade migration is provided.

Require explicit workload scope for new evaluation registrations and `pod` when an upstream arm is selected. Direct Jobs, Agent OS and upstream then use the same Pod clock. Existing registrations retain their original JSON and hashes; missing scope preserves Agent OS/direct Job clocks and upstream native Pod timing. Reports disclose that mismatch and do not treat it as matched deadline/failure proof. The deterministic collector does not qualify faults, networking or isolation. New failure comparisons need their own registered protocol and live evidence.

## Consequences

New default runs receive more time when scheduling is delayed before kubelet acknowledgement. Admission/startup waiting remains bounded separately while the controller operates. The API exposes the selected clock so clients can interpret their actual run. Earlier acceptance and optimization reports remain evidence for the Job clock they tested; manifest, unit and fake-client checks do not replace the API-stopped live Pod-deadline case or EKS qualification.
