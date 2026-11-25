# 0010: Bound polling uncertainty before a latency decision

Date: November 24, 2026. Status: accepted for new format-3 comparison registrations. This extends [decision 0009](0009-comparison-boundaries-and-latency-budget.md); it changes no existing registration or runtime outcome contract.

## Context

At `7142fea`, Kubernetes comparison calls each started a `kubectl` process. In steady state the upstream cold-claim loop made four Kubernetes calls and read its gated Pod phase on the third; Agent OS made two Kubernetes calls plus two HTTP calls and read its gated API outcome first. Kubernetes submission also discarded a successful create response and fetched the object again. Process startup, authentication, connection setup and unequal polling periods could therefore influence a two-second latency allowance. Their exact cost on EKS has not been measured.

The one-second and sixty-second profiles, caller-outcome boundary, additive `baseline p95 * 1.10 + 2 seconds` allowance and 20% cost / 25% median operator-time benefit routes remain selected. The instrument needs a declared precision contract before it can apply them.

## Decision

Use a new protocol version and new evidence directories. Format 3 adds the hashed observation policy:

```json
{
  "mode": "fixed_cadence",
  "period_seconds": 0.1,
  "max_terminal_gap_seconds": 0.5
}
```

Half a second is the predeclared maximum bracket width per correct trial, one quarter of the two-second absolute allowance. It is an engineering precision requirement, not a measured EKS capability or a new product latency target. Register the cadence and precision limit before collection; do not widen them after seeing data. Existing format-1 and format-2 protocols and example files retain their original behavior and hashes.

Format 3 currently supports `outcome_visible_to_caller` as its gated boundary. Terminal-Pod and validated-completion observations remain diagnostics; unsupported format-3 boundary selections are rejected before registration. Format 2 retains its three selectable point-observation boundaries.

For the new collection path, start one authenticated `kubectl proxy` outside the timed trials on a Unix socket in a private temporary directory. Reuse persistent HTTP channels for baseline creation and Kubernetes reads, including the gated sampler. Separate channels let gate sampling proceed while ownership/profile checks and logs are collected. The proxy owns Kubernetes authentication and upstream connection reuse; this does not promise that an upstream server never reconnects or refreshes credentials. Keep request filtering, explicit context, finite time/size limits and secret-free diagnostics. The official [kubectl proxy reference](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_proxy/) documents Unix sockets and keep-alive behavior. Never expose an unauthenticated proxy on a public listener.

Warm the instrument before the trial clock. On a successful baseline POST, use its returned object; inspect the same intended identity only when the create result is ambiguous. Never retry submission under a new name. Submission itself remains inside each arm's elapsed window; its service/admission work is part of the native contract, while per-call process/login setup is removed.

Use the literal `127.0.0.1` address of the Agent OS port-forward for this pilot. Persistent network requests have bounded socket/TLS I/O, but the system hostname resolver is synchronous and has not been qualified as a bounded part of the instrument. HTTPS certificate/hostname validation remains required for remote endpoints; do not disable it to avoid that distinction.

Sample each arm's selected native outcome on the same fixed monotonic schedule, independently of log retrieval and other checks. Record missed cadence rather than issuing catch-up bursts or hiding delays. Ownership, profile, execution identity and exact-output checks still determine correctness; an unverified sampled object cannot make a trial correct. Job authority remains mandatory for Agent OS.

For the selected outcome, retain the last nonterminal request's start and receipt and the first terminal request's start and receipt. The conservative bracket starts at the last nonterminal **request start**, not its response receipt, and ends at the first terminal response receipt. If no nonterminal response was seen, keep that observation absent and use the trial origin as an explicit conservative lower bound; never invent a nonterminal observation at submission. Keep signed observation ordering and missing phase data visible.

For each arm, report bracket coverage, maximum and p95 width, and nearest-rank p95 lower/upper bounds. If any correct trial lacks its interval or has a width above 0.5 seconds, return `latency_gate.status=not_measurable` and `decision=insufficient_evidence`. Do not calculate a favorable subset. With complete sufficiently narrow brackets, let `B` and `C` be baseline and candidate p95 intervals:

```text
pass only if C.upper <= B.lower * 1.10 + 2.0
fail only if C.lower >  B.upper * 1.10 + 2.0
otherwise latency is not_measurable
```

This guards against a favorable choice of polling endpoint. It is not a statistical confidence interval, a correction for all observer effects or proof of equivalent native outcome guarantees. Qualification and correctness gates still precede latency; a latency pass alone establishes no benefit route.

## First cluster instrument check

Before the upstream comparison, register and run `direct-job` versus `agentos` with the short fixture, the same qualified nodes/images/resources and the new observation policy. Preserve both arms' request intervals, Pod/outcome/validated boundaries, missed cadence and complete output. Inspect the measured direction and magnitude of the delta, arm order and submission-to-Pod behavior before trusting a comparative result.

There is no independently known one-second answer. Direct Jobs start active; Agent OS also performs durable admission, suspended creation, activation and reconciliation. Separate executions encounter scheduling, Job-controller and network variability. An unexpectedly faster Agent OS result or a large positive delta is a prompt to investigate and repeat under a new retained registration, not proof that the collector is broken or a reason to tune a threshold until it passes. The instrument-check protocol is a diagnostic exercise, not a claim of a product advantage against plain Jobs.

For a direct-Job baseline and Agent OS candidate, emit an `instrument_check` diagnostic before qualification routing. Report the observed p95 difference and its conservative bounds `[C.lower - B.upper, C.upper - B.lower]`, with `expected_difference_seconds=null`. Flag a wholly negative interval as `candidate_faster_than_job_path`; flag a lower bound greater than the registered two-second absolute allowance as `additional_delay_requires_review`. These are review prompts, not calibrated truth or an automatic correction of the product gate.

## Acceptance and limits

Test persistent transport reuse and cleanup, ambiguous POSTs, fixed scheduling under slow auxiliary work, missing prior observations, terminal responses during submission, identity drift, oversized/ambiguous intervals, nonfinite input, legacy compatibility and hash binding. Run `make check`. Real EKS instrument precision and both comparison profiles remain unmeasured until executed on the selected cluster. No runtime, isolation, token, cost or density advantage follows from this collector change.
