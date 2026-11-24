# 0009: Register task and caller-outcome boundaries separately

Date: November 24, 2026. Status: accepted for the new pilot protocols. Existing registrations and their rules remain unchanged.

## Context

The original collector finishes an Agent OS trial after its authoritative API outcome and validated output, while its upstream arm uses the Pod's terminal phase and validated output. Those are distinct native service contracts. A single elapsed number hides task completion, outcome reporting and output-validation overhead. The [timing investigation](../robustness/JOB_CONTROLLER_TIMING_2026-11-24.md) also showed that sequential harness polling can distort an attribution denominator.

Job authority remains in place. Stop work on trimming reporting delay at this milestone. Short-call reuse/sessions remain a separate Phase 4 workload and lifecycle design, after EKS qualification and comparison; this change does not introduce them.

## Decision

For all three collector arms, record first observed terminal Pod, first native terminal outcome visible to the collector/caller, and completion of required output validation. Use harness monotonic offsets from submission start and retain UTC receipt timestamps and explicit source/authority labels. These are observations with polling and transport delay, not API-server commit timestamps or identical guarantees across arms.

Agent OS's caller outcome remains its authenticated terminal API response. The direct-Job arm uses the Job condition; the upstream cold-claim arm uses the selected Pod's terminal status under the implemented collector contract. Preserve the ownership/profile checks and exact output validation. A terminal Pod alone cannot create an Agent OS terminal outcome.

Keep existing `elapsed_seconds` semantics and all old protocol/registration JSON hashes. Missing, conflicting or unobserved boundaries must remain explicit. The decision report may use only its registered boundary and must not substitute another field or calculate a favorable subset from incomplete correct trials. Qualification and correctness gates still precede latency and the cost/operator routes.

The founder selected two separate future profiles:

- A nominal one-second elapsed short-tool fixture.
- A nominal sixty-second elapsed task fixture.

Both use the same pinned environment, resources and deterministic file/hash work, with a bounded wait establishing the declared minimum application elapsed time. They are synthetic duration probes, not CPU-throughput or real-agent benchmarks. Register, collect and decide each separately; never pool them or use the long task to erase a short-task regression. Customer workloads can replace either fixture only through a new precollection registration.

For these new format-2 protocols, the selected boundary is `outcome_visible_to_caller`, and the latency allowance is:

```text
candidate successful-task p95 <= baseline successful-task p95 * 1.10 + 2.0 seconds
```

This is an additive two-second allowance, selected before collecting either comparison arm. The rationale is a bounded pilot allowance for the confirmed native outcome while retaining the short-task profile's visible overhead. It is not a measured benefit, customer SLO, additive allowance for every request, or a claim that the native outcome guarantees are equal. Record the ratio, absolute seconds, selected boundary and rationale in the hashed protocol and decision output; freeze them once collection begins.

The existing format-1 strict 1.10 ratio continues to apply to its original validated-completion metric. Do not update an old registration or apply the new allowance to historical kind diagnostics. The cost/operator benefit gate remains 20% lower fully accounted cost per correct task or 25% less median operator time. Full trial count, correctness, qualification, billing and matched operator-session requirements are retained.

## Acceptance

Tests must cover all three arms, delayed API outcomes, early workload completion, missing/deleted Pod evidence, sticky UID ownership, failed outcomes, signed observation ordering, legacy metric/hash preservation, explicit formula boundaries and policy/rationale binding. Missing selected evidence must prevent a latency pass. A supplemental billing/operator file cannot replace measured timing.

Run both profiles on qualified EKS with warm selected workers and available images, retain failures and actual billing/operator evidence, then apply each predeclared rule. No EKS comparison, production latency promise, cost saving or token advantage is established by adding this protocol.
