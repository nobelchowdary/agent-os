# 0004: Audit requirements and enforce phase 0 contracts

Date: November 21, 2026. Status: accepted for local phase 0 work; EKS exit gates remain unmet.

## Context

A supplied robustness proposal defines 70 requirements and an EKS test matrix. It was written without repository access, so requested capabilities must be compared with source and actual evidence before they are treated as implemented. Some unconditional termination, cancellation, remote-effect and availability claims require narrower failure assumptions or an architecture change.

## Decision

Preserve the source proposal and freeze a full baseline audit at `e2ed1a4`. Rate each whole requirement and cite narrower local evidence separately. Retain Not run for all unexecuted EKS tests. Follow the upstream-first phase ordering; do not use a local audit to waive gateway, driver or hardware qualification gates.

Close two local core gaps now: C1 gets a centralized, acyclic lifecycle graph enforced inside Store CAS transactions, and C12 gets an embedded OpenAPI contract, stable application error codes, stricter bounded request validation and contract/fuzz tests. Keep existing wire states and resolved-spec fingerprints. Document forward backend-observation recovery edges and legacy optional API idempotency keys. Neither change completes its whole requirement or phase 0.

Do not add automatic infrastructure replay, orphan adoption, new runtime/gateway implementations, shared-store replicas or public release claims. Missing activated work remains unreplayed; cancellation retains quota until cleanup observations permit a terminal transition. A local journal cannot make arbitrary remote writes exactly once, and current bbolt ownership cannot provide zero-downtime HA merely by adding leader election.

## Consequences

Later contributors can find the proposal, every gap and the actual acceptance evidence through [the robustness index](../robustness/README.md). The remaining phase 0 work includes reason-code compatibility, backup/migration/restore, replay/fencing policy, admission/resource/fairness requirements and the conflicting C11/P6 availability timing. EKS preparation and a selected context/spend bound are required for the real cluster acceptance campaign.

API request compatibility tightenings are documented in [the contract guide](../API_CONTRACT.md). `make check`, short native fuzzing and image packaging checks provide local evidence only. No performance, isolation, production availability or commercial advantage follows from those checks.
