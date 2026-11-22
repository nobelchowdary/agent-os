# 0006: Restore epochs and evidence-based failure attribution

Date: November 22, 2026. Status: accepted for the experimental single-owner service. Cluster acceptance remains open.

## Context

Review of `8ec0900` identified ambiguous eviction attribution, startup waits after dispatch and saved cursors that could silently name a different restored history. The proposed nine failure-code names already existed; declaring them is separate from producing trustworthy causes.

## Decision

Keep bbolt as the single-owner alpha metadata store. Add schema 2 with a durable random epoch. Migrate supported 0/1 metadata without rewriting run/spec/event/idempotency values. Backup preserves source bytes. Drained offline restore verifies its trusted source checksum, rotates the epoch in a private temporary copy, independently validates the result and publishes without overwrite. Report source and destination hashes separately.

Bind pagination and SSE format 3 cursors to the epoch. A stale owned cursor fails with HTTP 409; clients reconcile acknowledged work and external actions before starting fresh. Preserve formats 1/2 for existing consumers, explicitly without restore detection. The updated Python client requires format 3 and checks epoch continuity through terminal status confirmation. Recovery does not reconstruct post-backup admissions or keys, restore task files, fence workers or authorize replay.

Use structured evidence for failure codes. A true recognized platform `DisruptionTarget` condition needs failed/deleting Pod evidence. Generic eviction stays `unknown`; `evicted` remains reserved until task-limit attribution is reliable. Persisted cleanup intent and terminal outcomes retain their cause across restart. Contradictory terminal Job conditions fail observation, retaining reservations. Bound pending startup after dispatch while preserving actual running/terminal outcomes. Never automatically retry infrastructure failures before external-effect and fencing contracts exist.

Use the proposed smaller Phase 0 scope (C1–C10 and C12) as a **local qualification target**, with a requirement-to-test matrix and a prepared isolated kind cluster. It has not passed: some required mechanisms do not yet exist, and fake clients cannot establish runtime/CNI enforcement. C11/C13–C15 production availability, tenancy and upgrades remain explicit later gates; current tenant-scoped admission limits remain enforced. The full frozen 70-requirement audit is not relabeled by this milestone. Decide shared storage/ownership before a separate gateway writes usage, budgets or side-effect state; a second process cannot write this bbolt file.

## Consequences

Cursor and checksum consumers need the documented compatibility changes. Offline maintenance still requires downtime and drained work; live backup/HA and active disaster recovery remain unfinished. Requirements, evidence and the historical review patch are in [the review bundle](../reviews/2026-11-22/README.md). No competitive ranking or performance claim advances without the corresponding measurements.
