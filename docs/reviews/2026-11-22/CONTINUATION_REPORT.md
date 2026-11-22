# Review corrections and local validation

Date: November 22, 2026. Revision: the commit containing this report, continuing from `8ec09000429ec277bd779f04d691dcc2ee3a1079`. The historical report and exact `e2ed1a4 → 8ec0900` patch are preserved separately. This report records local implementation and validation; Phase 0 has not exited.

## Changes against the four review concerns

| Concern | Result |
| --- | --- |
| One cause when facts collide, C2 | Added condition/Pod-order tables and CAS/restart tests. Committed terminal outcome or cleanup intent remains immutable. Authoritative noncontradictory Job outcomes precede Pod details; Job deadline precedes OOM. Contradictory terminal Job conditions fail observation, retaining capacity; competing Pod causes remain unknown. |
| Infrastructure versus task eviction, C2/C8 | Recognized true platform `DisruptionTarget` with failed/deleting owned Pod evidence produces `infra_failure`. A provisional marker on a healthy Pod does not trigger cleanup. Generic `Evicted` and own-limit prose produce `unknown`; absence of a marker cannot establish task fault. `evicted` is reserved until a trustworthy producer exists. No infrastructure retry or replay is enabled. |
| Complete vocabulary and bounded start, C2/C6/C12 | All nine C2 names already existed at `8ec0900`, plus `completed`/`unknown`. Current unproduced names are `evicted`, `policy_denied`, `budget_exceeded`; vocabulary is not enforcement. The existing five-minute admission-age startup bound now also covers dispatched Jobs whose agent has never started. Image-pull hints determine `image_pull_failed` at expiry; other positive pending observations use `infra_failure`. Actual running/terminal observations remain authoritative; transient API errors cannot establish expiry. |
| Restore promises and saved cursors, C9/C14 | Schema 2 stores a durable random epoch. Drained restore verifies the source hash, rotates epoch in a private copy and independently validates final bytes before no-overwrite publication. Pagination and SSE format 3 bind the epoch; stale owned cursors fail 409 even when the sequence still fits. The SDK validates header/frame/ID continuity, including terminal GET, and never resets or downgrades. Source and final checksums are returned separately. |

The [requirement-to-test matrix](REQUIREMENT_TEST_MATRIX.md) maps C1–C10, C12 and C14 to actual named tests and explicit remaining acceptance gaps. The [restore contract](RESTORE_CONTRACT.md) explains lost post-backup admissions/keys, missing Jobs and external effects. Active restore remains refused. Offline backup still needs downtime and drained work; it does not satisfy live backup or fresh-cluster recovery acceptance.

## Checks actually run

| Check | Observed result | Evidence limit |
| --- | --- | --- |
| `make check` | Passed Go format/vet, full Go race suite, **99 Python tests**, compiled binary and preview smoke. | Unit/property/fuzz seeds, local databases and HTTP, fake Kubernetes clients; no real task execution. |
| Scoped store/backend/controller races | Passed during implementation; integrated full race suite also passed. | Restart/CAS/ambiguous operation behavior under modeled faults; not the full process/node/cluster campaign. |
| API/client interoperability | `TestPythonClientAgainstRealAPIAndController` passed using real local HTTP, store and controller with a nonexecuting backend fixture. | The fixture acknowledges work and never runs the submitted command. |
| Drained restore API test | `TestEpochEventsAndPaginationAcrossOfflineRestore` passed using actual offline local backup/restore and HTTP handlers. | No PVC, Kubernetes Job or workload restore. |
| `make image IMAGE=agent-os:review-2026-11-22` | Passed; local image `sha256:7b77f1cbd0bee65871843226efdddcd739fd6af7c0f0a835d2ea1a849c79d643`, Linux/arm64, user 10001:10001. | Local packaging, no registry publication or node isolation qualification. |
| Restricted image invocation | `--help` succeeded with network disabled, read-only root, all capabilities dropped and no-new-privileges. | Confirms packaged executable starts under these restrictions; no agent task was launched. |
| Review artifacts | Historical patch/report checked byte-for-byte against Git objects; links and named test references checked. Downloaded workbook SHA remained unchanged. | Workbook ratings are source opinions, not an independently rerun competitive evaluation. |

The Python total grew from 93 to 99. Six new SDK methods cover cursor validation, format/epoch negotiation, pre-first-event restore conflicts, changed successful reconnect epochs, frame consistency and terminal-confirmation restore races. New Go tests cover cause permutations/CAS/restart, pending startup after dispatch, schema migration, epoch persistence/rotation, source preservation, transform failures and tenant-scoped cursor rejection. See the matrix for exact function names rather than interpreting the test count as requirement completion.

## Compatibility and phase decisions

SSE formats 1(default)/2 retain numeric IDs and original payloads, with weaker restore semantics. Updated Python consumers require format 3 and must store the opaque cursor with tenant/run identity; positive numeric resume values are rejected. Upgrade the API before these clients. Unbound old pagination cursors are rejected. Normal restart preserves schema-2 epoch and valid cursors. Restore changes destination metadata, so its `sha256` differs from `source_sha256` and from the original backup. Existing run/event/spec/idempotency bytes remain preserved.

[Decision 0006](../../decisions/0006-restore-epochs-and-cause-evidence.md) records C1–C10/C12 as the smaller local qualification target, with later production gates retained. Current tenant scoping and transactional active-run limits stay enforced. The original 70-requirement audit and historical report stay frozen. Neither a kind nor an EKS acceptance campaign ran, and no whole requirement is newly marked complete.

The store is confirmed as synchronous single-owner bbolt. A separate gateway or replica cannot directly write it. PostgreSQL/shared authority remains a proposal to decide before Phase 2, with ownership/fencing, outbox, accounting and migration contracts still to design. HA, live backup, safe infrastructure replay, connected-agent gateway enforcement and token-spend budgets remain unfinished. Runtime/CNI/real-node resource qualification and benchmark collection remain next evidence gates. No savings, performance lead or competitive-rank change is claimed.
