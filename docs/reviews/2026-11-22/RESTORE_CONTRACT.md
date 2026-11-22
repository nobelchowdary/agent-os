# Stopped, drained metadata restore

Date: November 22, 2026. Status: implemented local contract for the continuation after `8ec0900`, as recorded in [the continuation report](CONTINUATION_REPORT.md). This does not change the [historical schema-1 report](PHASE_ZERO_2026-11-22.at-8ec0900.md). It is not the Phase 4 active-session/checkpoint recovery contract.

## Scope and authority

The database is private bbolt metadata with one exclusive API owner. Supported restore requires a stopped service, drained backup and absent destination. Every backed-up run must be terminal: queued, starting, activating, running and cancelling records are refused. There is no active-restore, force-replay, orphan-adoption or overwrite switch.

The operator must also stop admissions to the deployment being replaced, drain it where possible, and stop its API owner before cutover. A file lock protects this supported local owner; it cannot prove that a different cluster, copied database or partitioned worker stopped. Offline maintenance does not contact Kubernetes. It neither deletes live Jobs nor reconciles external effects.

## Data preserved and data excluded

| Subject | Contract |
| --- | --- |
| Retained run metadata | Preserve run/tenant identity, resolved specifications, event order and bytes, timestamps, stored reason codes, indexes and every present idempotency binding from the validated backup. Do not invent missing historical cause codes. |
| Schema | Verify supported source layout before any source mutation. Check/backup remain read-only, including legacy schema 0/1. A restored private copy migrates to schema 2 before publication. Unsupported future or malformed schema fails closed. |
| Workload state | No workspace, memory checkpoint, live process, connection, log archive or Kubernetes resource state is included. Metadata recovery cannot claim execution recovery. |
| Post-backup writes | Admissions, state/history changes and idempotency keys committed after the backup are absent. No local integrity check can reconstruct them or infer a lost key that was never stored with a retained record. |
| External side effects | Model/tool writes may already have happened. Neither a restored terminal record nor the absence of a record proves an external effect did or did not occur. Resolve them separately against retained request/operation IDs and the authoritative external system. |

## Source checksum and destination identity

1. Inspect the source under the offline lock and validate bbolt pages plus application records, histories, indexes, reservations and every present key/fingerprint binding. Require the source to be drained. Copy to a private temporary file in the destination directory.
2. Hash the exact source bytes. The operator supplies the trusted retained SHA-256 of the input backup. Reject a mismatch before transforming or publishing the destination. This checks integrity relative to that value; it does not authenticate the author of a backup.
3. In the private copy, migrate accepted legacy metadata and persist a **fresh 32-character lowercase hexadecimal store epoch**. A schema-2 restore must rotate the epoch too. Check/backup and ordinary reopen preserve the existing epoch. A restore must not reuse its source epoch.
4. Independently validate the transformed, drained database and calculate its final SHA-256 and byte size. Publish atomically without replacing an existing target. Keep the source unchanged.
5. Return both `source_sha256` (the verified input) and `sha256` (the final published target), plus the target schema/epoch and counts. The target hash changes because legitimate restore metadata changed. Compare later `store check` with the target hash; do not expect equality with the old backup hash.

Private file/directory and alias checks, finite owner-lock acquisition, fsync and no-overwrite publication remain required. Publication/result-output errors may have ambiguous outcomes: inspect an existing target with `store check` before retrying; never overwrite it as a recovery shortcut. These are the supported Unix/private-filesystem semantics, not a claim about all storage drivers or network filesystems.

## Restore-aware cursors

Schema 2 persists an epoch independent of run IDs and event sequence numbers. The epoch is a history-generation marker, not a credential or authorization boundary. All reads and resume validation remain tenant scoped.

- List keyset cursor version 2 binds the epoch, tenant hash and timestamp/run-ID boundary. A same-tenant cursor from another epoch is refused; malformed/legacy list cursors are refused. Ordinary reopen must preserve valid cursors.
- The continuation's negotiated SSE format **3** uses opaque `<epoch>:<sequence>` event IDs and resumes with the complete ID. It returns `AgentOS-Store-Epoch`, includes `store_epoch` in run/end frames and represents terminal `last_event_id` as an opaque string. Caught-up terminal 204 also carries the epoch header. A known different epoch returns HTTP 409 with stable code `store_epoch_mismatch` before streaming. Client code must treat this as a reconciliation boundary and must not silently reset or resubmit work.
- Existing SSE formats 1 and 2 retain their numeric-only IDs for compatibility. They cannot detect a restored generation and are unsafe as restore-aware cursors. Format 3 is required for that property. Legacy compatibility does not make an old cursor safe across a rollback.
- A fresh epoch does not restore lost events or post-backup keys. It prevents an aware client from silently treating the restored history as a continuation of its previous history. The API's final header/frame schema and SDK behavior must match the embedded OpenAPI and contract tests before release.

## Cutover and unresolved work

Before reopening admissions, compare retained records with operator-held submission receipts, idempotency/operation keys, original cluster Jobs and external-system outcomes. Preserve any discovered post-backup information separately. Do not recreate a missing task automatically or submit a new idempotency key to “repair” uncertainty. If the original deployment cannot be drained or its external effects cannot be resolved, this contract does not authorize continued execution from that backup.

This iteration does not implement an automated live-Job reconciliation tool or an external side-effect journal. A future active recovery design must bind attempt identity, workload fencing, external operation idempotency and unknown outcomes. It must describe when a replacement can run without duplicating an old worker's effects, then pass driver-specific failure tests. Leader election and store epochs alone cannot establish that guarantee.

## Local implementation evidence for the continuation

The store owner reported `go test -race ./internal/store` passing after these additions. These tests use temporary local databases, not restored EKS PVCs:

- [internal/store/epoch_test.go](../../../internal/store/epoch_test.go): `TestEpochPersistsAndGetWithEpochPreservesTenantScope`, `TestSchemaOneMigrationAddsEpochWithoutRewritingData`, `TestEpochMetadataMalformedFailsWithoutMutation`.
- Same file: `TestRestoredEpochRejectsOldCursorPreservingRecordBytes`, `TestListCursorRejectsLegacyAndForeignEpoch`, `TestRestoreLegacyVersionsCreatesUniqueEpochsWithoutChangingInput`, `TestRestoreTransformationFailureCleansPrivateCopy`.

[internal/api/epoch_test.go](../../../internal/api/epoch_test.go)::`TestEpochEventsAndPaginationAcrossOfflineRestore` provides local handler/database coverage of format-3 IDs/headers, terminal 204, stale owned cursor 409, foreign tenant nondisclosure and legacy formats. The integrating owner reported full `make check` passing, including 99 Python tests; see the [continuation matrix](REQUIREMENT_TEST_MATRIX.md#continuation-after-8ec0900) for exact SDK regressions. Fresh-cluster restore, active disaster recovery, workload isolation, external-effect resolution and HA acceptance remain **Not run / unsupported**.
