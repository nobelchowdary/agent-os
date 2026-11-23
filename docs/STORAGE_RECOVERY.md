# Offline storage checks, backup and restore

These commands operate on a **stopped API's metadata database**. They do not contact Kubernetes, execute a task, restore workload files or fence a worker. They require filesystem access to the private database and do not require API/provider tokens. One API process owns a database; multi-replica availability is still unsupported.

## Maintenance sequence

1. Stop new submissions. Let accepted work finish or request cancellation and wait for observed cleanup. Preserve any required Kubernetes logs and artifacts.
2. Stop the API through the prepared cluster's maintenance procedure. Do not copy an open database or run a second owner against its PVC. An offline command encountering the live owner's lock refuses the operation.
3. Inspect the stopped database, then create a new backup in an existing private directory:

```sh
./bin/agentos store check --db /absolute/private/state/runs.db
./bin/agentos store backup --db /absolute/private/state/runs.db \
  --output /absolute/private/backups/pre-upgrade.db
```

The JSON result reports the operation, schema version, SHA-256, store epoch, run count, active count and byte size. Keep the backup and its checksum securely; the checksum detects drift relative to a trusted retained value and does not authenticate who produced the file. Database files can contain submitted code, captured workload output, run metadata and idempotency keys. Keep them out of Git and public logs.

4. Exercise restore to a **new** path using the actual checksum from the backup result:

```sh
./bin/agentos store restore --backup /absolute/private/backups/pre-upgrade.db \
  --db /absolute/private/restore-test/runs.db --sha256 SHA256_FROM_BACKUP_RESULT
./bin/agentos store check --db /absolute/private/restore-test/runs.db
```

5. Review the result and environment consistency before choosing which database the single API owner should open. Preserve the original database and matching binary/configuration until the maintenance validation is complete. This command does not install a restored PVC or migrate a live cluster.

## Enforced limits

`check` may inspect a valid database containing active records and reports their count. A successful check does **not** authorize restoring active work. Backup and restore both require a drained database: every run must be terminal, with no active reservation. Queued, starting, activating, running and cancelling records cause refusal. There is no override to replay old accepted work.

The restriction prevents a stale pre-activation record from recreating a task whose original execution already ran. It deliberately leaves active-task disaster recovery unresolved; a future recovery/fencing contract must reconcile Kubernetes and external effects before supporting that case. A drained metadata backup is not an always-current disaster-recovery service and cannot recover admissions made after it.

Commands validate the supported schema, database pages, records, event history, tenant/creation indexes, active reservations and every present idempotency binding/fingerprint. Records do not retain absent keys, so this check alone cannot infer a lost key; the trusted backup checksum and transaction history remain important. They reject inconsistent data rather than rebuilding indexes or inventing causes. Historical legal data is preserved: validation does not retroactively apply the new state graph to old nonterminal event sequences, and missing legacy reason codes are allowed.

Sources must already exist and pass private-file checks. Private destination directories must exist; files are created as 0600. Symlink/hard-link checks and database identity checks reduce accidental aliasing. Destinations must be absent: publication is atomic and does not replace an existing database. If an operation or its result output fails, inspect the destination before retrying; filesystem/directory-sync failures can leave an uncertain publication outcome. Use `store check` to recover the checksum of a completed drained backup, not an overwrite command.

Failures use fixed categories rather than printing database contents or raw path errors. `store_busy_stop_api_first` indicates the owner lock; `active_runs_require_reconciliation` means the drained-only contract is unmet. `unsupported_schema`, `integrity_check_failed` and `checksum_mismatch` must be investigated without bypassing validation.

## Version compatibility

The current store schema is version 3. Normal API startup transactionally initializes a new database or upgrades a complete version 0/1/2 database by adding version metadata and a random durable store epoch. This migration does not rewrite records, specification fingerprints, idempotency keys or old terminal history. Unsupported newer/malformed versions and incomplete layouts fail closed. Schema 2 upgrades preserve the existing epoch/cursors and add a separate result bucket. Ordinary restarts retain the epoch.

`check` and `backup` inspect/copy supported version 0/1/2/3 sources without migrating or mutating them. A legacy source without an epoch reports an empty epoch. Backup preserves exact bytes and checksum. Restore first verifies the trusted source checksum, then migrates a private temporary copy to schema 3 and rotates its epoch before publishing. It validates the transformed database again. Restore returns `source_sha256` for the input and `sha256` for the new destination; these differ because restore changes metadata. The source backup stays unchanged. A second restore of the same backup gets another new epoch.

## Client recovery contract

Restore recovers only metadata present in a drained backup. It does not restore Jobs, workload files, open connections or external actions. Active records are refused, so a supported restore cannot resume a stale running task or recreate a queued task. Admissions and idempotency keys committed after the backup are lost. Before reopening submissions, reconcile those acknowledged runs and their external effects with retained client records. An unknown idempotency key could otherwise admit duplicate work. The epoch detects a changed history; it cannot reconstruct it or make resubmission safe.

Pagination cursors include the store epoch and tenant binding. SSE format 3 uses opaque `epoch:sequence` event IDs, includes `store_epoch` in lifecycle/end payloads, and returns `AgentOS-Store-Epoch` on stream/catch-up responses. Authenticated GET of an owned run returns the same header from the record's read snapshot. Saved cursors must also retain their run and tenant identity. A well-formed stale owned cursor returns HTTP 409 `store_epoch_mismatch`, including when its sequence still fits the restored history. Clients must reconcile before explicitly starting fresh. Missing/foreign runs remain 404; lost post-backup admissions cannot be distinguished from never-existing runs by this endpoint.

Unbound legacy list cursors are rejected with 400. SSE formats 1/2 retain their original numeric IDs for existing clients and **cannot detect restore**. Upgrade consumers to format 3 before relying on recovery detection. The updated Python client requires format 3, validates epoch continuity on reconnect and terminal GET, and never silently downgrades or resets a stale cursor. Restores remain offline and require a maintenance window.

There is no downgrade migration or zero-downtime upgrade guarantee. Binaries predating version checks cannot be trusted to reject a newer database. Keep matched binary/configuration/backups and use a maintenance window. The server/maintenance binary currently targets Linux/macOS Unix file and locking semantics; Windows ACL behavior and network-filesystem failure modes are not qualified. Full upgrade/rollback and fresh-cluster/PVC recovery campaigns remain unqualified on EKS.
