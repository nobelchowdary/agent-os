# 0005: Structured outcomes and drained offline metadata recovery

Date: November 22, 2026. Status: accepted for local phase 0 work; EKS gates remain unmet.

## Context

C2 needs machine-readable causes without guessing from logs, while C14 needs versioned storage and a tested recovery path. Restoring stale active metadata could recreate pre-activation work after its original execution already ran. Existing terminal history and specification fingerprints must remain compatible.

## Decision

Add optional outcome codes to records/events and backend observations, not resolved execution specifications. New transitions validate state/code combinations transactionally. Preserve missing legacy fields and immutable terminal history. Carry cleanup intent codes through cancellation, retaining reservations until cleanup observation. Use structured ownership-checked Kubernetes evidence and explicit unknowns; keep policy/budget codes reserved until actual producers exist. Preserve transient image-pull recovery behavior.

Introduce store schema version 1 with a metadata-only transactional migration from the complete legacy layout. Refuse unsupported future/malformed schemas and inconsistent layouts. Provide filesystem-authorized offline check/backup/restore commands before API/config/credential/Kubernetes initialization, with private paths, owner locking, integrity checks and no-overwrite publication. Restore requires a trusted retained SHA-256 and a drained source; check may inspect active metadata, but backup/restore reject it without an override.

Do not add an HTTP backup endpoint, multi-owner access, automatic execution replay, live failover or downgrade claims. Offline checks and hashes cannot fence a worker or authenticate the backup's author. The schema migration and maintained backup path do not establish zero-downtime upgrades or full active-task disaster recovery.

## Consequences

Operators get a reproducible maintenance path; callers get stable observed causes and explicit uncertainty. C2 and C14 remain Partial against the entire supplied plan. Validate commands on local fixtures and packaged binaries, then perform the actual PVC/fresh-cluster and workload cause/fault campaigns on qualified EKS infrastructure.

See [outcome codes](../OUTCOME_CODES.md), [storage recovery](../STORAGE_RECOVERY.md), and the [phase 0 report](../robustness/PHASE_ZERO_2026-11-22.md).
