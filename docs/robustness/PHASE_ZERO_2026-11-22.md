# Phase 0 continuation report

Date: November 22, 2026. Previous implementation: `0076fd20dd402684b810eb2667db92d9f6c81341`; [its CI passed](https://github.com/nobelchowdary/agent-os/actions/runs/44985554639). The [original 70-ID audit](GAP_AUDIT_2026-11-21.md) remains frozen at its audited baseline. This report records subsequent C2/C14 progress rather than changing those historical counts.

## 1. Requirement progress

| ID | New local implementation | Remaining acceptance gap |
| --- | --- | --- |
| C2 | Optional reason codes flow through backend evidence, durable cleanup intent, terminal records, SSE and the Python client. Invalid code/state combinations fail without mutating history/quota. | Policy/budget causes have no producers; real-node OOM/eviction/deadline/cancellation campaigns have not run. Whole requirement remains Partial. |
| C14 | Schema v1, legacy metadata-only migration, offline integrity check, drained backup and checksum-required restore with private files/owner locking/no-overwrite publication. | Active-task disaster recovery, distributed-store failover, downgrade/rolling upgrades and fresh-cluster PVC restoration are not qualified. Whole requirement remains Partial. |
| C1/C3/C4/C7/C9 | Existing state graph, idempotency fingerprints, durable admission, cancellation reservations and append-only event semantics are preserved. | Full process-kill, cluster and load acceptance campaigns remain unexecuted. |

Code and evidence entry points: [outcome model](../../internal/model/run.go), [backend classification tests](../../internal/backend/kubernetes/reason_codes_test.go), [controller cause/health tests](../../internal/controller/reason_codes_test.go), [store reason tests](../../internal/store/reason_code_test.go), [store integrity/maintenance tests](../../internal/store/maintenance_test.go), [CLI integration tests](../../cmd/agentos/store_test.go), [API reason contract](../../internal/api/reasons_test.go), and [Python transport tests](../../scripts/tests/test_agentos_client.py).

## 2. Cluster facts

No EKS cluster has been connected. EKS version, nodes, CNI mode, installed runtimes and upstream component versions remain unobserved. No infrastructure, cluster workloads, model calls or provider credentials were created for this iteration.

## 3. Measurements

EKS admission, cold/warm start, cancellation, resource-use, real-agent success and cost-per-correct-task samples remain **0 / Not measured**. Local tests and storage-copy duration are not performance or billing evidence.

## 4. Test matrix and local validation

All full EKS scenarios T1–T16 remain **Not run**. In particular, stopped drained metadata round trips are narrower than T4 live store-restart behavior or T14 upgrades under load. Structured fake-client Pod/Job conditions are narrower than T8/T9 resource and physical-termination exercises. Python local HTTP tests do not establish T10–T13 gateway/network/budget properties.

| Local check | Result | Limit |
| --- | --- | --- |
| `make check` | Go vet/race tests, 93 Python tests, binary build and three profile preview smokes passed. | Not real node isolation or workload performance. |
| Cause/cleanup tests | Structured Pod/Job facts, unknown causes, retained cleanup reservations, legacy records and invalid-code health tests passed. | Fake clients/observations, not actual resource-pressure canaries. |
| Storage regressions | Legacy migration, codes/history/idempotency, corruption/checksum/index checks, exclusive owner lock and no-overwrite cleanup tests passed. | Stopped local databases; no active failover. |
| Independent startup reproductions | Existing zero-byte, unrelated empty bbolt and future unsynced-freelist stores reject with unchanged hashes; a missing path initializes. | Focused local reproductions, not a broad security scan. |
| Compiled CLI | A live preview owner prevented backup; after stop, check/backup/restore preserved the exact checksum. Wrong checksum was refused. Commands ran without API credentials/config. | Fixture metadata only; no task execution or cluster contact. |
| Linux container | Image built; default non-root/read-only CLI help passed. Check/backup/restore passed as fixture filesystem owner with read-only root, no capabilities and networking disabled. | Tests use a private shared-workspace fixture; the daemon could not mount the initial host temporary-directory path. No untrusted workload ran. |

SSE format negotiation also has local API/SDK coverage: format 1 omits new fields for older strict clients, while format 2 transports stored codes without changing sequence IDs or persisted data. These results stay separate from EKS acceptance.

## 5. Invariant gaps found and corrected

- Failed observations previously lacked structured causes; conservative codes now use owned structured status facts. No cause is guessed from log text, exit 137 alone, or a missing Pod. Transient image pulls retain their recovery opportunity.
- Invalid cause/state associations could be rejected as generic state conflicts and leave reconciliation looking healthy. They now produce sanitized non-CAS failures before transition or destructive cleanup, preserving quota/history until valid evidence arrives.
- Writable bbolt Open can flush an unsynced freelist before application schema validation. Acquisition now suppresses that startup write until schema acceptance under the same exclusive lock; future/malformed/incomplete schema rejection is tested for unchanged source bytes. Normal synchronous commits remain enabled.
- Existing zero-byte/truncated or unrelated empty bbolt files must not be treated as fresh stores. Initialization is authorized only by genuinely new exclusive file creation; existing incomplete sources fail unchanged.
- A backup of active pre-activation records could later recreate already-executed work. This first maintenance path refuses active backup/restore instead of offering a replay override.

## 6. Compatibility and deviations

No new public task states, runtime fallback, automatic retry, orphan adoption, gateway or distributed replica is introduced. `reason_code` is optional for old records/events and does not enter the resolved specification or idempotency hash. SSE format 1 remains the default for older strict clients; the new Python client requests format 2 for codes, using the same sequence/cursor/terminal framing. Historical codes and nonterminal event sequences are not rewritten. Policy/budget code names are reserved, not enforcement claims.

Version 1 is the first explicit store version. Legacy version 0 can be inspected/copied without migration; normal startup adds metadata transactionally. Backup restore preserves bytes/checksum before subsequent legitimate database writes. Existing future/unknown schemas and inconsistent indexes are refused rather than repaired. A trusted retained digest checks integrity, not the identity of the backup author.

The accepted local scope is stopped, drained, private metadata recovery. This is deliberately narrower than active-task disaster recovery and provides no zero-downtime guarantee. Old binaries predating schema checks cannot be assumed safe downgrade targets. See [storage recovery](../STORAGE_RECOVERY.md), [outcome codes](../OUTCOME_CODES.md) and [decision 0005](../decisions/0005-outcome-codes-and-offline-recovery.md).

## 7. Upstream outputs

No upstream issue, PR or message was filed. The bbolt startup behavior is accommodated by the application rather than reported as a library vulnerability. The project still needs observed EKS compatibility results and concrete reproductions before proposing upstream changes.

## 8. Next gates

Continue phase 0 fault/load and admission work, and settle the C11/P6 distributed ownership/availability design before adding replicas. Plan active recovery only with a workload identity, external-effect resolution/fencing contract and failure tests. Qualified EKS access, a declared spend/time bound and the prescribed runtime/CNI/node checks are still required before phase 1 can pass; later gateway/driver phases remain gated.
