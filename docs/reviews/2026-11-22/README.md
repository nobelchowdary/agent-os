# November 22 code-review bundle

This directory preserves the November 22 review context. Current implementation contracts are in the main documentation; compare later commits directly in GitHub.

| File | Scope |
| --- | --- |
| [CONTINUATION_REPORT.md](CONTINUATION_REPORT.md) | Implemented corrections, current local test/package results, compatibility changes and remaining gates. |
| [Historical comparison](https://github.com/nobelchowdary/agent-os/compare/e2ed1a4...8ec0900) | Reproduce the historical patch with `git diff e2ed1a4 8ec0900`. The redundant checked-in patch was removed; its original copy remains in commit `c14fcf0`. |
| [PHASE_ZERO_2026-11-22.at-8ec0900.md](PHASE_ZERO_2026-11-22.at-8ec0900.md) | Unchanged bytes from `8ec0900:docs/robustness/PHASE_ZERO_2026-11-22.md`. Its relative links retain the original report's context; use the matrix for links from this directory. |
| [REQUIREMENT_TEST_MATRIX.md](REQUIREMENT_TEST_MATRIX.md) | Source-backed C1–C10/C12/C14 evidence, exact test functions, per-code producers/negative tests and acceptance gaps. Later continuation changes are identified separately. |
| [RESTORE_CONTRACT.md](RESTORE_CONTRACT.md) | Stopped/drained metadata restore and store-epoch contract for the continuation. Active recovery and HA remain unsupported. |
| [WORKBOOK_REVIEW.md](WORKBOOK_REVIEW.md) | Read-only source/range/count check of the supplied workbook. No ratings changed. |

Historical endpoints are `e2ed1a4f82b19957baa2930c98cb32ffa2d9c7e6` and `8ec09000429ec277bd779f04d691dcc2ee3a1079`. Artifact checksums are in [SHA256SUMS](SHA256SUMS). The copied historical report is an immutable review input. The original baseline audit and phase report remain unchanged.

Only local/fake/HTTP/recorded packaging evidence is available. The bundle does not claim an EKS or kind campaign, runtime/CNI qualification, production availability, benchmark advantage or exactly-once external effects. Architecture and phase-order changes explicitly labeled proposals need a separate decision; the no-runtime-fallback and no-automatic-replay boundaries stay in force.
