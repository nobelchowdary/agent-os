# Read-only review of the supplied workbook

Date: November 22, 2026. Source: the downloaded `Agent OS competitive landscape.xlsx`, inspected read-only with the bundled Python runtime. File SHA-256 before and after inspection was `ab19b51a2f03d49e31f2601cb7c7138674ef077c7af764ce0c99006d540f1af9`. No workbook values, formulas, ratings, tabs or export were changed. The local export may lag the linked living artifact.

## What the actual export contains

| Observation | Exact source range |
| --- | --- |
| 26 capability rows | `Scorecard!A3:A28` |
| Status counts: Done locally **3**; Partial **6**; Not yet **15**; Not reported **2** | `Scorecard!B3:B28` |
| Position counts: Lead if proven **1**; Par **4**; Open field **4**; Behind **17** | `Scorecard!C3:C28` |
| Agent OS baseline is explicitly **e2ed1a4**, based on status notes without code access | `Scorecard!A1`, `Scorecard!D2`, `Read me!B4:C4` |
| The only review column is **Next commit**, and every data cell in it is blank | `Scorecard!G2:G28` |
| 54 landscape entries across seven categories | `Landscape!A2:B55` |
| Four actual tabs: Scorecard, Landscape, Read me, Dropdown options | Workbook structure; dropdown source cells `Dropdown options!A1:B4` |

The landscape category counts are Sandbox cloud 16, Developer sandbox 1, Cloud platform 11, Open source infrastructure 11, Gateway and policy 4, Named Agent OS 10 and Governance vendor 1 (`Landscape!B2:B55`). This confirms the workbook's inventory size, not each vendor capability.

The supplied November 22 note describes an “At 8ec0900 (reported)” column and three tabs. **That description does not match this downloaded file.** No 8ec0900 column exists here. The source export should be identified before anyone updates or compares later commit columns.

## Meaning of the ratings

`Read me!B4:C18` defines these as reported Agent OS status and research judgments. “Par” means comparable **on paper** (`B10:C10`); “Lead if proven” is conditional on live evidence (`B9:C9`); “Open field” means the author did not find a product doing it well (`B12:C12`). None establishes measured superiority, exclusive ownership of a problem or the absence of competitors. The market-bar descriptions and landscape capability cells were not independently reverified in this code review.

The counts reconcile exactly to 26. Keeping a position unchanged is reasonable while live qualification is absent, but the labels should not replace requirement-level acceptance. In particular, “Done locally” is not a pass of the real-cluster property/fault campaigns.

## Suggested scorecard corrections

- `Scorecard!A3:F7`: split current lifecycle/idempotency/cancellation implementation from unrun crash, termination and HA acceptance. The [requirement matrix](REQUIREMENT_TEST_MATRIX.md) supplies exact tests. A separate reviewed-commit column should retain the e2ed1a4 baseline.
- `Scorecard!A22:F22`: “EKS doctor and conformance tooling — Not yet” omits existing opt-in preflight/evaluation tooling. That tooling reports declarations/unverified evidence; it does not make the cluster qualified. Separate “tool exists” from “EKS conformance passed.”
- `Scorecard!A23:F23`: “Tenancy and quotas — Not reported” does not reflect code-visible tenant-scoped API/store queries and atomic active-run quotas. Namespace-per-tenant isolation, CPU/memory fairness and subject-based identity remain separate gaps.
- `Scorecard!A24:F24`: distinguish existing controller health/status from absent full metrics, traces, dashboards and alerts. Current health tests cannot satisfy all of O1/O2/O5.
- `Read me!B23:D28`: comparison targets, harness adapters, naming and licensing points are source proposals. They are not approved plan changes or verified legal/commercial conclusions. No phase gate, connector access, repository location or account setting changes follows from reading them.

The review note's C2 “five of nine” statement is corrected by source, not by a workbook rating change: all nine requested names, plus `completed` and `unknown`, are declared at `8ec0900`; policy and budget enforcement are reserved. See the per-code table in the matrix and the continuation's narrower eviction classification.
