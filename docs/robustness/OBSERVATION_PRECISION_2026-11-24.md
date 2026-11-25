# Comparison observer precision before EKS

Date: November 24, 2026. Scope: the new format-3 comparison instrument and its preregistered precision policy. [Decision 0010](../decisions/0010-comparison-observation-precision.md) records the contract; [decision 0009](../decisions/0009-comparison-boundaries-and-latency-budget.md) retains the selected workload and benefit gates.

## Problem and change

The review of `7142fea` identified unequal sampling work inside the timed polling loops. The cold-claim arm used four `kubectl` processes per steady-state pass and read its gated Pod phase on the third. Agent OS used two `kubectl` processes plus two API requests and read its gated outcome first. Baseline submission also fetched its object again after a successful POST. These differences can influence observed latency; their actual EKS magnitude is not yet measured.

The new collection path uses one authenticated proxy on a private Unix socket, persistent request channels, successful-POST responses and a separate fixed-cadence native-outcome sampler. Proxy/client preparation precedes the trial clock. Log retrieval, ownership checks and output validation remain required, with explicit identities and outcome authorities; they no longer set the sampler's polling cadence. Submission service work still counts in the elapsed measurement.

Private evidence files are bounded at 2 MiB so a registered 200-trial arm can retain its intervals and diagnostics; individual Kubernetes/API JSON responses remain bounded at 512 KiB, and workload logs retain their separate bound. This storage limit does not change legacy input bytes, hashes, observation rules or the production API contract.

New protocols freeze a 0.1-second cadence and 0.5-second maximum terminal-observation bracket. The collector retains the last nonterminal request start/receipt next to the first terminal request start/receipt. The lower endpoint uses the last nonterminal request's start. If there was no prior nonterminal response, its absence remains explicit and the lower bound conservatively uses the trial origin.

Format 3 currently gates caller-visible outcome only; unsupported boundary selections fail before registration. Pod-terminal and validated-completion observations remain diagnostics, and format 2 retains its existing point-observation boundary choices. Use the literal `127.0.0.1` port-forward address for the pilot: network socket/TLS I/O is bounded, while synchronous system DNS resolution remains an explicit instrument limitation.

The report publishes interval coverage, maximum/p95 bracket widths and p95 lower/upper bounds for each arm. Missing or oversized intervals make latency `not_measurable`; valid intervals that straddle the allowed comparison also remain undecided. The gate passes only when the candidate's upper bound is at most `baseline lower bound * 1.10 + 2 seconds`, and fails only when its lower bound is greater than `baseline upper bound * 1.10 + 2 seconds`. A point estimate alone cannot decide a marginal result.

## New registrations

| Purpose | Protocol | Workload |
| --- | --- | --- |
| First cluster instrument check | [Job-path instrument check](../../evaluation/decision.job-instrument-check.example.json) | [Short fixture](../../examples/evaluation/python-short-workload.json) |
| Upstream short comparison | [Short measured protocol](../../evaluation/decision.short-measured.example.json) | [Short fixture](../../examples/evaluation/python-short-workload.json) |
| Upstream minute comparison | [Minute measured protocol](../../evaluation/decision.minute-measured.example.json) | [Minute fixture](../../examples/evaluation/python-minute-workload.json) |

The instrument check compares direct Jobs with Agent OS on the same qualified environment. Its `instrument_check` output reports the observed p95 difference and interval `[candidate lower - baseline upper, candidate upper - baseline lower]`, with no assumed expected difference. A wholly negative interval or an additional-delay lower bound above the registered two-second allowance raises a review flag. Inspect the delta and decomposition rather than assuming an exact one-second reference: Agent OS adds durable staging/activation and reconciliation, while distinct executions also encounter scheduler, Job-controller and network variation. These flags require retained investigation, not automatic proof of an instrument fault or a rewritten acceptance rule.

Keep short and minute results separate. The caller-outcome allowance remains ten percent plus two seconds, with unchanged correctness/qualification requirements and the 20% fully accounted cost / 25% median operator-time benefit routes. Existing format-1/format-2 examples and registrations are not rewritten. Neither a simulation nor the diagnostic protocol establishes an EKS performance advantage.

## Validation status

Final local `make check` passed Go formatting/vet/race tests, **256 Python tests**, the build and authenticated compiled preview smokes for legacy, gVisor and Nitro profiles. The relevant suites include eight measured-collector tests, five observer tests, seventeen Kubernetes-transport tests, six Agent OS transport tests and fifty-eight decision tests. They cover independent sampling under slow auxiliary work, request-inclusive brackets, unsupported state/boundary rejection, identity/ancestry checks, ambiguous creation, connection reuse, response/time bounds, shutdown cleanup, the asynchronous API-cleanup race, policy/hash binding and conservative decisions.

The first full local check passed its 255 Python tests but the preview process exited before readiness; that smoke path suppressed stderr, so the cause remains unclassified. The same binary then passed standalone and diagnostic preview checks, followed by the complete successful `make check` with 256 tests after the last test addition. Do not infer a resolved environment or runtime cause from those successful repeats.

An additional offline CLI compatibility exercise used actual collector-emitted reports from fake Kubernetes/API clients: two simulated trials per arm for both named workload profiles across all three arms, plus the two-arm instrument protocol (**16 simulated trials**). Each unqualified decision correctly remained `insufficient_evidence`. Eighteen operator-journal CLI sessions with explicitly synthetic clocks and fake qualification exercised the operator route through decision reporting; registration bytes stayed unchanged. Socket connections were disabled. Those results validate compatibility, not task performance or operator savings.

Transport tests use local fake HTTP/Unix-socket servers. No authenticated `kubectl proxy` was started against a real cluster, and no EKS cluster, AWS billing configuration, live instrument check or comparative workload trial has run for this change. Preview checks executed no agent code. These checks do not qualify node isolation, actual EKS timing precision or product economics.

The prior automatic [full kind/gVisor rerun at `7142fea`](https://github.com/nobelchowdary/agent-os/actions/runs/45498122196) is now marked successful on GitHub; its metadata binds it to `7142fea57f65db78c97535434823994393d74245`. This update checks its workflow status, not the artifact's individual measurements. The separately [artifact-verified full campaign at `6389b17`](JOB_CONTROLLER_TIMING_2026-11-24.md#verified-full-instrumented-regression) remains the detailed execution evidence. Neither run validates this new comparison instrument.

Use the [runbook](../EVALUATION_RUNBOOK.md) to prepare billing, qualify the cluster, run the instrument check and then collect the separately registered profiles. Job authority and the production runtime remain unchanged; reporting-delay tuning stays stopped.
