# 0003: Durable live lifecycle and a harness client

Date: November 21, 2026. Status: accepted for the experimental alpha.

## Problem

An agent harness needs to submit work, handle an ambiguous response and observe completion without repeatedly downloading entire run histories. Operators also need to distinguish a responsive HTTP process from a stopped or failing execution controller. The existing durable run/event contract is the authority; introducing an in-memory message broker would add another consistency problem.

## Decision

Publish Store invalidations only after a committed admission or actual state change. Capture the current invalidation channel before reading state. Notifications may coalesce; consumers read persisted state again. No-op transitions, matching idempotent retries and rolled-back writes do not publish. Wake the existing controller on invalidation while keeping periodic reconciliation for Kubernetes progress. No new scheduler or Kubernetes watcher is introduced.

Serve tenant-authorized SSE using positions in the existing append-only per-run history as stable cursors. Replay committed events after reconnect/restart, bound connection lifetime and count, and disconnect slow consumers without holding a database transaction. Terminal state remains immutable. This adds lifecycle delivery and does not provide physical-stop fencing, stdout streaming or a hard latency SLO.

Expose controller snapshots and tenant-scoped admission status, use controller health in executable readiness and redact raw operation errors in controller logs. Normal asynchronous cleanup is an expected pending result and retains quota. A legitimate task failure does not fail the controller health check.

Use tenant-scoped keyset pagination over the existing creation index. Add a standard-library Python client with explicit idempotent submission, bounded reconnects and SSE validation. The example is an external harness invoking an offline sandbox tool. It does not embed provider credentials or claim a complete network-connected agent runs inside the sandbox.

## Acceptance and limitations

Behavioral tests must cover commit/replay/reconnect, cancellation races, no-op/rollback notifications, tenant boundaries, cursor validation, stream limits, disconnect/shutdown behavior, controller failure/recovery and retained pending-cleanup reservations. Exercise real HTTP/2 idle behavior because its deadline semantics can reset an idle stream. `make check` and packaging checks remain required; node isolation, real-agent quality and cloud performance require a separately qualified environment.

See [live API and launch contract](../LIVE_API.md) and the [roadmap](../ROADMAP.md). Controlled provider/tool access and an actual EKS harness trial remain the next integration gates.
