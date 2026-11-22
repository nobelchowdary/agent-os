# API contract and compatibility

The checked-in [OpenAPI document](../internal/api/openapi.json) describes the current experimental HTTP surface. The binary embeds that exact file and serves it at authenticated `GET /v1/openapi.json`. It uses the pinned [OpenAPI 3.1.1 format](https://spec.openapis.org/oas/v3.1.1.html).

The local contract suite derives route authorization and request-example checks from the document, validates internal references and state names, and exercises stable errors and parser fuzz seeds. It is not a complete external OpenAPI/JSON Schema validator or proof that every response under every failure conforms. C12 therefore remains Partial in the robustness audit.

## Stable application errors

Application errors preserve the existing human-readable `error` field and add `code`:

```json
{"code":"idempotency_conflict","error":"Idempotency-Key already used with a different request"}
```

Use `code` and the HTTP status for decisions. Human-readable messages can change. The schema lists the supported code vocabulary. Unknown routes and unsupported methods return structured `route_not_found` / `method_not_allowed`; the latter includes `Allow`. Transient storage, controller, runtime-declaration and log-read failures include `Retry-After: 5`; overload already supplies its own retry header. Errors before application routing, such as malformed HTTP framing or connection timeouts, may be produced by the HTTP server instead.

These codes describe API outcomes, not workload terminal reason codes. The planned C2 taxonomy (`oom_killed`, `deadline_exceeded`, etc.) remains unimplemented. Provider-spend and policy decisions must not be inferred from a generic HTTP error.

## Submission validation

Bodies are bounded at 32 KiB and must be one UTF-8 JSON object with exact field names: `template`, optional `command` and optional `timeout_seconds`. Duplicate, unknown or case-aliased fields, null values, non-string command arguments, empty command arrays, fractional/bool timeouts and invalid UTF-8 are rejected before admission. An explicit timeout must be positive and within administrator policy. Omit optional fields to select defaults. Command argument/total byte bounds remain enforced by configuration resolution.

An explicitly supplied Content-Type must be `application/json` (parameters are allowed). A missing header remains accepted for legacy callers. Multiple Authorization or Idempotency-Key values are rejected; a provided key must be nonempty visible ASCII and at most 128 bytes. Legacy API calls can still omit the key and receive a fresh admission each time; the Python client requires one. Persist the exact request and key before submitting, then retry that pair after an ambiguous response.

These are deliberate validation tightenings in the experimental API: callers relying on duplicate/case-aliased fields, null/default substitutions, explicit zero timeouts or empty provided keys must send the documented form. Existing wire state names, resolved specification fingerprints, tenant scoping and matching-key result lookup are preserved.

## Lifecycle enforcement

[`model.CanTransition`](../internal/model/run.go) defines the allowed graph. The Store checks the actual persisted source state, expected-state CAS and allowed edge within one transaction before appending an event or changing active reservations. Illegal edges fail without a write or notification. Known-state no-ops remain reads. Forward observation/recovery edges preserve authoritative backend outcomes after downtime; they do not submit or recreate execution.

No state migration is introduced here. `queued` still represents durable admission; rejected admission is an HTTP outcome; task deadlines still produce `failed`. Cancellation keeps its active reservation until observed cleanup. The [live API](LIVE_API.md) describes event replay and the limits of cancellation observations.
