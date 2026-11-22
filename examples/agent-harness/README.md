# Launch a tool from an external agent harness

An external harness can call a model, decide which tool to run, and submit that tool's code to Agent OS. In the current alpha, the harness owns the model loop and keeps provider credentials outside the workload sandbox. Agent OS supplies a bounded execution environment and lifecycle API. A provider gateway, in-sandbox model access and a full autonomous-agent runtime are separate capabilities.

`run_tool.py` demonstrates the execution step with a **fixed deterministic Python task**. It does not invoke a model or claim to be an AI agent. The API operator must already have a prepared cluster, selected sandbox runtime, enforcement configuration, tenant token and administrator-owned Python template. Preview mode returns HTTP 503 for execution.

From the repository root, make a secure tenant token available as `AGENTOS_TOKEN`, then run:

```sh
python3 examples/agent-harness/run_tool.py \
  --api-url http://127.0.0.1:8080 \
  --template python \
  --idempotency-key my-persisted-tool-call-001
```

The script submits fixed sum-of-squares code with a 30-second execution budget, waits up to 360 seconds by default, and prints the run ID, terminal state and bounded logs. `--wait-seconds` changes the client wait bound, which is separate from the server execution budget and startup wait. Use HTTPS for a remote API. A local port-forward is suitable for the loopback URL shown here.

Use a fresh key for each intended tool execution. When recovering an uncertain submission, reuse its printed key and identical input. Without `--idempotency-key`, the script generates one once and prints it before submission; retain it for recovery across process restarts. No creation retry is automatic. Interrupting after an ID is known requests best-effort cancellation; before an ID is known, admission can remain ambiguous. Waiting to its client deadline does not cancel the run. Inspect it by ID or recover with the same key before deciding what to do next.

For an actual agent harness, replace the fixed tool-selection logic outside this script while keeping administrator-owned templates, explicit idempotency and tenant-scoped run tracking. Do not pass provider tokens through the workload command, and keep external actions idempotent. This example has local stub validation only; no live cluster, runtime isolation, model call or provider access is claimed.
