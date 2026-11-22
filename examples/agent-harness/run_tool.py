#!/usr/bin/env python3
"""Submit a fixed deterministic Python tool task to an operator-run Agent OS API."""

import argparse
import json
import math
import os
from pathlib import Path
import secrets
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk/python"))
from agentos_client import AgentOSClient, AgentOSError


# This is a tool workload, not an LLM agent. No model API or credentials are
# placed inside the sandbox. The selected administrator template must run Python.
COMMAND = ["python", "-c", "import json; print(json.dumps({'task':'sum-of-squares','result':sum(i*i for i in range(1000))}))"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default=os.environ.get("AGENTOS_API_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--token-env", default="AGENTOS_TOKEN")
    parser.add_argument("--template", default="python", help="Administrator-owned template with Python available.")
    parser.add_argument("--idempotency-key", help="Reuse this exact key and request after an ambiguous submission.")
    parser.add_argument("--wait-seconds", type=float, default=360)
    args = parser.parse_args(argv)
    key = args.idempotency_key or "tool-" + secrets.token_hex(16)
    if not math.isfinite(args.wait_seconds) or not 0 < args.wait_seconds <= 86400:
        parser.error("wait-seconds must be positive and at most 86400")
    if not 1 <= len(key) <= 128 or any(not 33 <= ord(c) <= 126 for c in key):
        parser.error("idempotency key must contain 1..128 visible ASCII characters")
    client, run_id = None, None
    # The key is not an API credential. Print before POST so it survives a
    # timeout/interrupt; retrying must reuse the exact request and this key.
    print("Idempotency key: " + key, flush=True)
    try:
        client = AgentOSClient(args.api_url, token_env=args.token_env)
        run = client.create_run(args.template, COMMAND, 30, idempotency_key=key)
        run_id = run["id"]
        print("Run ID: " + run_id, flush=True)
        run = client.wait(run_id, timeout=args.wait_seconds)
        print("State: " + run["state"], flush=True)
        try:
            # JSON quoting keeps terminal control characters in untrusted logs inert.
            print("Logs: " + json.dumps(client.logs(run_id)["logs"]))
        except AgentOSError as error:
            print("Logs unavailable: " + error.code, file=sys.stderr)
        return 0 if run["state"] == "succeeded" else 1
    except KeyboardInterrupt:
        if client is not None and run_id is not None:
            try:
                client.cancel(run_id)
                print("Cancellation requested; Kubernetes shutdown is asynchronous.", file=sys.stderr)
            except AgentOSError:
                print("Cancellation outcome unknown; inspect the run using its ID.", file=sys.stderr)
        else:
            print("Submission outcome may be unknown; recover with the same key and identical request.", file=sys.stderr)
        return 130
    except (AgentOSError, ValueError) as error:
        # Client errors never contain response bodies, URLs or bearer tokens.
        message = error.code if isinstance(error, AgentOSError) else "invalid_client_configuration_or_input"
        print("Tool launch failed: " + message + ". Keep the same key when recovering this submission.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
