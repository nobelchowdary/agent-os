#!/usr/bin/env python3
"""Opt-in continuity smoke against a PRE-CREATED, dedicated Substrate actor.

Only get/suspend/resume that actor, plus two bounded fixture increments. No
installation, provisioning, actor creation/deletion or arbitrary shell hooks.
Physical resource reclamation and production isolation remain unverified.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from qualify_cluster import write_private


REVISION = "1117caed19f0cdee42e5f045bad2043ea08c207d"
BASE = "https://github.com/agent-substrate/substrate/blob/" + REVISION + "/"
SOURCES = {"cli": BASE + "cmd/kubectl-ate/internal/cmd/actor.go",
           "flags": BASE + "cmd/kubectl-ate/internal/cmd/root.go",
           "protojson": BASE + "cmd/kubectl-ate/internal/printer/printer.go",
           "lifecycle": BASE + "api/ateapi/v1/ateapi.proto#L879",
           "installation": BASE + "docs/guides/operator-install.md"}


class ProbeError(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise ProbeError("fixture_redirect_refused")


class Client:
    def __init__(self, args):
        self.args = args
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def actor(self, verb, *, timeout=20):
        # Direct binary invocation avoids kubectl plugin/global flag ambiguity.
        command = [str(self.args.cli), "--context=" + self.args.context,
                   "--endpoint=" + self.args.api_endpoint, "--token-file=" + str(self.args.token_file),
                   "--output=json", verb, "actor", self.args.actor, "--atespace=" + self.args.atespace]
        if verb == "suspend":
            command.append("--mode=sync")
        try:
            with tempfile.TemporaryFile() as output:
                result = subprocess.run(command, stdout=output, stderr=subprocess.DEVNULL, check=False, timeout=timeout)
                output.seek(0)
                encoded = output.read(1024 * 1024 + 1)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ProbeError("cli_unavailable_or_timeout") from error
        # Neither raw stdout nor stderr is logged: either could contain secrets.
        if result.returncode or len(encoded) > 1024 * 1024:
            raise ProbeError("cli_request_failed")
        try:
            value = json.loads(encoded)
        except ValueError as error:
            raise ProbeError("cli_invalid_json") from error
        if not isinstance(value, dict) or not isinstance(value.get("status"), dict):
            raise ProbeError("unsupported_actor_output")
        metadata = value.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ProbeError("unsupported_actor_output")
        if metadata.get("name") != self.args.actor or metadata.get("atespace") != self.args.atespace:
            raise ProbeError("actor_identity_mismatch")
        return value

    def fixture(self, increment=False):
        path = "/increment" if increment else "/state"
        body = json.dumps({"probe_id": self.args.probe_id}).encode() if increment else None
        request = urllib.request.Request(self.args.http_endpoint.rstrip("/") + path, data=body,
                                         headers={"Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=5) as response:
                encoded = response.read(4097)
                if response.status != 200 or len(encoded) > 4096:
                    raise ProbeError("fixture_response_rejected")
                value = json.loads(encoded)
        except (OSError, ValueError, urllib.error.URLError) as error:
            raise ProbeError("fixture_unavailable_or_invalid") from error
        expected = {"probe_id", "memory_nonce", "memory_count", "file_nonce", "file_count"}
        if not isinstance(value, dict) or set(value) != expected or value["probe_id"] != self.args.probe_id:
            raise ProbeError("fixture_identity_or_schema_mismatch")
        if any(not isinstance(value[key], str) or not re.fullmatch("[a-f0-9]{64}", value[key]) for key in ["memory_nonce", "file_nonce"]):
            raise ProbeError("fixture_nonce_invalid")
        if any(type(value[key]) is not int or not 0 <= value[key] < 2**53 for key in ["memory_count", "file_count"]):
            raise ProbeError("fixture_count_invalid")
        return value


def actor_state(actor):
    return actor.get("status", {}).get("state")


def memory_snapshot(actor, previous_generation):
    status = actor.get("status", {})
    try:
        generation = int(status.get("lastAssignedGeneration", 0))
    except (ValueError, TypeError):
        return None
    if generation <= previous_generation:
        return None
    snapshots = status.get("snapshots", [])
    if not isinstance(snapshots, list):
        return None
    for snapshot in snapshots:
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get("durableSnapshot", {}), dict):
            continue
        if snapshot.get("generation") == generation and snapshot.get("fidelity") == "SNAPSHOT_FIDELITY_MEMORY" and snapshot.get("durableSnapshot", {}).get("state") == "SNAPSHOT_STORAGE_STATE_COMPLETED":
            return generation
    return None


def wait_actor(client, predicate, seconds):
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ProbeError("actor_transition_timeout")
        value = client.actor("get", timeout=min(10, remaining))
        if predicate(value):
            return value
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))


def run_probe(args, client):
    report = {"schema_version": 1, "generated_at": datetime.now(timezone.utc).isoformat(),
        "qualification": "NOT_QUALIFIED", "compatibility": "NOT_OBSERVED", "execute": True,
        "context": args.context, "actor": args.actor, "atespace": args.atespace,
        "source_revision": REVISION, "binary_source_match": "operator_attested_not_verified",
        "endpoint_context_binding": "operator_attested_not_verified",
        "sources": SOURCES, "checks": [], "cleanup": "not_needed",
        "unverified": ["Loopback forwards bind the supplied endpoints to the selected context and dedicated actor; the helper cannot independently authenticate this mapping.", "Server build revision and CLI/source correspondence beyond the operator attestation.", "Physical worker memory/CPU reclamation and fencing.", "Automatic idle detection, warm-pool cost and measured savings.",
                       "Cross-node recovery, node loss, external connections, credentials and external effects.", "Production isolation and general EKS compatibility beyond this installation."]}
    suspend_attempted, resumed = False, False

    def observed(name):
        report["checks"].append({"name": name, "status": "observed"})

    try:
        before = client.fixture(increment=True)
        active = wait_actor(client, lambda a: actor_state(a) == "ACTOR_STATE_RUNNING" and bool(a.get("status", {}).get("workerAssignment")), args.transition_timeout)
        uid = active.get("metadata", {}).get("uid")
        if not uid:
            raise ProbeError("actor_uid_missing")
        observed("dedicated_fixture_and_running_actor")
        previous = int(active.get("status", {}).get("lastAssignedGeneration", 0))
        suspend_attempted = True  # A timed-out CLI request may still have suspended it.
        client.actor("suspend", timeout=args.transition_timeout)
        suspended = wait_actor(client, lambda a: actor_state(a) == "ACTOR_STATE_SUSPENDED" and not a.get("status", {}).get("workerAssignment") and memory_snapshot(a, previous) is not None, args.transition_timeout)
        if suspended.get("metadata", {}).get("uid") != uid:
            raise ProbeError("actor_replaced")
        report["snapshot_generation"] = memory_snapshot(suspended, previous)
        observed("synchronous_durable_memory_snapshot")
        observed("control_plane_worker_assignment_released")
        # Poll only the control API while suspended; fixture GETs would wake it.
        client.actor("resume", timeout=args.transition_timeout)
        resumed = True
        running = wait_actor(client, lambda a: actor_state(a) == "ACTOR_STATE_RUNNING" and bool(a.get("status", {}).get("workerAssignment")), args.transition_timeout)
        if running.get("metadata", {}).get("uid") != uid:
            raise ProbeError("actor_replaced")
        after = client.fixture()
        if before != after:
            raise ProbeError("memory_or_filesystem_continuity_failed")
        observed("ram_and_filesystem_continuity_after_resume")
        incremented = client.fixture(increment=True)
        if any(incremented[k] != before[k] for k in ["probe_id", "memory_nonce", "file_nonce"]) or any(incremented[k] != before[k] + 1 for k in ["memory_count", "file_count"]):
            raise ProbeError("post_resume_increment_failed")
        observed("post_resume_process_progress")
        report["compatibility"] = "OBSERVED_CONTINUITY"
    except (ProbeError, ValueError, TypeError) as error:
        report["compatibility"] = "FAILED_OR_UNSUPPORTED"
        report["checks"].append({"name": "continuity_probe", "status": "failed", "reason": str(error) if isinstance(error, ProbeError) else "unsupported_actor_schema"})
    finally:
        if suspend_attempted and not resumed:
            try:
                client.actor("resume", timeout=10)
                report["cleanup"] = "resume_requested_after_failure_operator_must_verify"
            except ProbeError:
                report["cleanup"] = "actor_may_be_suspended_operator_must_resume"
    return report


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--execute", action="store_true", help="Authorize lifecycle changes and two fixture increments on the dedicated actor.")
    result.add_argument("--context", required=True)
    result.add_argument("--api-endpoint", required=True, help="Explicit Substrate gRPC target, e.g. 127.0.0.1:18080; no URL scheme.")
    result.add_argument("--http-endpoint", required=True, help="Existing dedicated actor proxy; only loopback HTTP accepted.")
    result.add_argument("--actor", required=True)
    result.add_argument("--atespace", required=True)
    result.add_argument("--probe-id", required=True, help="Must equal AGENTOS_PROBE_ID in the dedicated fixture.")
    result.add_argument("--cli", type=Path, required=True, help="kubectl-ate binary built by the operator from the pinned source revision.")
    result.add_argument("--cli-source-revision", required=True, choices=[REVISION], help="Operator attestation of build source; binary hash is recorded independently.")
    result.add_argument("--token-file", type=Path, required=True, help="Existing private Substrate bearer-token file; contents are never reported.")
    result.add_argument("--transition-timeout", type=int, default=30, choices=range(1, 61), metavar="1..60")
    result.add_argument("--output", type=Path, required=True, help="New private JSON file, outside Git.")
    return result


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    if not args.execute:
        print("NOT_EXECUTED: --execute is required. No endpoint or cluster was contacted.")
        return 2
    http = urllib.parse.urlsplit(args.http_endpoint)
    if http.scheme != "http" or http.hostname not in {"127.0.0.1", "::1", "localhost"} or http.username or http.password or http.query or http.fragment or http.path not in {"", "/"}:
        cli.error("http endpoint must be a plain loopback actor proxy origin")
    if not re.fullmatch(r"(?:127\.0\.0\.1|localhost|\[::1\]):[0-9]{1,5}", args.api_endpoint):
        cli.error("API endpoint must be an existing loopback port-forward host:port")
    if not args.context.strip() or not 16 <= len(args.probe_id) <= 128:
        cli.error("explicit context and probe ID of 16..128 characters required")
    if any(not re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?", name) or len(name) > 253 for name in [args.actor, args.atespace]):
        cli.error("actor and atespace must be lowercase DNS names")
    if args.output.exists() or args.output.is_symlink() or not args.output.parent.is_dir():
        cli.error("output must be a new file in an existing directory")
    if not args.cli.is_file() or not os.access(args.cli, os.X_OK):
        cli.error("CLI must be an existing executable built from the pinned revision")
    if not args.token_file.is_file() or args.token_file.stat().st_mode & 0o077:
        cli.error("token file must exist with no group/other permissions")
    binary_hash = hashlib.sha256(args.cli.read_bytes()).hexdigest()
    report = run_probe(args, Client(args))
    report["cli_binary_sha256"] = binary_hash
    try:
        write_private(args.output, report)
    except OSError:
        print("Could not write private report; verify the actor lifecycle in the selected context.", file=sys.stderr)
        return 1
    print(report["compatibility"] + "; NOT_QUALIFIED. Private report written; physical reclamation was not measured.")
    return 0 if report["compatibility"] == "OBSERVED_CONTINUITY" else 1


if __name__ == "__main__":
    sys.exit(main())
