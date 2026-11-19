"""Offline Substrate contract/fixture tests; no Kubernetes or Substrate required."""

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import probe_substrate as p

FIXTURE = SCRIPTS.parent / "examples/conformance/continuity_actor.py"
SPEC = importlib.util.spec_from_file_location("continuity_actor", FIXTURE)
f = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(f)


def arguments():
    return p.parser().parse_args([
        "--execute", "--context", "explicit-pilot", "--api-endpoint", "127.0.0.1:18080",
        "--http-endpoint", "http://127.0.0.1:18081", "--actor", "dedicated-probe",
        "--atespace", "probe-only", "--probe-id", "dedicated-probe-1234",
        "--cli", "/not-executed", "--cli-source-revision", p.REVISION,
        "--token-file", "/not-read", "--output", "/tmp/not-written-by-tests.json",
        "--transition-timeout", "1",
    ])


class FixtureClient:
    def __init__(self):
        self.calls = []
        self.suspended = False
        self.broken_ram = False
        self.snapshot_fidelity = "SNAPSHOT_FIDELITY_MEMORY"
        self.assignment_when_suspended = False
        self.fail_suspend = False
        self.count = 0

    def actor(self, verb, **_):
        self.calls.append(verb)
        if verb == "suspend":
            self.suspended = True
            if self.fail_suspend:
                raise p.ProbeError("cli_request_failed")
        elif verb == "resume":
            self.suspended = False
        status = {"state": "ACTOR_STATE_SUSPENDED" if self.suspended else "ACTOR_STATE_RUNNING", "lastAssignedGeneration": 2 if "suspend" in self.calls else 1}
        if not self.suspended or self.assignment_when_suspended:
            status["workerAssignment"] = {"workerPod": "probe-worker"}
        if self.suspended:
            status["snapshots"] = [{"generation": 2, "fidelity": self.snapshot_fidelity, "durableSnapshot": {"state": "SNAPSHOT_STORAGE_STATE_COMPLETED"}}]
        return {"metadata": {"uid": "same-actor", "name": "dedicated-probe", "atespace": "probe-only"}, "status": status}

    def fixture(self, increment=False):
        if increment:
            self.count += 1
        return {"probe_id": "dedicated-probe-1234", "memory_nonce": ("b" if self.broken_ram and "resume" in self.calls else "a") * 64,
                "memory_count": self.count, "file_nonce": "c" * 64, "file_count": self.count}


class SubstrateProbeTests(unittest.TestCase):
    def test_observed_continuity_does_not_claim_physical_release(self):
        client = FixtureClient()
        report = p.run_probe(arguments(), client)
        self.assertEqual(report["compatibility"], "OBSERVED_CONTINUITY")
        self.assertEqual(report["qualification"], "NOT_QUALIFIED")
        self.assertEqual(client.calls, ["get", "suspend", "get", "resume", "get"])
        self.assertEqual(client.count, 2)
        self.assertIn("Physical worker memory/CPU reclamation and fencing.", report["unverified"])
        self.assertNotIn("a" * 64, json.dumps(report))

    def test_cold_restart_with_files_surviving_is_failure(self):
        client = FixtureClient()
        client.broken_ram = True
        report = p.run_probe(arguments(), client)
        self.assertEqual(report["compatibility"], "FAILED_OR_UNSUPPORTED")
        self.assertEqual(report["checks"][-1]["reason"], "memory_or_filesystem_continuity_failed")

    def test_filesystem_snapshot_alone_is_not_memory_checkpoint(self):
        client = FixtureClient()
        client.snapshot_fidelity = "SNAPSHOT_FIDELITY_ROOTFS"
        with patch.object(p, "wait_actor", side_effect=[client.actor("get"), p.ProbeError("actor_transition_timeout")]):
            report = p.run_probe(arguments(), client)
        self.assertEqual(report["compatibility"], "FAILED_OR_UNSUPPORTED")
        self.assertIn("resume", client.calls)

    def test_ambiguous_suspend_attempts_bounded_resume(self):
        client = FixtureClient()
        client.fail_suspend = True
        report = p.run_probe(arguments(), client)
        self.assertEqual(report["compatibility"], "FAILED_OR_UNSUPPORTED")
        self.assertEqual(report["cleanup"], "resume_requested_after_failure_operator_must_verify")
        self.assertEqual(client.calls[-1], "resume")

    def test_snapshot_must_be_new_memory_and_durable(self):
        actor = FixtureClient().actor("get")
        actor["status"].update({"lastAssignedGeneration": 3, "snapshots": [{"generation": 3, "fidelity": "SNAPSHOT_FIDELITY_MEMORY", "durableSnapshot": {"state": "SNAPSHOT_STORAGE_STATE_COMPLETED"}}]})
        self.assertEqual(p.memory_snapshot(actor, 2), 3)
        self.assertIsNone(p.memory_snapshot(actor, 3))
        actor["status"]["snapshots"][0]["durableSnapshot"]["state"] = "SNAPSHOT_STORAGE_STATE_PENDING"
        self.assertIsNone(p.memory_snapshot(actor, 2))

    def test_cli_explicit_flags_and_no_secret_error_output(self):
        args = arguments()
        value = FixtureClient().actor("get")
        result = subprocess.CompletedProcess([], 0, json.dumps(value).encode(), b"secret-sentinel")
        def output(*_args, **kwargs):
            kwargs["stdout"].write(json.dumps(value).encode())
            return result

        with patch.object(p.subprocess, "run", side_effect=output) as run:
            p.Client(args).actor("suspend")
        command = run.call_args.args[0]
        for value in ["--context=explicit-pilot", "--endpoint=127.0.0.1:18080", "--token-file=/not-read", "--mode=sync", "--output=json"]:
            self.assertIn(value, command)
        self.assertEqual(run.call_args.kwargs["stderr"], subprocess.DEVNULL)
        result.returncode = 1
        with patch.object(p.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(p.ProbeError, "^cli_request_failed$"):
                p.Client(args).actor("get")

    def test_missing_execute_never_contacts_endpoint(self):
        argv = ["--context", "pilot", "--api-endpoint", "127.0.0.1:1", "--http-endpoint", "http://127.0.0.1:2",
                "--actor", "probe", "--atespace", "probe", "--probe-id", "dedicated-probe-1234", "--cli", "/missing",
                "--cli-source-revision", p.REVISION, "--token-file", "/missing", "--output", "/not-created"]
        with patch.object(p, "Client") as client:
            self.assertEqual(p.main(argv), 2)
        client.assert_not_called()

    def test_fixture_file_survives_restart_while_ram_does_not(self):
        with tempfile.TemporaryDirectory() as directory:
            original = f.State(directory, "dedicated-probe-1234")
            before = original.read(increment=True)
            restarted = f.State(directory, "dedicated-probe-1234").read()
            self.assertNotEqual(before["memory_nonce"], restarted["memory_nonce"])
            self.assertEqual(restarted["memory_count"], 0)
            self.assertEqual(restarted["file_nonce"], before["file_nonce"])
            self.assertEqual(restarted["file_count"], before["file_count"])
            self.assertNotIn(before["memory_nonce"], (Path(directory) / "continuity.json").read_text())


if __name__ == "__main__":
    unittest.main()
