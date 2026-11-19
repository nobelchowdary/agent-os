import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("operator_journal", Path(__file__).parents[1] / "operator_journal.py")
journal = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(journal)


class JournalTests(unittest.TestCase):
    def records(self):
        one = {"experiment_id": "e", "baseline": "b", "protocol_sha256": "a" * 64, "phase": "install",
               "registration_sha256": "b" * 64, "workload_sha256": "c" * 64, "environment_sha256": "d" * 64,
               "task_ref": "install-standard-gvisor-v1",
               "qualified": True, "elapsed_seconds": 600, "steps": [{"privilege": "node-admin", "node_customization": True, "outcome": "failed"}]}
        return [copy.deepcopy(one) for _ in range(3)]

    def test_counts_failed_steps_and_node_privileges(self):
        output = journal.summarize(self.records())
        summary = output["operator_effort"]
        self.assertEqual(summary["session_minutes"], [10, 10, 10])
        self.assertEqual(summary["failed_steps"], 3)
        self.assertEqual(summary["privilege_steps"], {"node-admin": 3})
        for key in journal.BINDINGS:
            self.assertEqual(summary[key], output[key])

    def test_cannot_mix_arms_or_unfinished_sessions(self):
        for key, value in [(key, "other") for key in journal.BINDINGS] + [("qualified", False)]:
            records = self.records()
            records[1][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                journal.summarize(records)

    def test_repeated_sessions_required(self):
        with self.assertRaises(ValueError):
            journal.summarize(self.records()[:1])

    def test_clock_rollback_rejected(self):
        with self.assertRaises(ValueError):
            journal.elapsed({"started_unix": 200}, 100)

    def registration(self, directory):
        protocol = {"experiment_id": "registered-experiment", "baseline": "upstream", "candidate": "agentos"}
        workload = {"test": "fixture-only"}
        registration = {"format_version": 1, "experiment_id": protocol["experiment_id"], "protocol": protocol,
            "protocol_sha256": journal.canonical_hash(protocol), "workload": workload,
            "workload_sha256": journal.canonical_hash(workload), "environment": {"sha256": "a" * 64},
            "context": "offline-only", "trial_count": 30}
        path = Path(directory) / "registration.json"
        path.write_text(json.dumps(registration, indent=4))
        os.chmod(path, 0o400)
        return path, registration

    def test_start_binds_to_actual_canonical_registration(self):
        with tempfile.TemporaryDirectory() as temporary:
            path, registration = self.registration(temporary)
            directory = Path(temporary) / "session"
            with patch.object(journal.time, "time", return_value=100), patch("builtins.print"):
                status = journal.main(["start", "--registration", str(path), "--arm", "upstream", "--directory", str(directory),
                    "--operator", "operator-a", "--phase", "install", "--task-ref", "install-standard-gvisor-v1"])
            self.assertEqual(status, 0)
            record = json.loads((directory / "session.json").read_text())
            self.assertEqual(record["registration_sha256"], journal.canonical_hash(registration))
            self.assertEqual(record["workload_sha256"], registration["workload_sha256"])
            self.assertEqual(record["environment_sha256"], registration["environment"]["sha256"])
            self.assertEqual(record["experiment_id"], "registered-experiment")
            self.assertEqual(record["task_ref"], "install-standard-gvisor-v1")
            self.assertEqual((directory / "session.json").stat().st_mode & 0o777, 0o600)

    def test_registration_drift_and_unregistered_arm_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            path, registration = self.registration(temporary)
            with self.assertRaises(ValueError):
                journal.registration_bindings(path, "unregistered-arm")
            registration["workload"]["test"] = "changed-after-registration"
            os.chmod(path, 0o600)
            path.write_text(json.dumps(registration))
            with self.assertRaises(ValueError):
                journal.registration_bindings(path, "upstream")

    def test_legacy_unbound_start_arguments_refused(self):
        with self.assertRaises(SystemExit), patch.object(journal.sys, "stderr", io.StringIO()):
            journal.main(["start", "--directory", "/not-created", "--experiment-id", "e", "--protocol", "/not-read",
                "--arm", "upstream", "--operator", "operator-a", "--phase", "install"])


if __name__ == "__main__":
    unittest.main()
