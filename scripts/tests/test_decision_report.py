import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("decision_report", Path(__file__).parents[1] / "decision_report.py")
decision = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(decision)


class DecisionTests(unittest.TestCase):
    def setUp(self):
        self.protocol = {"format_version": 1, "experiment_id": "e", "baseline": "agent-sandbox", "candidate": "agentos",
                         "minimum_trials": 30, "minimum_correct_fraction": 1.0, "max_p95_latency_ratio": 1.1,
                         "min_cost_improvement_fraction": 0.2, "min_operator_improvement_fraction": 0.25}
        self.digest = decision.collector.digest(self.protocol)
        workload = decision.collector.read_json(Path(__file__).resolve().parents[2] / "examples/evaluation/python-workload.json")
        self.registration = {"format_version": 1, "experiment_id": "e", "protocol": self.protocol,
            "protocol_sha256": self.digest, "workload": workload, "workload_sha256": decision.collector.digest(workload),
            "context": "offline-only", "namespace": "agentos-workloads", "trial_count": 30,
            "environment": {"reference": "offline-environment.json", "sha256": "b" * 64}}
        self.baseline = {"format_version": 1, "experiment_id": "e", "baseline": "agent-sandbox", "protocol_sha256": self.digest,
                         "registration_sha256": decision.collector.digest(self.registration), "requested_trials": 30,
                         "workload_sha256": self.registration["workload_sha256"], "environment_sha256": "b" * 64, "completed": True,
                         "qualification": {"status": "passed", "evidence_ref": "live-cases.json"},
                         "trials": [{"index": i, "correct": True, "elapsed_seconds": 10} for i in range(30)]}
        self.candidate = copy.deepcopy(self.baseline)
        self.candidate["baseline"] = "agentos"

    def evaluate(self):
        return decision.decide(self.protocol, self.digest, self.baseline, self.candidate, self.registration)

    def effort(self, report, minutes, task_ref="install-standard-gvisor-v1"):
        return {**{key: report[key] for key in decision.OPERATOR_BINDINGS},
                "task_ref": task_ref, "completed": True, "phase": "install",
                "record_ref": "ops.json", "session_minutes": minutes}

    def test_declarations_cannot_pass(self):
        self.candidate["qualification"] = {"status": "not_verified"}
        self.assertEqual(self.evaluate()["decision"], "insufficient_evidence")

    def test_failed_boundary_stops_experiment(self):
        self.candidate["qualification"]["status"] = "failed"
        self.assertEqual(self.evaluate()["decision"], "stop_experiment")

    def test_changed_protocol_cannot_be_compared(self):
        self.candidate["protocol_sha256"] = "changed"
        with self.assertRaises(ValueError):
            self.evaluate()

    def test_different_task_or_environment_cannot_win(self):
        self.candidate["workload_sha256"] = "c" * 64
        with self.assertRaises(ValueError):
            self.evaluate()

    def test_optional_stopping_does_not_win(self):
        self.candidate["completed"] = False
        self.assertEqual(self.evaluate()["decision"], "insufficient_evidence")

    def test_different_registration_cannot_be_compared(self):
        self.candidate["registration_sha256"] = "d" * 64
        with self.assertRaises(ValueError):
            self.evaluate()

    def test_registered_trial_count_cannot_be_truncated_or_changed(self):
        for count in (True, 0, 29, 100, None):
            with self.subTest(requested_trials=count), self.assertRaises(ValueError):
                bad = copy.deepcopy(self.candidate)
                bad["requested_trials"] = count
                decision.decide(self.protocol, self.digest, self.baseline, bad, self.registration)
        bad = copy.deepcopy(self.candidate)
        bad["requested_trials"] = 31
        bad["trials"].append({"index": 30, "correct": True, "elapsed_seconds": 10})
        with self.assertRaises(ValueError):
            decision.decide(self.protocol, self.digest, self.baseline, bad, self.registration)

    def test_both_arms_cannot_shorten_actual_registered_trial_count(self):
        self.registration["trial_count"] = 60
        for report in (self.baseline, self.candidate):
            report["registration_sha256"] = decision.collector.digest(self.registration)
            report["requested_trials"] = 60
            report["trials"] = [{"index": i, "correct": True, "elapsed_seconds": 10} for i in range(60)]
        self.assertEqual(self.evaluate()["decision"], "insufficient_evidence")
        for report in (self.baseline, self.candidate):
            report["requested_trials"] = 30
            report["trials"] = report["trials"][:30]
        with self.assertRaises(ValueError):
            self.evaluate()

    def test_both_reports_cannot_relabel_registered_hashes(self):
        for key in ("registration_sha256", "workload_sha256", "environment_sha256"):
            baseline, candidate = copy.deepcopy(self.baseline), copy.deepcopy(self.candidate)
            baseline[key] = candidate[key] = "f" * 64
            with self.subTest(binding=key), self.assertRaises(ValueError):
                decision.decide(self.protocol, self.digest, baseline, candidate, self.registration)

    def test_registration_context_namespace_count_and_input_drift_rejected(self):
        for key, value in (("context", ""), ("namespace", "-invalid"), ("trial_count", True),
                           ("protocol_sha256", "f" * 64), ("workload_sha256", "f" * 64)):
            registration = copy.deepcopy(self.registration)
            registration[key] = value
            with self.subTest(field=key), self.assertRaises(ValueError):
                decision.registration_bindings(registration, self.protocol, self.digest)
        registration = copy.deepcopy(self.registration)
        registration["workload"]["command"] = ["python", "-c", "print('changed')"]
        with self.assertRaises(ValueError):
            decision.registration_bindings(registration, self.protocol, self.digest)

    def test_library_decision_requires_actual_registration(self):
        with self.assertRaises(TypeError):
            decision.decide(self.protocol, self.digest, self.baseline, self.candidate)

    def test_trial_indexes_must_be_unique_contiguous_integers(self):
        for index in (True, 1, 30, -1, None):
            with self.subTest(index=index), self.assertRaises(ValueError):
                bad = copy.deepcopy(self.candidate)
                bad["trials"][0]["index"] = index
                decision.decide(self.protocol, self.digest, self.baseline, bad, self.registration)

    def test_successful_zero_timing_rejected_in_either_arm(self):
        for arm in (self.baseline, self.candidate):
            with self.subTest(arm=arm["baseline"]), self.assertRaises(ValueError):
                old = arm["trials"][0]["elapsed_seconds"]
                arm["trials"][0]["elapsed_seconds"] = 0
                try:
                    self.evaluate()
                finally:
                    arm["trials"][0]["elapsed_seconds"] = old

    def test_lost_work_cannot_appear_efficient(self):
        self.candidate["trials"][0] = {"index": 0, "correct": False, "elapsed_seconds": 0.01}
        self.assertEqual(self.evaluate()["decision"], "change_direction")

    def test_synthetic_costs_do_not_win(self):
        self.baseline.update(full_cost_usd=100)
        self.candidate.update(full_cost_usd=1)
        result = self.evaluate()
        self.assertFalse(result["routes"]["billing_cost"]["eligible"])
        self.assertEqual(result["decision"], "insufficient_evidence")

    def test_observed_cost_route_can_continue_validation(self):
        for report, cost in ((self.baseline, 100), (self.candidate, 70)):
            report.update(full_cost_usd=cost, billing_complete=True, node_reduction_demonstrated=True, cost_accounting_ref="billing.json")
        self.assertEqual(self.evaluate()["decision"], "continue_validation")

    def test_operator_route_needs_repeated_recorded_sessions(self):
        self.baseline["operator_effort"] = self.effort(self.baseline, [60, 55, 65])
        self.candidate["operator_effort"] = self.effort(self.candidate, [30, 32, 29])
        self.assertEqual(self.evaluate()["decision"], "continue_validation")

    def test_operator_records_bind_every_registered_input(self):
        for key in decision.OPERATOR_BINDINGS:
            report = copy.deepcopy(self.candidate)
            report["operator_effort"] = self.effort(report, [30, 32, 29])
            report["operator_effort"][key] = "other"
            with self.subTest(binding=key), self.assertRaises(ValueError):
                decision.decide(self.protocol, self.digest, self.baseline, report, self.registration)

    def test_operator_route_requires_same_concrete_task(self):
        self.baseline["operator_effort"] = self.effort(self.baseline, [60, 55, 65])
        self.candidate["operator_effort"] = self.effort(self.candidate, [30, 32, 29], task_ref="different-install-task")
        self.assertFalse(self.evaluate()["routes"]["operator_effort"]["eligible"])
        self.assertEqual(self.evaluate()["decision"], "insufficient_evidence")

    def test_summary_binding_cannot_disagree_with_nested_effort(self):
        effort = self.effort(self.candidate, [30, 32, 29])
        summary = {"format_version": 1, **effort, "operator_effort": dict(effort)}
        summary["operator_effort"]["task_ref"] = "another-task"
        with self.assertRaises(ValueError):
            decision.attach_operations(self.candidate, summary)

    def test_nan_bool_and_negative_measurements_rejected(self):
        for invalid in (float("nan"), True, -1):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                bad = copy.deepcopy(self.candidate)
                bad["trials"][0]["elapsed_seconds"] = invalid
                decision.decide(self.protocol, self.digest, self.baseline, bad, self.registration)

    def test_latency_regression_blocks_a_cost_win(self):
        self.candidate["trials"][0:3] = [{"index": i, "correct": True, "elapsed_seconds": 50} for i in range(3)]
        for report, cost in ((self.baseline, 100), (self.candidate, 1)):
            report.update(full_cost_usd=cost, billing_complete=True, node_reduction_demonstrated=True, cost_accounting_ref="billing.json")
        self.assertEqual(self.evaluate()["decision"], "change_direction")

    def test_supplemental_evidence_cannot_replace_trials(self):
        metadata = {k: self.baseline[k] for k in ("experiment_id", "protocol_sha256", "registration_sha256", "workload_sha256", "environment_sha256")}
        metadata.update(format_version=1, arms={"agentos": {"trials": []}})
        with self.assertRaises(ValueError):
            decision.apply_supplemental(metadata, self.baseline, self.candidate)

    def test_supplemental_evidence_is_bound_to_registration(self):
        metadata = {key: self.baseline[key] for key in ("experiment_id", "protocol_sha256", "registration_sha256", "workload_sha256", "environment_sha256")}
        metadata.update(format_version=1, arms={})
        metadata["registration_sha256"] = "d" * 64
        with self.assertRaises(ValueError):
            decision.apply_supplemental(metadata, self.baseline, self.candidate)

    def test_offline_registration_and_decision_cli_agree_on_hashes(self):
        root = Path(__file__).resolve().parents[2]
        specification = importlib.util.spec_from_file_location("registration_interop", root / "scripts/evaluate.py")
        collector = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(collector)
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            args = SimpleNamespace(protocol=root / "evaluation/decision.example.json",
                workload=root / "examples/evaluation/python-workload.json",
                environment=root / "examples/evaluation/environment.example.json",
                context="offline-test-only", namespace="agentos-workloads", trials=60,
                upstream_controller_ref="registry/controller@sha256:" + "a" * 64, poll_seconds=0.5, trial_timeout_seconds=360,
                evidence_dir=directory / "registered", cache_state="uncontrolled")
            collector.register(args)
            registration = json.loads((args.evidence_dir / "registration.json").read_text())
            for arm in ("agent-sandbox", "agentos"):
                report = {"format_version": 1, "experiment_id": registration["experiment_id"], "baseline": arm,
                    "protocol_sha256": registration["protocol_sha256"], "workload_sha256": registration["workload_sha256"],
                    "registration_sha256": collector.digest(registration), "requested_trials": registration["trial_count"],
                    "environment_sha256": registration["environment"]["sha256"], "completed": True,
                    "qualification": {"status": "not_verified"},
                    "trials": [{"index": i, "correct": True, "elapsed_seconds": 1} for i in range(registration["trial_count"])]}
                (directory / (arm + ".json")).write_text(json.dumps(report))
            result = subprocess.run([sys.executable, str(root / "scripts/decision_report.py"),
                "--registration", str(args.evidence_dir / "registration.json"), "--protocol", str(args.protocol), "--baseline", str(directory / "agent-sandbox.json"),
                "--candidate", str(directory / "agentos.json"), "--output", str(directory / "decision.json")],
                capture_output=True, timeout=10, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads((directory / "decision.json").read_text())["decision"], "insufficient_evidence")

            # Exercise the real journal CLI/file contract against collector registration.
            spec = importlib.util.spec_from_file_location("journal_interop", root / "scripts/operator_journal.py")
            journal = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(journal)

            def run_journal(argv):
                with patch("builtins.print"):
                    return journal.main(argv)

            for arm, duration in (("agent-sandbox", 3600), ("agentos", 1800)):
                sessions = []
                for index in range(3):
                    session = directory / (arm + "-ops-" + str(index))
                    sessions.append(str(session))
                    with patch.object(journal.time, "time", return_value=100):
                        self.assertEqual(run_journal(["start", "--directory", str(session), "--registration", str(args.evidence_dir / "registration.json"),
                            "--arm", arm, "--operator", "offline-operator", "--phase", "install", "--task-ref", "install-standard-gvisor-v1"]), 0)
                    with patch.object(journal.time, "time", return_value=101):
                        self.assertEqual(run_journal(["step", "--directory", str(session), "--name", "offline-fixture-step", "--runbook-ref", "install-v1",
                            "--privilege", "node-admin", "--node-customization", "--outcome", "succeeded"]), 0)
                    with patch.object(journal.time, "time", return_value=100 + duration):
                        self.assertEqual(run_journal(["finish", "--directory", str(session), "--qualified", "--evidence-ref", "offline-test-fixture-only"]), 0)
                self.assertEqual(run_journal(["summary", "--sessions", *sessions, "--output", str(directory / (arm + "-ops.json"))]), 0)
            sidecar = {key: report[key] for key in ("experiment_id", "protocol_sha256", "registration_sha256", "workload_sha256", "environment_sha256")}
            sidecar.update(format_version=1, arms={arm: {"qualification": {"status": "passed", "evidence_ref": "offline-test-fixture-only"}} for arm in ("agent-sandbox", "agentos")})
            (directory / "supplemental.json").write_text(json.dumps(sidecar))
            result = subprocess.run([sys.executable, str(root / "scripts/decision_report.py"), "--registration", str(args.evidence_dir / "registration.json"), "--protocol", str(args.protocol),
                "--baseline", str(directory / "agent-sandbox.json"), "--candidate", str(directory / "agentos.json"),
                "--supplemental", str(directory / "supplemental.json"), "--baseline-operations", str(directory / "agent-sandbox-ops.json"),
                "--candidate-operations", str(directory / "agentos-ops.json"), "--output", str(directory / "operator-decision.json")],
                capture_output=True, timeout=10, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads((directory / "operator-decision.json").read_text())["decision"], "continue_validation")

            # Both arms can otherwise collude in optional stopping while retaining
            # the original registration hash. The actual registration is authoritative.
            for arm in ("agent-sandbox", "agentos"):
                path = directory / (arm + ".json")
                shortened = json.loads(path.read_text())
                shortened["requested_trials"] = 30
                shortened["trials"] = shortened["trials"][:30]
                path.write_text(json.dumps(shortened))
            output = directory / "shortened-decision.json"
            result = subprocess.run([sys.executable, str(root / "scripts/decision_report.py"),
                "--registration", str(args.evidence_dir / "registration.json"), "--protocol", str(args.protocol),
                "--baseline", str(directory / "agent-sandbox.json"), "--candidate", str(directory / "agentos.json"),
                "--output", str(output)], capture_output=True, timeout=10, check=False)
            self.assertEqual(result.returncode, 2)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
