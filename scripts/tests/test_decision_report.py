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

    def use_explicit_latency(self, boundary="outcome_visible_to_caller", baseline=1, candidate=3.05):
        self.protocol["format_version"] = 2
        self.protocol["latency_policy"] = {"boundary": boundary, "mode": "ratio_plus_absolute",
            "absolute_allowance_seconds": 2.0,
            "rationale": "Confirmed outcome allowance agreed before this offline fixture was registered."}
        self.digest = decision.collector.digest(self.protocol)
        self.registration["protocol"] = copy.deepcopy(self.protocol)
        self.registration["protocol_sha256"] = self.digest
        for report, observed in ((self.baseline, baseline), (self.candidate, candidate)):
            report["protocol_sha256"] = self.digest
            report["registration_sha256"] = decision.collector.digest(self.registration)
            report["latency_policy"] = copy.deepcopy(self.protocol["latency_policy"])
            report["timing_contract"] = decision.collector.timing_contract(report["baseline"])
            for trial in report["trials"]:
                trial["elapsed_seconds"] = observed if boundary == "validated_completion" else 20
                contract = report["timing_contract"]
                parent_uid, pod_uid = f"parent-{trial['index']}", f"pod-{trial['index']}"
                binding = {"pod_uid": pod_uid, "pod_name": "fixture-pod", "parent_uid": parent_uid}
                upstream = report["baseline"] == "agent-sandbox"
                trial["resources"] = [{"kind": "SandboxClaim" if upstream else "Job", "name": "fixture-parent",
                                       "uid": "fixture-claim" if upstream else parent_uid}]
                trial["descendants"] = [{"kind": "Pod", "name": "fixture-pod", "uid": pod_uid, "parent_uid": parent_uid}]
                if upstream:
                    trial["descendants"].append({"kind": "Sandbox", "name": "fixture-parent", "uid": parent_uid,
                                                 "parent_uid": "fixture-claim"})
                if report["baseline"] == "agentos":
                    trial["run_id"] = "a" * 32
                trial["timing"] = {"format_version": 1, "clock_basis": "harness_monotonic_offsets", "execution": binding, "events": {
                    name: {"status": "observed", "elapsed_seconds": (
                        observed if name == boundary else (20 if name == "validated_completion" else 0.5)),
                        "observed_at": "2026-11-24T20:00:00Z", "source": contract["events"][name]["source"]}
                    for name in decision.TIMING_BOUNDARIES}}
                for event in trial["timing"]["events"].values():
                    source = event["source"]
                    event["state"] = {"agentos_run_api": "succeeded", "kubernetes_job_condition": "Complete",
                        "kubernetes_pod_phase": "Succeeded", "sandbox_pod_phase": "Succeeded",
                        "collector_validated_output": "succeeded"}[source]
                    if source == "agentos_run_api":
                        event["run_id"] = trial["run_id"]
                    elif source == "kubernetes_job_condition":
                        event["resource_uid"] = parent_uid
                    elif source == "kubernetes_pod_phase":
                        event.update(binding)
                    elif source == "sandbox_pod_phase":
                        event.update(pod_uid=pod_uid, parent_uid=parent_uid)

    def measured_cost_win(self):
        for report, cost in ((self.baseline, 100), (self.candidate, 70)):
            report.update(full_cost_usd=cost, billing_complete=True, node_reduction_demonstrated=True,
                          cost_accounting_ref="offline-billing-fixture.json")

    def test_legacy_ratio_and_output_contract_stay_unchanged(self):
        expected = {"trials": 30, "correct": 30, "correct_fraction": 1.0,
                    "successful_p95_seconds": 10, "qualification": "passed"}
        result = self.evaluate()
        self.assertEqual(result["format_version"], 1)
        self.assertEqual(result["baseline"], expected)
        self.assertNotIn("latency_gate", result)
        self.measured_cost_win()
        for trial in self.candidate["trials"]:
            trial["elapsed_seconds"] = 11
        self.assertEqual(self.evaluate()["decision"], "continue_validation")
        self.candidate["trials"][0:2] = [
            {"index": i, "correct": True, "elapsed_seconds": 11.01} for i in range(2)]
        self.assertEqual(self.evaluate()["decision"], "change_direction")

    def test_explicit_additive_allowance_is_not_maximum_of_the_terms(self):
        self.use_explicit_latency()
        self.measured_cost_win()
        result = self.evaluate()
        self.assertEqual(result["format_version"], 2)
        self.assertEqual(result["decision"], "continue_validation")
        self.assertAlmostEqual(result["successful_p95_latency_ratio"], 3.05)
        gate = result["latency_gate"]
        self.assertTrue(gate["passes"])
        self.assertAlmostEqual(gate["allowed_candidate_p95_seconds"], 3.1)
        self.assertEqual(gate["policy"], self.protocol["latency_policy"])
        self.assertEqual(gate["baseline_p95_seconds"], 1)
        self.assertEqual(gate["candidate_p95_seconds"], 3.05)
        summary = result["candidate"]
        self.assertEqual(summary["latency_boundary"], "outcome_visible_to_caller")
        self.assertEqual(summary["timing_boundaries"]["pod_terminal"], {
            "observed_successful_trials": 30, "missing_successful_trials": 0, "successful_p95_seconds": 0.5})
        self.assertEqual(summary["timing_boundaries"]["validated_completion"]["successful_p95_seconds"], 20)

    def test_explicit_latency_still_blocks_a_cost_win_above_the_additive_limit(self):
        self.use_explicit_latency(candidate=3.11)
        self.measured_cost_win()
        result = self.evaluate()
        self.assertEqual(result["decision"], "change_direction")
        self.assertFalse(result["latency_gate"]["passes"])
        self.assertEqual(result["routes"], {})

    def test_explicit_alternative_boundaries_select_their_own_complete_measurement(self):
        for boundary in ("pod_terminal", "validated_completion"):
            with self.subTest(boundary=boundary):
                self.setUp()
                self.use_explicit_latency(boundary=boundary, baseline=10, candidate=12.5)
                self.measured_cost_win()
                result = self.evaluate()
                self.assertEqual(result["decision"], "continue_validation")
                self.assertEqual(result["candidate"]["latency_boundary"], boundary)
                self.assertEqual(result["candidate"]["successful_p95_seconds"], 12.5)

    def test_missing_selected_boundary_on_one_correct_trial_cannot_create_a_fast_subset(self):
        for omission in ("timing", "event", "missing"):
            with self.subTest(omission=omission):
                self.setUp()
                self.use_explicit_latency()
                self.measured_cost_win()
                trial = self.candidate["trials"][0]
                if omission == "timing":
                    trial.pop("timing")
                elif omission == "event":
                    trial["timing"]["events"].pop("outcome_visible_to_caller")
                else:
                    trial["timing"]["events"]["outcome_visible_to_caller"] = {
                        "status": "missing", "elapsed_seconds": None, "observed_at": None,
                        "source": "agentos_run_api", "reason": "not_observed"}
                result = self.evaluate()
                self.assertEqual(result["decision"], "insufficient_evidence")
                self.assertIsNone(result["candidate"]["successful_p95_seconds"])
                self.assertEqual(result["candidate"]["missing_latency_samples"], 1)
                self.assertIn("complete coverage", result["reasons"][0])
                self.assertIsNone(result["latency_gate"]["passes"])
                self.assertEqual(result["routes"], {})

    def test_unselected_missing_boundaries_remain_visible_without_changing_selected_gate(self):
        self.use_explicit_latency()
        self.measured_cost_win()
        self.candidate["trials"][0]["timing"]["events"].pop("pod_terminal")
        result = self.evaluate()
        self.assertEqual(result["decision"], "continue_validation")
        pod = result["candidate"]["timing_boundaries"]["pod_terminal"]
        self.assertEqual(pod["missing_successful_trials"], 1)
        self.assertIsNone(pod["successful_p95_seconds"])

    def test_correct_trial_events_require_consistent_native_success_states(self):
        self.use_explicit_latency()
        for arm in ("baseline", "candidate"):
            for boundary in decision.TIMING_BOUNDARIES:
                with self.subTest(arm=arm, boundary=boundary), self.assertRaisesRegex(ValueError, "native outcome"):
                    baseline, candidate = copy.deepcopy(self.baseline), copy.deepcopy(self.candidate)
                    report = baseline if arm == "baseline" else candidate
                    report["trials"][0]["timing"]["events"][boundary]["state"] = "Failed"
                    decision.decide(self.protocol, self.digest, baseline, candidate, self.registration)
        for trial in self.candidate["trials"]:
            trial["timing"]["events"]["validated_completion"]["elapsed_seconds"] = 19
        with self.assertRaisesRegex(ValueError, "legacy trial duration"):
            self.evaluate()

    def test_observed_timing_identities_must_agree_with_recorded_execution(self):
        self.use_explicit_latency()
        mutations = (
            lambda trial: trial.update(run_id="b" * 32),
            lambda trial: trial["timing"]["events"]["outcome_visible_to_caller"].update(run_id="b" * 32),
            lambda trial: trial["timing"]["events"]["outcome_visible_to_caller"].pop("run_id"),
            lambda trial: trial["timing"]["execution"].update(pod_uid="other-pod"),
            lambda trial: trial["timing"]["execution"].update(parent_uid="other-parent"),
            lambda trial: trial["timing"]["execution"].update(pod_name="other-name"),
            lambda trial: trial["timing"]["events"]["pod_terminal"].update(pod_uid="other-pod"),
            lambda trial: trial["timing"]["events"]["pod_terminal"].update(parent_uid="other-parent"),
            lambda trial: trial["timing"]["events"]["pod_terminal"].update(pod_name="other-name"),
            lambda trial: trial["descendants"][0].update(uid="other-pod"),
            lambda trial: trial["descendants"][0].update(parent_uid="other-parent"),
            lambda trial: trial["resources"][0].update(uid="other-parent"),
        )
        for index, mutate in enumerate(mutations):
            candidate = copy.deepcopy(self.candidate)
            mutate(candidate["trials"][0])
            with self.subTest(variant=index), self.assertRaises(ValueError):
                decision.decide(self.protocol, self.digest, self.baseline, candidate, self.registration)
        for key in ("pod_uid", "parent_uid"):
            baseline = copy.deepcopy(self.baseline)
            baseline["trials"][0]["timing"]["events"]["outcome_visible_to_caller"][key] = "different"
            with self.subTest(upstream=key), self.assertRaises(ValueError):
                decision.decide(self.protocol, self.digest, baseline, self.candidate, self.registration)

    def test_direct_job_timing_identity_matches_job_and_execution_parent(self):
        self.protocol["baseline"] = self.baseline["baseline"] = "direct-job"
        self.use_explicit_latency()
        self.assertEqual(self.evaluate()["format_version"], 2)
        self.baseline["trials"][0]["timing"]["events"]["outcome_visible_to_caller"]["resource_uid"] = "other-job"
        with self.assertRaisesRegex(ValueError, "Job identity"):
            self.evaluate()

    def test_caller_metric_allows_honestly_missing_pod_identity(self):
        self.use_explicit_latency()
        self.measured_cost_win()
        for trial in self.candidate["trials"]:
            trial["timing"]["execution"] = None
            trial["timing"]["events"]["pod_terminal"] = {"status": "missing", "elapsed_seconds": None,
                "observed_at": None, "source": "kubernetes_pod_phase", "reason": "pod_terminal_not_observed"}
            trial["descendants"] = []
        result = self.evaluate()
        self.assertEqual(result["decision"], "continue_validation")
        self.assertEqual(result["candidate"]["timing_boundaries"]["pod_terminal"]["missing_successful_trials"], 30)

    def test_explicit_policy_contract_sources_and_observations_cannot_be_relabelled(self):
        self.use_explicit_latency()
        variants = []
        for mutate in (
                lambda report: report.pop("latency_policy"),
                lambda report: report["latency_policy"].update(boundary="pod_terminal"),
                lambda report: report.pop("timing_contract"),
                lambda report: report.update(timing_contract=decision.collector.timing_contract("direct-job")),
                lambda report: report["trials"][0]["timing"].update(clock_basis="node_clock"),
                lambda report: report["trials"][0]["timing"].update(format_version=2),
                lambda report: report["trials"][0]["timing"].update(format_version=True),
                lambda report: report["timing_contract"].update(format_version=True),
                lambda report: report["trials"][0]["timing"]["events"]["outcome_visible_to_caller"].update(source="kubernetes_pod_phase"),
                lambda report: report["trials"][0]["timing"]["events"]["outcome_visible_to_caller"].update(observed_at="not-a-date"),
                lambda report: report["trials"][0]["timing"]["events"]["outcome_visible_to_caller"].update(observed_at="2026-11-24T20:00:00"),
                lambda report: report["trials"][0]["timing"]["events"]["outcome_visible_to_caller"].update(status="conflicting"),
                lambda report: report["trials"][0]["timing"]["events"].update(outcome_visible={}),
        ):
            bad = copy.deepcopy(self.candidate)
            mutate(bad)
            variants.append(bad)
        for index, bad in enumerate(variants):
            with self.subTest(variant=index), self.assertRaises(ValueError):
                decision.decide(self.protocol, self.digest, self.baseline, bad, self.registration)

    def test_explicit_timing_rejects_nonfinite_negative_bool_zero_and_late_selected_values(self):
        self.use_explicit_latency()
        for value in (float("nan"), float("inf"), -1, True, 0, 21):
            bad = copy.deepcopy(self.candidate)
            bad["trials"][0]["timing"]["events"]["outcome_visible_to_caller"]["elapsed_seconds"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                decision.decide(self.protocol, self.digest, self.baseline, bad, self.registration)

    def test_explicit_protocol_requires_bound_policy_and_rationale(self):
        self.use_explicit_latency()
        for field, value in (("absolute_allowance_seconds", float("nan")), ("absolute_allowance_seconds", True),
                             ("absolute_allowance_seconds", -1), ("absolute_allowance_seconds", 3601),
                             ("rationale", " "), ("rationale", "é" * 513), ("boundary", "elapsed_seconds"),
                             ("mode", "max_ratio_or_absolute")):
            bad = copy.deepcopy(self.protocol)
            bad["latency_policy"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                decision.validate_protocol(bad)
        for key in ("absolute_allowance_seconds", "rationale", "boundary", "mode"):
            bad = copy.deepcopy(self.protocol)
            bad["latency_policy"].pop(key)
            with self.subTest(missing=key), self.assertRaises(ValueError):
                decision.validate_protocol(bad)
        for field, value in (("rationale", "Retuned after the data"), ("absolute_allowance_seconds", 3.0)):
            bad = copy.deepcopy(self.protocol)
            bad["latency_policy"][field] = value
            with self.subTest(drift=field), self.assertRaises(ValueError):
                decision.decide(bad, self.digest, self.baseline, self.candidate, self.registration)
        legacy = copy.deepcopy(self.protocol)
        legacy["format_version"] = 1
        with self.assertRaises(ValueError):
            decision.validate_protocol(legacy)
        for version in (True, 1.0, 2.0, "2"):
            bad = copy.deepcopy(self.protocol)
            bad["format_version"] = version
            with self.subTest(version=version), self.assertRaises(ValueError):
                decision.validate_protocol(bad)

    def test_finite_timing_inputs_cannot_overflow_the_explicit_gate(self):
        for baseline, candidate in ((1e-308, 1e308), (1.7e308, 1.7e308)):
            with self.subTest(baseline=baseline, candidate=candidate):
                self.setUp()
                self.use_explicit_latency(baseline=baseline, candidate=candidate)
                for report, value in ((self.baseline, baseline), (self.candidate, candidate)):
                    for trial in report["trials"]:
                        trial["elapsed_seconds"] = max(20, value)
                        trial["timing"]["events"]["validated_completion"]["elapsed_seconds"] = trial["elapsed_seconds"]
                with self.assertRaises(ValueError):
                    self.evaluate()

    def test_explicit_timing_cannot_be_supplied_through_a_supplemental_sidecar(self):
        self.use_explicit_latency()
        metadata = {key: self.baseline[key] for key in (
            "experiment_id", "protocol_sha256", "registration_sha256", "workload_sha256", "environment_sha256")}
        metadata.update(format_version=1, arms={"agentos": {"latency_policy": self.protocol["latency_policy"]}})
        with self.assertRaisesRegex(ValueError, "cannot replace trial measurements"):
            decision.apply_supplemental(metadata, self.baseline, self.candidate)

    def test_new_profile_examples_register_separately_without_changing_legacy_examples(self):
        root = Path(__file__).resolve().parents[2]
        protocols, workloads = [], []
        for label in ("short", "minute"):
            protocol = decision.collector.read_json(root / f"evaluation/decision.{label}-confirmed.example.json")
            workload = decision.collector.read_json(root / f"examples/evaluation/python-{label}-workload.json")
            decision.validate_protocol(protocol)
            decision.collector.validate_protocol(protocol)
            decision.collector.validate_workload(workload)
            self.assertEqual(protocol["format_version"], 2)
            self.assertEqual(protocol["latency_policy"]["boundary"], "outcome_visible_to_caller")
            self.assertEqual(protocol["latency_policy"]["absolute_allowance_seconds"], 2.0)
            self.assertEqual(protocol["max_p95_latency_ratio"], 1.1)
            protocols.append(protocol)
            workloads.append(workload)
        self.assertNotEqual(protocols[0]["experiment_id"], protocols[1]["experiment_id"])
        self.assertNotEqual(decision.collector.digest(workloads[0]), decision.collector.digest(workloads[1]))
        self.assertEqual({key for key in workloads[0] if workloads[0][key] != workloads[1][key]},
                         {"command", "timeout_seconds"})
        self.assertEqual(self.protocol["format_version"], 1)
        self.assertNotIn("latency_policy", self.protocol)

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
