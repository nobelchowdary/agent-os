"""Actual v3 collector output with fake cluster I/O; not benchmark evidence."""

import argparse
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

HERE = Path(__file__).parent
SPEC = importlib.util.spec_from_file_location("evaluation_fixtures", HERE / "test_evaluate.py")
fixtures = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixtures)
ev = fixtures.ev
DSPEC = importlib.util.spec_from_file_location("measured_decision", HERE.parent / "decision_report.py")
decision = importlib.util.module_from_spec(DSPEC)
DSPEC.loader.exec_module(decision)


class Channel:
    def __init__(self, kube):
        self.kube, self.closed = kube, False

    def get(self, *args, **kwargs):
        return self.kube.get(*args, **kwargs)

    def close(self):
        self.closed = True


class MeasuredKube(fixtures.FakeKube):
    def __init__(self, registration, slow_logs=False):
        super().__init__(registration)
        self.job_polls = self.pod_polls = 0
        self.slow_logs, self.closed = slow_logs, False
        self.channels = []

    def start(self):
        return self

    def fork(self):
        channel = Channel(self)
        self.channels.append(channel)
        return channel

    def close(self):
        self.closed = True

    def create(self, manifest):
        value = super().create(manifest)
        if value["kind"] == "Job":
            value["status"]["conditions"] = []
            self.objects[("Job", value["metadata"]["name"])] = copy.deepcopy(value)
        return value

    def get(self, resource, name=None, selector=None):
        value = super().get(resource, name, selector)
        sampling = threading.current_thread().name == "evaluation-outcome"
        if resource == "job" and value:
            if sampling:
                self.job_polls += 1
            if self.job_polls >= 3:
                value["status"]["conditions"] = [{"type": "Complete", "status": "True"}]
        if resource == "pods" and not name:
            if sampling:
                self.pod_polls += 1
            for pod in value["items"]:
                pod["metadata"]["labels"][ev.LABEL] = self.registration["registration_id"]
                if sampling:
                    pod["status"]["phase"] = "Running" if self.pod_polls < 3 else "Succeeded"
        return value

    def logs(self, pod):
        if self.slow_logs:
            time.sleep(.2)
        return super().logs(pod)


class MeasuredAPI(fixtures.FakeAPI):
    def __init__(self, kube):
        super().__init__(kube)
        self.polls = 0
        self.closed = False

    def fork(self):
        return self

    def close(self):
        self.closed = True

    def record(self):
        value = super().record()
        value["state"] = "succeeded" if self.polls >= 3 else "running"
        return value

    def call(self, path, body=None, key=None):
        if path == "/v1/runs":
            self.polls = 0
        if path == "/v1/runs/" + self.run_id:
            self.polls += 1
        return super().call(path, body, key)


class MeasuredEvaluationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.protocol = ev.read_json(fixtures.ROOT / "evaluation/decision.short-measured.example.json")
        self.protocol["minimum_trials"] = 2
        self.protocol["observation_policy"]["period_seconds"] = .01
        self.registration = {"format_version": 1, "registration_id": "1" * 32, "experiment_id": self.protocol["experiment_id"],
            "protocol": self.protocol, "protocol_sha256": ev.digest(self.protocol), "workload": fixtures.workload(),
            "workload_sha256": ev.digest(fixtures.workload()), "context": "offline-test", "namespace": "agentos-workloads",
            "environment": {"reference": "offline-test.json", "sha256": "b" * 64}, "trial_count": 2,
            "poll_seconds": .01, "trial_timeout_seconds": 60, "cache_state": "uncontrolled", "upstream_controller_ref": "test@sha256:" + "1" * 64}

    def run_trial(self, arm, kube=None, api=None):
        kube = kube or MeasuredKube(self.registration)
        api = api or MeasuredAPI(kube)
        result = {"index": 0}
        ev.trial(kube, self.registration, arm, 0, result, lambda: None, api if arm == "agentos" else None)
        return result, kube

    def test_all_native_arms_emit_decision_compatible_intervals(self):
        for arm in ev.BASELINES:
            with self.subTest(arm=arm):
                result, kube = self.run_trial(arm)
                self.assertTrue(result["correct"], result)
                self.assertEqual(result["cleanup"], "observed_objects_absent")
                event = result["timing"]["events"]["outcome_visible_to_caller"]
                interval = decision.observation_interval(result, "outcome_visible_to_caller", event["elapsed_seconds"])
                self.assertLess(interval["gap"], .5)
                self.assertIsNotNone(event["observation_interval"]["last_nonterminal_request_start_seconds"])
                decision.timing_seconds(result, "outcome_visible_to_caller", ev.timing_contract(arm))
                self.assertTrue(all(channel.closed for channel in kube.channels))
                maximum = {"trials": [dict(copy.deepcopy(result), index=i) for i in range(200)]}
                self.assertLess(len(ev.encoded(maximum)), ev.MAX_EVIDENCE)

    def test_slow_log_validation_does_not_determine_caller_sampling(self):
        for arm in ev.BASELINES:
            with self.subTest(arm=arm):
                result, _ = self.run_trial(arm, MeasuredKube(self.registration, slow_logs=True))
                self.assertTrue(result["correct"], result)
                timing = result["timing"]
                event = timing["events"]["outcome_visible_to_caller"]
                self.assertGreater(result["elapsed_seconds"] - event["elapsed_seconds"], .1)
                self.assertLess(event["observation_interval"]["gap_seconds"], .5)

    def test_unrelated_upstream_owner_cannot_turn_sample_into_success(self):
        kube = MeasuredKube(self.registration)
        original = kube.get
        def get(resource, name=None, selector=None):
            value = original(resource, name, selector)
            if resource == "pods" and not name:
                for pod in value["items"]:
                    pod["metadata"]["ownerReferences"] = [{"uid": "foreign-sandbox", "controller": True}]
            return value
        kube.get = get
        result, _ = self.run_trial("agent-sandbox", kube)
        self.assertFalse(result["correct"])
        self.assertEqual(result["error"], "descendant_owner_changed")

    def test_unsupported_api_state_cannot_narrow_a_nonterminal_bound(self):
        kube = MeasuredKube(self.registration)
        api = MeasuredAPI(kube)
        original = api.record
        def record():
            value = original()
            if api.polls:
                value["state"] = "future_unknown_state"
            return value
        api.record = record
        # Avoid a fake cancellation loop: this test targets the sampling failure.
        with patch.object(ev, "cancel_api"):
            result, _ = self.run_trial("agentos", kube, api)
        self.assertFalse(result["correct"])
        self.assertEqual(result["error"], "invalid_api_run_state")
        self.assertEqual(result["timing"]["events"]["outcome_visible_to_caller"]["status"], "missing")

    def test_shutdown_failure_still_retains_trial_and_cleans_owned_execution(self):
        original = ev.OutcomeObserver.close
        def close(observer):
            original(observer)
            raise ev.Failure("outcome_observer_shutdown_timeout")
        with patch.object(ev.OutcomeObserver, "close", close):
            result, kube = self.run_trial("direct-job")
        self.assertFalse(result["correct"])
        self.assertEqual(result["error"], "outcome_observer_shutdown_timeout")
        self.assertEqual(result["status"], "finished")
        self.assertEqual(result["cleanup"], "observed_objects_absent")
        self.assertTrue(all(channel.closed for channel in kube.channels))

    def test_v3_registration_derives_cadence_and_rejects_flag_drift(self):
        for boundary in ("pod_terminal", "validated_completion"):
            protocol = copy.deepcopy(self.protocol)
            protocol["latency_policy"]["boundary"] = boundary
            with self.assertRaisesRegex(ev.Failure, "unsupported_observation_boundary"):
                ev.validate_protocol(protocol)
        (self.directory / "protocol.json").write_text(json.dumps(self.protocol))
        (self.directory / "workload.json").write_text(json.dumps(self.registration["workload"]))
        (self.directory / "environment.json").write_text('{}')
        args = argparse.Namespace(protocol=self.directory / "protocol.json", workload=self.directory / "workload.json",
            environment=self.directory / "environment.json", evidence_dir=self.directory / "evidence", context="test",
            namespace="agentos-workloads", trials=None, poll_seconds=.5, trial_timeout_seconds=60,
            cache_state="uncontrolled", upstream_controller_ref="test@sha256:" + "1" * 64)
        with self.assertRaisesRegex(ev.Failure, "poll_must_match"):
            ev.register(args)
        self.assertFalse(args.evidence_dir.exists())
        args.poll_seconds = None
        ev.register(args)
        self.assertEqual(ev.read_json(args.evidence_dir / "registration.json")["poll_seconds"], .01)

    def test_verified_job_cleanup_can_precede_next_api_sampling_tick(self):
        kube = MeasuredKube(self.registration)
        api = MeasuredAPI(kube)
        original = kube.get
        removed = False
        def get(resource, name=None, selector=None):
            nonlocal removed
            value = original(resource, name, selector)
            if resource == "job" and value and not removed and any(op[0] == "patch" for op in kube.operations):
                kube.objects.pop(("Job", name))
                api.polls = 2
                removed = True
                return None
            return value
        kube.get = get
        result, _ = self.run_trial("agentos", kube, api)
        self.assertTrue(removed)
        self.assertTrue(result["correct"], result)
        self.assertTrue(result["execution_disappearance_observed"])
        self.assertEqual(result["cleanup"], "observed_objects_absent")
        self.assertEqual(result["timing"]["events"]["pod_terminal"]["status"], "missing")

    def test_full_collector_reports_bind_to_registration_and_remain_unqualified(self):
        registration_path = self.directory / "registration.json"
        ev.write_json(registration_path, self.registration, exclusive=True)
        original = registration_path.read_bytes()
        reports = {}
        for arm in ev.BASELINES:
            kube = MeasuredKube(self.registration)
            api = MeasuredAPI(kube)
            args = argparse.Namespace(context=self.registration["context"], evidence_dir=self.directory,
                execute=True, baseline=arm, api_url=api.base, token_env="TEST_TOKEN")
            with patch.object(ev.transport, "KubernetesProxy", return_value=kube), \
                 patch.object(ev, "PersistentAgentOS", return_value=api):
                reports[arm] = ev.read_json(ev.collect(args))
            self.assertTrue(kube.closed)
            self.assertTrue(reports[arm]["completed"])
            decision.summarize(reports[arm], self.protocol, ev.digest(self.protocol), arm)
        result = decision.decide(self.protocol, ev.digest(self.protocol), reports["agent-sandbox"],
                                 reports["agentos"], self.registration)
        self.assertEqual(result["decision"], "insufficient_evidence")
        self.assertEqual(registration_path.read_bytes(), original)
        self.assertFalse((self.directory / ".collect.lock").exists())


if __name__ == "__main__":
    unittest.main()
