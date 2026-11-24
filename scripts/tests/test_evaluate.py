"""Offline contract tests. These are not cluster, isolation or benchmark evidence."""

import argparse
import contextlib
import copy
import http.server
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("evaluate", ROOT / "scripts/evaluate.py")
ev = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ev)


def workload(nitro=False):
    name = "python-nitro-workload.json" if nitro else "python-workload.json"
    return ev.read_json(ROOT / "examples/evaluation" / name)


class FakeKube:
    def __init__(self, registration, token_env=None):
        self.registration = registration
        self.objects = {}
        self.operations = []

    def create(self, manifest):
        obj = copy.deepcopy(manifest)
        obj["metadata"].update(uid="uid-" + manifest["metadata"]["name"], resourceVersion="1")
        if obj["kind"] == "Job":
            obj["status"] = {"conditions": [{"type": "Complete", "status": "True"}]}
        if obj["kind"] == "SandboxClaim":
            obj["status"] = {"sandbox": {"name": obj["metadata"]["name"]}}
            sandbox = {"apiVersion": ev.CORE, "kind": "Sandbox", "metadata": {
                "name": obj["metadata"]["name"], "namespace": self.registration["namespace"],
                "uid": "sandbox-" + obj["metadata"]["uid"], "resourceVersion": "1",
                "labels": {"agents.x-k8s.io/launch-type": "cold"}, "ownerReferences": [
                    {"uid": obj["metadata"]["uid"], "controller": True}]}}
            self.objects[("Sandbox", sandbox["metadata"]["name"])] = sandbox
        self.objects[(obj["kind"], obj["metadata"]["name"])] = obj
        self.operations.append(("create", obj["kind"], obj["metadata"]["name"]))
        return copy.deepcopy(obj)

    def get(self, resource, name=None, selector=None):
        if resource == "runtimeclasses.node.k8s.io":
            return {"handler": self.registration["workload"]["runtime_profile"]["required_handler"],
                    "scheduling": {"nodeSelector": {"eks.amazonaws.com/nitro-sandbox": "true"}},
                    "overhead": {"podFixed": {"memory": "256Mi", "eks.amazonaws.com/nitro-sandbox": "1"}}}
        if resource == "serviceaccount":
            return {"metadata": {"name": name}}
        if resource == "networkpolicy":
            return {"spec": {"podSelector": {"matchLabels": {ev.PART: "agent-os"}},
                             "policyTypes": ["Ingress", "Egress"], "ingress": [], "egress": []}}
        if resource == "pods" and name:
            return None
        if resource == "pods":
            assert selector in {ev.LABEL + "=" + self.registration["registration_id"], "agentos.dev/run-id=" + "a" * 32}
            pods = []
            for (kind, obj_name), obj in self.objects.items():
                if kind in {"Job", "Sandbox"}:
                    template = ev.pod_template(self.registration["workload"], {ev.PART: "agent-os"}, execution_deadline=(kind == "Sandbox"))
                    spec = template["spec"]
                    # Actual API servers canonicalize 1000m to 1.
                    if spec["containers"][0]["resources"]["requests"]["cpu"] == "1000m":
                        for section in ("requests", "limits"):
                            spec["containers"][0]["resources"][section]["cpu"] = "1"
                    pods.append({"apiVersion": "v1", "kind": "Pod", "metadata": dict(template["metadata"], **{
                                              "name": obj_name + "-pod", "uid": "pod-" + obj_name,
                                              "namespace": self.registration["namespace"],
                                              "ownerReferences": [{"uid": obj["metadata"]["uid"], "controller": True}]},
                                             ), "spec": spec, "status": {"phase": "Succeeded"}})
            return {"items": pods}
        kinds = {"job": "Job", "jobs": "Job", "sandboxes.agents.x-k8s.io": "Sandbox",
                 "sandboxclaims.extensions.agents.x-k8s.io": "SandboxClaim",
                 "sandboxtemplates.extensions.agents.x-k8s.io": "SandboxTemplate",
                 "sandboxwarmpools.extensions.agents.x-k8s.io": "SandboxWarmPool"}
        return copy.deepcopy(self.objects.get((kinds[resource], name)))

    def logs(self, pod):
        check = self.registration["workload"]["validation"]
        return check["ready_line"] + "\n" + check["complete_line"] + "\n"

    def call(self, args, body=None, limit=None):
        self.operations.append((args[0], args, body))
        if args[0] == "delete":
            parts = args[2].split("/")
            plural, name = parts[-2:]
            kinds = {"jobs": "Job", "sandboxclaims": "SandboxClaim", "sandboxtemplates": "SandboxTemplate",
                     "sandboxwarmpools": "SandboxWarmPool"}
            obj = self.objects[(kinds[plural], name)]
            assert body["preconditions"] == {"uid": obj["metadata"]["uid"], "resourceVersion": "1"}
            assert body["propagationPolicy"] == "Foreground"
            del self.objects[(kinds[plural], name)]
            if kinds[plural] == "SandboxClaim":
                self.objects.pop(("Sandbox", name), None)
        elif args[0] == "patch":
            obj = self.objects[("Job", args[2])]
            operations = json.loads(args[args.index("--patch") + 1])
            assert operations[0]["value"] == obj["metadata"]["uid"]
            assert operations[1]["value"] == "1"
            obj["metadata"]["labels"][ev.LABEL] = operations[2]["value"]
        return "{}"


class FakeAPI:
    base = "http://127.0.0.1:8080"

    def __init__(self, kube):
        self.kube = kube
        self.registration = kube.registration
        self.run_id = "a" * 32
        self.submissions = []

    def record(self):
        w = self.registration["workload"]
        return {"id": self.run_id, "state": "succeeded", "spec": {
            **{k: w[k] for k in ("template", "image", "command", "resources", "timeout_seconds", "runtime_profile")},
            **({"termination_grace_period_seconds": w["termination_grace_period_seconds"]} if "termination_grace_period_seconds" in w else {}),
            "runtime_class": w["runtime_profile"]["runtime_class"]}}

    def call(self, path, body=None, key=None):
        if path == "/readyz":
            return {"status": "ready"}
        if path == "/v1/plan":
            return {"execution": False, "job": ev.job_manifest(self.registration, "plan", 0)}
        if path == "/v1/runs":
            self.submissions.append(key)
            job = ev.job_manifest(self.registration, "agentos-" + self.run_id, 0)
            job["metadata"]["labels"] = {ev.MANAGED: "agent-os", "agentos.dev/run-id": self.run_id, ev.PART: "agent-os"}
            self.kube.create(job)
            return self.record()
        if path.endswith("/logs"):
            return {"logs": self.kube.logs("unused")}
        return self.record()


class EvaluationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.protocol = {"format_version": 1, "experiment_id": "test-evaluation", "baseline": "agent-sandbox",
                         "candidate": "agentos", "minimum_trials": 2, "minimum_correct_fraction": 1.0,
                         "max_p95_latency_ratio": 1.1, "min_cost_improvement_fraction": 0.2,
                         "min_operator_improvement_fraction": 0.25}
        self.protocol_path = self.directory / "protocol.json"
        self.environment_path = self.directory / "environment.json"
        self.workload_path = self.directory / "workload.json"
        self.protocol_path.write_text(json.dumps(self.protocol))
        self.environment_path.write_text('{"node_count": 1}')
        self.workload_path.write_text(json.dumps(workload()))
        self.args = argparse.Namespace(protocol=str(self.protocol_path), environment=str(self.environment_path),
                                       workload=str(self.workload_path), context="explicit-test-context", namespace="agentos-workloads",
                                       evidence_dir=str(self.directory / "evidence"), trials=None, poll_seconds=0.1,
                                       trial_timeout_seconds=60, cache_state="uncontrolled",
                                       upstream_controller_ref="registry.example/controller@sha256:" + "1" * 64,
                                       execute=True, api_url=FakeAPI.base, token_env="TEST_EVAL_TOKEN", baseline="direct-job")

    def registered(self):
        ev.register(self.args)
        return ev.load_registration(self.args)[1]

    def test_registration_is_private_offline_and_immutable(self):
        with patch.object(ev, "Kubectl", side_effect=AssertionError("must be offline")):
            registration = self.registered()
        directory = Path(self.args.evidence_dir)
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual((directory / "registration.json").stat().st_mode & 0o777, 0o400)
        self.assertEqual(registration["protocol_sha256"], ev.digest(self.protocol))
        self.assertEqual(registration["environment"]["sha256"], ev.digest({"node_count": 1}))
        with self.assertRaises(FileExistsError):
            ev.register(self.args)
        self.args.context = "different-context"
        with self.assertRaisesRegex(ev.Failure, "registered_context_required"):
            ev.load_registration(self.args)

    def test_cannot_lower_preregistered_trials(self):
        self.args.trials = 1
        with self.assertRaisesRegex(ev.Failure, "trial_count_bounds"):
            ev.register(self.args)
        self.assertFalse(Path(self.args.evidence_dir).exists())

    def test_digest_pin_and_runtime_cannot_fallback(self):
        w = workload()
        w["image"] = "python:latest"
        with self.assertRaisesRegex(ev.Failure, "digest_pinned"):
            ev.validate_workload(w)
        w = workload()
        w["runtime_profile"]["required_handler"] = "runc"
        with self.assertRaisesRegex(ev.Failure, "gvisor_handler_required"):
            ev.validate_workload(w)
        w = workload(True)
        w["runtime_profile"]["runtime_class"] = "gvisor"
        with self.assertRaisesRegex(ev.Failure, "nitro_profile_required"):
            ev.validate_workload(w)

    def test_nitro_scratch_placement_and_profile_drift(self):
        w = workload(True)
        ev.validate_workload(w)
        pod = ev.pod_template(w, {})
        self.assertEqual(pod["metadata"]["annotations"]["sandbox.amazonaws.com/network-isolated"], "true")
        self.assertEqual(pod["spec"]["nodeSelector"], {"eks.amazonaws.com/nitro-sandbox": "true"})
        self.assertTrue(all(v["emptyDir"]["medium"] == "Memory" for v in pod["spec"]["volumes"]))
        actual = copy.deepcopy(pod["spec"])
        for section in ("requests", "limits"):
            actual["containers"][0]["resources"][section]["cpu"] = "1"
        ev.assert_pod_profile(actual, pod["spec"])
        actual["containers"].append(copy.deepcopy(actual["containers"][0]))
        with self.assertRaisesRegex(ev.Failure, "pod_profile_changed"):
            ev.assert_pod_profile(actual, pod["spec"])
        meta = {"labels": {ev.PART: "agent-os"}, "annotations": {"sandbox.amazonaws.com/network-isolated": "false"}}
        with self.assertRaisesRegex(ev.Failure, "annotations_changed"):
            ev.assert_pod_metadata(meta, pod["metadata"])
        actual = copy.deepcopy(pod["spec"])
        actual["automountServiceAccountToken"] = True
        with self.assertRaisesRegex(ev.Failure, "pod_profile_changed"):
            ev.assert_pod_profile(actual, pod["spec"])
        meta["annotations"] = dict(pod["metadata"]["annotations"], **{"sandbox.amazonaws.com/extra-control": "true"})
        with self.assertRaisesRegex(ev.Failure, "annotations_changed"):
            ev.assert_pod_metadata(meta, pod["metadata"])

    def test_upstream_has_bounded_pod_deadline(self):
        registration = self.registered()
        template, pool = ev.upstream_manifests(registration)
        self.assertEqual(template["spec"]["podTemplate"]["spec"]["activeDeadlineSeconds"], 30)
        self.assertEqual(pool["spec"]["replicas"], 0)

    def test_grace_is_registered_and_compared_without_changing_legacy_inputs(self):
        current = workload()
        legacy = copy.deepcopy(current)
        legacy.pop("termination_grace_period_seconds", None)
        ev.validate_workload(legacy)
        self.assertNotIn("termination_grace_period_seconds", legacy)
        self.assertNotIn("terminationGracePeriodSeconds", ev.pod_template(legacy, {})["spec"])
        current["termination_grace_period_seconds"] = 10
        ev.validate_workload(current)
        expected = ev.pod_template(current, {})["spec"]
        self.assertEqual(expected["terminationGracePeriodSeconds"], 10)
        drift = copy.deepcopy(expected)
        drift["terminationGracePeriodSeconds"] = 30
        with self.assertRaisesRegex(ev.Failure, "pod_profile_changed"):
            ev.assert_pod_profile(drift, expected)
        for invalid in (0, -1, 31, True, 1.5):
            current["termination_grace_period_seconds"] = invalid
            with self.assertRaisesRegex(ev.Failure, "termination_grace_bounds"):
                ev.validate_workload(current)

    def test_late_create_absence_remains_unknown(self):
        registration = self.registered()
        kube = FakeKube(registration)
        kube.create = lambda _: (_ for _ in ()).throw(ev.Failure("creation_outcome_unknown"))
        with patch.object(ev, "Kubectl", return_value=kube), self.assertRaises(ev.Failure):
            ev.collect(self.args)
        result = ev.read_json(Path(self.args.evidence_dir) / "direct-job.json")
        self.assertFalse(result["completed"])
        self.assertTrue(result["trials"][0]["submission_uncertain"])
        self.assertEqual(result["trials"][0]["cleanup"], "submission_outcome_unknown")
        with patch.object(ev, "Kubectl", return_value=kube), self.assertRaisesRegex(ev.Failure, "retains_uncertain"):
            ev.cleanup(self.args)

    def test_parent_absence_does_not_prove_descendant_absence(self):
        registration = self.registered()
        kube = FakeKube(registration)
        ref = {"kind": "Pod", "name": "stray-pod", "uid": "known-pod", "parent_uid": "known-parent"}
        kube.get = lambda *_: {"metadata": {"uid": "known-pod", "ownerReferences": [
            {"uid": "known-parent", "controller": True}]}}
        with patch.object(ev.time, "monotonic", side_effect=[0, 31]), self.assertRaisesRegex(ev.Failure, "descendant_cleanup_not_observed"):
            ev.descendants_absent(kube, registration, [ref])

    def test_exact_workload_output_and_order(self):
        w = workload()
        ready, complete = w["validation"].values()
        self.assertFalse(ev.validate_logs("", w))
        self.assertTrue(ev.validate_logs(ready + "\n", w))
        ev.validate_logs(ready + "\n" + complete + "\n", w, complete=True)
        for wrong in (complete + "\n" + ready + "\n", ready + "\n" + complete + "-wrong\n", ready + "\n" + ready + "\n"):
            with self.assertRaises(ev.Failure):
                ev.validate_logs(wrong, w, complete=True)
        with self.assertRaisesRegex(ev.Failure, "incomplete"):
            ev.validate_logs(ready + "\n", w, complete=True)

    def test_all_three_cold_arms_collect_and_guard_cleanup(self):
        registration = self.registered()
        kube = FakeKube(registration)
        api = FakeAPI(kube)
        with patch.object(ev, "Kubectl", return_value=kube), patch.object(ev, "AgentOS", return_value=api):
            for baseline in ev.BASELINES:
                self.args.baseline = baseline
                path = ev.collect(self.args)
                result = ev.read_json(path)
                self.assertTrue(result["completed"])
                self.assertEqual(len(result["trials"]), 2)
                self.assertTrue(all(x["correct"] and x["cleanup"] == "observed_objects_absent" for x in result["trials"]))
                self.assertEqual(result["qualification"]["status"], "not_verified")
                self.assertFalse(result["billing_complete"])
                self.assertNotIn("full_cost_usd", result)
                self.assertEqual(result["environment_sha256"], registration["environment"]["sha256"])
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            ev.cleanup(self.args)
        self.assertFalse(kube.objects)
        self.assertEqual(len(api.submissions), 2)
        # One arm completes and cleans each trial before the next create.
        actions = [op[0] for op in kube.operations]
        self.assertIn("delete", actions)
        self.assertEqual(ev.read_json(Path(self.args.evidence_dir) / "agent-sandbox.json")["pool_replicas"], 0)

    def test_failed_output_is_preserved_without_raw_logs(self):
        registration = self.registered()
        kube = FakeKube(registration)
        kube.logs = lambda _: "PRIVATE_UNEXPECTED_CONTENT"
        with patch.object(ev, "Kubectl", return_value=kube):
            with self.assertRaisesRegex(ev.Failure, "collection_contains_failures"):
                ev.collect(self.args)
        result_path = Path(self.args.evidence_dir) / "direct-job.json"
        result = ev.read_json(result_path)
        self.assertTrue(result["completed"])
        self.assertTrue(all(not x["correct"] and x["error"] == "workload_output_mismatch" for x in result["trials"]))
        self.assertNotIn("PRIVATE_UNEXPECTED_CONTENT", result_path.read_text())
        before = result_path.read_bytes()
        with patch.object(ev, "Kubectl", return_value=kube), self.assertRaises(ev.Failure):
            ev.collect(self.args)
        self.assertEqual(result_path.read_bytes(), before)

    def test_ambiguous_submission_uses_same_key_and_stops_arm(self):
        registration = self.registered()
        kube = FakeKube(registration)
        api = FakeAPI(kube)
        original = api.call
        keys = []

        def uncertain(path, body=None, key=None):
            if path == "/v1/runs":
                keys.append(key)
                raise ev.Failure("api_transport_outcome_unknown")
            return original(path, body, key)

        api.call = uncertain
        self.args.baseline = "agentos"
        with patch.object(ev, "Kubectl", return_value=kube), patch.object(ev, "AgentOS", return_value=api):
            with self.assertRaises(ev.Failure):
                ev.collect(self.args)
        result = ev.read_json(Path(self.args.evidence_dir) / "agentos.json")
        self.assertFalse(result["completed"])
        self.assertEqual(len(result["trials"]), 1)
        self.assertEqual(result["trials"][0]["error"], "api_submission_outcome_unknown")
        self.assertEqual(len(keys), 2)
        self.assertEqual(keys[0], keys[1])

    def test_cleanup_refuses_unlabelled_or_replaced_uid(self):
        registration = self.registered()
        kube = FakeKube(registration)
        obj = kube.create(ev.job_manifest(registration, "test-job", 0))
        ref = ev.reference(obj)
        kube.objects[("Job", "test-job")]["metadata"]["uid"] = "replacement"
        with self.assertRaisesRegex(ev.Failure, "uid_changed"):
            ev.delete_owned(kube, registration, ref)
        kube.objects[("Job", "test-job")]["metadata"].pop("labels")
        with self.assertRaisesRegex(ev.Failure, "ownership_mismatch"):
            ev.delete_owned(kube, registration, ref)
        self.assertFalse(any(x[0] == "delete" for x in kube.operations))

    def test_collect_requires_execute_and_preflight_failure_is_evidence(self):
        registration = self.registered()
        self.args.execute = False
        with self.assertRaisesRegex(ev.Failure, "requires_execute"):
            ev.collect(self.args)
        self.args.execute = True
        kube = FakeKube(registration)
        original = kube.get
        kube.get = lambda resource, name=None, **kwargs: {"handler": "runc"} if resource == "runtimeclasses.node.k8s.io" else original(resource, name, **kwargs)
        with patch.object(ev, "Kubectl", return_value=kube), self.assertRaises(ev.Failure):
            ev.collect(self.args)
        result = ev.read_json(Path(self.args.evidence_dir) / "direct-job.json")
        self.assertFalse(result["completed"])
        self.assertEqual(result["error"], "required_runtime_unavailable")
        self.assertEqual(result["trials"], [])
        self.assertEqual(kube.operations, [])


class TransportTest(unittest.TestCase):
    def test_subprocess_output_timeout_and_diagnostic_bounds(self):
        self.assertEqual(ev.bounded_command([sys.executable, "-c", "import sys;sys.stderr.write('SECRET');print('ok')"]), "ok\n")
        with self.assertRaisesRegex(ev.Failure, "output_too_large"):
            ev.bounded_command([sys.executable, "-c", "print('x'*1000000)"], limit=100)
        with self.assertRaisesRegex(ev.Failure, "timeout"):
            ev.bounded_command([sys.executable, "-c", "import time;time.sleep(2)"], timeout=0.03)

    def test_http_bearer_is_not_in_redirect_or_error_output(self):
        token = "private-test-token-" + "x" * 32
        observations = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                observations.append((self.path, self.headers.get("Authorization")))
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "/must-not-be-called")
                    self.end_headers()
                elif self.path == "/too-large":
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"x" * (ev.MAX_JSON + 1))
                else:
                    self.send_response(401)
                    self.end_headers()
                    self.wfile.write(token.encode())

            def log_message(self, *_):
                pass

        with patch("socket.getfqdn", return_value="localhost"):
            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.dict(os.environ, TEST_EVAL_TOKEN=token):
                api = ev.AgentOS("http://127.0.0.1:" + str(server.server_port), "TEST_EVAL_TOKEN")
                for path, code in (("/redirect", "api_http_302"), ("/error", "api_http_401"), ("/too-large", "api_response_too_large")):
                    with self.assertRaises(ev.Failure) as raised:
                        api.call(path)
                    self.assertEqual(str(raised.exception), code)
                    self.assertNotIn(token, str(raised.exception))
                with self.assertRaisesRegex(ev.Failure, "requires_https"):
                    ev.AgentOS("http://remote.example", "TEST_EVAL_TOKEN")
            self.assertFalse(any(path == "/must-not-be-called" for path, _ in observations))
            self.assertTrue(all(header == "Bearer " + token for _, header in observations))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
