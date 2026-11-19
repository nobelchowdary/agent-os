"""Fake resource observations only; no Metrics Server or cluster is contacted."""

import argparse
import copy
import datetime as dt
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("sample_resources", ROOT / "scripts/sample_resources.py")
sampler = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sampler)


def nodes(uid="node-uid"):
    return {"kind": "NodeList", "items": [{"metadata": {"name": "node-a", "uid": uid},
                                           "status": {"allocatable": {"cpu": "2", "memory": "2Gi"}}}]}


def metrics(timestamp=None):
    return {"apiVersion": "metrics.k8s.io/v1beta1", "kind": "NodeMetricsList", "items": [{
        "metadata": {"name": "node-a"}, "timestamp": timestamp or dt.datetime.now(dt.timezone.utc).isoformat(),
        "window": "30.5s", "usage": {"cpu": "100000000n", "memory": "1024Ki"}}]}


class FakeClock:
    def __init__(self):
        self.value = 0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


class ResourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "new-resources.json"
        self.args = argparse.Namespace(context="explicit-context", node_selector="agent-sandbox/runtime=gvisor",
                                       experiment_id="test-resources", duration_seconds=2, interval_seconds=1,
                                       max_age_seconds=120, output=str(self.path), execute=True)

    def run_sample(self, query):
        clock = FakeClock()
        with patch.object(sampler, "kube_json", side_effect=query), patch.object(sampler.time, "monotonic", clock.monotonic), \
                patch.object(sampler.time, "sleep", clock.sleep):
            return sampler.sample(self.args)

    def test_normalized_quantities_windows_and_private_report(self):
        metric = metrics()
        self.run_sample(lambda args, command, **kwargs: metric if "--raw" in command else nodes())
        report = sampler.ev.read_json(self.path)
        self.assertEqual(report["status"], "observed")
        self.assertTrue(report["completed"])
        self.assertEqual(report["collector_elapsed_seconds"], 2)
        self.assertEqual(len(report["samples"]), 2)
        first, second = [x["nodes"][0] for x in report["samples"]]
        self.assertEqual(first["cpu_cores"], 0.1)
        self.assertEqual(first["memory_working_set_bytes"], 1048576)
        self.assertEqual(first["window_seconds"], 30.5)
        self.assertFalse(first["repeated_timestamp"])
        self.assertTrue(second["repeated_timestamp"])
        self.assertEqual(report["allocatable_summary"], {"cpu_cores": 2.0, "memory_bytes": 2147483648})
        self.assertEqual(report["billing_inference"], "none")
        self.assertNotIn("cost_usd", report)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            self.run_sample(lambda *_: nodes())

    def test_missing_metrics_are_failed_not_zero(self):
        missing = metrics()
        missing["items"] = []
        with self.assertRaises(sampler.ev.Failure):
            self.run_sample(lambda args, command, **kwargs: missing if "--raw" in command else nodes())
        report = sampler.ev.read_json(self.path)
        self.assertTrue(report["completed"])
        self.assertEqual(report["status"], "failed")
        self.assertTrue(all(x["error"] == "selected_node_metrics_missing" and "nodes" not in x for x in report["samples"]))

    def test_unavailable_metrics_api_is_explicit_failure(self):
        def query(args, command, **kwargs):
            if "--raw" in command:
                raise sampler.ev.Failure("kubectl_request_failed")
            return nodes()

        with self.assertRaises(sampler.ev.Failure):
            self.run_sample(query)
        report = sampler.ev.read_json(self.path)
        self.assertEqual(report["samples"][0]["error"], "metrics_api_unavailable_forbidden_or_timed_out")
        self.assertNotIn("nodes", report["samples"][0])

    def test_stale_or_malformed_metrics_cannot_be_observations(self):
        selected = sampler.inventory(nodes())
        observed = dt.datetime.now(dt.timezone.utc)
        stale = metrics((observed - dt.timedelta(minutes=10)).isoformat())
        with self.assertRaisesRegex(sampler.ev.Failure, "stale_or_future"):
            sampler.parse_metrics(stale, selected, observed, {}, 120)
        invalid = metrics(observed.isoformat())
        invalid["items"][0]["usage"]["memory"] = "bad"
        with self.assertRaises(sampler.ev.Failure):
            sampler.parse_metrics(invalid, selected, observed, {}, 120)
        with self.assertRaisesRegex(sampler.ev.Failure, "invalid_metrics_window"):
            sampler.duration_seconds("0s")

    def test_node_replacement_is_not_silently_pooled(self):
        calls = 0

        def query(args, command, **kwargs):
            nonlocal calls
            if "--raw" in command:
                return metrics()
            calls += 1
            return nodes("original" if calls == 1 else "replacement")

        with self.assertRaises(sampler.ev.Failure):
            self.run_sample(query)
        report = sampler.ev.read_json(self.path)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["samples"][0]["error"], "selected_node_identity_or_allocatable_changed")

    def test_opt_in_selector_and_duration_required_before_queries(self):
        with patch.object(sampler, "kube_json") as query:
            self.args.execute = False
            with self.assertRaisesRegex(sampler.ev.Failure, "sampling_requires_execute"):
                sampler.sample(self.args)
            self.assertFalse(query.called)
            self.assertFalse(self.path.exists())
            self.args.execute = True
            self.args.node_selector = ""
            with self.assertRaises(sampler.ev.Failure):
                sampler.sample(self.args)
            self.args.node_selector = "key=value"
            self.args.duration_seconds = 10000
            with self.assertRaises(sampler.ev.Failure):
                sampler.sample(self.args)
            self.assertFalse(query.called)

    def test_queries_are_get_only_and_context_is_explicit(self):
        with patch.object(sampler.ev, "bounded_command", return_value='{"kind":"NodeList","items":[]}') as command:
            sampler.kube_json(self.args, ["get", "nodes", "--selector", self.args.node_selector, "-o", "json"])
            args = command.call_args.args[0]
            self.assertEqual(args[:3], ["kubectl", "--context", "explicit-context"])
            self.assertIn("get", args)
            self.assertNotIn("exec", args)
            self.assertNotIn("apply", args)
            self.assertNotIn("create", args)


if __name__ == "__main__":
    unittest.main()
