"""Offline safety/interpretation tests; none of these tests contacts Kubernetes."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "qualify_cluster.py"
SPEC = importlib.util.spec_from_file_location("qualify_cluster", MODULE_PATH)
q = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(q)


def arguments(cni="cilium"):
    args = q.parser().parse_args([
        "--context", "explicit-pilot", "--profile", "eks-standard-gvisor",
        "--runtime-class", "gvisor", "--runtime-handler", "runsc", "--architecture", "amd64",
        "--node-selector", "agent-sandbox/runtime=gvisor", "--cni", cni,
        "--output", "/tmp/not-created-by-tests.json",
    ])
    args.node_labels = q.labels(args.node_selector)
    args.workload_labels = {"app.kubernetes.io/part-of": "agent-os"}
    return args


class FixtureClient:
    def __init__(self, cni="cilium"):
        self.calls = []
        self.runtime = {"handler": "runsc", "scheduling": {"nodeSelector": {"agent-sandbox/runtime": "gvisor"}, "tolerations": [{"key": "runtime", "operator": "Equal", "value": "gvisor", "effect": "NoSchedule"}]}}
        self.node = {"metadata": {"name": "pilot-node", "labels": {"agent-sandbox/runtime": "gvisor", "kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64"}},
            "spec": {"providerID": "aws:///zone/i-private", "taints": [{"key": "runtime", "value": "gvisor", "effect": "NoSchedule"}]},
            "status": {"nodeInfo": {"operatingSystem": "linux", "architecture": "amd64", "kernelVersion": "6.1.0", "containerRuntimeVersion": "containerd://2.0", "kubeletVersion": "v1.37.1"}, "conditions": [{"type": "Ready", "status": "True"}]}}
        self.policies = [{"metadata": {"name": "agentos-deny-all"}, "spec": {"podSelector": {"matchLabels": {"app.kubernetes.io/part-of": "agent-os"}}, "policyTypes": ["Ingress", "Egress"], "ingress": [], "egress": []}}]
        self.ds = {"metadata": {"generation": 2, "uid": "cni-owner"}, "spec": {"selector": {"matchLabels": {"k8s-app": "cni"}}, "template": {"spec": {"containers": [
            {"name": "aws-node", "image": "registry/cni:v1.21.1-eksbuild.1", "env": [{"name": "NETWORK_POLICY_ENFORCING_MODE", "value": "strict"}, {"name": "DO_NOT_REPORT", "value": "secret-sentinel"}]},
            {"name": "aws-network-policy-agent", "args": ["--enable-network-policy=true"]}]}}},
            "status": {"observedGeneration": 2, "desiredNumberScheduled": 1, "numberReady": 1, "updatedNumberScheduled": 1}}
        self.config = {"data": {"enable-policy": "default", "enable-network-policy-controller": "true", "password": "secret-sentinel"}}
        self.pods = {"items": [{"metadata": {"ownerReferences": [{"uid": "cni-owner"}]}, "spec": {"nodeName": "pilot-node"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}

    def json(self, *args):
        self.calls.append(args)
        if args == ("version",):
            return {"serverVersion": {"gitVersion": "v1.37.1-eks"}}
        if args[:2] == ("get", "runtimeclass"):
            return self.runtime
        if args == ("get", "nodes"):
            return {"items": [self.node]}
        if args[:2] == ("get", "namespace"):
            return {"metadata": {"labels": {"pod-security.kubernetes.io/enforce": "restricted"}}}
        if args[:2] == ("get", "networkpolicies"):
            return {"items": self.policies}
        if args[:2] == ("get", "daemonset"):
            return self.ds
        if args[:2] == ("get", "configmap"):
            return self.config
        if args[:2] == ("get", "pods"):
            return self.pods
        if args[:2] == ("get", "customresourcedefinition"):
            raise q.InventoryError("not_found")
        raise AssertionError(args)


class QualificationTests(unittest.TestCase):
    def check_status(self, report, name):
        return next(item["status"] for item in report["checks"] if item["name"] == name)

    def test_complete_declarations_never_qualify(self):
        client = FixtureClient()
        report = q.build_report(arguments(), client)
        self.assertEqual(report["preflight"], "DECLARATIONS_ONLY")
        self.assertEqual(report["qualification"], "NOT_QUALIFIED")
        self.assertEqual(report["dynamic_tests"], "not_run")
        self.assertTrue(report["unverified"])
        self.assertTrue(all(args[0] in {"version", "get"} for args in client.calls))
        self.assertNotIn("secret-sentinel", json.dumps(report))
        self.assertNotIn("i-private", json.dumps(report))

    def test_runtime_handler_no_fallback(self):
        client = FixtureClient()
        client.runtime["handler"] = "runc"
        report = q.build_report(arguments(), client)
        self.assertEqual(self.check_status(report, "runsc_handler"), "blocked")
        self.assertEqual(report["preflight"], "BLOCKED")

    def test_cli_selector_cannot_conceal_unconstrained_runtime(self):
        client = FixtureClient()
        client.runtime["scheduling"]["nodeSelector"] = {}
        report = q.build_report(arguments(), client)
        self.assertEqual(self.check_status(report, "runtimeclass_scheduling_labels"), "blocked")
        self.assertEqual(report["observations"]["runtime_nodes"], [])

    def test_node_profile_and_taints(self):
        for mutation in ["architecture", "auto", "fargate", "windows", "provider"]:
            with self.subTest(mutation=mutation):
                client = FixtureClient()
                if mutation == "architecture":
                    client.node["status"]["nodeInfo"]["architecture"] = "arm64"
                elif mutation in {"auto", "fargate"}:
                    client.node["metadata"]["labels"]["eks.amazonaws.com/compute-type"] = mutation
                elif mutation == "windows":
                    client.node["status"]["nodeInfo"]["operatingSystem"] = "windows"
                else:
                    client.node["spec"]["providerID"] = "other://node"
                self.assertEqual(self.check_status(q.build_report(arguments(), client), "node_profile"), "blocked")
        client = FixtureClient()
        client.runtime["scheduling"]["tolerations"] = []
        self.assertEqual(self.check_status(q.build_report(arguments(), client), "schedulable_ready_node"), "blocked")

    def test_all_runtime_selected_nodes_are_inspected(self):
        client = FixtureClient()
        other = copy.deepcopy(client.node)
        other["metadata"]["name"] = "wrong-arch"
        other["status"]["nodeInfo"]["architecture"] = "arm64"
        original = client.json

        def read(*args):
            return {"items": [client.node, other]} if args == ("get", "nodes") else original(*args)

        client.json = read
        report = q.build_report(arguments(), client)
        self.assertEqual(len(report["observations"]["runtime_nodes"]), 2)
        self.assertEqual(self.check_status(report, "node_profile"), "blocked")

    def test_namespace_additive_allow_blocks(self):
        client = FixtureClient()
        client.policies.append({"metadata": {"name": "allow-all"}, "spec": {"podSelector": {"matchExpressions": [{"key": "app.kubernetes.io/part-of", "operator": "Exists"}]}, "egress": [{}]}})
        report = q.build_report(arguments(), client)
        self.assertEqual(self.check_status(report, "no_additive_namespace_allow_policy"), "blocked")
        self.assertEqual(report["observations"]["selecting_allow_policies"], ["allow-all"])

    def test_vpc_cni_requires_strict_and_explicit_enablement(self):
        client = FixtureClient("aws-vpc")
        report = q.build_report(arguments("aws-vpc"), client)
        self.assertEqual(report["preflight"], "DECLARATIONS_ONLY")
        client.ds["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = "standard"
        report = q.build_report(arguments("aws-vpc"), client)
        self.assertEqual(self.check_status(report, "aws_vpc_strict_startup"), "blocked")
        client.ds["spec"]["template"]["spec"]["containers"][1]["args"] = []
        self.assertEqual(self.check_status(q.build_report(arguments("aws-vpc"), client), "aws_vpc_policy_enabled"), "blocked")

    def test_unready_or_wrong_owner_cni_pod_is_not_coverage(self):
        client = FixtureClient()
        client.pods["items"][0]["metadata"]["ownerReferences"][0]["uid"] = "other-controller"
        report = q.build_report(arguments(), client)
        self.assertEqual(self.check_status(report, "cni_target_node_coverage"), "blocked")

    def test_credential_plugin_errors_are_sanitized(self):
        result = subprocess.CompletedProcess([], 1, b"", b"Forbidden token=secret-sentinel")
        with patch.object(q.subprocess, "run", return_value=result) as run:
            with self.assertRaisesRegex(q.InventoryError, "^request_failed$"):
                q.Kubectl("explicit-pilot").json("get", "nodes")
        command = run.call_args.args[0]
        self.assertIn("--context=explicit-pilot", command)
        self.assertNotIn("--raw", command)
        self.assertEqual(run.call_args.kwargs["stderr"], subprocess.DEVNULL)

    def test_private_exclusive_file_and_symlink_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            q.write_private(path, {"qualification": "NOT_QUALIFIED"})
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                q.write_private(path, {})
            link = Path(directory) / "link.json"
            link.symlink_to(path)
            with self.assertRaises(FileExistsError):
                q.write_private(link, {})
            self.assertEqual(json.loads(path.read_text())["qualification"], "NOT_QUALIFIED")


if __name__ == "__main__":
    unittest.main()
