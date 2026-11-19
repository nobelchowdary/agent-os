#!/usr/bin/env python3
"""Read-only, explicit-context inventory; declarations never qualify isolation.

No workload, cloud resource, secret, kubeconfig or dynamic probe is read/created.
kubectl may execute the credential plugin configured by the operator's context.
Raw API objects and kubectl stderr are never written to the report or console.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


AWS_REVISION = "cfccca1a82071826f38a6308d298010be0a45baa"
SUBSTRATE_REVISION = "1117caed19f0cdee42e5f045bad2043ea08c207d"
SOURCES = {
    "aws_blueprint": "https://github.com/awslabs/ai-on-eks/tree/" + AWS_REVISION + "/infra/agent-sandbox",
    "aws_reference": "https://github.com/awslabs/ai-on-eks/tree/" + AWS_REVISION + "/blueprints/agent-sandbox",
    "aws_network_policy": "https://docs.aws.amazon.com/eks/latest/userguide/cni-network-policy-configure.html",
    "aws_nitro_release": "https://docs.aws.amazon.com/eks/latest/userguide/auto-change.html",
    "kueue_sandbox": "https://kueue.sigs.k8s.io/docs/tasks/run/external_workloads/sandbox/",
    "substrate": "https://github.com/agent-substrate/substrate/tree/" + SUBSTRATE_REVISION,
}
CRDS = {
    "agent_sandbox": "sandboxes.agents.x-k8s.io",
    "sandbox_template": "sandboxtemplates.extensions.agents.x-k8s.io",
    "sandbox_warm_pool": "sandboxwarmpools.extensions.agents.x-k8s.io",
    "sandbox_claim": "sandboxclaims.extensions.agents.x-k8s.io",
    "kueue_workload": "workloads.kueue.x-k8s.io",
    "substrate_worker_pool": "workerpools.ate.dev",
}
UNVERIFIED = [
    "The declared runsc handler is installed, selected, and providing the intended isolation on every eligible node.",
    "Live ingress/egress denial, startup-window enforcement, DNS/IP bypasses, same-node/cross-node paths, IPv6 and metadata access.",
    "Cluster/admin policies, CNI-specific policies, admission mutation, extra interfaces and privileged host components do not weaken the intended boundary.",
    "Node PID limits, cgroup accounting, scratch eviction, resource exhaustion and physical worker fencing.",
    "Snapshot/resume support, workload compatibility, recovery correctness, performance and total cost.",
]


class InventoryError(Exception):
    """A sanitized inventory failure, without credential-plugin stderr."""


class Kubectl:
    def __init__(self, context, timeout=15):
        self.context = context
        self.timeout = timeout

    def json(self, *args):
        # Callers below use only version/get; never shell out with user input.
        command = ["kubectl", "--context=" + self.context,
                   "--request-timeout=" + str(self.timeout) + "s", *args, "-o", "json"]
        try:
            # Spool stdout to a private temporary file so API/exec-plugin output
            # cannot grow the Python heap without bound. Stderr is discarded.
            with tempfile.TemporaryFile() as output:
                result = subprocess.run(command, stdout=output, stderr=subprocess.DEVNULL,
                                        timeout=self.timeout + 5, check=False)
                output.seek(0)
                encoded = output.read(32 * 1024 * 1024 + 1)
        except FileNotFoundError as error:
            raise InventoryError("kubectl_not_found") from error
        except subprocess.TimeoutExpired as error:
            raise InventoryError("request_timeout") from error
        if result.returncode:
            raise InventoryError("request_failed")
        if len(encoded) > 32 * 1024 * 1024:
            raise InventoryError("response_too_large")
        try:
            value = json.loads(encoded)
        except (ValueError, UnicodeDecodeError) as error:
            raise InventoryError("invalid_json") from error
        if not isinstance(value, dict):
            raise InventoryError("invalid_object")
        return value


def labels(values):
    result = {}
    for value in values:
        if "=" not in value:
            raise argparse.ArgumentTypeError("labels must be key=value")
        key, item = value.split("=", 1)
        if not key or not item or key in result:
            raise argparse.ArgumentTypeError("label keys must be unique and values nonempty")
        result[key] = item
    return result


def version_tuple(value):
    match = re.match(r"^v?(\d+)\.(\d+)", value or "")
    return tuple(map(int, match.groups())) if match else None


def selector_matches(selector, target):
    if any(target.get(key) != value for key, value in selector.get("matchLabels", {}).items()):
        return False
    for expression in selector.get("matchExpressions", []):
        key, operator = expression.get("key"), expression.get("operator")
        values = expression.get("values", [])
        if operator == "In" and target.get(key) not in values:
            return False
        if operator == "NotIn" and key in target and target[key] in values:
            return False
        if operator == "Exists" and key not in target:
            return False
        if operator == "DoesNotExist" and key in target:
            return False
        if operator not in {"In", "NotIn", "Exists", "DoesNotExist"}:
            raise InventoryError("unsupported_label_selector")
    return True


def tolerated(taint, tolerations):
    if taint.get("effect") not in {"NoSchedule", "NoExecute"}:
        return True
    for item in tolerations:
        if item.get("effect") and item["effect"] != taint.get("effect"):
            continue
        if item.get("operator", "Equal") == "Exists":
            if not item.get("key") or item["key"] == taint.get("key"):
                return True
        elif item.get("key") == taint.get("key") and item.get("value", "") == taint.get("value", ""):
            return True
    return False


def add_check(report, name, ok, detail):
    report["checks"].append({"name": name, "status": "observed" if ok else "blocked", "detail": detail})


def optional(client, report, key, *args):
    try:
        return client.json(*args)
    except InventoryError as error:
        report["inventory_gaps"][key] = str(error)
        return None


def container_named(daemonset, name):
    containers = (daemonset or {}).get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    return next((item for item in containers if item.get("name") == name), {})


def literal_env(container, name):
    item = next((item for item in container.get("env", []) if item.get("name") == name), {})
    # Never resolve secret/config references, and retain only known enum values.
    value = item.get("value", "").lower()
    return value if value in {"strict", "standard", "true", "false"} else "unverified"


def inspect_cni(client, report, args, nodes):
    name = "aws-node" if args.cni == "aws-vpc" else "cilium"
    ds = optional(client, report, "cni_daemonset", "get", "daemonset", name, "-n", args.cni_namespace)
    config_name = "amazon-vpc-cni" if args.cni == "aws-vpc" else "cilium-config"
    config = optional(client, report, "cni_config", "get", "configmap", config_name, "-n", args.cni_namespace)
    add_check(report, "cni_daemonset_exists", ds is not None, "Expected CNI DaemonSet in explicit namespace.")
    if ds is None:
        return
    status = ds.get("status", {})
    ready = status.get("desiredNumberScheduled", 0) > 0 and status.get("numberReady", 0) == status.get("desiredNumberScheduled")
    ready = ready and status.get("updatedNumberScheduled", 0) == status.get("desiredNumberScheduled")
    ready = ready and status.get("observedGeneration", 0) >= ds.get("metadata", {}).get("generation", 1)
    add_check(report, "cni_daemonset_ready", ready, "Ready/updated counts and observed generation; not packet-level enforcement proof.")
    if args.cni == "aws-vpc":
        main = container_named(ds, "aws-node")
        agent = container_named(ds, "aws-network-policy-agent")
        mode = literal_env(main, "NETWORK_POLICY_ENFORCING_MODE")
        enabled = "--enable-network-policy=true" in agent.get("args", [])
        controller = (config or {}).get("data", {}).get("enable-network-policy-controller") == "true"
        image = main.get("image", "")
        match = re.search(r":v?(\d+\.\d+)(?:[.\-]|$)", image)
        cni_version = version_tuple(match.group(1)) if match else None
        report["observations"]["cni"] = {"kind": args.cni, "startup_mode": mode,
            "node_policy_agent_enabled": enabled, "controller_enabled": controller,
            "image_minor": match.group(1) if match else "unverified"}
        add_check(report, "aws_vpc_strict_startup", mode == "strict", "NETWORK_POLICY_ENFORCING_MODE must explicitly be strict; standard mode initially allows traffic.")
        add_check(report, "aws_vpc_policy_enabled", enabled and controller, "Require explicit node-agent argument and controller ConfigMap declaration; managed-only declarations need manual review.")
        add_check(report, "aws_vpc_profile_version", cni_version is not None and cni_version >= (1, 21), "This conservative profile requires an identifiable VPC CNI version >=1.21.")
        add_check(report, "aws_vpc_kernel", all((version_tuple(n.get("status", {}).get("nodeInfo", {}).get("kernelVersion")) or (0, 0)) >= (5, 10) for n in nodes), "Selected Linux nodes must report kernel >=5.10.")
    else:
        mode = (config or {}).get("data", {}).get("enable-policy")
        mode = mode if mode in {"always", "default", "never"} else "unverified"
        report["observations"]["cni"] = {"kind": args.cni, "policy_mode": mode}
        add_check(report, "cilium_policy_enabled", mode in {"always", "default"}, "Recognized Cilium policy declaration; startup and chaining behavior still require live proof.")
    selector = ds.get("spec", {}).get("selector", {})
    if not selector.get("matchLabels") or selector.get("matchExpressions"):
        add_check(report, "cni_target_node_coverage", False, "Cannot safely construct this DaemonSet's pod selector.")
        return
    query = ",".join(key + "=" + value for key, value in sorted(selector["matchLabels"].items()))
    pods = optional(client, report, "cni_pods", "get", "pods", "-n", args.cni_namespace, "-l", query)
    uid = ds.get("metadata", {}).get("uid")
    covered = set()
    for pod in (pods or {}).get("items", []):
        owned = bool(uid) and any(owner.get("uid") == uid for owner in pod.get("metadata", {}).get("ownerReferences", []))
        is_ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod.get("status", {}).get("conditions", []))
        if owned and is_ready and not pod.get("metadata", {}).get("deletionTimestamp"):
            covered.add(pod.get("spec", {}).get("nodeName"))
    required = {node.get("metadata", {}).get("name") for node in nodes}
    add_check(report, "cni_target_node_coverage", bool(required) and required <= covered, "Ready CNI-owned Pod observed on every RuntimeClass-eligible node.")


def build_report(args, client):
    report = {"schema_version": 1, "generated_at": datetime.now(timezone.utc).isoformat(),
        "qualification": "NOT_QUALIFIED", "preflight": "BLOCKED", "dynamic_tests": "not_run",
        "selection": {"context": args.context, "profile": args.profile, "architecture": args.architecture,
            "runtime_class": args.runtime_class, "runtime_handler": args.runtime_handler,
            "namespace": args.namespace, "expected_node_labels": args.node_labels,
            "workload_labels": args.workload_labels},
        "checks": [], "observations": {}, "inventory_gaps": {}, "unverified": list(UNVERIFIED), "sources": SOURCES}
    try:
        version = client.json("version").get("serverVersion", {}).get("gitVersion", "unverified")
        report["observations"]["kubernetes_version"] = version
        add_check(report, "server_version_reported", version_tuple(version) is not None, "Explicit context reached a Kubernetes server; provider identity remains unverified.")
        runtime = client.json("get", "runtimeclass", args.runtime_class)
        add_check(report, "runsc_handler", args.runtime_handler == "runsc" and runtime.get("handler") == args.runtime_handler,
                  "Only the Standard EKS runsc profile is supported; RuntimeClass strings cannot select Nitro or ordinary containers.")
        scheduling = runtime.get("scheduling", {})
        runtime_labels = scheduling.get("nodeSelector", {})
        constrained = bool(runtime_labels) and all(runtime_labels.get(k) == v for k, v in args.node_labels.items())
        add_check(report, "runtimeclass_scheduling_labels", constrained, "RuntimeClass itself must enforce all expected node labels; a CLI selector does not constrain Agent OS Jobs.")
        report["observations"]["runtimeclass_node_selector"] = {
            "expected_labels_enforced": constrained,
            "expected_keys_present": [key for key in args.node_labels if key in runtime_labels],
            "additional_key_count": len(set(runtime_labels) - set(args.node_labels)),
        }
        nodes = client.json("get", "nodes").get("items", [])
        # Inspect EVERY RuntimeClass-selected node, not just nodes matching a CLI-only filter.
        selected = [n for n in nodes if all(n.get("metadata", {}).get("labels", {}).get(k) == v for k, v in runtime_labels.items())] if runtime_labels else []
        add_check(report, "runtime_nodes_exist", bool(selected), "At least one existing node matches RuntimeClass scheduling; scale-from-zero pools are not inferred.")
        summaries = []
        usable = 0
        profile_ok = bool(selected)
        for node in selected:
            meta, spec, status = node.get("metadata", {}), node.get("spec", {}), node.get("status", {})
            node_labels, info = meta.get("labels", {}), status.get("nodeInfo", {})
            linux = node_labels.get("kubernetes.io/os") == "linux" and info.get("operatingSystem") == "linux"
            arch = node_labels.get("kubernetes.io/arch") == args.architecture and info.get("architecture") == args.architecture
            compute = node_labels.get("eks.amazonaws.com/compute-type", "unverified")
            aws = spec.get("providerID", "").startswith("aws://")
            standard = compute not in {"auto", "fargate"} and "eks.amazonaws.com/nodeclass" not in node_labels
            profile_ok = profile_ok and linux and arch and aws and standard
            ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions", []))
            schedulable = not spec.get("unschedulable", False) and all(tolerated(t, scheduling.get("tolerations", [])) for t in spec.get("taints", []))
            usable += int(ready and schedulable)
            summaries.append({"name": meta.get("name"), "linux": linux, "architecture_matches": arch,
                "aws_provider_id_present": aws, "compute_type": compute, "standard_eks_profile": standard,
                "ready": ready, "runtime_taints_tolerated_and_uncordoned": schedulable,
                "kernel_version": info.get("kernelVersion"), "container_runtime": info.get("containerRuntimeVersion"),
                "kubelet_version": info.get("kubeletVersion")})
        report["observations"]["runtime_nodes"] = summaries
        add_check(report, "node_profile", profile_ok, "All RuntimeClass-eligible nodes must report Linux, expected architecture, AWS provider and no Auto Mode/Fargate selection.")
        add_check(report, "schedulable_ready_node", usable > 0, "A Ready, uncordoned node tolerates the RuntimeClass taints; resource availability is unverified.")
        namespace = client.json("get", "namespace", args.namespace)
        security = namespace.get("metadata", {}).get("labels", {}).get("pod-security.kubernetes.io/enforce")
        add_check(report, "restricted_pod_security", security == "restricted", "Namespace declares restricted Pod Security enforcement.")
        policies = client.json("get", "networkpolicies", "-n", args.namespace).get("items", [])
        deny = next((p for p in policies if p.get("metadata", {}).get("name") == args.network_policy_name), {})
        policy_spec = deny.get("spec", {})
        deny_ok = bool(deny) and selector_matches(policy_spec.get("podSelector", {}), args.workload_labels)
        deny_ok = deny_ok and set(policy_spec.get("policyTypes", [])) == {"Ingress", "Egress"}
        deny_ok = deny_ok and not policy_spec.get("ingress") and not policy_spec.get("egress")
        add_check(report, "default_deny_policy", deny_ok, "Named policy must select workload labels and declare empty ingress and egress.")
        additive = [p.get("metadata", {}).get("name") for p in policies if selector_matches(p.get("spec", {}).get("podSelector", {}), args.workload_labels) and (p.get("spec", {}).get("ingress") or p.get("spec", {}).get("egress"))]
        report["observations"]["selecting_allow_policies"] = additive
        add_check(report, "no_additive_namespace_allow_policy", not additive, "Any selecting namespace allow rules block this offline profile; cluster/CNI-specific policies still require independent review.")
        inspect_cni(client, report, args, selected)
        report["observations"]["upstream_apis"] = {}
        for name, resource in CRDS.items():
            crd = optional(client, report, name, "get", "customresourcedefinition", resource)
            report["observations"]["upstream_apis"][name] = {
                "observed": crd is not None,
                "served_versions": [v.get("name") for v in (crd or {}).get("spec", {}).get("versions", []) if v.get("served")],
            }
    except InventoryError as error:
        add_check(report, "inventory_complete", False, str(error))
    if report["checks"] and all(check["status"] == "observed" for check in report["checks"]):
        report["preflight"] = "DECLARATIONS_ONLY"
    return report


def write_private(path, report):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--context", required=True, help="Explicit trusted kubeconfig context; current-context is never used implicitly.")
    result.add_argument("--profile", required=True, choices=["eks-standard-gvisor"])
    result.add_argument("--runtime-class", required=True)
    result.add_argument("--runtime-handler", required=True)
    result.add_argument("--architecture", required=True, choices=["amd64", "arm64"])
    result.add_argument("--node-selector", action="append", required=True, metavar="KEY=VALUE", help="Required scheduling label; repeat for more labels.")
    result.add_argument("--namespace", default="agentos-workloads")
    result.add_argument("--network-policy-name", default="agentos-deny-all")
    result.add_argument("--workload-label", action="append", metavar="KEY=VALUE")
    result.add_argument("--cni", choices=["aws-vpc", "cilium"], required=True, help="Enforcement implementation being assessed, including Cilium in AWS chaining mode.")
    result.add_argument("--cni-namespace", default="kube-system")
    result.add_argument("--output", required=True, type=Path, help="New private JSON file outside Git; existing files/symlinks are refused.")
    return result


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    try:
        args.node_labels = labels(args.node_selector)
        args.workload_labels = labels(args.workload_label or ["app.kubernetes.io/part-of=agent-os"])
    except argparse.ArgumentTypeError as error:
        cli.error(str(error))
    if not args.context.strip() or not args.runtime_class.strip() or not args.runtime_handler.strip():
        cli.error("context, runtime class and handler must be nonempty")
    for value in [args.runtime_class, args.runtime_handler, args.namespace, args.cni_namespace, args.network_policy_name]:
        if len(value) > 253 or not re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?", value):
            cli.error("Kubernetes names must be lowercase DNS names")
    # Refuse an unusable output before invoking even read-only cluster commands.
    if args.output.exists() or args.output.is_symlink() or not args.output.parent.is_dir():
        cli.error("output must be a new file in an existing directory")
    report = build_report(args, Kubectl(args.context))
    try:
        write_private(args.output, report)
    except OSError:
        print("Could not create private report; no raw cluster output was printed.", file=sys.stderr)
        return 1
    print("NOT_QUALIFIED: " + report["preflight"] + "; private JSON written. Dynamic isolation tests were not run.")
    return 0 if report["preflight"] == "DECLARATIONS_ONLY" else 1


if __name__ == "__main__":
    sys.exit(main())
