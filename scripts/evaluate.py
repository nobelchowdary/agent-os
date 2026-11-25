#!/usr/bin/env python3
"""Opt-in, sequential execution measurements. Never provisions cloud resources.

Registration and rendering are offline. Collection requires --execute and an
explicit kubectl context. Output is private evidence, not an isolation verdict.
"""

import argparse
import copy
import datetime as dt
from decimal import Decimal, InvalidOperation
import hashlib
import http.client
import importlib.util
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import selectors
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


MAX_JSON = 512 * 1024
MAX_EVIDENCE = 2 * 1024 * 1024
MAX_LOG = 64 * 1024
LABEL = "agentos.dev/evaluation"
MANAGED = "app.kubernetes.io/managed-by"
PART = "app.kubernetes.io/part-of"
BASELINES = ("direct-job", "agentos", "agent-sandbox")
EXT = "extensions.agents.x-k8s.io/v1beta1"
CORE = "agents.x-k8s.io/v1beta1"
TERMINAL = {"succeeded", "failed", "cancelled"}
RUN_STATES = TERMINAL | {"queued", "starting", "activating", "running", "cancelling"}
NAME = re.compile(r"[a-z][a-z0-9-]{0,61}[a-z0-9]$|[a-z]$")
DIGEST = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[a-f0-9]{64}$")


class Failure(Exception):
    """Only fixed, non-sensitive error codes reach evidence or the console."""


def require(condition, code):
    if not condition:
        raise Failure(code)


def support_module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


transport = support_module("evaluation_transport")
OutcomeObserver = support_module("evaluation_observer").OutcomeObserver


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def read_json(path, limit=MAX_EVIDENCE):
    with open(path, "rb") as stream:
        raw = stream.read(limit + 1)
    require(len(raw) <= limit, "input_too_large")
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=unique_pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, UnicodeError):
        raise Failure("invalid_json") from None
    require(isinstance(value, dict), "expected_json_object")
    return value


def private_directory(path):
    require(path.is_dir() and not path.is_symlink(), "invalid_evidence_directory")
    require(path.stat().st_mode & 0o077 == 0, "evidence_directory_must_be_private")


def write_json(path, value, exclusive=False):
    data = encoded(value) + b"\n"
    require(len(data) <= MAX_EVIDENCE, "evidence_too_large")
    private_directory(path.parent)
    require(not path.is_symlink(), "evidence_symlink_refused")
    if exclusive:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    else:
        fd, temporary = tempfile.mkstemp(prefix=".evidence-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def validate_workload(workload):
    fields = {"format_version", "template", "image", "command", "resources",
              "timeout_seconds", "runtime_profile", "validation"}
    require(fields <= set(workload) <= fields | {"termination_grace_period_seconds", "deadline_scope"}, "workload_fields")
    require(workload["format_version"] == 1, "workload_version")
    require(isinstance(workload["template"], str) and NAME.fullmatch(workload["template"]), "template_name")
    require(isinstance(workload["image"], str) and DIGEST.fullmatch(workload["image"]), "image_must_be_digest_pinned")
    command = workload["command"]
    require(isinstance(command, list) and 1 <= len(command) <= 64, "command_bounds")
    require(all(isinstance(x, str) and "\0" not in x and len(x.encode()) <= 8192 for x in command)
            and command[0] and sum(len(x.encode()) for x in command) <= 16384, "command_bounds")
    resources = workload["resources"]
    require(isinstance(resources, dict) and set(resources) == {"cpu_milli", "memory_mib", "disk_mib"}, "resource_fields")
    for key, lower, upper in (("cpu_milli", 1, 64000), ("memory_mib", 16, 65536), ("disk_mib", 16, 65536)):
        require(type(resources[key]) is int and lower <= resources[key] <= upper, "resource_bounds")
    require(type(workload["timeout_seconds"]) is int and 1 <= workload["timeout_seconds"] <= 3600, "timeout_bounds")
    if "deadline_scope" in workload:
        require(workload["deadline_scope"] in ("job", "pod"), "deadline_scope")
    if "termination_grace_period_seconds" in workload:
        grace = workload["termination_grace_period_seconds"]
        require(type(grace) is int and 1 <= grace <= 30, "termination_grace_bounds")
    profile = workload["runtime_profile"]
    require(isinstance(profile, dict) and set(profile) == {"kind", "runtime_class", "required_handler"}, "runtime_profile_fields")
    require(isinstance(profile["runtime_class"], str) and NAME.fullmatch(profile["runtime_class"]), "runtime_class_name")
    if profile["kind"] == "gvisor":
        require(profile["required_handler"] == "runsc" and profile["runtime_class"] not in {
            "default", "runc", "runc-v2", "crun", "isolated-sandbox", "confidential-sandbox"}, "gvisor_handler_required")
    elif profile["kind"] == "aws-nitro-isolated":
        require(profile["runtime_class"] == profile["required_handler"] == "isolated-sandbox", "nitro_profile_required")
        require(resources["disk_mib"] < resources["memory_mib"], "nitro_scratch_memory_bound")
    else:
        raise Failure("unsupported_runtime_profile")
    check = workload["validation"]
    require(isinstance(check, dict) and set(check) == {"ready_line", "complete_line"}, "validation_fields")
    require(all(isinstance(x, str) and 1 <= len(x.encode()) <= 256 and "\n" not in x and "\r" not in x
                for x in check.values()), "validation_bounds")
    require(check["ready_line"] != check["complete_line"], "distinct_markers_required")
    return workload


def validate_protocol(protocol):
    fields = {"format_version", "experiment_id", "baseline", "candidate", "minimum_trials",
              "minimum_correct_fraction", "max_p95_latency_ratio", "min_cost_improvement_fraction",
              "min_operator_improvement_fraction"}
    version = protocol.get("format_version")
    require(type(version) is int and version in (1, 2, 3)
            and set(protocol) == fields | ({"latency_policy"} if version >= 2 else set())
                | ({"observation_policy"} if version == 3 else set()),
            "protocol_fields_or_version")
    require(protocol["baseline"] in BASELINES and protocol["candidate"] in BASELINES
            and protocol["baseline"] != protocol["candidate"], "protocol_baselines")
    require(type(protocol["minimum_trials"]) is int and 2 <= protocol["minimum_trials"] <= 200, "protocol_trial_bounds")
    for key, lower, upper in (("minimum_correct_fraction", 0.01, 1), ("max_p95_latency_ratio", 1, 10),
                              ("min_cost_improvement_fraction", 0.001, 0.99), ("min_operator_improvement_fraction", 0.001, 0.99)):
        value = protocol[key]
        require(type(value) in (int, float) and math.isfinite(value) and lower <= value <= upper, "protocol_metric_bounds")
    if version >= 2:
        policy = protocol["latency_policy"]
        require(isinstance(policy, dict) and set(policy) == {
            "boundary", "mode", "absolute_allowance_seconds", "rationale"}, "latency_policy_fields")
        require(isinstance(policy["boundary"], str) and policy["boundary"] in {
            "pod_terminal", "outcome_visible_to_caller", "validated_completion"}
                and policy["mode"] == "ratio_plus_absolute", "latency_policy_contract")
        allowance = policy["absolute_allowance_seconds"]
        require(type(allowance) in (int, float) and math.isfinite(allowance) and 0 <= allowance <= 3600,
                "latency_policy_allowance")
        rationale = policy["rationale"]
        try:
            valid_rationale = isinstance(rationale, str) and rationale.strip() and len(rationale.encode()) <= 1024
        except UnicodeError:
            valid_rationale = False
        require(valid_rationale, "latency_policy_rationale")
    if version == 3:
        require(protocol["latency_policy"]["boundary"] == "outcome_visible_to_caller", "unsupported_observation_boundary")
        policy = protocol["observation_policy"]
        require(isinstance(policy, dict) and set(policy) == {
            "mode", "period_seconds", "max_terminal_gap_seconds"}, "observation_policy_fields")
        require(policy["mode"] == "fixed_cadence", "observation_policy_mode")
        period, gap = policy["period_seconds"], policy["max_terminal_gap_seconds"]
        require(type(period) in (int, float) and math.isfinite(period) and .01 <= period <= 5,
                "observation_period_bounds")
        require(type(gap) in (int, float) and math.isfinite(gap) and period <= gap <= 3600,
                "observation_gap_bounds")


def deadline_scope(workload, baseline="agentos"):
    """Keep omitted legacy workload inputs unchanged, including native clocks."""
    return "pod" if baseline == "agent-sandbox" else workload.get("deadline_scope", "job")


def deadline_contract(workload, baseline):
    scope = deadline_scope(workload, baseline)
    registered_scope = deadline_scope(workload)
    return {"scope": scope, "registered_scope": registered_scope,
            "registration": "explicit" if "deadline_scope" in workload else "legacy_implicit_job",
            "matches_registered_scope": scope == registered_scope,
            "boundary": "pod_start_time" if scope == "pod" else "job_start_time_after_activation",
            "fault_qualification": "not_collected"}


def pod_template(workload, labels, baseline="agentos"):
    """Mirror backend.Render's administrator-owned execution/security contract."""
    resources = workload["resources"]
    quantities = {"cpu": str(resources["cpu_milli"]) + "m", "memory": str(resources["memory_mib"]) + "Mi",
                  "ephemeral-storage": str(resources["disk_mib"]) + "Mi"}
    security = {"runAsUser": 10001, "runAsGroup": 10001, "runAsNonRoot": True,
                "seccompProfile": {"type": "RuntimeDefault"}}
    volumes = [{"name": name, "emptyDir": {"sizeLimit": str(size) + "Mi"}}
               for name, size in (("workspace", resources["disk_mib"] // 2),
                                  ("tmp", resources["disk_mib"] - resources["disk_mib"] // 2))]
    metadata = {"labels": dict(labels)}
    if workload["runtime_profile"]["kind"] == "aws-nitro-isolated":
        metadata["annotations"] = {"sandbox.amazonaws.com/network-isolated": "true"}
        for volume in volumes:
            volume["emptyDir"]["medium"] = "Memory"
    template = {"metadata": metadata, "spec": {
        "restartPolicy": "Never", "runtimeClassName": workload["runtime_profile"]["runtime_class"],
        "serviceAccountName": "agentos-workload", "automountServiceAccountToken": False,
        "securityContext": dict(security, fsGroup=10001, fsGroupChangePolicy="OnRootMismatch"),
        "containers": [{"name": "agent", "image": workload["image"], "command": workload["command"],
                        "workingDir": "/workspace", "imagePullPolicy": "IfNotPresent",
                        "terminationMessagePath": "/dev/termination-log", "terminationMessagePolicy": "File",
                        "securityContext": dict(security, allowPrivilegeEscalation=False,
                                                readOnlyRootFilesystem=True, capabilities={"drop": ["ALL"]}),
                        "resources": {"requests": quantities, "limits": dict(quantities)},
                        "volumeMounts": [{"name": "workspace", "mountPath": "/workspace"},
                                         {"name": "tmp", "mountPath": "/tmp"}]}], "volumes": volumes}}
    if workload["runtime_profile"]["kind"] == "aws-nitro-isolated":
        template["spec"]["nodeSelector"] = {"eks.amazonaws.com/nitro-sandbox": "true"}
    if "termination_grace_period_seconds" in workload:
        template["spec"]["terminationGracePeriodSeconds"] = workload["termination_grace_period_seconds"]
    if deadline_scope(workload, baseline) == "pod":
        template["spec"]["activeDeadlineSeconds"] = workload["timeout_seconds"]
    return template


def labels(registration, trial=None):
    result = {LABEL: registration["registration_id"], MANAGED: "agentos-evaluation", PART: "agent-os"}
    if trial is not None:
        result["agentos.dev/evaluation-trial"] = str(trial)
    return result


def job_manifest(registration, name, trial):
    workload = registration["workload"]
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {
        "name": name, "namespace": registration["namespace"], "labels": labels(registration, trial)}, "spec": {
            "backoffLimit": 0, "parallelism": 1, "completions": 1, "suspend": False,
            "template": pod_template(workload, labels(registration, trial))}}
    if deadline_scope(workload) == "job":
        job["spec"]["activeDeadlineSeconds"] = workload["timeout_seconds"]
    return job


def upstream_manifests(registration):
    name = "ae-" + registration["registration_id"][:16]
    metadata = {"name": name, "namespace": registration["namespace"], "labels": labels(registration)}
    return [{"apiVersion": EXT, "kind": "SandboxTemplate", "metadata": metadata,
             "spec": {"podTemplate": pod_template(registration["workload"], labels(registration), baseline="agent-sandbox"),
                      "service": False, "networkPolicyManagement": "Unmanaged",
                      "envVarsInjectionPolicy": "Disallowed"}},
            {"apiVersion": EXT, "kind": "SandboxWarmPool", "metadata": copy.deepcopy(metadata),
             "spec": {"replicas": 0, "sandboxTemplateRef": {"name": name}}}]


def validate_logs(logs, workload, complete=False):
    require(len(logs.encode()) <= MAX_LOG, "logs_too_large")
    ready = workload["validation"]["ready_line"]
    final = workload["validation"]["complete_line"]
    expected = ready + "\n" + final + "\n"
    require(expected.startswith(logs), "workload_output_mismatch")
    if complete:
        require(logs == expected, "workload_output_incomplete")
    return logs.startswith(ready + "\n")


def bounded_command(arguments, body=None, timeout=15, limit=MAX_JSON, token_env=None):
    """Never forward kubectl diagnostics, which can contain endpoint/auth details."""
    environment = dict(os.environ)
    if token_env:
        environment.pop(token_env, None)
    with tempfile.TemporaryFile() as source:
        if body is not None:
            source.write(encoded(body))
            source.seek(0)
        try:
            process = subprocess.Popen(arguments, stdin=source, stdout=subprocess.PIPE,
                                       stderr=subprocess.DEVNULL, env=environment)
        except OSError:
            raise Failure("kubectl_unavailable") from None
        output = bytearray()
        deadline = time.monotonic() + timeout
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while selector.get_map():
                    require(time.monotonic() < deadline, "kubectl_timeout")
                    for key, _ in selector.select(min(0.1, max(0, deadline - time.monotonic()))):
                        block = os.read(key.fileobj.fileno(), 16384)
                        if not block:
                            selector.unregister(key.fileobj)
                            continue
                        output.extend(block)
                        require(len(output) <= limit, "kubectl_output_too_large")
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            require(process.returncode == 0, "kubectl_request_failed")
        except subprocess.TimeoutExpired:
            raise Failure("kubectl_timeout") from None
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()
    try:
        return output.decode("utf-8")
    except UnicodeError:
        raise Failure("invalid_utf8_output") from None


class Kubectl:
    def __init__(self, registration, token_env=None):
        self.registration = registration
        self.token_env = token_env

    def call(self, arguments, body=None, limit=MAX_JSON):
        return bounded_command(["kubectl", "--context", self.registration["context"],
                                "--namespace", self.registration["namespace"], "--request-timeout=10s"] + arguments,
                               body, limit=limit, token_env=self.token_env)

    def get(self, resource, name=None, selector=None):
        value = self.call(["get", resource] + ([name] if name else [])
                          + (["--selector", selector] if selector else []) + ["--ignore-not-found", "-o", "json"])
        if not value.strip():
            return None
        try:
            result = json.loads(value)
        except ValueError:
            raise Failure("invalid_kubernetes_json") from None
        require(isinstance(result, dict), "invalid_kubernetes_json")
        return result

    def create(self, manifest):
        try:
            self.call(["create", "-f", "-", "-o", "json"], manifest)
        except Failure:
            # Creation can have succeeded despite a lost response. Never change
            # the name or recreate after activation; inspect this exact identity.
            pass
        obj = self.get(resource_name(manifest), manifest["metadata"]["name"])
        require(obj is not None, "creation_outcome_unknown")
        return obj

    def logs(self, pod):
        return self.call(["logs", pod, "--container=agent", "--limit-bytes=" + str(MAX_LOG + 1)], limit=MAX_LOG + 1)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class AgentOS:
    def __init__(self, base, token_env):
        parsed = urllib.parse.urlsplit(base)
        require(parsed.scheme in {"http", "https"} and parsed.hostname and not parsed.username
                and not parsed.password and not parsed.query and not parsed.fragment and parsed.path in {"", "/"}, "invalid_api_url")
        local = parsed.hostname == "localhost"
        try:
            local = local or ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            pass
        require(parsed.scheme == "https" or local, "api_requires_https_or_loopback")
        require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token_env), "invalid_token_env")
        self.token = os.environ.get(token_env, "")
        require(32 <= len(self.token) <= 512 and all(32 < ord(c) < 127 for c in self.token), "api_token_env_missing_or_invalid")
        self.base = base.rstrip("/")
        # Deliberately ignore proxy environment variables for bearer requests.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def call(self, path, body=None, key=None):
        headers = {"Authorization": "Bearer " + self.token, "Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(self.base + path, data=None if body is None else encoded(body), headers=headers)
        try:
            with self.opener.open(request, timeout=10) as response:
                raw = response.read(MAX_JSON + 1)
        except urllib.error.HTTPError as error:
            error.close()
            raise Failure("api_http_" + str(error.code)) from None
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            raise Failure("api_transport_outcome_unknown") from None
        require(len(raw) <= MAX_JSON, "api_response_too_large")
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            raise Failure("invalid_api_json") from None
        require(isinstance(value, dict), "invalid_api_json")
        return value


class PersistentAgentOS(AgentOS):
    """Dedicated persistent channel; no proxy environment or redirects."""
    def __init__(self, base, token_env):
        super().__init__(base, token_env)
        self.token_env = token_env
        parsed = urllib.parse.urlsplit(self.base)
        self.connection = transport.persistent_http_connection(parsed.scheme, parsed.hostname, parsed.port, timeout=10)
        self.lock = threading.Lock()

    def call(self, path, body=None, key=None):
        require(path.startswith("/") and not path.startswith("//") and "\r" not in path and "\n" not in path,
                "invalid_api_path")
        headers = {"Authorization": "Bearer " + self.token, "Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        with self.lock:
            try:
                self.connection.begin_request()
                self.connection.request("GET" if body is None else "POST", path,
                                        body=None if body is None else encoded(body), headers=headers)
                response = self.connection.getresponse()
                raw = response.read(MAX_JSON + 1)
                require(len(raw) <= MAX_JSON, "api_response_too_large")
                require(200 <= response.status < 300, "api_http_" + str(response.status))
                value = json.loads(raw)
                require(isinstance(value, dict), "invalid_api_json")
                return value
            except Failure:
                self.connection.close()
                raise
            except (ValueError, UnicodeError):
                self.connection.close()
                raise Failure("invalid_api_json") from None
            except (OSError, http.client.HTTPException):
                self.connection.close()
                raise Failure("api_transport_outcome_unknown") from None

    def fork(self):
        child = PersistentAgentOS(self.base, self.token_env)
        try:
            require(child.call("/readyz").get("status") == "ready", "api_not_execution_ready")
            return child
        except BaseException:
            child.close()
            raise

    def close(self):
        self.connection.close()


def resource_name(obj):
    return {"Job": "jobs", "Pod": "pods", "SandboxTemplate": "sandboxtemplates.extensions.agents.x-k8s.io",
            "SandboxWarmPool": "sandboxwarmpools.extensions.agents.x-k8s.io",
            "SandboxClaim": "sandboxclaims.extensions.agents.x-k8s.io",
            "Sandbox": "sandboxes.agents.x-k8s.io"}[obj["kind"]]


def check_owned(obj, registration, reference):
    meta = obj.get("metadata", {})
    require(meta.get("name") == reference["name"] and meta.get("namespace") == registration["namespace"]
            and meta.get("labels", {}).get(LABEL) == registration["registration_id"]
            and meta.get("uid") and meta.get("resourceVersion"), "resource_ownership_mismatch")
    if reference.get("uid"):
        require(meta["uid"] == reference["uid"], "resource_uid_changed")


def owned_by(obj, parent):
    return any(ref.get("uid") == parent["metadata"]["uid"] and ref.get("controller") is True
               for ref in obj.get("metadata", {}).get("ownerReferences", []))


def reference(obj):
    return {"kind": obj["kind"], "name": obj["metadata"]["name"], "uid": obj["metadata"].get("uid")}


def quantity(value):
    """Compare Kubernetes' canonical quantity spellings, e.g. 1000m and 1."""
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([numkKMGTEP]|[KMGTPE]i)?", str(value))
    require(match is not None, "unsupported_resource_quantity")
    factors = {"": 1, "n": Decimal("0.000000001"), "u": Decimal("0.000001"), "m": Decimal("0.001"),
               "k": 1000, "K": 1000, **{s: 1000 ** p for p, s in enumerate("MGTPE", 2)},
               **{s + "i": 1024 ** p for p, s in enumerate("KMGTPE", 1)}}
    try:
        return Decimal(match[1]) * factors[match[2] or ""]
    except (InvalidOperation, KeyError):
        raise Failure("unsupported_resource_quantity") from None


def normalized_pod(spec):
    spec = copy.deepcopy(spec)
    for container in spec.get("containers", []):
        for kind in ("requests", "limits"):
            amounts = container.get("resources", {}).get(kind, {})
            for key in amounts:
                amounts[key] = quantity(amounts[key])
    for volume in spec.get("volumes", []):
        scratch = volume.get("emptyDir", {})
        if "sizeLimit" in scratch:
            scratch["sizeLimit"] = quantity(scratch["sizeLimit"])
    return spec


def assert_pod_profile(actual, expected):
    # Admission may add scheduling defaults. All execution, volume, token and
    # security fields must match; injected sidecars/credentials cannot pass.
    actual, expected = normalized_pod(actual), normalized_pod(expected)
    require(actual.get("terminationGracePeriodSeconds", 30) == expected.get("terminationGracePeriodSeconds", 30),
            "pod_profile_changed")
    for key in ("containers", "volumes", "securityContext", "runtimeClassName", "restartPolicy",
                "serviceAccountName", "automountServiceAccountToken", "activeDeadlineSeconds"):
        require(actual.get(key) == expected.get(key), "pod_profile_changed")
    for key in ("hostNetwork", "hostPID", "hostIPC", "initContainers", "ephemeralContainers", "imagePullSecrets", "hostAliases"):
        require(not actual.get(key), "pod_profile_changed")
    for key, value in expected.get("nodeSelector", {}).items():
        require(actual.get("nodeSelector", {}).get(key) == value, "pod_placement_changed")


def assert_job_profile(job, registration):
    spec = job.get("spec", {})
    workload = registration["workload"]
    expected_deadline = workload["timeout_seconds"] if deadline_scope(workload) == "job" else None
    for key, value in {"backoffLimit": 0, "parallelism": 1, "completions": 1,
                       "activeDeadlineSeconds": expected_deadline}.items():
        require(spec.get(key) == value, "job_profile_changed")
    expected = pod_template(registration["workload"], {})
    require(not spec.get("template", {}).get("spec", {}).get("nodeName"), "job_node_pin_changed")
    assert_pod_profile(spec.get("template", {}).get("spec", {}), expected["spec"])
    assert_pod_metadata(spec["template"].get("metadata", {}), expected["metadata"])
    require(spec["template"].get("metadata", {}).get("labels", {}).get(PART) == "agent-os", "pod_policy_label_missing")


def assert_pod_metadata(actual, expected):
    annotations = actual.get("annotations", {})
    protected = {k: v for k, v in annotations.items() if k.startswith("sandbox.amazonaws.com/")}
    require(protected == expected.get("annotations", {}), "pod_runtime_annotations_changed")
    require(actual.get("labels", {}).get(PART) == "agent-os", "pod_policy_label_missing")


def remember_descendant(result, obj, parent):
    require(owned_by(obj, parent), "descendant_owner_changed")
    ref = dict(reference(obj), parent_uid=parent["metadata"]["uid"])
    descendants = result.setdefault("descendants", [])
    if ref not in descendants:
        require(len(descendants) < 8, "too_many_execution_descendants")
        descendants.append(ref)


def descendants_absent(kube, registration, descendants):
    deadline = time.monotonic() + 30
    while True:
        pending = False
        for ref in descendants:
            obj = kube.get(resource_name(ref), ref["name"])
            if obj:
                require(obj.get("metadata", {}).get("uid") == ref["uid"], "descendant_uid_changed")
                require(any(x.get("uid") == ref["parent_uid"] and x.get("controller") is True
                            for x in obj.get("metadata", {}).get("ownerReferences", [])), "descendant_owner_changed")
                pending = True
        if not pending:
            return
        require(time.monotonic() < deadline, "descendant_cleanup_not_observed")
        time.sleep(registration["poll_seconds"])


def preflight(kube, registration, api=None):
    profile = registration["workload"]["runtime_profile"]
    runtime = kube.get("runtimeclasses.node.k8s.io", profile["runtime_class"])
    require(runtime is not None and runtime.get("handler") == profile["required_handler"], "required_runtime_unavailable")
    require(kube.get("serviceaccount", "agentos-workload") is not None, "workload_service_account_missing")
    policy = kube.get("networkpolicy", "agentos-deny-all")
    require(policy is not None and policy.get("spec", {}).get("podSelector") == {"matchLabels": {PART: "agent-os"}}
            and not policy["spec"].get("ingress") and not policy["spec"].get("egress")
            and set(policy["spec"].get("policyTypes", [])) == {"Ingress", "Egress"}, "default_deny_prerequisite_missing")
    if profile["kind"] == "aws-nitro-isolated":
        require(runtime.get("scheduling", {}).get("nodeSelector", {}).get("eks.amazonaws.com/nitro-sandbox") == "true",
                "nitro_runtime_placement_missing")
        require(runtime.get("overhead", {}).get("podFixed", {}).get("eks.amazonaws.com/nitro-sandbox") == "1",
                "nitro_runtime_overhead_missing")
        require(quantity(runtime.get("overhead", {}).get("podFixed", {}).get("memory", "0")) >= quantity("256Mi"),
                "nitro_runtime_memory_overhead_missing")
    if api:
        require(api.call("/readyz").get("status") == "ready", "api_not_execution_ready")
        plan = api.call("/v1/plan", request_body(registration))
        require(plan.get("execution") is False, "invalid_plan_response")
        job = plan.get("job", {})
        require(job.get("metadata", {}).get("namespace") == registration["namespace"], "api_namespace_mismatch")
        assert_job_profile(job, registration)


def request_body(registration):
    workload = registration["workload"]
    return {key: workload[key] for key in ("template", "command", "timeout_seconds")}


def assert_record_spec(record, registration):
    spec = record.get("spec", {})
    workload = registration["workload"]
    for key in ("template", "image", "command", "resources", "timeout_seconds"):
        require(spec.get(key) == workload[key], "api_resolved_spec_mismatch")
    require(spec.get("deadline_scope", "job") == deadline_scope(workload), "api_resolved_spec_mismatch")
    require(spec.get("termination_grace_period_seconds", 30) == workload.get("termination_grace_period_seconds", 30),
            "api_resolved_spec_mismatch")
    require(spec.get("runtime_class") == workload["runtime_profile"]["runtime_class"], "api_resolved_spec_mismatch")
    if spec.get("runtime_profile") is not None:
        require(spec["runtime_profile"] == workload["runtime_profile"], "api_resolved_spec_mismatch")
    else:
        require(workload["runtime_profile"]["kind"] == "gvisor", "api_resolved_spec_mismatch")


def delete_owned(kube, registration, ref):
    resource = resource_name(ref)
    for _ in range(3):
        obj = kube.get(resource, ref["name"])
        if obj is None:
            return
        check_owned(obj, registration, ref)
        meta = obj["metadata"]
        path = "/apis/" + obj["apiVersion"] + "/namespaces/" + registration["namespace"] + "/" + resource.split(".")[0] + "/" + meta["name"]
        options = {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground",
                   "preconditions": {"uid": meta["uid"], "resourceVersion": meta["resourceVersion"]}}
        try:
            kube.call(["delete", "--raw", path, "-f", "-"], options)
            break
        except Failure:
            continue
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        obj = kube.get(resource, ref["name"])
        if obj is None:
            return
        check_owned(obj, registration, ref)
        time.sleep(registration["poll_seconds"])
    raise Failure("cleanup_not_observed")


def tag_api_job(kube, registration, job):
    meta = job["metadata"]
    require(meta.get("labels", {}).get("agentos.dev/run-id") == meta["name"].removeprefix("agentos-")
            and meta.get("labels", {}).get(MANAGED) == "agent-os", "api_job_ownership_mismatch")
    assert_job_profile(job, registration)
    old_label = meta["labels"].get(LABEL)
    require(old_label in (None, registration["registration_id"]), "api_job_already_claimed")
    patch = [{"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
             {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
             {"op": "add", "path": "/metadata/labels/agentos.dev~1evaluation", "value": registration["registration_id"]}]
    kube.call(["patch", "job", meta["name"], "--type=json", "--patch", json.dumps(patch), "-o", "json"])
    return reference(job)


def cancel_api(api, run_id, registration):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        state = api.call("/v1/runs/" + run_id)
        if state.get("state") in TERMINAL:
            return
        try:
            api.call("/v1/runs/" + run_id + "/cancel", {})
        except Failure as error:
            if str(error) != "api_http_409":
                raise
        time.sleep(registration["poll_seconds"])
    raise Failure("api_cancellation_not_observed")


def timing_contract(baseline):
    """Describe the client's actual arm-specific outcome, not equivalent guarantees."""
    outcomes = {
        "agentos": ("agentos_run_api", "first verified terminal run response; Job-authoritative Agent OS outcome"),
        "direct-job": ("kubernetes_job_condition", "first owned Job Complete/Failed condition received by the collector"),
        "agent-sandbox": ("sandbox_pod_phase", "collector-defined outcome from an owned Sandbox Pod terminal phase"),
    }
    source, meaning = outcomes[baseline]
    return {"format_version": 1, "clock_basis": "harness_monotonic_offsets",
            "observation": "poll receipts include polling, transport and observer overhead; not API-server write times",
            "wall_clock": "harness UTC receipt timestamps; wall-clock synchronization unqualified",
            "events": {
                "pod_terminal": {"source": "kubernetes_pod_phase",
                    "boundary": "first verified workload Pod Succeeded/Failed phase received by the collector"},
                "outcome_visible_to_caller": {"source": source, "boundary": meaning},
                "validated_completion": {"source": "collector_validated_output",
                    "boundary": "legacy elapsed_seconds endpoint: observed arm success and exact validated stdout, before cleanup"}}}


def trial_timing(baseline):
    contract = timing_contract(baseline)
    reasons = {"pod_terminal": "pod_terminal_not_observed", "outcome_visible_to_caller": "caller_outcome_not_observed",
               "validated_completion": "validated_completion_not_reached"}
    return {"format_version": 1, "clock_basis": contract["clock_basis"], "execution": None,
            "events": {name: {"status": "missing", "observed_at": None, "elapsed_seconds": None,
                               "source": event["source"], "reason": reasons[name]}
                       for name, event in contract["events"].items()},
            "outcome_after_pod_terminal_seconds": None,
            "ordering": "unobserved; signed receipt intervals can reflect polling order rather than execution order"}


def timing_receipt(start):
    elapsed = time.monotonic() - start
    require(math.isfinite(elapsed) and elapsed >= 0, "invalid_trial_observation_clock")
    return {"observed_at": now(), "elapsed_seconds": elapsed}


def observe_trial_event(result, name, receipt, **facts):
    event = result["timing"]["events"][name]
    if event["status"] == "observed":
        # Outcomes are immutable; repeated polls cannot silently combine two
        # different execution states or object identities into one timestamp.
        require(all(event.get(key) == value for key, value in facts.items()), "timing_event_changed")
        return
    event.update(receipt, status="observed", **facts)
    event.pop("reason", None)


def observe_trial_pod(result, pod, parent, receipt):
    """Only call after the Pod's owner, execution profile and metadata checks."""
    metadata = pod.get("metadata", {})
    require(owned_by(pod, parent), "descendant_owner_changed")
    binding = {"pod_uid": metadata.get("uid"), "pod_name": metadata.get("name"),
               "parent_uid": parent.get("metadata", {}).get("uid")}
    require(all(isinstance(value, str) and value for value in binding.values()), "pod_timing_identity_missing")
    existing = result["timing"]["execution"]
    require(existing is None or existing == binding, "execution_pod_identity_changed")
    result["timing"]["execution"] = binding
    phase = pod.get("status", {}).get("phase")
    if phase in {"Succeeded", "Failed"}:
        observe_trial_event(result, "pod_terminal", receipt, state=phase, **binding)


def finish_trial_timing(result):
    events = result["timing"]["events"]
    pod, outcome = events["pod_terminal"], events["outcome_visible_to_caller"]
    if pod["status"] == outcome["status"] == "observed":
        result["timing"]["outcome_after_pod_terminal_seconds"] = outcome["elapsed_seconds"] - pod["elapsed_seconds"]


def job_terminal_state(job):
    outcomes = {condition.get("type") for condition in job.get("status", {}).get("conditions", [])
                if condition.get("status") == "True" and condition.get("type") in {"Complete", "Failed"}}
    require(len(outcomes) <= 1, "conflicting_job_outcome")
    return next(iter(outcomes)) if outcomes else None


def legacy_trial(kube, registration, baseline, index, result, save, api=None):
    start = time.monotonic()
    result.update({"correct": False, "elapsed_seconds": 0.0, "status": "running", "resources": [], "descendants": [],
                   "timing": trial_timing(baseline)})
    name = "ae-" + registration["registration_id"][:16] + "-" + {"direct-job": "d", "agentos": "a", "agent-sandbox": "s"}[baseline] + "-" + str(index)
    parent = None
    save()
    try:
        if baseline == "agentos":
            result["idempotency_key"] = name
            result["submission_uncertain"] = True
            save()
            record = None
            for _ in range(2):
                try:
                    record = api.call("/v1/runs", request_body(registration), key=name)
                    record_receipt = timing_receipt(start)
                    break
                except Failure as error:
                    if str(error) not in {"api_transport_outcome_unknown", "api_http_500", "api_http_502", "api_http_503",
                                          "api_http_504", "invalid_api_json", "api_response_too_large"}:
                        raise
            require(record is not None, "api_submission_outcome_unknown")
            run_id = record.get("id", "")
            require(isinstance(run_id, str) and re.fullmatch(r"[a-f0-9]{32}", run_id), "invalid_api_run_id")
            result["run_id"] = run_id
            result["submission_uncertain"] = False
            name = "agentos-" + run_id
            save()
            assert_record_spec(record, registration)
            if record.get("state") in TERMINAL:
                observe_trial_event(result, "outcome_visible_to_caller", record_receipt,
                                    state=record["state"], run_id=run_id)
        else:
            if baseline == "direct-job":
                manifest = job_manifest(registration, name, index)
            else:
                manifest = {"apiVersion": EXT, "kind": "SandboxClaim", "metadata": {
                    "name": name, "namespace": registration["namespace"], "labels": labels(registration, index)},
                    "spec": {"warmPoolRef": {"name": "ae-" + registration["registration_id"][:16]}}}
            result["resources"].append(reference(manifest))
            result["submission_uncertain"] = True
            save()  # Persist the exact intended identity before the API call.
            parent = kube.create(manifest)
            parent_receipt = timing_receipt(start)
            check_owned(parent, registration, result["resources"][0])
            result["resources"][0] = reference(parent)
            result["submission_uncertain"] = False
            save()
            if baseline == "direct-job":
                assert_job_profile(parent, registration)
                state = job_terminal_state(parent)
                if state:
                    observe_trial_event(result, "outcome_visible_to_caller", parent_receipt,
                                        state=state, resource_uid=parent["metadata"]["uid"])
        logs = ""
        while time.monotonic() - start < registration["trial_timeout_seconds"]:
            terminal = False
            successful = False
            if baseline == "agentos":
                record = api.call("/v1/runs/" + result["run_id"])
                receipt = timing_receipt(start)
                require(record.get("id") == result["run_id"], "api_run_identity_changed")
                assert_record_spec(record, registration)
                terminal = record.get("state") in TERMINAL
                successful = record.get("state") == "succeeded"
                if terminal:
                    observe_trial_event(result, "outcome_visible_to_caller", receipt,
                                        state=record["state"], run_id=result["run_id"])
                if parent is None:
                    parent = kube.get("job", name)
                    if parent is not None:
                        result["resources"] = [tag_api_job(kube, registration, parent)]
                        save()
                if parent is not None:
                    current = kube.get("job", name)
                    if current is not None:
                        check_owned(current, registration, result["resources"][0])
                    else:
                        # The API may already have captured its result and
                        # deleted a previously verified execution. Absence is
                        # not Pod-terminal evidence, but must not erase the
                        # caller's authoritative terminal API observation.
                        require(terminal, "execution_disappeared")
                    pods = kube.get("pods", selector="agentos.dev/run-id=" + result["run_id"]) or {"items": []}
                    pod_receipt = timing_receipt(start)
                    candidates = [p for p in pods.get("items", []) if owned_by(p, parent)]
                    require(len(candidates) <= 1, "multiple_execution_pods")
                    for pod in candidates:
                        expected = pod_template(registration["workload"], {})
                        assert_pod_profile(pod.get("spec", {}), expected["spec"])
                        assert_pod_metadata(pod.get("metadata", {}), expected["metadata"])
                        remember_descendant(result, pod, parent)
                        observe_trial_pod(result, pod, parent, pod_receipt)
                        save()
                try:
                    logs = api.call("/v1/runs/" + result["run_id"] + "/logs").get("logs", "")
                    require(isinstance(logs, str), "invalid_api_logs")
                except Failure as error:
                    if str(error) not in {"api_http_404", "api_http_503"}:
                        raise
            else:
                if baseline == "direct-job":
                    current = kube.get("job", name)
                    receipt = timing_receipt(start)
                    require(current is not None, "execution_disappeared")
                    check_owned(current, registration, result["resources"][0])
                    assert_job_profile(current, registration)
                    state = job_terminal_state(current)
                    terminal, successful = state is not None, state == "Complete"
                    if terminal:
                        observe_trial_event(result, "outcome_visible_to_caller", receipt,
                                            state=state, resource_uid=current["metadata"]["uid"])
                    pod_parent = current
                else:
                    claim = kube.get("sandboxclaims.extensions.agents.x-k8s.io", name)
                    require(claim is not None, "execution_disappeared")
                    check_owned(claim, registration, result["resources"][0])
                    sandbox_name = claim.get("status", {}).get("sandbox", {}).get("name")
                    pod_parent = kube.get("sandboxes.agents.x-k8s.io", sandbox_name) if sandbox_name else None
                    if pod_parent:
                        require(owned_by(pod_parent, parent), "sandbox_owner_changed")
                        require(pod_parent.get("metadata", {}).get("labels", {}).get("agents.x-k8s.io/launch-type") != "warm",
                                "cold_baseline_received_warm_sandbox")
                        remember_descendant(result, pod_parent, parent)
                        save()
                pods = kube.get("pods", selector=LABEL + "=" + registration["registration_id"]) or {"items": []}
                pod_receipt = timing_receipt(start)
                candidates = [x for x in pods.get("items", []) if pod_parent and owned_by(x, pod_parent)]
                require(len(candidates) <= 1, "multiple_execution_pods")
                if candidates:
                    pod = candidates[0]
                    expected = pod_template(registration["workload"], {}, baseline=baseline)
                    assert_pod_profile(pod.get("spec", {}), expected["spec"])
                    assert_pod_metadata(pod.get("metadata", {}), expected["metadata"])
                    remember_descendant(result, pod, pod_parent)
                    observe_trial_pod(result, pod, pod_parent, pod_receipt)
                    save()
                    if baseline == "agent-sandbox":
                        terminal = pod.get("status", {}).get("phase") in {"Succeeded", "Failed"}
                        successful = pod.get("status", {}).get("phase") == "Succeeded"
                        if terminal:
                            observe_trial_event(result, "outcome_visible_to_caller", pod_receipt,
                                                state=pod["status"]["phase"], pod_uid=pod["metadata"]["uid"],
                                                parent_uid=pod_parent["metadata"]["uid"])
                    try:
                        logs = kube.logs(pod["metadata"]["name"])
                    except Failure as error:
                        if terminal or str(error) != "kubectl_request_failed":
                            raise
            ready = validate_logs(logs, registration["workload"])
            if ready and "readiness_seconds" not in result:
                result["readiness_seconds"] = time.monotonic() - start
                save()
            if terminal:
                require(successful, "execution_failed")
                require(parent is not None, "api_execution_not_found_in_selected_context")
                validate_logs(logs, registration["workload"], complete=True)
                result["correct"] = True
                result["stdout_sha256"] = hashlib.sha256(logs.encode()).hexdigest()
                break
            time.sleep(registration["poll_seconds"])
        else:
            raise Failure("trial_observer_timeout")
    except KeyboardInterrupt:
        result["error"] = "interrupted"
        raise
    except Failure as error:
        result["error"] = str(error)
    except Exception:
        result["error"] = "unexpected_observer_failure"
    finally:
        result["elapsed_seconds"] = time.monotonic() - start
        if result["correct"]:
            observe_trial_event(result, "validated_completion", {
                "observed_at": now(), "elapsed_seconds": result["elapsed_seconds"]}, state="succeeded")
        finish_trial_timing(result)
        result["status"] = "finished"
        save()
        try:
            if result.get("run_id"):
                cancel_api(api, result["run_id"], registration)
            for ref in reversed(result["resources"]):
                delete_owned(kube, registration, ref)
            descendants_absent(kube, registration, result["descendants"])
            result["cleanup"] = ("submission_outcome_unknown" if result.get("submission_uncertain") else
                                 "api_execution_context_unverified" if result.get("run_id") and parent is None else
                                 "observed_objects_absent")
        except Failure as error:
            result["cleanup"] = str(error)
        save()


def measured_trial(kube, registration, baseline, index, result, save, api=None):
    """v3: one independent gated read per fixed tick; validation never schedules it."""
    result.update(correct=False, elapsed_seconds=0.0, status="running", resources=[], descendants=[],
                  timing=trial_timing(baseline))
    name = "ae-" + registration["registration_id"][:16] + "-" + {
        "direct-job": "d", "agentos": "a", "agent-sandbox": "s"}[baseline] + "-" + str(index)
    parent = observer = sampling_kube = sampling_api = None
    start = None
    target = {"run_id": None}
    identity = None

    def read_outcome():
        nonlocal identity
        if baseline == "agentos":
            run_id = target["run_id"]
            if run_id is None:
                return None
            obj = sampling_api.call("/v1/runs/" + run_id)
            require(obj.get("id") == run_id, "api_run_identity_changed")
            assert_record_spec(obj, registration)
            state, facts = obj.get("state"), {"run_id": run_id}
            require(isinstance(state, str) and state in RUN_STATES, "invalid_api_run_state")
            terminal = state in TERMINAL
        elif baseline == "direct-job":
            obj = sampling_kube.get("job", name)
            if obj is None:
                return None
            check_owned(obj, registration, {"name": name})
            assert_job_profile(obj, registration)
            state = job_terminal_state(obj)
            terminal, facts = state is not None, {"resource_uid": obj["metadata"]["uid"]}
            state = state or "Nonterminal"
        else:
            objects = sampling_kube.get("pods", selector=LABEL + "=" + registration["registration_id"]) or {"items": []}
            candidates = objects.get("items", [])
            require(len(candidates) <= 1, "multiple_execution_pods")
            if not candidates:
                return None
            obj = candidates[0]
            expected = pod_template(registration["workload"], {}, baseline=baseline)
            assert_pod_profile(obj.get("spec", {}), expected["spec"])
            assert_pod_metadata(obj.get("metadata", {}), expected["metadata"])
            meta = obj.get("metadata", {})
            require(meta.get("namespace") == registration["namespace"]
                    and meta.get("labels", {}).get(LABEL) == registration["registration_id"], "pod_sample_identity_missing")
            owners = [owner.get("uid") for owner in meta.get("ownerReferences", []) if owner.get("controller") is True]
            require(len(owners) == 1 and owners[0] and meta.get("uid") and meta.get("name"), "pod_sample_identity_missing")
            state = obj.get("status", {}).get("phase")
            require(state in {None, "", "Pending", "Running", "Unknown", "Succeeded", "Failed"}, "invalid_pod_phase")
            if state in (None, ""):
                return None
            terminal = state in {"Succeeded", "Failed"}
            facts = {"pod_uid": meta["uid"], "parent_uid": owners[0]}
        fingerprint = tuple(sorted(facts.items()))
        require(identity is None or identity == fingerprint, "sampled_execution_identity_changed")
        identity = fingerprint
        return {"terminal": terminal, "state": state, "facts": facts, "object": obj}

    try:
        # Establish/warm dedicated channels before starting the submission clock.
        sampling_kube = kube.fork()
        if api:
            sampling_api = api.fork()
            result.update(idempotency_key=name, submission_uncertain=True)
        else:
            manifest = (job_manifest(registration, name, index) if baseline == "direct-job" else {
                "apiVersion": EXT, "kind": "SandboxClaim", "metadata": {
                    "name": name, "namespace": registration["namespace"], "labels": labels(registration, index)},
                "spec": {"warmPoolRef": {"name": "ae-" + registration["registration_id"][:16]}}})
            result["resources"].append(reference(manifest))
            result["submission_uncertain"] = True
        save()  # Durable exact submission intent precedes the timed request.
        start = time.monotonic()
        observer = OutcomeObserver(read_outcome, start, registration["poll_seconds"], Failure).launch()
        if api:
            record = None
            for _ in range(2):
                request_start = time.monotonic()
                try:
                    record = api.call("/v1/runs", request_body(registration), key=result["idempotency_key"])
                    response_end = time.monotonic()
                    break
                except Failure as error:
                    if str(error) not in {"api_transport_outcome_unknown", "api_http_500", "api_http_502", "api_http_503",
                                          "api_http_504", "invalid_api_json", "api_response_too_large"}:
                        raise
            require(record is not None, "api_submission_outcome_unknown")
            run_id = record.get("id", "")
            require(isinstance(run_id, str) and re.fullmatch(r"[a-f0-9]{32}", run_id), "invalid_api_run_id")
            result.update(run_id=run_id, submission_uncertain=False)
            assert_record_spec(record, registration)
            require(record.get("state") in RUN_STATES, "invalid_api_run_state")
            if record.get("state") in TERMINAL:
                observer.accept({"terminal": True, "state": record["state"], "facts": {"run_id": run_id},
                                 "object": record}, request_start, response_end)
            target["run_id"] = run_id
            name = "agentos-" + run_id
        else:
            request_start = time.monotonic()
            parent = kube.create(manifest)
            response_end = time.monotonic()
            check_owned(parent, registration, result["resources"][0])
            result["resources"][0] = reference(parent)
            result["submission_uncertain"] = False
            if baseline == "direct-job":
                assert_job_profile(parent, registration)
                state = job_terminal_state(parent)
                if state:
                    observer.accept({"terminal": True, "state": state,
                        "facts": {"resource_uid": parent["metadata"]["uid"]}, "object": parent}, request_start, response_end)
        save()
        while time.monotonic() - start < registration["trial_timeout_seconds"]:
            latest, terminal, failure, _ = observer.snapshot()
            require(failure is None, failure or "outcome_observer_failed")
            pod = None
            pod_receipt = None
            if api:
                if parent is None:
                    parent = kube.get("job", name)
                    if parent is not None:
                        result["resources"] = [tag_api_job(kube, registration, parent)]
                if parent is not None:
                    current = kube.get("job", name)
                    if current is not None:
                        check_owned(current, registration, result["resources"][0])
                        assert_job_profile(current, registration)
                    else:
                        # Cleanup can race this independent sampler's next tick.
                        # A previously verified Job's absence supplies no API
                        # outcome; keep waiting for the authoritative response.
                        result["execution_disappearance_observed"] = True
                    pods = kube.get("pods", selector="agentos.dev/run-id=" + result["run_id"]) or {"items": []}
                    pod_receipt = timing_receipt(start)
                    candidates = [item for item in pods.get("items", []) if owned_by(item, parent)]
                    require(len(candidates) <= 1, "multiple_execution_pods")
                    pod = candidates[0] if candidates else None
                try:
                    logs = api.call("/v1/runs/" + result["run_id"] + "/logs").get("logs", "")
                    require(isinstance(logs, str), "invalid_api_logs")
                except Failure as error:
                    if str(error) not in {"api_http_404", "api_http_503"}:
                        raise
                    logs = ""
                pod_parent = parent
            elif baseline == "direct-job":
                if latest:
                    check_owned(latest["object"], registration, result["resources"][0])
                pods = kube.get("pods", selector=LABEL + "=" + registration["registration_id"]) or {"items": []}
                pod_receipt = timing_receipt(start)
                candidates = [item for item in pods.get("items", []) if owned_by(item, parent)]
                require(len(candidates) <= 1, "multiple_execution_pods")
                pod, pod_parent = candidates[0] if candidates else None, parent
                logs = ""
            else:
                claim = kube.get("sandboxclaims.extensions.agents.x-k8s.io", name)
                require(claim is not None, "execution_disappeared")
                check_owned(claim, registration, result["resources"][0])
                sandbox_name = claim.get("status", {}).get("sandbox", {}).get("name")
                pod_parent = kube.get("sandboxes.agents.x-k8s.io", sandbox_name) if sandbox_name else None
                if pod_parent:
                    require(owned_by(pod_parent, parent), "sandbox_owner_changed")
                    require(pod_parent.get("metadata", {}).get("labels", {}).get("agents.x-k8s.io/launch-type") != "warm",
                            "cold_baseline_received_warm_sandbox")
                    remember_descendant(result, pod_parent, parent)
                    pod = latest["object"] if latest else None
                logs = ""
            if pod is not None:
                expected = pod_template(registration["workload"], {}, baseline=baseline)
                assert_pod_profile(pod.get("spec", {}), expected["spec"])
                assert_pod_metadata(pod.get("metadata", {}), expected["metadata"])
                remember_descendant(result, pod, pod_parent)
                observe_trial_pod(result, pod, pod_parent, pod_receipt or {
                    "observed_at": latest["observed_at"], "elapsed_seconds": latest["elapsed_seconds"]})
                if not api:
                    try:
                        logs = kube.logs(pod["metadata"]["name"])
                    except Failure as error:
                        if terminal is not None or str(error) != "kubectl_request_failed":
                            raise
            if validate_logs(logs, registration["workload"]) and "readiness_seconds" not in result:
                result["readiness_seconds"] = time.monotonic() - start
            if terminal is not None:
                receipt = {key: terminal[key] for key in ("observed_at", "elapsed_seconds", "observation_interval")}
                observe_trial_event(result, "outcome_visible_to_caller", receipt, state=terminal["state"], **terminal["facts"])
                if baseline == "agent-sandbox":
                    require(pod is not None and pod_parent is not None, "sampled_pod_ancestry_unverified")
                    require(pod["metadata"]["uid"] == terminal["facts"]["pod_uid"], "execution_pod_identity_changed")
                    # The same verified response supplies both native boundaries.
                    result["timing"]["events"]["pod_terminal"].update(receipt)
                successful = terminal["state"] == {"agentos": "succeeded", "direct-job": "Complete", "agent-sandbox": "Succeeded"}[baseline]
                require(successful, "execution_failed")
                require(parent is not None, "api_execution_not_found_in_selected_context")
                validate_logs(logs, registration["workload"], complete=True)
                result.update(correct=True, stdout_sha256=hashlib.sha256(logs.encode()).hexdigest())
                break
            save()
            # Control work can be slow; it never determines the sampling grid.
            observer.changed.wait(.5)
            observer.changed.clear()
        else:
            raise Failure("trial_observer_timeout")
    except KeyboardInterrupt:
        result["error"] = "interrupted"
        raise
    except Failure as error:
        result["error"] = str(error)
    except Exception:
        result["error"] = "unexpected_observer_failure"
    finally:
        result["elapsed_seconds"] = max(0.0, time.monotonic() - start) if start is not None else 0.0
        if observer:
            try:
                observer.close()
            except Failure as error:
                result.update(correct=False, error=str(error))
            _, terminal, failure, stats = observer.snapshot()
            result["timing"]["sampling"] = stats
            if terminal is not None and result["timing"]["events"]["outcome_visible_to_caller"]["status"] == "missing":
                observe_trial_event(result, "outcome_visible_to_caller", {
                    key: terminal[key] for key in ("observed_at", "elapsed_seconds", "observation_interval")},
                    state=terminal["state"], **terminal["facts"])
            if failure:
                result.update(correct=False, error=failure)
        if result["correct"]:
            observe_trial_event(result, "validated_completion", {"observed_at": now(),
                "elapsed_seconds": result["elapsed_seconds"]}, state="succeeded")
        finish_trial_timing(result)
        for channel in (sampling_api, sampling_kube):
            if channel:
                channel.close()
        result["status"] = "finished"
        save()
        try:
            if result.get("run_id"):
                cancel_api(api, result["run_id"], registration)
            for ref in reversed(result["resources"]):
                delete_owned(kube, registration, ref)
            descendants_absent(kube, registration, result["descendants"])
            result["cleanup"] = ("submission_outcome_unknown" if result.get("submission_uncertain") else
                                 "api_execution_context_unverified" if result.get("run_id") and parent is None else
                                 "observed_objects_absent")
        except Failure as error:
            result["cleanup"] = str(error)
        save()


def trial(kube, registration, baseline, index, result, save, api=None):
    implementation = measured_trial if registration["protocol"]["format_version"] == 3 else legacy_trial
    return implementation(kube, registration, baseline, index, result, save, api)


def register(args):
    protocol = read_json(args.protocol, 64 * 1024)
    validate_protocol(protocol)
    experiment = protocol.get("experiment_id", "")
    require(isinstance(experiment, str) and NAME.fullmatch(experiment), "experiment_id")
    workload = validate_workload(read_json(args.workload, 32 * 1024))
    require("deadline_scope" in workload, "new_registration_requires_explicit_deadline_scope")
    if "agent-sandbox" in {protocol["baseline"], protocol["candidate"]}:
        require(workload["deadline_scope"] == "pod", "upstream_comparison_requires_pod_deadline")
    environment = read_json(args.environment, 64 * 1024)
    require(isinstance(args.context, str) and 0 < len(args.context) <= 256 and not args.context.startswith("-"), "explicit_context_required")
    require(NAME.fullmatch(args.namespace), "namespace_name")
    trials = args.trials if args.trials is not None else protocol.get("minimum_trials", 30)
    minimum = protocol.get("minimum_trials", 30)
    require(type(minimum) is int and type(trials) is int and 1 <= minimum <= trials <= 200, "trial_count_bounds")
    require(not args.upstream_controller_ref or DIGEST.fullmatch(args.upstream_controller_ref), "upstream_controller_must_be_digest_pinned")
    require("agent-sandbox" not in {protocol["baseline"], protocol["candidate"]} or args.upstream_controller_ref,
            "upstream_controller_ref_required")
    measured = protocol["format_version"] == 3
    period = protocol["observation_policy"]["period_seconds"] if measured else .5
    poll_seconds = args.poll_seconds if args.poll_seconds is not None else period
    require(type(poll_seconds) in (int, float) and math.isfinite(poll_seconds)
            and (.01 if measured else .1) <= poll_seconds <= 10, "poll_bounds")
    require(not measured or poll_seconds == period, "poll_must_match_observation_policy")
    require(workload["timeout_seconds"] <= args.trial_timeout_seconds <= 3900, "observer_timeout_bounds")
    directory = Path(args.evidence_dir)
    directory.mkdir(mode=0o700)
    private_directory(directory)
    registration = {"format_version": 1, "experiment_id": experiment, "registration_id": uuid.uuid4().hex,
                    "registered_at": now(), "context": args.context, "namespace": args.namespace,
                    "protocol": protocol, "protocol_sha256": digest(protocol), "workload": workload,
                    "workload_sha256": digest(workload), "environment": {
                        "reference": str(Path(args.environment).resolve()), "sha256": digest(environment)},
                    "trial_count": trials, "poll_seconds": poll_seconds,
                    "trial_timeout_seconds": args.trial_timeout_seconds, "cache_state": args.cache_state,
                    "upstream_controller_ref": args.upstream_controller_ref}
    write_json(directory / "registration.json", registration, exclusive=True)
    os.chmod(directory / "registration.json", 0o400)
    return directory


def load_registration(args):
    directory = Path(args.evidence_dir)
    private_directory(directory)
    path = directory / "registration.json"
    require(not path.is_symlink() and path.stat().st_mode & 0o077 == 0, "registration_must_be_private")
    registration = read_json(path)
    require(registration.get("format_version") == 1 and registration.get("context") == args.context, "registered_context_required")
    require(digest(registration["protocol"]) == registration["protocol_sha256"]
            and digest(registration["workload"]) == registration["workload_sha256"], "registration_digest_mismatch")
    validate_protocol(registration["protocol"])
    validate_workload(registration["workload"])
    if registration["protocol"]["format_version"] == 3:
        require(registration.get("poll_seconds") == registration["protocol"]["observation_policy"]["period_seconds"],
                "poll_must_match_observation_policy")
    return directory, registration


def collect(args):
    directory, registration = load_registration(args)
    require(args.execute, "collection_requires_execute")
    require(args.baseline in {registration["protocol"]["baseline"], registration["protocol"]["candidate"], "direct-job"}, "baseline_not_registered")
    lock = directory / ".collect.lock"
    fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    path = directory / (args.baseline + ".json")
    result = {"format_version": 1, "experiment_id": registration["experiment_id"], "baseline": args.baseline,
              "protocol_sha256": registration["protocol_sha256"], "registration_sha256": digest(registration),
              "workload_sha256": registration["workload_sha256"], "environment": registration["environment"],
              "environment_sha256": registration["environment"]["sha256"],
              "started_at": now(), "status": "running", "trials": [], "resources": [],
              "completed": False, "requested_trials": registration["trial_count"],
              "qualification": {"status": "not_verified", "evidence_ref": "separate dynamic conformance required"},
              "billing_complete": False, "node_reduction_demonstrated": False,
              "cache_state": registration["cache_state"], "poll_seconds": registration["poll_seconds"],
              "deadline_contract": deadline_contract(registration["workload"], args.baseline),
              "timing_contract": timing_contract(args.baseline),
              "timing_boundary": "client submission start to observed terminal success and validated stdout",
              "readiness_boundary": "client submission start to first observed workload readiness log line"}
    measured = registration["protocol"]["format_version"] == 3
    kube = api = None
    if registration["protocol"]["format_version"] >= 2:
        result["latency_policy"] = copy.deepcopy(registration["protocol"]["latency_policy"])
    if measured:
        result["observation_policy"] = copy.deepcopy(registration["protocol"]["observation_policy"])
        result["measurement_transport"] = "private_unix_kubectl_proxy_persistent_http_channels"
    if args.baseline == "agent-sandbox":
        result.update({"baseline_provider": "aws-blueprint-compatible-upstream", "pool_replicas": 0,
                       "upstream_api_version": EXT, "upstream_contract_ref": "kubernetes-sigs/agent-sandbox@v1.0.1",
                       "upstream_controller_ref": registration["upstream_controller_ref"],
                       "upstream_version_verification": "operator_declaration_only"})
    try:
        write_json(path, result, exclusive=True)
        save = lambda: write_json(path, result)
        api = (PersistentAgentOS if measured else AgentOS)(args.api_url, args.token_env) if args.baseline == "agentos" else None
        if api:
            result["api_endpoint_sha256"] = digest(api.base)
            save()
        kube = (transport.KubernetesProxy(registration, args.token_env, failure_type=Failure).start()
                if measured else Kubectl(registration, args.token_env))
        preflight(kube, registration, api)
        if args.baseline == "agent-sandbox":
            require(registration["upstream_controller_ref"], "upstream_controller_ref_required")
            for manifest in upstream_manifests(registration):
                ref = reference(manifest)
                result["resources"].append(ref)
                save()
                obj = kube.create(manifest)
                check_owned(obj, registration, ref)
                if manifest["kind"] == "SandboxTemplate":
                    assert_pod_profile(obj["spec"]["podTemplate"]["spec"], manifest["spec"]["podTemplate"]["spec"])
                    assert_pod_metadata(obj["spec"]["podTemplate"]["metadata"], manifest["spec"]["podTemplate"]["metadata"])
                    require(not obj["spec"]["podTemplate"]["spec"].get("nodeName"), "upstream_node_pin_changed")
                    require(obj["spec"].get("networkPolicyManagement") == "Unmanaged" and obj["spec"].get("service") is False,
                            "upstream_template_changed")
                else:
                    require(obj["spec"].get("replicas") == 0 and obj["spec"].get("sandboxTemplateRef") == manifest["spec"]["sandboxTemplateRef"],
                            "upstream_pool_changed")
                result["resources"][-1] = reference(obj)
                save()
        for index in range(registration["trial_count"]):
            item = {"index": index}
            result["trials"].append(item)
            trial(kube, registration, args.baseline, index, item, save, api)
            if item.get("cleanup") != "observed_objects_absent" or item.get("error") in {
                    "creation_outcome_unknown", "api_submission_outcome_unknown", "invalid_api_run_id"}:
                raise Failure("collection_stopped_with_unresolved_execution")
        result["status"] = "finished"
        result["completed"] = True
    except (Failure, KeyboardInterrupt) as error:
        result["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
        result["error"] = "interrupted" if isinstance(error, KeyboardInterrupt) else str(error)
    except Exception:
        result["status"] = "failed"
        result["error"] = "unexpected_collection_failure"
    finally:
        if measured:
            for channel in (api, kube):
                if channel:
                    channel.close()
        result["finished_at"] = now()
        # Never overwrite an earlier arm, including its failed trials.
        if path.exists() and result.get("started_at") == read_json(path).get("started_at"):
            write_json(path, result)
        lock.unlink()
    require(result["status"] == "finished" and all(x["correct"] for x in result["trials"]), "collection_contains_failures")
    return path


def cleanup_contents(args, directory, registration, kube):
    unresolved = False
    for baseline in BASELINES:
        path = directory / (baseline + ".json")
        if not path.exists():
            continue
        result = read_json(path)
        require(result.get("registration_sha256") == digest(registration), "result_registration_mismatch")
        api = AgentOS(args.api_url, args.token_env) if baseline == "agentos" else None
        if api:
            require(result.get("api_endpoint_sha256") == digest(api.base), "registered_api_endpoint_required")
        for item in result["trials"]:
            if item.get("run_id"):
                cancel_api(api, item["run_id"], registration)
                # A late Job may not have existed when the trial first polled.
                job = kube.get("job", "agentos-" + item["run_id"])
                if job:
                    item["resources"] = [tag_api_job(kube, registration, job)]
            for ref in reversed(item.get("resources", [])):
                delete_owned(kube, registration, ref)
            descendants_absent(kube, registration, item.get("descendants", []))
            item["cleanup"] = "submission_outcome_unknown" if item.get("submission_uncertain") else "observed_objects_absent"
            unresolved = unresolved or item.get("submission_uncertain", False)
        for ref in reversed(result["resources"]):
            delete_owned(kube, registration, ref)
        result["cleanup_at"] = now()
        write_json(path, result)
    require(not unresolved, "cleanup_retains_uncertain_submission")
    return directory


def cleanup(args):
    directory, registration = load_registration(args)
    require(args.execute, "cleanup_requires_execute")
    require(not (directory / ".collect.lock").exists(), "collection_lock_present")
    measured = registration["protocol"]["format_version"] == 3
    kube = (transport.KubernetesProxy(registration, args.token_env, failure_type=Failure).start()
            if measured else Kubectl(registration, args.token_env))
    try:
        return cleanup_contents(args, directory, registration, kube)
    finally:
        if measured:
            kube.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    registration = commands.add_parser("register", help="offline immutable registration before collection")
    registration.add_argument("--protocol", required=True)
    registration.add_argument("--workload", required=True)
    registration.add_argument("--environment", required=True, help="private JSON record; only its path/hash are copied")
    registration.add_argument("--context", required=True)
    registration.add_argument("--namespace", required=True)
    registration.add_argument("--evidence-dir", required=True, help="new directory, never a previous experiment")
    registration.add_argument("--trials", type=int)
    registration.add_argument("--poll-seconds", type=float, default=None,
                              help="legacy default 0.5; v3 derives from and must match its observation policy")
    registration.add_argument("--trial-timeout-seconds", type=int, default=360)
    registration.add_argument("--cache-state", choices=("uncontrolled", "operator-declared-cached", "operator-declared-cold"), default="uncontrolled")
    registration.add_argument("--upstream-controller-ref", default="", help="digest-pinned installed controller image; declaration only")
    for name in ("collect", "cleanup", "render"):
        sub = commands.add_parser(name)
        sub.add_argument("--context", required=True)
        sub.add_argument("--evidence-dir", required=True)
        if name != "cleanup":
            sub.add_argument("--baseline", required=True, choices=BASELINES)
        if name != "render":
            sub.add_argument("--execute", action="store_true", help="explicitly permit workload/resource mutations")
            sub.add_argument("--api-url", default="http://127.0.0.1:8080")
            sub.add_argument("--token-env", default="AGENTOS_EVAL_TOKEN")
    args = parser.parse_args(argv)
    try:
        if args.command == "render":
            _, reg = load_registration(args)
            require(args.baseline != "agentos", "agentos_render_uses_authenticated_v1_plan")
            manifests = upstream_manifests(reg) if args.baseline == "agent-sandbox" else [job_manifest(reg, "preview-evaluation", 0)]
            print(json.dumps({"apiVersion": "v1", "kind": "List", "items": manifests}, indent=2))
            return 0
        output = {"register": register, "collect": collect, "cleanup": cleanup}[args.command](args)
        print(str(output))
        return 0
    except Exception as error:
        # Never print arbitrary exception messages from HTTP, files or kubeconfig.
        print("evaluation: " + (str(error) if isinstance(error, Failure) else "local_operation_failed"), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
