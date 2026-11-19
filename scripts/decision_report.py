#!/usr/bin/env python3
"""Evaluate preregistered experiment gates; never infer isolation from YAML."""

import argparse
import importlib.util
import json
import math
import os
from pathlib import Path
import statistics
import sys
from types import SimpleNamespace

_COLLECTOR_SPEC = importlib.util.spec_from_file_location(
    "agentos_evaluation_collector", Path(__file__).with_name("evaluate.py"))
collector = importlib.util.module_from_spec(_COLLECTOR_SPEC)
_COLLECTOR_SPEC.loader.exec_module(collector)

OPERATOR_BINDINGS = ("experiment_id", "baseline", "protocol_sha256", "registration_sha256",
                     "workload_sha256", "environment_sha256")


def load_json(path):
    data = Path(path).read_bytes()
    if len(data) > 8 * 1024 * 1024:
        raise ValueError("evidence file exceeds 8 MiB")
    return json.loads(data, parse_constant=lambda _: (_ for _ in ()).throw(
        ValueError("non-finite JSON number")))


def number(value, name, minimum=0, maximum=None):
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{name} must be numeric")
    if not math.isfinite(value) or value < minimum or (maximum is not None and value > maximum):
        raise ValueError(f"{name} is outside its supported range")
    return value


def validate_protocol(protocol):
    fields = {"format_version", "experiment_id", "baseline", "candidate", "minimum_trials",
              "minimum_correct_fraction", "max_p95_latency_ratio", "min_cost_improvement_fraction",
              "min_operator_improvement_fraction"}
    if set(protocol) != fields or protocol["format_version"] != 1:
        raise ValueError("unsupported decision protocol fields or version")
    for key in ("experiment_id", "baseline", "candidate"):
        if not isinstance(protocol[key], str) or not protocol[key] or len(protocol[key]) > 128:
            raise ValueError(f"invalid {key}")
    if protocol["baseline"] == protocol["candidate"]:
        raise ValueError("baseline and candidate must differ")
    trials = number(protocol["minimum_trials"], "minimum_trials", 2, 100000)
    if not isinstance(trials, int):
        raise ValueError("minimum_trials must be an integer")
    number(protocol["minimum_correct_fraction"], "minimum_correct_fraction", 0.01, 1)
    number(protocol["max_p95_latency_ratio"], "max_p95_latency_ratio", 1, 10)
    for key in ("min_cost_improvement_fraction", "min_operator_improvement_fraction"):
        number(protocol[key], key, 0.001, 0.99)


def registration_bindings(registration, protocol, digest):
    """Validate the actual registration, rather than trusting report hash labels."""
    if not isinstance(registration, dict) or registration.get("format_version") != 1:
        raise ValueError("unsupported registration")
    try:
        collector.validate_protocol(registration["protocol"])
        collector.validate_workload(registration["workload"])
    except collector.Failure:
        raise ValueError("invalid registered protocol or workload") from None
    if (registration["protocol"] != protocol or collector.digest(protocol) != digest
            or registration.get("protocol_sha256") != digest
            or registration.get("experiment_id") != protocol["experiment_id"]
            or registration.get("workload_sha256") != collector.digest(registration["workload"])):
        raise ValueError("registration protocol, experiment or workload drift")
    context = registration.get("context")
    if (not isinstance(context, str) or not 0 < len(context) <= 256 or context.startswith("-")
            or not isinstance(registration.get("namespace"), str)
            or not collector.NAME.fullmatch(registration["namespace"])):
        raise ValueError("registration requires its explicit context and namespace")
    count = registration.get("trial_count")
    if type(count) is not int or not protocol["minimum_trials"] <= count <= 200:
        raise ValueError("invalid registered trial count")
    environment = registration.get("environment")
    if (not isinstance(environment, dict) or not isinstance(environment.get("reference"), str)
            or not environment["reference"] or not isinstance(environment.get("sha256"), str)
            or len(environment["sha256"]) != 64
            or any(c not in "0123456789abcdef" for c in environment["sha256"])):
        raise ValueError("registration requires referenced environment evidence")
    return {"experiment_id": registration["experiment_id"], "protocol_sha256": digest,
            "registration_sha256": collector.digest(registration),
            "workload_sha256": registration["workload_sha256"],
            "environment_sha256": environment["sha256"], "requested_trials": count}


def load_registration(path):
    """Reuse the collector's offline private-file and workload validation."""
    path = Path(path)
    if path.name != "registration.json" or not path.is_file():
        raise ValueError("registration must be the collector's registration.json")
    try:
        initial = collector.read_json(path)
        _, registration = collector.load_registration(SimpleNamespace(
            evidence_dir=path.parent, context=initial.get("context")))
    except collector.Failure:
        raise ValueError("invalid or nonprivate registration") from None
    return registration


def summarize(report, protocol, digest, label):
    if report.get("format_version") != 1 or report.get("experiment_id") != protocol["experiment_id"]:
        raise ValueError("report experiment/version does not match protocol")
    if report.get("baseline") != label or report.get("protocol_sha256") != digest:
        raise ValueError("report arm or protocol digest does not match registration")
    for key in ("registration_sha256", "workload_sha256", "environment_sha256"):
        value = report.get(key)
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"report requires registered {key}")
    trials = report.get("trials")
    if not isinstance(trials, list) or not trials:
        raise ValueError("report requires completed or failed trials")
    requested = report.get("requested_trials")
    if type(requested) is not int or requested < 1 or requested != len(trials):
        raise ValueError("report must contain exactly its registered requested_trials")
    indexes = [trial.get("index") if isinstance(trial, dict) else None for trial in trials]
    if any(type(index) is not int for index in indexes) or sorted(indexes) != list(range(requested)):
        raise ValueError("trial indexes must be unique and contiguous from zero")
    times, successes = [], 0
    for trial in trials:
        if not isinstance(trial, dict) or type(trial.get("correct")) is not bool:
            raise ValueError("each trial needs an explicit correctness outcome")
        elapsed = number(trial.get("elapsed_seconds"), "elapsed_seconds")
        successes += int(trial["correct"])
        # Timeouts and failed work count in completion rate, never as fast successful work.
        if trial["correct"]:
            if elapsed <= 0:
                raise ValueError("successful trial elapsed_seconds must be positive")
            times.append(elapsed)
    qualification = report.get("qualification", {})
    status = qualification.get("status", "not_verified")
    if status not in ("passed", "failed", "not_verified"):
        raise ValueError("unsupported qualification status")
    if status in ("passed", "failed") and not qualification.get("evidence_ref"):
        raise ValueError("qualification result needs an evidence reference")
    p95 = sorted(times)[math.ceil(0.95 * len(times)) - 1] if times else None
    return {"trials": len(trials), "correct": successes, "correct_fraction": successes / len(trials),
            "successful_p95_seconds": p95, "qualification": status}


def apply_supplemental(metadata, baseline, candidate):
    """Attach referenced operator/billing evidence without rewriting raw trials."""
    if metadata.get("format_version") != 1:
        raise ValueError("unsupported supplemental evidence version")
    allowed = {"qualification", "full_cost_usd", "billing_complete", "node_reduction_demonstrated", "cost_accounting_ref"}
    for report in (baseline, candidate):
        keys = ("experiment_id", "protocol_sha256", "registration_sha256", "workload_sha256", "environment_sha256")
        if any(metadata.get(k) != report.get(k) for k in keys):
            raise ValueError("supplemental evidence belongs to a different comparison")
        arm = metadata.get("arms", {}).get(report["baseline"], {})
        if not isinstance(arm, dict) or not set(arm) <= allowed:
            raise ValueError("supplemental evidence cannot replace trial measurements")
        report.update(arm)


def validate_operations(report, effort):
    if not isinstance(effort, dict) or any(effort.get(key) != report.get(key) for key in OPERATOR_BINDINGS):
        raise ValueError("operator record does not match registration, arm, workload and environment")
    task = effort.get("task_ref")
    if not isinstance(task, str) or not task.strip() or len(task) > 128:
        raise ValueError("operator record requires a shared concrete task reference")
    if effort.get("phase") not in ("install", "upgrade", "failure-recovery"):
        raise ValueError("unsupported operator task phase")


def attach_operations(report, operations):
    if operations.get("format_version") != 1:
        raise ValueError("unsupported operator summary version")
    validate_operations(report, operations)
    effort = operations.get("operator_effort")
    validate_operations(report, effort)
    if any(effort.get(key) != operations.get(key) for key in ("phase", "task_ref")):
        raise ValueError("operator summary task bindings disagree")
    report["operator_effort"] = dict(effort)


def decide(protocol, digest, baseline, candidate, registration):
    validate_protocol(protocol)
    bindings = registration_bindings(registration, protocol, digest)
    for report in (baseline, candidate):
        if any(report.get(key) != value for key, value in bindings.items()):
            raise ValueError("report differs from actual registered inputs or trial count")
        if "environment" in report and report["environment"] != registration["environment"]:
            raise ValueError("report environment reference differs from registration")
    b = summarize(baseline, protocol, digest, protocol["baseline"])
    c = summarize(candidate, protocol, digest, protocol["candidate"])
    if any(baseline[k] != candidate[k] for k in ("registration_sha256", "workload_sha256", "environment_sha256", "requested_trials")):
        raise ValueError("different registrations/workload/environment: publish exploratory results separately")
    result = {"format_version": 1, "experiment_id": protocol["experiment_id"],
              "protocol_sha256": digest, "baseline": b, "candidate": c,
              **{key: baseline[key] for key in ("registration_sha256", "workload_sha256", "environment_sha256", "requested_trials")},
              "decision": "insufficient_evidence", "reasons": [], "routes": {}}
    reasons = result["reasons"]
    if "failed" in (b["qualification"], c["qualification"]):
        result["decision"] = "stop_experiment"
        reasons.append("A documented qualification check failed; repair the boundary before continuing.")
        return result
    if "not_verified" in (b["qualification"], c["qualification"]):
        reasons.append("Live qualification evidence is required for both arms; declarations are insufficient.")
    if any(r.get("completed") is not True for r in (baseline, candidate)):
        reasons.append("Both preregistered trial sets must finish, including failed outcomes.")
    if min(b["trials"], c["trials"]) < protocol["minimum_trials"]:
        reasons.append("The preregistered minimum trial count has not been reached.")
    if reasons:
        return result
    if b["correct_fraction"] < protocol["minimum_correct_fraction"]:
        reasons.append("The reference workload is not qualified at the required correctness rate.")
        return result
    if c["correct_fraction"] < max(b["correct_fraction"], protocol["minimum_correct_fraction"]):
        result["decision"] = "change_direction"
        reasons.append("The candidate fails the correctness gate; optimize only after preserving useful work.")
        return result
    if not b["successful_p95_seconds"] or c["successful_p95_seconds"] is None:
        reasons.append("Positive successful-task timing is required in both arms.")
        return result
    ratio = c["successful_p95_seconds"] / b["successful_p95_seconds"]
    result["successful_p95_latency_ratio"] = ratio
    if ratio > protocol["max_p95_latency_ratio"]:
        result["decision"] = "change_direction"
        reasons.append("The candidate exceeds the preregistered latency regression allowance.")
        return result
    # Actual billing and demonstrated node reduction are deliberately required.
    # Fixed-pool capacity estimates alone cannot qualify the billing-savings route.
    billing_ready = (all(r.get("billing_complete") is True and r.get("cost_accounting_ref") for r in (baseline, candidate))
                     and candidate.get("node_reduction_demonstrated") is True)
    if billing_ready:
        bc = number(baseline.get("full_cost_usd"), "baseline full_cost_usd") / b["correct"]
        cc = number(candidate.get("full_cost_usd"), "candidate full_cost_usd") / c["correct"]
        if bc > 0:
            improvement = 1 - cc / bc
            result["routes"]["billing_cost"] = {"eligible": True, "improvement_fraction": improvement,
                "passes": improvement >= protocol["min_cost_improvement_fraction"]}
    if "billing_cost" not in result["routes"]:
        result["routes"]["billing_cost"] = {"eligible": False,
            "reason": "Complete billing and demonstrated capacity reduction required; resource estimates do not qualify."}
    # Operator observations are explicitly human-reported and require repeated sessions.
    efforts = [r.get("operator_effort", {}) for r in (baseline, candidate)]
    for report, effort in zip((baseline, candidate), efforts):
        if effort:
            validate_operations(report, effort)
    if (all(e.get("completed") is True and e.get("record_ref") and
            isinstance(e.get("session_minutes"), list) and len(e["session_minutes"]) >= 3 for e in efforts)
            and efforts[0].get("phase") in ("install", "upgrade", "failure-recovery")
            and efforts[0]["phase"] == efforts[1].get("phase")
            and efforts[0].get("task_ref") == efforts[1].get("task_ref")):
        values = [[number(v, "operator session minutes", 0.001) for v in e["session_minutes"]] for e in efforts]
        improvement = 1 - statistics.median(values[1]) / statistics.median(values[0])
        result["routes"]["operator_effort"] = {"eligible": True, "source": "human_reported",
            "improvement_fraction": improvement,
            "passes": improvement >= protocol["min_operator_improvement_fraction"]}
    else:
        result["routes"]["operator_effort"] = {"eligible": False,
            "reason": "Three completed operator sessions per arm and a task/privilege record are required."}
    routes = result["routes"].values()
    if any(r.get("passes") for r in routes):
        result["decision"] = "continue_validation"
        reasons.append("A preregistered value route passed; validate with a paying design partner.")
    elif all(r["eligible"] for r in routes):
        result["decision"] = "change_direction"
        reasons.append("Neither measured value route met its preregistered threshold.")
    else:
        reasons.append("No value route has passed and at least one remains unmeasured.")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registration", required=True, help="private registration.json created before collection")
    parser.add_argument("--protocol", required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--supplemental", help="referenced live qualification/billing evidence; does not overwrite raw trials")
    parser.add_argument("--baseline-operations", help="optional operator-journal summary")
    parser.add_argument("--candidate-operations", help="optional operator-journal summary")
    parser.add_argument("--output", required=True, help="new private decision report; existing files are refused")
    args = parser.parse_args()
    try:
        protocol = load_json(args.protocol)
        digest = collector.digest(protocol)
        registration = load_registration(args.registration)
        baseline, candidate = load_json(args.baseline), load_json(args.candidate)
        if args.supplemental:
            supplemental = load_json(args.supplemental)
            apply_supplemental(supplemental, baseline, candidate)
        for report, path in ((baseline, args.baseline_operations), (candidate, args.candidate_operations)):
            if path:
                attach_operations(report, load_json(path))
        result = decide(protocol, digest, baseline, candidate, registration)
        encoded = json.dumps(result, indent=2, allow_nan=False) + "\n"
        fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as output:
            output.write(encoded)
        print(f"Decision: {result['decision']}; private report written.")
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(f"Decision report failed: {type(error).__name__}. Check inputs; no gate result emitted.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
