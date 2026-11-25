#!/usr/bin/env python3
"""Evaluate preregistered experiment gates; never infer isolation from YAML."""

import argparse
import datetime as dt
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
TIMING_BOUNDARIES = ("pod_terminal", "outcome_visible_to_caller", "validated_completion")


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
    if not isinstance(protocol, dict):
        raise ValueError("unsupported decision protocol object")
    fields = {"format_version", "experiment_id", "baseline", "candidate", "minimum_trials",
              "minimum_correct_fraction", "max_p95_latency_ratio", "min_cost_improvement_fraction",
              "min_operator_improvement_fraction"}
    version = protocol.get("format_version")
    extra = {"latency_policy"} if version in (2, 3) else set()
    if version == 3:
        extra.add("observation_policy")
    if type(version) is not int or version not in (1, 2, 3) or set(protocol) != fields | extra:
        raise ValueError("unsupported decision protocol fields or version")
    if version in (2, 3):
        policy = protocol["latency_policy"]
        if not isinstance(policy, dict) or set(policy) != {"boundary", "mode", "absolute_allowance_seconds", "rationale"}:
            raise ValueError("unsupported latency policy fields")
        if policy["boundary"] not in TIMING_BOUNDARIES or policy["mode"] != "ratio_plus_absolute":
            raise ValueError("unsupported latency policy boundary or mode")
        number(policy["absolute_allowance_seconds"], "absolute_allowance_seconds", 0, 3600)
        rationale = policy["rationale"]
        if not isinstance(rationale, str) or not rationale.strip() or len(rationale.encode()) > 1024:
            raise ValueError("latency policy requires a bounded rationale")
    if version == 3:
        if protocol["latency_policy"]["boundary"] != "outcome_visible_to_caller":
            raise ValueError("format 3 requires the independently sampled caller-outcome boundary")
        observation = protocol["observation_policy"]
        if not isinstance(observation, dict) or set(observation) != {"mode", "period_seconds", "max_terminal_gap_seconds"}:
            raise ValueError("unsupported observation policy fields")
        if observation["mode"] != "fixed_cadence":
            raise ValueError("unsupported observation policy mode")
        period = number(observation["period_seconds"], "observation period_seconds", 0.01, 5)
        number(observation["max_terminal_gap_seconds"], "max_terminal_gap_seconds", period, 3600)
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
    if protocol["format_version"] == 3:
        registered_period = number(registration.get("poll_seconds"), "registered polling period", .01, 5)
        if registered_period != protocol["observation_policy"]["period_seconds"]:
            raise ValueError("registration polling differs from its observation policy")
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


def validate_timing_identity(trial, timing, event, source, contract):
    """Cross-check recorded identities; these consistency checks are not attestation."""
    binding = timing.get("execution")
    if binding is not None and (not isinstance(binding, dict) or any(
            not isinstance(binding.get(key), str) or not binding[key]
            for key in ("pod_uid", "pod_name", "parent_uid"))):
        raise ValueError("invalid trial timing execution identity")
    references = []
    for key in ("resources", "descendants"):
        values = trial.get(key, [])
        if not isinstance(values, list) or any(not isinstance(value, dict) for value in values):
            raise ValueError("invalid trial resource identities")
        references.extend(values)
    pods = [ref for ref in references if ref.get("kind") == "Pod" and ref.get("uid")]
    parent_kind = ("Sandbox" if contract["events"]["outcome_visible_to_caller"]["source"] == "sandbox_pod_phase"
                   else "Job")
    parents = [ref["uid"] for ref in references if ref.get("kind") == parent_kind and ref.get("uid")]

    def check_pod(facts):
        if binding is not None and any(facts.get(key) != binding[key]
                for key in ("pod_uid", "parent_uid") if key in facts):
            raise ValueError("trial timing Pod identity differs from execution")
        if binding is not None and "pod_name" in facts and facts["pod_name"] != binding["pod_name"]:
            raise ValueError("trial timing Pod name differs from execution")
        if parents and facts.get("parent_uid") not in parents:
            raise ValueError("trial timing parent differs from recorded resources")
        if pods:
            matched = [ref for ref in pods if ref["uid"] == facts.get("pod_uid")]
            if not matched or any((ref.get("parent_uid") and ref["parent_uid"] != facts.get("parent_uid"))
                    or ("pod_name" in facts and ref.get("name") != facts["pod_name"]) for ref in matched):
                raise ValueError("trial timing Pod differs from recorded descendants")

    if binding is not None:
        check_pod(binding)
    identity_fields = {
        "agentos_run_api": ("run_id",),
        "kubernetes_job_condition": ("resource_uid",),
        "kubernetes_pod_phase": ("pod_uid", "pod_name", "parent_uid"),
        "sandbox_pod_phase": ("pod_uid", "parent_uid"),
        "collector_validated_output": (),
    }[source]
    if any(not isinstance(event.get(key), str) or not event[key] for key in identity_fields):
        raise ValueError("observed trial timing requires its native identity")
    if source == "agentos_run_api" and "run_id" in trial and event["run_id"] != trial["run_id"]:
        raise ValueError("trial timing API run identity differs from recorded run")
    if source == "kubernetes_job_condition":
        if parents and event["resource_uid"] not in parents:
            raise ValueError("trial timing Job identity differs from recorded resource")
        if binding is not None and event["resource_uid"] != binding["parent_uid"]:
            raise ValueError("trial timing Job identity differs from execution parent")
    if source in ("kubernetes_pod_phase", "sandbox_pod_phase"):
        check_pod(event)


def timing_seconds(trial, boundary, contract):
    """Read one declared observation; absence is missing evidence, never a zero."""
    timing = trial.get("timing")
    if timing is None:
        return None
    if (not isinstance(timing, dict) or type(timing.get("format_version")) is not int or timing.get("format_version") != 1
            or timing.get("clock_basis") != "harness_monotonic_offsets"):
        raise ValueError("unsupported trial timing contract")
    events = timing.get("events")
    if not isinstance(events, dict) or not set(events) <= set(TIMING_BOUNDARIES):
        raise ValueError("unsupported trial timing events")
    event = events.get(boundary)
    if event is None:
        return None
    if not isinstance(event, dict):
        raise ValueError("invalid trial timing event")
    if event.get("status") == "missing":
        if event.get("elapsed_seconds") is not None or event.get("observed_at") is not None:
            raise ValueError("missing trial timing event contains an observation")
        return None
    if event.get("status") != "observed":
        raise ValueError("unsupported trial timing event status")
    elapsed = number(event.get("elapsed_seconds"), "timing elapsed_seconds")
    if elapsed > trial["elapsed_seconds"]:
        raise ValueError("trial timing observation exceeds validated trial duration")
    expected_source = contract["events"][boundary]["source"]
    if event.get("source") != expected_source:
        raise ValueError("trial timing source differs from its arm contract")
    successful_state = {"kubernetes_pod_phase": "Succeeded", "sandbox_pod_phase": "Succeeded",
                        "agentos_run_api": "succeeded", "kubernetes_job_condition": "Complete",
                        "collector_validated_output": "succeeded"}[expected_source]
    if event.get("state") != successful_state:
        raise ValueError("correct trial timing contains a contradictory native outcome")
    if boundary == "validated_completion" and elapsed != trial["elapsed_seconds"]:
        raise ValueError("validated completion differs from legacy trial duration")
    validate_timing_identity(trial, timing, event, expected_source, contract)
    observed = event.get("observed_at")
    try:
        stamp = dt.datetime.fromisoformat(observed.replace("Z", "+00:00")) if isinstance(observed, str) else None
        if stamp is None or stamp.tzinfo is None or stamp.year < 2:
            raise ValueError("missing or naive observation timestamp")
    except (ValueError, OverflowError):
        raise ValueError("trial timing requires a valid receipt timestamp") from None
    return elapsed


def observation_interval(trial, boundary, measured):
    """Bound a sampled transition; submission start is not a nonterminal observation."""
    interval = trial.get("timing", {}).get("events", {}).get(boundary, {}).get("observation_interval")
    if interval is None:
        return None
    required = {"last_nonterminal_request_start_seconds", "last_nonterminal_response_elapsed_seconds",
                "terminal_request_start_seconds", "lower_bound_elapsed_seconds", "upper_bound_elapsed_seconds",
                "gap_seconds", "lower_bound_basis"}
    optional = {"last_nonterminal_state", "last_nonterminal_observed_at"}
    if not isinstance(interval, dict) or not required <= set(interval) <= required | optional:
        raise ValueError("unsupported transition observation interval")
    lower = number(interval["lower_bound_elapsed_seconds"], "observation lower bound")
    upper = number(interval["upper_bound_elapsed_seconds"], "observation upper bound")
    terminal_start = number(interval["terminal_request_start_seconds"], "terminal request start")
    gap = number(interval["gap_seconds"], "observation gap_seconds")
    if not lower <= terminal_start <= upper or upper != measured or gap != upper - lower:
        raise ValueError("inconsistent transition observation bounds")
    last_start = interval["last_nonterminal_request_start_seconds"]
    last_end = interval["last_nonterminal_response_elapsed_seconds"]
    if last_start is None or last_end is None:
        if (last_start is not None or last_end is not None or lower != 0
                or interval["lower_bound_basis"] != "submission_start_no_nonterminal"
                or any(interval.get(key) is not None for key in optional)):
            raise ValueError("missing nonterminal observation cannot supply a sampled lower bound")
    else:
        last_start = number(last_start, "last nonterminal request start")
        last_end = number(last_end, "last nonterminal response")
        if not last_start == lower <= last_end <= terminal_start or interval["lower_bound_basis"] != "last_nonterminal_request_start":
            raise ValueError("inconsistent last nonterminal observation")
        if "last_nonterminal_state" in interval:
            source = trial["timing"]["events"][boundary]["source"]
            allowed = {
                "agentos_run_api": {"queued", "starting", "activating", "running", "cancelling"},
                "kubernetes_job_condition": {"Nonterminal"},
                "kubernetes_pod_phase": {"Pending", "Running", "Unknown"},
                "sandbox_pod_phase": {"Pending", "Running", "Unknown"},
                "collector_validated_output": set(),
            }[source]
            if not isinstance(interval["last_nonterminal_state"], str) or interval["last_nonterminal_state"] not in allowed:
                raise ValueError("invalid last nonterminal state")
        if "last_nonterminal_observed_at" in interval:
            value = interval["last_nonterminal_observed_at"]
            try:
                stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else None
                if stamp is None or stamp.tzinfo is None or stamp.year < 2:
                    raise ValueError("missing receipt")
            except (ValueError, OverflowError):
                raise ValueError("invalid last nonterminal receipt timestamp") from None
    return {"lower": lower, "upper": upper, "gap": upper - lower,
            "nonterminal_missing": last_start is None}


def percentile95(values):
    return sorted(values)[math.ceil(0.95 * len(values)) - 1] if values else None


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
    explicit_timing = protocol["format_version"] in (2, 3)
    bounded_observation = protocol["format_version"] == 3
    selected = protocol["latency_policy"]["boundary"] if explicit_timing else None
    if explicit_timing:
        contract = collector.timing_contract(label)
        if (type(report.get("format_version")) is not int
                or not isinstance(report.get("timing_contract"), dict)
                or collector.digest(report["timing_contract"]) != collector.digest(contract)):
            raise ValueError("report timing contract differs from its arm")
        if (not isinstance(report.get("latency_policy"), dict)
                or collector.digest(report["latency_policy"]) != collector.digest(protocol["latency_policy"])):
            raise ValueError("report latency policy differs from its registration")
    if bounded_observation:
        report_period = number(report.get("poll_seconds"), "report observation policy period", .01, 5)
        if (not isinstance(report.get("observation_policy"), dict)
                or collector.digest(report["observation_policy"]) != collector.digest(protocol["observation_policy"])
                or report_period != protocol["observation_policy"]["period_seconds"]):
            raise ValueError("report observation policy differs from its registration")
    times, successes = [], 0
    intervals, sampling = [], []
    boundary_times = {boundary: [] for boundary in TIMING_BOUNDARIES}
    missing = {boundary: 0 for boundary in TIMING_BOUNDARIES}
    for trial in trials:
        if not isinstance(trial, dict) or type(trial.get("correct")) is not bool:
            raise ValueError("each trial needs an explicit correctness outcome")
        elapsed = number(trial.get("elapsed_seconds"), "elapsed_seconds")
        successes += int(trial["correct"])
        # Timeouts and failed work count in completion rate, never as fast successful work.
        if trial["correct"]:
            if elapsed <= 0:
                raise ValueError("successful trial elapsed_seconds must be positive")
            if explicit_timing:
                if bounded_observation:
                    diagnostics = trial.get("timing", {}).get("sampling") if isinstance(trial.get("timing"), dict) else None
                    if diagnostics is not None:
                        keys = {"scheduled_polls", "completed_polls", "missed_ticks", "max_start_lateness_seconds"}
                        if (not isinstance(diagnostics, dict) or set(diagnostics) != keys or any(
                                type(diagnostics[key]) is not int or diagnostics[key] < 0
                                for key in keys - {"max_start_lateness_seconds"})):
                            raise ValueError("invalid fixed-cadence sampling diagnostics")
                        number(diagnostics["max_start_lateness_seconds"], "maximum sample start lateness")
                        if diagnostics["completed_polls"] > diagnostics["scheduled_polls"]:
                            raise ValueError("completed sampling polls exceed scheduled polls")
                        sampling.append(diagnostics)
                for boundary in TIMING_BOUNDARIES:
                    measured = timing_seconds(trial, boundary, contract)
                    if measured is None:
                        missing[boundary] += 1
                    else:
                        if boundary == selected and measured <= 0:
                            raise ValueError("successful selected timing must be positive")
                        boundary_times[boundary].append(measured)
                        if bounded_observation and boundary == selected:
                            observed = observation_interval(trial, boundary, measured)
                            if observed is not None:
                                intervals.append(observed)
            else:
                times.append(elapsed)
    qualification = report.get("qualification", {})
    status = qualification.get("status", "not_verified")
    if status not in ("passed", "failed", "not_verified"):
        raise ValueError("unsupported qualification status")
    if status in ("passed", "failed") and not qualification.get("evidence_ref"):
        raise ValueError("qualification result needs an evidence reference")
    if explicit_timing:
        # Never compute a gate on a favorable subset of successful executions.
        times = boundary_times[selected] if missing[selected] == 0 else []
    p95 = sorted(times)[math.ceil(0.95 * len(times)) - 1] if times else None
    result = {"trials": len(trials), "correct": successes, "correct_fraction": successes / len(trials),
              "successful_p95_seconds": p95, "qualification": status}
    if explicit_timing:
        result.update(latency_boundary=selected, missing_latency_samples=missing[selected],
                      timing_boundaries={name: {
                          "observed_successful_trials": len(values), "missing_successful_trials": missing[name],
                          "successful_p95_seconds": sorted(values)[math.ceil(.95 * len(values)) - 1]
                              if values and missing[name] == 0 else None,
                      } for name, values in boundary_times.items()})
    if bounded_observation:
        gap_limit = protocol["observation_policy"]["max_terminal_gap_seconds"]
        oversized = sum(interval["gap"] > gap_limit for interval in intervals)
        complete = len(intervals) == successes and successes > 0
        result["latency_observation"] = {
            "interval_samples": len(intervals), "missing_intervals": successes - len(intervals),
            "missing_nonterminal_observations": sum(interval["nonterminal_missing"] for interval in intervals),
            "oversized_gaps": oversized,
            "max_gap_seconds": max((interval["gap"] for interval in intervals), default=None),
            "p95_gap_seconds": percentile95([interval["gap"] for interval in intervals]),
            "p95_lower_bound_seconds": percentile95([interval["lower"] for interval in intervals]) if complete else None,
            "p95_upper_bound_seconds": percentile95([interval["upper"] for interval in intervals]) if complete else None,
            "measurable": complete and not oversized,
            "sampling": {
                "trials_with_diagnostics": len(sampling), "trials_missing_diagnostics": successes - len(sampling),
                **{key: sum(item[key] for item in sampling) for key in ("scheduled_polls", "completed_polls", "missed_ticks")},
                "max_start_lateness_seconds": max((item["max_start_lateness_seconds"] for item in sampling), default=None),
            },
        }
    return result


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


def instrument_check(baseline, candidate, protocol):
    """Expose a diagnostic difference without treating it as a known calibration truth."""
    b, c = baseline["latency_observation"], candidate["latency_observation"]
    result = {"status": "not_measurable", "flags": [], "observed_p95_difference_seconds": None,
              "p95_difference_bounds_seconds": None, "expected_difference_seconds": None,
              "interpretation": "Diagnostic only: inspect admission, scheduling, Job and reporting components; no exact one-second answer is assumed."}
    if baseline["successful_p95_seconds"] is not None and candidate["successful_p95_seconds"] is not None:
        result["observed_p95_difference_seconds"] = candidate["successful_p95_seconds"] - baseline["successful_p95_seconds"]
    if all(arm["p95_lower_bound_seconds"] is not None and arm["p95_upper_bound_seconds"] is not None for arm in (b, c)):
        lower = c["p95_lower_bound_seconds"] - b["p95_upper_bound_seconds"]
        upper = c["p95_upper_bound_seconds"] - b["p95_lower_bound_seconds"]
        result["p95_difference_bounds_seconds"] = {"lower": lower, "upper": upper}
        if b["measurable"] and c["measurable"]:
            if upper < 0:
                result["flags"].append("candidate_faster_than_job_path")
            if lower > protocol["latency_policy"]["absolute_allowance_seconds"]:
                result["flags"].append("additional_delay_requires_review")
            result["status"] = "review_required" if result["flags"] else "inspect_components"
    return result


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
    result = {"format_version": protocol["format_version"], "experiment_id": protocol["experiment_id"],
              "protocol_sha256": digest, "baseline": b, "candidate": c,
              **{key: baseline[key] for key in ("registration_sha256", "workload_sha256", "environment_sha256", "requested_trials")},
              "decision": "insufficient_evidence", "reasons": [], "routes": {}}
    explicit_timing = protocol["format_version"] in (2, 3)
    bounded_observation = protocol["format_version"] == 3
    if explicit_timing:
        policy = dict(protocol["latency_policy"])
        result["latency_gate"] = {"policy": policy,
            "max_p95_latency_ratio": protocol["max_p95_latency_ratio"],
            "formula": "candidate_p95 <= baseline_p95 * max_p95_latency_ratio + absolute_allowance_seconds",
            "baseline_p95_seconds": b["successful_p95_seconds"],
            "candidate_p95_seconds": c["successful_p95_seconds"],
            "allowed_candidate_p95_seconds": None, "passes": None}
    if bounded_observation:
        result["latency_gate"].update(
            observation_policy=dict(protocol["observation_policy"]),
            status="not_assessed" if all(summary["latency_observation"]["measurable"] for summary in (b, c)) else "not_measurable",
            interval_formula={
                "pass": "candidate_p95_upper <= baseline_p95_lower * max_p95_latency_ratio + absolute_allowance_seconds",
                "fail": "candidate_p95_lower > baseline_p95_upper * max_p95_latency_ratio + absolute_allowance_seconds",
            },
            pass_limit_seconds=None, fail_limit_seconds=None)
        if protocol["baseline"] == "direct-job" and protocol["candidate"] == "agentos":
            result["instrument_check"] = instrument_check(b, c, protocol)
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
    if explicit_timing and any(summary["missing_latency_samples"] for summary in (b, c)):
        reasons.append("The selected preregistered latency boundary is missing on a correct trial; complete coverage is required.")
        return result
    if bounded_observation and not all(summary["latency_observation"]["measurable"] for summary in (b, c)):
        reasons.append("Latency is not measurable: complete transition intervals within the preregistered maximum observation gap are required.")
        return result
    if not b["successful_p95_seconds"] or c["successful_p95_seconds"] is None:
        reasons.append("Positive successful-task timing is required in both arms.")
        return result
    ratio = c["successful_p95_seconds"] / b["successful_p95_seconds"]
    if explicit_timing:
        number(ratio, "successful_p95_latency_ratio")
    result["successful_p95_latency_ratio"] = ratio
    if explicit_timing:
        allowance = protocol["latency_policy"]["absolute_allowance_seconds"]
        limit = b["successful_p95_seconds"] * protocol["max_p95_latency_ratio"] + allowance
        number(limit, "allowed_candidate_p95_seconds")
        passes = c["successful_p95_seconds"] <= limit
        result["latency_gate"].update(allowed_candidate_p95_seconds=limit, passes=passes)
        if bounded_observation:
            baseline_interval, candidate_interval = b["latency_observation"], c["latency_observation"]
            pass_limit = baseline_interval["p95_lower_bound_seconds"] * protocol["max_p95_latency_ratio"] + allowance
            fail_limit = baseline_interval["p95_upper_bound_seconds"] * protocol["max_p95_latency_ratio"] + allowance
            number(pass_limit, "conservative pass limit")
            number(fail_limit, "conservative fail limit")
            result["latency_gate"].update(pass_limit_seconds=pass_limit, fail_limit_seconds=fail_limit)
            if candidate_interval["p95_upper_bound_seconds"] <= pass_limit:
                passes, status = True, "pass"
            elif candidate_interval["p95_lower_bound_seconds"] > fail_limit:
                passes, status = False, "fail"
            else:
                result["latency_gate"].update(passes=None, status="not_measurable")
                reasons.append("Latency is not measurable: the sampled p95 intervals overlap the preregistered decision boundary.")
                return result
            result["latency_gate"].update(passes=passes, status=status)
    else:
        passes = ratio <= protocol["max_p95_latency_ratio"]
    if not passes:
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
