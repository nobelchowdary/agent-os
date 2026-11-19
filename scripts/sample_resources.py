#!/usr/bin/env python3
"""Opt-in read-only node resource observations from metrics.k8s.io.

CPU is a metrics-window observation; memory is working set. Neither represents
an attributable workload bill, per-process measurement or isolation verdict.
"""

import argparse
import datetime as dt
import json
import math
from pathlib import Path
import re
import sys
import time

# Also works when launched by absolute path outside the repository directory.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import evaluate as ev


METRICS_PATH = "/apis/metrics.k8s.io/v1beta1/nodes"


def validate_selector(value):
    ev.require(isinstance(value, str) and 1 <= len(value) <= 512, "explicit_node_selector_required")
    terms = value.split(",")
    ev.require(len(terms) <= 8 and all(re.fullmatch(
        r"(?:[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?/)?[A-Za-z0-9][A-Za-z0-9_.-]{0,62}=[A-Za-z0-9][A-Za-z0-9_.-]{0,62}", x)
        for x in terms), "equality_node_selector_required")


def parse_json(raw):
    try:
        value = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, UnicodeError):
        raise ev.Failure("invalid_metrics_or_inventory_json") from None
    ev.require(isinstance(value, dict) and isinstance(value.get("items"), list), "invalid_metrics_or_inventory_json")
    return value


def inventory(value):
    ev.require(value.get("kind") == "NodeList" and 1 <= len(value["items"]) <= 32, "selected_node_count_unsupported")
    nodes = {}
    for node in value["items"]:
        meta = node.get("metadata", {})
        name, uid = meta.get("name"), meta.get("uid")
        ev.require(isinstance(name, str) and re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", name)
                   and isinstance(uid, str) and 1 <= len(uid) <= 128 and name not in nodes, "invalid_node_identity")
        allocatable = node.get("status", {}).get("allocatable", {})
        cpu = ev.quantity(allocatable.get("cpu", "-1"))
        memory = ev.quantity(allocatable.get("memory", "-1"))
        ev.require(0 < cpu <= 100000 and 0 < memory <= 10 ** 18 and memory == int(memory), "invalid_node_allocatable")
        nodes[name] = {"name": name, "uid": uid, "allocatable_cpu_cores": float(cpu), "allocatable_memory_bytes": int(memory)}
    return nodes


def duration_seconds(value):
    ev.require(isinstance(value, str) and len(value) <= 64, "invalid_metrics_window")
    match = re.fullmatch(r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?", value)
    ev.require(match is not None and any(match.groups()), "invalid_metrics_window")
    seconds = sum(float(part or 0) * scale for part, scale in zip(match.groups(), (3600, 60, 1)))
    ev.require(0 < seconds <= 3600 and math.isfinite(seconds), "invalid_metrics_window")
    return seconds


def parse_metrics(value, selected, observed, previous, max_age):
    ev.require(value.get("apiVersion") == "metrics.k8s.io/v1beta1" and value.get("kind") == "NodeMetricsList", "unsupported_metrics_api")
    by_name = {}
    for item in value["items"]:
        name = item.get("metadata", {}).get("name")
        if name not in selected:
            continue
        ev.require(name not in by_name, "duplicate_selected_node_metrics")
        by_name[name] = item
    ev.require(set(by_name) == set(selected), "selected_node_metrics_missing")
    result = []
    for name in sorted(selected):
        metric = by_name[name]
        stamp = metric.get("timestamp", "")
        ev.require(isinstance(stamp, str) and len(stamp) <= 64, "invalid_metrics_timestamp")
        try:
            timestamp = dt.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            ev.require(timestamp.tzinfo is not None, "invalid_metrics_timestamp")
            age = (observed - timestamp).total_seconds()
        except ValueError:
            raise ev.Failure("invalid_metrics_timestamp") from None
        ev.require(-30 <= age <= max_age, "selected_node_metrics_stale_or_future")
        window = duration_seconds(metric.get("window"))
        usage = metric.get("usage", {})
        cpu = ev.quantity(usage.get("cpu", "-1"))
        memory = ev.quantity(usage.get("memory", "-1"))
        ev.require(0 <= cpu <= 100000 and 0 <= memory <= 10 ** 18 and memory == int(memory), "invalid_node_usage")
        result.append({"node": name, "cpu_cores": float(cpu), "memory_working_set_bytes": int(memory),
                       "metrics_timestamp": stamp, "metrics_window": metric["window"], "window_seconds": window,
                       "window_start": (timestamp - dt.timedelta(seconds=window)).isoformat(),
                       "age_seconds": age, "repeated_timestamp": previous.get(name) == stamp})
    return result


def kube_json(args, arguments, remaining=10):
    budget = min(10, max(0.01, remaining))
    raw = ev.bounded_command(["kubectl", "--context", args.context,
                              "--request-timeout=" + str(max(1, math.ceil(budget))) + "s"] + arguments,
                             timeout=budget, limit=ev.MAX_JSON)
    return parse_json(raw)


def sample(args):
    validate_selector(args.node_selector)
    ev.require(isinstance(args.context, str) and 1 <= len(args.context) <= 256 and not args.context.startswith("-"), "explicit_context_required")
    ev.require(isinstance(args.experiment_id, str) and ev.NAME.fullmatch(args.experiment_id), "experiment_id_required")
    ev.require(math.isfinite(args.duration_seconds) and 1 <= args.duration_seconds <= 600, "duration_bounds")
    ev.require(math.isfinite(args.interval_seconds) and 0.5 <= args.interval_seconds <= 60, "interval_bounds")
    ev.require(1 <= args.max_age_seconds <= 600, "metrics_age_bounds")
    count = math.ceil(args.duration_seconds / args.interval_seconds)
    ev.require(count <= 120, "sample_count_bounds")
    ev.require(args.execute, "sampling_requires_execute")
    path = Path(args.output)
    ev.private_directory(path.parent)
    report = {"format_version": 1, "kind": "node_resource_observations", "experiment_id": args.experiment_id,
              "context": args.context, "node_selector": args.node_selector, "started_at": ev.now(),
              "requested_duration_seconds": args.duration_seconds, "interval_seconds": args.interval_seconds,
              "max_metric_age_seconds": args.max_age_seconds, "scheduled_samples": count, "completed": False,
              "status": "running", "samples": [], "billing_inference": "none",
              "scope": "all node activity on the initially selected nodes, including system and unrelated work",
              "pod_request_summary": {"status": "not_collected", "reason": "pod requests are not node usage"}}
    ev.write_json(path, report, exclusive=True)
    started = time.monotonic()
    try:
        selected = inventory(kube_json(args, ["get", "nodes", "--selector", args.node_selector, "-o", "json"]))
        ev.require(len(selected) * count <= 512, "node_sample_product_unsupported")
        report["selected_nodes"] = list(selected.values())
        report["allocatable_summary"] = {
            "cpu_cores": sum(x["allocatable_cpu_cores"] for x in selected.values()),
            "memory_bytes": sum(x["allocatable_memory_bytes"] for x in selected.values())}
        previous = {}
        window_start = time.monotonic()
        deadline = window_start + args.duration_seconds
        report["observation_window_started_at"] = ev.now()
        ev.write_json(path, report)
        for index in range(count):
            due = window_start + index * args.interval_seconds
            delay = due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            item = {"index": index, "observed_at": ev.now(), "elapsed_seconds": time.monotonic() - window_start}
            report["samples"].append(item)
            try:
                ev.require(time.monotonic() < deadline, "observation_window_exhausted")
                current = inventory(kube_json(args, ["get", "nodes", "--selector", args.node_selector, "-o", "json"],
                                              remaining=deadline - time.monotonic()))
                ev.require(current == selected, "selected_node_identity_or_allocatable_changed")
                ev.require(time.monotonic() < deadline, "observation_window_exhausted")
                try:
                    metrics = kube_json(args, ["get", "--raw", METRICS_PATH], remaining=deadline - time.monotonic())
                except ev.Failure:
                    raise ev.Failure("metrics_api_unavailable_forbidden_or_timed_out") from None
                observed = dt.datetime.now(dt.timezone.utc)
                item["observed_at"] = observed.isoformat()
                item["nodes"] = parse_metrics(metrics, selected, observed, previous, args.max_age_seconds)
                previous = {x["node"]: x["metrics_timestamp"] for x in item["nodes"]}
                item["status"] = "observed"
            except ev.Failure as error:
                item.update(status="failed", error=str(error))
            ev.write_json(path, report)
            # Do not burst a backlog of late polls and call them regular samples.
            ev.require(index + 1 == count or time.monotonic() <= window_start + (index + 1) * args.interval_seconds,
                       "sampling_cadence_missed")
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        report["completed"] = True
        report["status"] = "observed" if all(x["status"] == "observed" for x in report["samples"]) else "failed"
    except KeyboardInterrupt:
        report.update(status="interrupted", error="interrupted")
    except ev.Failure as error:
        report.update(status="failed", error=str(error))
    except Exception:
        report.update(status="failed", error="unexpected_sampling_failure")
    finally:
        report["finished_at"] = ev.now()
        report["collector_elapsed_seconds"] = time.monotonic() - started
        ev.write_json(path, report)
    ev.require(report["completed"] and report["status"] == "observed", "resource_observation_incomplete_or_failed")
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--node-selector", required=True, help="explicit comma-separated key=value selector")
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--duration-seconds", type=float, default=60)
    parser.add_argument("--interval-seconds", type=float, default=5)
    parser.add_argument("--max-age-seconds", type=float, default=120)
    parser.add_argument("--output", required=True, help="new JSON file under an existing private directory")
    parser.add_argument("--execute", action="store_true", help="opt in to read-only cluster queries")
    args = parser.parse_args(argv)
    try:
        print(sample(args))
        return 0
    except Exception as error:
        print("resources: " + (str(error) if isinstance(error, ev.Failure) else "local_operation_failed"), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
