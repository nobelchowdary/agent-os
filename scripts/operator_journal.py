#!/usr/bin/env python3
"""Time operator sessions and record privileges without recording command output."""

import argparse
from collections import Counter
from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import sys
import time

LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_. /:-]{0,127}$")
BINDINGS = ("experiment_id", "baseline", "protocol_sha256", "registration_sha256",
            "workload_sha256", "environment_sha256", "phase", "task_ref")


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def registration_bindings(path, arm):
    path = Path(path)
    if not path.is_file() or path.is_symlink() or path.stat().st_mode & 0o077:
        raise ValueError("registration must be a private regular file")
    with path.open("rb") as stream:
        raw = stream.read(512 * 1024 + 1)
    if len(raw) > 512 * 1024:
        raise ValueError("registration exceeds supported size")
    registration = json.loads(raw)
    if not isinstance(registration, dict) or registration.get("format_version") != 1:
        raise ValueError("unsupported registration")
    protocol, workload = registration["protocol"], registration["workload"]
    if (registration["protocol_sha256"] != canonical_hash(protocol)
            or registration["workload_sha256"] != canonical_hash(workload)
            or registration["experiment_id"] != protocol["experiment_id"]
            or arm not in (protocol["baseline"], protocol["candidate"])):
        raise ValueError("registration input drift or unregistered arm")
    environment_hash = registration["environment"]["sha256"]
    if not isinstance(environment_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", environment_hash):
        raise ValueError("registration requires an environment hash")
    return {"experiment_id": registration["experiment_id"], "baseline": arm,
            "protocol_sha256": registration["protocol_sha256"], "registration_sha256": canonical_hash(registration),
            "workload_sha256": registration["workload_sha256"], "environment_sha256": environment_hash}


def label(value):
    if not LABEL.fullmatch(value):
        raise ValueError("use a short descriptive label without credentials")
    return value


def write_new(path, record):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
        json.dump(record, output, indent=2, allow_nan=False)
        output.write("\n")


@contextmanager
def locked(directory):
    directory = Path(directory)
    with open(directory / ".lock", "a") as lock:
        os.chmod(directory / ".lock", 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        record = json.loads((directory / "session.json").read_text())
        yield record
        temporary = directory / ".session.next.json"
        write_new(temporary, record)
        os.replace(temporary, directory / "session.json")


def elapsed(record, now):
    seconds = now - record["started_unix"]
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("clock changed; invalidate and repeat the operator session")
    return seconds


def summarize(records):
    if len(records) < 3:
        raise ValueError("at least three completed sessions are required")
    first = records[0]
    for record in records:
        if any(record[key] != first[key] for key in BINDINGS) or record.get("qualified") is not True:
            raise ValueError("sessions must have the same registered task and qualified outcome")
        value = record.get("elapsed_seconds")
        if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
            raise ValueError("invalid session duration")
    minutes = [r["elapsed_seconds"] / 60 for r in records]
    privileges = Counter(s["privilege"] for r in records for s in r["steps"])
    binding = {key: first[key] for key in BINDINGS}
    return {"format_version": 1, **binding,
            "operator_effort": {**binding, "completed": True, "source": "human_reported_wall_clock",
                "record_ref": "session.json files listed in session_refs", "session_minutes": minutes,
                "median_minutes": statistics.median(minutes), "privilege_steps": dict(privileges),
                "node_customization_steps": sum(s["node_customization"] for r in records for s in r["steps"]),
                "failed_steps": sum(s["outcome"] == "failed" for r in records for s in r["steps"]),
                "total_steps": sum(len(r["steps"]) for r in records)}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    start = commands.add_parser("start")
    for key in ("directory", "registration", "arm", "operator", "task-ref"):
        start.add_argument("--" + key, required=True)
    start.add_argument("--phase", choices=("install", "upgrade", "failure-recovery"), required=True)
    step = commands.add_parser("step")
    for key in ("directory", "name", "runbook-ref"):
        step.add_argument("--" + key, required=True)
    step.add_argument("--privilege", choices=("none", "namespace-admin", "cluster-admin", "node-admin", "aws-admin"), required=True)
    step.add_argument("--node-customization", action="store_true")
    step.add_argument("--outcome", choices=("succeeded", "failed"), required=True)
    finish = commands.add_parser("finish")
    finish.add_argument("--directory", required=True)
    finish.add_argument("--qualified", action="store_true")
    finish.add_argument("--evidence-ref", required=True)
    summary = commands.add_parser("summary")
    summary.add_argument("--sessions", nargs="+", required=True)
    summary.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == "start":
            binding = registration_bindings(args.registration, label(args.arm))
            task_ref, operator = label(args.task_ref), label(args.operator)
            directory = Path(args.directory)
            directory.mkdir(mode=0o700, parents=True, exist_ok=False)
            record = {"format_version": 1, **binding, "task_ref": task_ref,
                      "operator": operator, "phase": args.phase, "started_unix": time.time(), "steps": []}
            write_new(directory / "session.json", record)
        elif args.action in ("step", "finish"):
            with locked(args.directory) as record:
                if "elapsed_seconds" in record:
                    raise ValueError("session already finished")
                seconds = elapsed(record, time.time())
                if args.action == "step":
                    record["steps"].append({"name": label(args.name), "runbook_ref": label(args.runbook_ref),
                        "privilege": args.privilege, "node_customization": args.node_customization,
                        "outcome": args.outcome, "seconds_since_start": seconds})
                else:
                    record.update(elapsed_seconds=seconds, qualified=args.qualified, evidence_ref=label(args.evidence_ref))
        else:
            records = [json.loads((Path(p) / "session.json").read_text()) for p in args.sessions]
            if len(set(str(Path(p).resolve()) for p in args.sessions)) != len(args.sessions):
                raise ValueError("duplicate session path")
            output = summarize(records)
            output["session_refs"] = args.sessions
            write_new(args.output, output)
        print("Operator record saved; observations are human-reported, not independently verified.")
    except (OSError, ValueError, KeyError, TypeError):
        print("Operator record failed; check arguments, registration and session state.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
