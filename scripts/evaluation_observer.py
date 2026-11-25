"""Bounded fixed-cadence outcome observations, independent of log/owner work."""

import copy
import datetime as dt
import math
import threading
import time


class OutcomeObserver:
    def __init__(self, read, start, period, failure_type):
        self.read, self.start, self.period, self.failure_type = read, start, period, failure_type
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.changed = threading.Event()
        self.latest = self.terminal = self.last_nonterminal = None
        self.failure = None
        self.stats = {"scheduled_polls": 0, "completed_polls": 0, "missed_ticks": 0,
                      "max_start_lateness_seconds": 0.0}
        self.thread = threading.Thread(target=self._run, name="evaluation-outcome", daemon=True)

    def launch(self):
        self.thread.start()
        return self

    def accept(self, value, request_start, response_end, observed_at=None):
        """Only locally validated native objects enter the instrument.

        A missing object is not an observed nonterminal object. Last-negative
        bounds use request *start*, conservatively including response latency.
        """
        if value is None:
            return
        sample = copy.deepcopy(value)
        sample.update(request_start_seconds=request_start - self.start,
                      elapsed_seconds=response_end - self.start,
                      observed_at=observed_at or dt.datetime.now(dt.timezone.utc).isoformat())
        with self.lock:
            if self.terminal is not None:
                return
            self.latest = sample
            if sample["terminal"]:
                previous = self.last_nonterminal
                if previous is not None and previous["elapsed_seconds"] > sample["request_start_seconds"]:
                    previous = None  # Concurrent submission responses cannot narrow a bound retroactively.
                lower = previous["request_start_seconds"] if previous else 0.0
                sample["observation_interval"] = {
                    "last_nonterminal_request_start_seconds": lower if previous else None,
                    "last_nonterminal_response_elapsed_seconds": previous["elapsed_seconds"] if previous else None,
                    "terminal_request_start_seconds": sample["request_start_seconds"],
                    "lower_bound_elapsed_seconds": lower,
                    "upper_bound_elapsed_seconds": sample["elapsed_seconds"],
                    "gap_seconds": sample["elapsed_seconds"] - lower,
                    "lower_bound_basis": "last_nonterminal_request_start" if previous else "submission_start_no_nonterminal",
                    "last_nonterminal_state": previous["state"] if previous else None,
                    "last_nonterminal_observed_at": previous["observed_at"] if previous else None,
                }
                self.terminal = sample
            else:
                self.last_nonterminal = sample
        if sample["terminal"]:
            self.changed.set()

    @staticmethod
    def next_tick(due, period, completed):
        next_due = due + period
        missed = max(0, math.floor((completed - next_due) / period) + 1) if completed >= next_due else 0
        return next_due + missed * period, missed

    def _run(self):
        due = self.start + self.period
        try:
            while not self.stopping.wait(max(0.0, due - time.monotonic())):
                request_start = time.monotonic()
                with self.lock:
                    self.stats["scheduled_polls"] += 1
                    self.stats["max_start_lateness_seconds"] = max(
                        self.stats["max_start_lateness_seconds"], max(0.0, request_start - due))
                value = self.read()
                response_end = time.monotonic()
                self.accept(value, request_start, response_end)
                with self.lock:
                    self.stats["completed_polls"] += 1
                    terminal = self.terminal is not None
                if terminal:
                    break
                due, missed = self.next_tick(due, self.period, response_end)
                with self.lock:
                    self.stats["missed_ticks"] += missed
        except Exception as error:
            with self.lock:
                self.failure = str(error) if isinstance(error, self.failure_type) else "outcome_observer_failed"
            self.changed.set()

    def snapshot(self):
        with self.lock:
            return copy.deepcopy((self.latest, self.terminal, self.failure, self.stats))

    def close(self):
        self.stopping.set()
        if self.thread.ident is not None:
            self.thread.join(timeout=12)
            if self.thread.is_alive():
                raise self.failure_type("outcome_observer_shutdown_timeout")
