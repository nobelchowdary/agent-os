"""Instrument contract tests; no cluster or performance claim."""

import importlib.util
from pathlib import Path
import threading
import time
import unittest

SPEC = importlib.util.spec_from_file_location("evaluation_observer", Path(__file__).parents[1] / "evaluation_observer.py")
observer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(observer)


class Failure(Exception):
    pass


class ObserverTests(unittest.TestCase):
    def test_interval_includes_both_request_latency_and_sampling_gap(self):
        value = observer.OutcomeObserver(lambda: None, 10, .1, Failure)
        value.accept({"terminal": False, "state": "Running"}, 11, 11.2, "nonterminal")
        value.accept({"terminal": True, "state": "Succeeded"}, 11.3, 11.6, "terminal")
        _, sample, _, _ = value.snapshot()
        bounds = sample["observation_interval"]
        self.assertEqual(bounds["lower_bound_elapsed_seconds"], 1)
        self.assertAlmostEqual(bounds["upper_bound_elapsed_seconds"], 1.6)
        self.assertAlmostEqual(bounds["gap_seconds"], .6)
        self.assertEqual(bounds["last_nonterminal_observed_at"], "nonterminal")
        self.assertEqual(bounds["terminal_request_start_seconds"], 1.3000000000000007)
        value.accept({"terminal": True, "state": "Failed"}, 12, 13)
        self.assertEqual(value.snapshot()[1], sample)

    def test_absence_never_becomes_a_fabricated_nonterminal_observation(self):
        value = observer.OutcomeObserver(lambda: None, 10, .1, Failure)
        value.accept(None, 11, 11.1)
        value.accept({"terminal": True, "state": "Succeeded"}, 12, 12.1)
        bounds = value.snapshot()[1]["observation_interval"]
        self.assertIsNone(bounds["last_nonterminal_request_start_seconds"])
        self.assertEqual(bounds["lower_bound_elapsed_seconds"], 0)
        self.assertEqual(bounds["lower_bound_basis"], "submission_start_no_nonterminal")
        self.assertAlmostEqual(bounds["gap_seconds"], 2.1)

    def test_fixed_grid_skips_overruns_instead_of_sliding_or_catching_up(self):
        due, missed = observer.OutcomeObserver.next_tick(1, .1, 1.36)
        self.assertEqual(missed, 3)
        self.assertAlmostEqual(due, 1.4)
        due, missed = observer.OutcomeObserver.next_tick(1, .1, 1.04)
        self.assertEqual(missed, 0)
        self.assertAlmostEqual(due, 1.1)

    def test_sampler_runs_independently_of_blocked_control_work(self):
        observed = threading.Event()
        calls = []
        def read():
            calls.append(time.monotonic())
            if len(calls) == 3:
                observed.set()
            return {"terminal": len(calls) == 3, "state": "Succeeded" if len(calls) == 3 else "Running"}
        value = observer.OutcomeObserver(read, time.monotonic(), .01, Failure).launch()
        self.addCleanup(value.close)
        self.assertTrue(observed.wait(2))  # Control thread performs no reads/log work.
        value.close()
        self.assertEqual(value.snapshot()[3]["completed_polls"], 3)
        self.assertIsNotNone(value.snapshot()[1])

    def test_failure_is_bounded_and_does_not_publish_terminal_evidence(self):
        def read():
            raise RuntimeError("SECRET diagnostics")
        value = observer.OutcomeObserver(read, time.monotonic(), .01, Failure).launch()
        self.addCleanup(value.close)
        self.assertTrue(value.changed.wait(2))
        self.assertEqual(value.snapshot()[2], "outcome_observer_failed")
        self.assertIsNone(value.snapshot()[1])


if __name__ == "__main__":
    unittest.main()
