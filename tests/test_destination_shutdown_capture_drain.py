"""Destination shutdown snapshot capture: bounded descendant-apply drain before persist."""

from __future__ import annotations

import os
import unittest
from collections import deque
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


class DestinationShutdownCaptureDrainTests(unittest.TestCase):
    def test_finalize_drain_runs_under_shutdown_gate_drains_queue(self):
        """Shutdown snapshot drain must run descendant ticks (not skip them) until queue is idle."""
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_forensic_destination_model_counts = MainWindow._destination_forensic_destination_model_counts.__get__(
            w, MainWindow
        )
        w._snapshot_capture_shutdown_drain_deadline_s = MainWindow._snapshot_capture_shutdown_drain_deadline_s.__get__(
            w, MainWindow
        )
        w._destination_descendant_apply_queue = deque([1, 2, 3])
        w._destination_descendant_apply_state = None
        tick_calls = {"n": 0}

        def _tick():
            tick_calls["n"] += 1
            q = w._destination_descendant_apply_queue
            if q:
                q.pop()

        w._run_destination_descendant_apply_tick = _tick
        w._destination_flush_pending_leaf_batches_for_snapshot_capture = lambda: None

        MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
        self.assertEqual(len(w._destination_descendant_apply_queue), 0)
        self.assertEqual(tick_calls["n"], 3)

    def test_finalize_drain_respects_shutdown_wall_budget(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_forensic_destination_model_counts = MainWindow._destination_forensic_destination_model_counts.__get__(
            w, MainWindow
        )
        os.environ["OZLINK_SNAPSHOT_DRAIN_SHUTDOWN_S"] = "0.001"
        try:
            w._snapshot_capture_shutdown_drain_deadline_s = MainWindow._snapshot_capture_shutdown_drain_deadline_s.__get__(
                w, MainWindow
            )
            w._destination_descendant_apply_queue = deque(range(5000))
            w._destination_descendant_apply_state = {"hold": True}

            def _tick():
                pass

            w._run_destination_descendant_apply_tick = _tick
            w._destination_flush_pending_leaf_batches_for_snapshot_capture = lambda: None

            MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
        finally:
            os.environ.pop("OZLINK_SNAPSHOT_DRAIN_SHUTDOWN_S", None)

        self.assertGreater(len(w._destination_descendant_apply_queue), 0)

    def test_shutdown_guard_allows_descendant_tick_only_during_snapshot_drain(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._destination_snapshot_capture_drain_active = False
        with patch("ozlink_console.main_window.log_info"):
            self.assertTrue(MainWindow._if_shutdown_skip_mutation(w, "_run_destination_descendant_apply_tick"))
            w._destination_snapshot_capture_drain_active = True
            self.assertFalse(MainWindow._if_shutdown_skip_mutation(w, "_run_destination_descendant_apply_tick"))
            self.assertTrue(MainWindow._if_shutdown_skip_mutation(w, "_apply_destination_planning_overlays"))

    def test_finalize_drain_no_progress_exits_before_max_ticks(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_forensic_destination_model_counts = MainWindow._destination_forensic_destination_model_counts.__get__(
            w, MainWindow
        )
        w._snapshot_capture_shutdown_drain_deadline_s = MainWindow._snapshot_capture_shutdown_drain_deadline_s.__get__(
            w, MainWindow
        )
        w._destination_descendant_apply_queue = deque()
        w._destination_descendant_apply_state = {
            "graph_walk": True,
            "walk_phase": "walk",
            "added_count": 0,
            "desc_index": 0,
            "pending_leaf_batches": {},
        }

        def _tick():
            pass

        w._run_destination_descendant_apply_tick = _tick
        w._destination_flush_pending_leaf_batches_for_snapshot_capture = lambda: None
        drain_payloads: list[dict] = []

        def _capture(msg: str, **data):
            if msg == "destination_snapshot_capture_descendant_apply_drain":
                drain_payloads.append(dict(data))

        os.environ["OZLINK_SNAPSHOT_DRAIN_NO_PROGRESS_TICKS"] = "8"
        try:
            with patch("ozlink_console.main_window.log_info", side_effect=_capture):
                MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
        finally:
            os.environ.pop("OZLINK_SNAPSHOT_DRAIN_NO_PROGRESS_TICKS", None)

        self.assertTrue(drain_payloads)
        self.assertEqual(drain_payloads[-1].get("exit_reason"), "no_progress_stall")
        self.assertLess(int(drain_payloads[-1].get("tick_count") or 0), 600)

    def test_finalize_drain_flushes_pending_leaves_bookends(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_forensic_destination_model_counts = MainWindow._destination_forensic_destination_model_counts.__get__(
            w, MainWindow
        )
        w._snapshot_capture_shutdown_drain_deadline_s = MainWindow._snapshot_capture_shutdown_drain_deadline_s.__get__(
            w, MainWindow
        )
        w._destination_descendant_apply_queue = deque()
        w._destination_descendant_apply_state = None
        flush_calls = {"n": 0}

        def _flush():
            flush_calls["n"] += 1

        w._destination_flush_pending_leaf_batches_for_snapshot_capture = _flush
        w._run_destination_descendant_apply_tick = lambda: None
        with patch("ozlink_console.main_window.log_info"):
            MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
        self.assertGreaterEqual(flush_calls["n"], 2)

    def test_shutdown_descendant_tick_skip_logging_throttled(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._destination_snapshot_capture_drain_active = False
        w._shutdown_descendant_tick_skip_logs_suppressed = 0
        w._shutdown_descendant_tick_skip_event_logged = False
        with patch("ozlink_console.main_window.log_info") as m_log:
            for _ in range(50):
                MainWindow._if_shutdown_skip_mutation(w, "_run_destination_descendant_apply_tick")
        skip_events = [
            c
            for c in m_log.call_args_list
            if c[0] and c[0][0] == "shutdown_trace" and c[1].get("event") == "shutdown_mutation_skipped"
        ]
        self.assertEqual(len(skip_events), 1)
        self.assertEqual(int(getattr(w, "_shutdown_descendant_tick_skip_logs_suppressed", 0) or 0), 49)

    def test_shutdown_drain_graph_walk_cutoff_runs_once_and_clears_state(self):
        """Shutdown + snapshot drain must not run deep graph-walk segment loops; cutoff settles state."""
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._destination_snapshot_capture_drain_active = True
        w._destination_user_scroll_interaction_active = None
        parent_ix = MagicMock()
        parent_ix.isValid = MagicMock(return_value=True)
        st = {
            "graph_walk": True,
            "walk_phase": "walk",
            "model": MagicMock(),
            "move": {},
            "parent_ix": parent_ix,
            "overlay_count": 0,
            "allocation_destination_path": "",
            "child_map_cache": {},
            "pending_leaf_batches": {},
        }
        w._destination_descendant_apply_state = st
        calls = {"flush": 0, "finalize": 0}

        def _flush(*_a, **_k):
            calls["flush"] += 1

        def _finalize(_st):
            calls["finalize"] += 1

        w._allocation_apply_flush_all_pending_model = _flush
        w._destination_descendant_apply_finalize_job = _finalize
        with patch("ozlink_console.main_window.log_info"):
            MainWindow._run_destination_descendant_apply_tick_body(w)
        self.assertIsNone(w._destination_descendant_apply_state)
        self.assertEqual(calls["flush"], 1)
        self.assertEqual(calls["finalize"], 1)

    def test_graph_walk_cutoff_helper_noop_without_graph_flag(self):
        w = MainWindow.__new__(MainWindow)
        w._allocation_apply_flush_all_pending_model = MagicMock()
        w._destination_descendant_apply_finalize_job = MagicMock()
        MainWindow._destination_shutdown_capture_cutoff_graph_walk_for_snapshot(w, {"graph_walk": False})
        w._allocation_apply_flush_all_pending_model.assert_not_called()
        w._destination_descendant_apply_finalize_job.assert_not_called()


if __name__ == "__main__":
    unittest.main()
