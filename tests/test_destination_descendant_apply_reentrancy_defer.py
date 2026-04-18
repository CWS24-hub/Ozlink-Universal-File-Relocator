"""Descendant apply must not run snapshot drain ticks while tick_running (reentrancy starvation)."""

from __future__ import annotations

import unittest
from collections import deque
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


class DestinationDescendantApplyReentrancyDeferTests(unittest.TestCase):
    def test_finalize_drain_deferred_when_tick_running_no_nested_tick(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_descendant_apply_tick_running = True
        w._post_tick_descendant_drain_pending = False
        tick_calls = {"n": 0}

        def _tick():
            tick_calls["n"] += 1

        w._run_destination_descendant_apply_tick = _tick
        w._destination_flush_pending_leaf_batches_for_snapshot_capture = MagicMock()
        MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
        self.assertEqual(tick_calls["n"], 0)
        self.assertTrue(w._post_tick_descendant_drain_pending)
        w._destination_flush_pending_leaf_batches_for_snapshot_capture.assert_called()

    def test_post_tick_runs_deferred_finalize_once(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_snapshot_capture_drain_depth = 0
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_descendant_apply_tick_running = False
        w._post_tick_descendant_drain_pending = True
        w._post_tick_snapshot_refresh_pending = False
        w._post_tick_inline_descendant_schedule_pending = False
        w._post_tick_descendant_deferred_reenter_depth = 0
        fin = {"n": 0}

        def _finalize():
            fin["n"] += 1

        w._destination_finalize_inflight_descendant_apply_for_snapshot_capture = _finalize
        MainWindow._destination_run_post_tick_descendant_deferred(w)
        self.assertEqual(fin["n"], 1)
        self.assertFalse(w._post_tick_descendant_drain_pending)

    def test_promote_deferred_when_tick_running(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_snapshot_capture_drain_active = False
        w._destination_descendant_apply_tick_running = True
        w._post_tick_snapshot_refresh_pending = False
        calls = {"refresh": 0}

        def _refresh(pk):
            calls["refresh"] += 1
            return []

        w._refresh_runtime_tree_snapshot = _refresh
        w._schedule_workspace_ui_persist = MagicMock()
        MainWindow._promote_destination_workspace_snapshot_after_structure_change(w)
        self.assertEqual(calls["refresh"], 0)
        self.assertTrue(w._post_tick_snapshot_refresh_pending)

    def test_graph_finalize_sets_snapshot_defer_not_synchronous_promote(self):
        w = MainWindow.__new__(MainWindow)
        w._post_tick_snapshot_refresh_pending = False
        promoted = {"n": 0}

        def _bad_promote():
            promoted["n"] += 1

        w._promote_destination_workspace_snapshot_after_structure_change = _bad_promote
        w._mark_allocation_descendants_applied_on_allocation_folder_model_index = MagicMock()
        w._stamp_allocation_projection_cache_metadata_index = MagicMock()
        w._try_mark_allocation_resolved_after_descendant_apply_finalize = MagicMock(return_value=False)
        st = {
            "graph_walk": True,
            "move": {"allocation_id": "x"},
            "overlay_count": 0,
            "model": None,
            "parent_ix": None,
            "allocation_destination_path": "",
            "on_complete": None,
        }
        MainWindow._destination_descendant_apply_finalize_job(w, st)
        self.assertEqual(promoted["n"], 0)
        self.assertTrue(w._post_tick_snapshot_refresh_pending)

    def test_inline_schedule_deferred_when_timer_none_and_tick_running(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        w._destination_descendant_apply_paused_for_finalize_alloc = False
        w._destination_descendant_apply_timer = None
        w._destination_descendant_apply_tick_running = True
        w._post_tick_inline_descendant_schedule_pending = False
        tick_calls = {"n": 0}

        def _tick():
            tick_calls["n"] += 1

        w._run_destination_descendant_apply_tick = _tick
        MainWindow._schedule_destination_descendant_apply_tick(w)
        self.assertEqual(tick_calls["n"], 0)
        self.assertTrue(w._post_tick_inline_descendant_schedule_pending)

    def test_capture_destination_skips_finalize_when_tick_running(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_descendant_apply_tick_running = True
        w._post_tick_descendant_drain_pending = False
        w._runtime_session_tree_snapshots = {"destination": [{"text": "cached"}]}
        w._get_tree_and_status = MagicMock(return_value=(MagicMock(), None))
        fin = {"n": 0}

        def _finalize():
            fin["n"] += 1

        w._destination_finalize_inflight_descendant_apply_for_snapshot_capture = _finalize
        with patch.object(MainWindow, "_destination_flush_descendant_apply_resume_to_model_payloads", lambda _s: None):
            out = MainWindow._capture_tree_items_snapshot(w, "destination")
        self.assertEqual(fin["n"], 0)
        self.assertTrue(w._post_tick_descendant_drain_pending)
        self.assertEqual(out, [{"text": "cached"}])

    def test_contractor_class_path_deferred_drain_then_completes_without_nested_tick(self):
        """Simulate: tick body holds guard; capture defers; after tick ends post_tick drains once."""
        w = MainWindow.__new__(MainWindow)
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
        w._post_tick_descendant_drain_pending = False
        w._post_tick_snapshot_refresh_pending = False
        w._post_tick_inline_descendant_schedule_pending = False
        w._post_tick_descendant_deferred_reenter_depth = 0

        def run_capture_while_tick():
            w._destination_descendant_apply_tick_running = True
            try:
                w._post_tick_descendant_drain_pending = False
                MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture(w)
                self.assertTrue(w._post_tick_descendant_drain_pending)
            finally:
                w._destination_descendant_apply_tick_running = False

        drain_runs = {"n": 0}
        real_finalize = MainWindow._destination_finalize_inflight_descendant_apply_for_snapshot_capture.__get__(w, MainWindow)

        def counting_finalize():
            drain_runs["n"] += 1
            return real_finalize()

        w._destination_finalize_inflight_descendant_apply_for_snapshot_capture = counting_finalize
        w._destination_flush_pending_leaf_batches_for_snapshot_capture = lambda: None
        with patch("ozlink_console.main_window.log_info"):
            run_capture_while_tick()
            MainWindow._destination_run_post_tick_descendant_deferred(w)
        self.assertEqual(drain_runs["n"], 1)
        self.assertFalse(w._post_tick_descendant_drain_pending)


if __name__ == "__main__":
    unittest.main()
