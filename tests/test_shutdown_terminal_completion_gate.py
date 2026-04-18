"""Shutdown: block post-tick revival, stop expand/background timers, terminal completion gate."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


class ShutdownTerminalCompletionGateTests(unittest.TestCase):
    def test_post_tick_deferred_skips_all_work_when_shutting_down(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._destination_descendant_apply_tick_running = False
        w._post_tick_descendant_drain_pending = True
        w._post_tick_snapshot_refresh_pending = True
        w._post_tick_inline_descendant_schedule_pending = True
        w._post_tick_descendant_deferred_reenter_depth = 3
        w._destination_finalize_inflight_descendant_apply_for_snapshot_capture = MagicMock()
        w._promote_destination_workspace_snapshot_after_structure_change = MagicMock()
        w._schedule_destination_descendant_apply_tick = MagicMock()
        MainWindow._destination_run_post_tick_descendant_deferred(w)
        self.assertFalse(w._post_tick_descendant_drain_pending)
        self.assertFalse(w._post_tick_snapshot_refresh_pending)
        self.assertFalse(w._post_tick_inline_descendant_schedule_pending)
        self.assertEqual(int(getattr(w, "_post_tick_descendant_deferred_reenter_depth", -1)), 0)
        w._destination_finalize_inflight_descendant_apply_for_snapshot_capture.assert_not_called()
        w._promote_destination_workspace_snapshot_after_structure_change.assert_not_called()
        w._schedule_destination_descendant_apply_tick.assert_not_called()

    def test_promote_snapshot_noop_when_shutting_down(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._destination_snapshot_capture_drain_active = False
        w._destination_descendant_apply_tick_running = False
        w._refresh_runtime_tree_snapshot = MagicMock()
        w._schedule_workspace_ui_persist = MagicMock()
        MainWindow._promote_destination_workspace_snapshot_after_structure_change(w)
        w._refresh_runtime_tree_snapshot.assert_not_called()
        w._schedule_workspace_ui_persist.assert_not_called()

    def test_schedule_workspace_ui_persist_noop_when_shutting_down(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._workspace_ui_snapshot_dirty_panels = set()
        w._expand_all_pending = {"source": False, "destination": False}
        w._workspace_ui_persist_timer = MagicMock()
        w._workspace_ui_persist_timer.start = MagicMock()
        MainWindow._schedule_workspace_ui_persist(w, delay_ms=100, panel_key="destination")
        w._workspace_ui_persist_timer.start.assert_not_called()

    def test_stop_all_shutdown_timers_stops_expand_and_background_maps(self):
        w = MainWindow.__new__(MainWindow)
        t_s = MagicMock()
        t_d = MagicMock()
        b_s = MagicMock()
        w._expand_all_timers = {"source": t_s, "destination": t_d}
        w._deferred_background_load_timers = {"source": b_s}
        MainWindow._stop_all_shutdown_mutation_timers(w)
        t_s.stop.assert_called_once()
        t_d.stop.assert_called_once()
        b_s.stop.assert_called_once()

    def test_run_deferred_background_load_skips_when_shutting_down(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._sharepoint_lazy_mode = False
        w._deferred_background_load_targets = {"source": "id1"}
        w._schedule_full_count_with_restore_backoff = MagicMock()
        w._ensure_sharepoint_destination_full_tree_worker_scheduled = MagicMock()
        MainWindow._run_deferred_background_load(w, "source")
        w._schedule_full_count_with_restore_backoff.assert_not_called()
        w._ensure_sharepoint_destination_full_tree_worker_scheduled.assert_not_called()

    def test_ensure_destination_full_tree_returns_immediately_when_shutting_down(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        MainWindow._ensure_sharepoint_destination_full_tree_worker_scheduled(w, "any-drive-id")

    @patch.object(MainWindow, "_drain_global_qthreadpool_for_shutdown")
    @patch.object(MainWindow, "_shutdown_worker_safe_join")
    @patch.object(MainWindow, "_stop_session_keepalive")
    @patch.object(MainWindow, "_stop_all_shutdown_mutation_timers")
    def test_terminal_gate_calls_timer_stop_and_pool_drain(
        self, mock_stop_timers, mock_stop_ka, mock_join, mock_drain
    ):
        w = MainWindow.__new__(MainWindow)
        w._destination_descendant_apply_queue = None
        w._destination_descendant_apply_state = None
        w._workspace_ui_persist_timer = MagicMock()
        w._workspace_ui_persist_timer.isActive = MagicMock(return_value=False)
        w._destination_descendant_apply_timer = MagicMock()
        w._destination_descendant_apply_timer.isActive = MagicMock(return_value=False)
        w._deferred_planning_refresh_timer = MagicMock()
        w._deferred_planning_refresh_timer.isActive = MagicMock(return_value=False)
        w._graph_ids_refresh_batch_timer = MagicMock()
        w._graph_ids_refresh_batch_timer.isActive = MagicMock(return_value=False)
        w._session_keepalive_timer = MagicMock()
        w._session_keepalive_timer.isActive = MagicMock(return_value=False)
        MainWindow._shutdown_terminal_completion_gate(w, phase="close_event")
        mock_stop_timers.assert_called_once()
        mock_stop_ka.assert_called_once()
        mock_drain.assert_called_once()


if __name__ == "__main__":
    unittest.main()
