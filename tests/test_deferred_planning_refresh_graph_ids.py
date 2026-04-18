"""Deferred planning refresh: graph_ids path skips redundant destination enumeration when overlays do not run."""

from __future__ import annotations

import time
import unittest
from types import MethodType
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


class DeferredPlanningRefreshGraphIdsTests(unittest.TestCase):
    def _bare_window_for_deferred_refresh(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        w._deferred_planning_refresh_pending = True
        w._deferred_planning_refresh_reasons = ["graph_ids_resolved_from_sharepoint_paths"]
        w._deferred_source_projection_paths = set()
        w._destination_require_deferred_full_materialize_once = False
        w._destination_quiet_startup_overlay_structural_suppress = False
        w._unresolved_proposed_queue_size = lambda: 0
        w._unresolved_allocation_queue_size = lambda: 0
        w._destination_lifecycle_trace_TEMP = lambda **kwargs: None
        w._destination_graph_authority_supersedes_memory_restore_gate = lambda: False
        w._memory_restore_in_progress = False
        w.update_progress_summaries = lambda: None
        w._set_window_title_status = lambda: None
        w._restore_abort_active = lambda: False
        w.destination_planning_model = MagicMock()
        return w

    def test_graph_ids_refresh_one_enumerate_when_destination_overlay_skipped(self):
        w = self._bare_window_for_deferred_refresh()
        w.destination_tree_widget = MagicMock()
        w._destination_steady_state_full_materialize_redundant = lambda: True

        enum_calls: list[int] = []

        def _counting_enum(_self):
            enum_calls.append(1)
            return (["p1"], ["p1", "p2"])

        w._destination_enumerate_visible_planned_paths_and_all_visible = MethodType(_counting_enum, w)

        with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
            with patch("ozlink_console.main_window.log_info"):
                with patch(
                    "ozlink_console.main_window.QTimer.singleShot",
                    side_effect=lambda _delay_ms, callback: callback(),
                ):
                    MainWindow._run_deferred_planning_refresh(w)

        self.assertEqual(len(enum_calls), 1, "second full-tree enumerate must be skipped when overlay does not run")

    def test_full_materialize_forces_second_enumerate(self):
        w = self._bare_window_for_deferred_refresh()
        w._destination_require_deferred_full_materialize_once = True
        w.destination_tree_widget = MagicMock()
        w._destination_steady_state_full_materialize_redundant = lambda: False
        w._apply_destination_planning_overlays = lambda *a, **k: None

        enum_calls: list[int] = []

        def _counting_enum(_self):
            enum_calls.append(1)
            return (["p1"], ["p1"])

        w._destination_enumerate_visible_planned_paths_and_all_visible = MethodType(_counting_enum, w)

        with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
            with patch("ozlink_console.main_window.log_info"):
                with patch(
                    "ozlink_console.main_window.QTimer.singleShot",
                    side_effect=lambda _delay_ms, callback: callback(),
                ):
                    MainWindow._run_deferred_planning_refresh(w)

        self.assertEqual(len(enum_calls), 2)

    def test_graph_ids_refresh_deferred_while_destination_scroll_active(self):
        """Do not run graph_ids deferred planning refresh on the GUI thread while the user scrolls."""
        w = self._bare_window_for_deferred_refresh()
        w._deferred_source_projection_paths = {"a\\b"}
        w._destination_tree_scroll_idle_ms = 280
        w._destination_tree_scroll_activity_until = time.monotonic() + 3600.0
        inner_calls: list[int] = []

        def _no_inner(_self, *_a, **_k):
            inner_calls.append(1)

        w._run_deferred_planning_refresh_inner = MethodType(_no_inner, w)
        tmr = MagicMock()
        w._deferred_planning_refresh_timer = tmr

        with patch("ozlink_console.main_window.is_dev_mode", return_value=False):
            with patch("ozlink_console.main_window.log_info"):
                MainWindow._run_deferred_planning_refresh(w)

        self.assertEqual(inner_calls, [], "inner refresh must not run while scroll interaction active")
        self.assertTrue(w._deferred_planning_refresh_pending)
        self.assertEqual(w._deferred_planning_refresh_reasons, ["graph_ids_resolved_from_sharepoint_paths"])
        self.assertEqual(w._deferred_source_projection_paths, {"a\\b"})
        tmr.stop.assert_called()
        tmr.start.assert_called_once()
        self.assertGreaterEqual(tmr.start.call_args[0][0], 120)


if __name__ == "__main__":
    unittest.main()
