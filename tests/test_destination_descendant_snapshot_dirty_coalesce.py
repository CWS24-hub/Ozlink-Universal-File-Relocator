"""Phase 1: descendant snapshot dirty coalescing / post-tick promotion batching."""

import os
import unittest
from collections import Counter
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


def _stub_snapshot_coalesce_host():
    w = MainWindow.__new__(MainWindow)
    w._destination_descendant_apply_tick_running = False
    w._destination_descendant_apply_state = None
    w._destination_descendant_apply_queue = []
    w._application_shutting_down = False
    w._workspace_ui_snapshot_dirty_panels = set()
    w._destination_tree_snapshot_dirty_for_persist = False
    w._post_tick_snapshot_refresh_pending = False
    w._post_tick_descendant_drain_pending = False
    w._post_tick_inline_descendant_schedule_pending = False
    w._post_tick_descendant_deferred_reenter_depth = 0
    w._destination_descendant_snapshot_dirty_batch_active = False
    w._destination_descendant_snapshot_dirty_pending = False
    w._destination_descendant_snapshot_dirty_reason_counts = Counter()
    w._destination_descendant_snapshot_dirty_first_mono = 0.0
    w._destination_descendant_snapshot_dirty_last_flush_ms = 0.0
    w._destination_descendant_snapshot_dirty_affected_paths = set()
    w._destination_descendant_snapshot_dirty_max_delay_timer = None
    w._destination_descendant_snapshot_dirty_suppressed_post_tick = 0
    w._destination_descendant_snapshot_dirty_flush_pending_after_scroll = False
    w._destination_descendant_snapshot_coalesce_dirty_mark_calls = 0
    w._destination_descendant_snapshot_coalesce_suppressed_marks = 0
    w._destination_descendant_snapshot_coalesce_actual_flushes = 0
    w._destination_descendant_snapshot_coalesce_post_tick_refreshes = 0
    w._destination_descendant_snapshot_coalesce_guard_skip_observed = 0
    w._destination_user_scroll_interaction_active = MagicMock(return_value=False)
    return w


class DestinationDescendantSnapshotDirtyCoalesceTests(unittest.TestCase):
    def test_many_alloc_descendant_marks_do_not_touch_post_tick_until_flush(self):
        """100 coalesced marks with high chunk threshold: no post_tick and no flushes."""
        w = _stub_snapshot_coalesce_host()
        w._destination_descendant_apply_tick_running = True
        w._destination_descendant_apply_state = {"graph_walk": True}
        w._destination_descendant_apply_queue = [1, 2]

        with (
            patch.dict(os.environ, {"OZLINK_DESCENDANT_SNAPSHOT_BATCH_CHUNK_SUPPRESSIONS": "10000"}),
            patch.object(w.__class__, "_destination_maybe_schedule_descendant_snapshot_max_delay_timer", MagicMock()),
        ):
            for i in range(100):
                MainWindow._mark_destination_tree_snapshot_dirty_after_injection(
                    w, reason="allocation_descendant_bind_segment", affected_path=f"p{i}"
                )

        self.assertEqual(w._destination_descendant_snapshot_coalesce_actual_flushes, 0)
        self.assertFalse(w._post_tick_snapshot_refresh_pending)
        self.assertEqual(int(w._destination_descendant_snapshot_coalesce_suppressed_marks), 100)
        self.assertTrue(w._destination_descendant_snapshot_dirty_pending)

    def test_chunk_threshold_triggers_flush(self):
        w = _stub_snapshot_coalesce_host()
        w._destination_descendant_apply_tick_running = True
        w._destination_descendant_apply_state = {"graph_walk": True}
        w._destination_descendant_apply_queue = [1, 2]
        with (
            patch.dict(os.environ, {"OZLINK_DESCENDANT_SNAPSHOT_BATCH_CHUNK_SUPPRESSIONS": "8"}),
            patch.object(w.__class__, "_destination_maybe_schedule_descendant_snapshot_max_delay_timer", MagicMock()),
            patch.object(w.__class__, "_destination_run_post_tick_descendant_deferred", MagicMock()),
        ):
            for i in range(8):
                MainWindow._mark_destination_tree_snapshot_dirty_after_injection(
                    w, reason="allocation_descendant_project_leaf_batch"
                )
        self.assertGreaterEqual(w._destination_descendant_snapshot_coalesce_actual_flushes, 1)

    def test_flush_includes_reason_counts_in_log(self):
        w = _stub_snapshot_coalesce_host()
        w._destination_descendant_apply_tick_running = True
        w._destination_descendant_apply_state = {"graph_walk": True}
        w._destination_descendant_apply_queue = [1]
        with patch("ozlink_console.main_window.log_info") as m_log:
            MainWindow._mark_destination_tree_snapshot_dirty_after_injection(
                w, reason="allocation_descendant_graph_overlay"
            )
            MainWindow._destination_flush_descendant_snapshot_dirty_batch(w, "unit_flush", force=True, skip_scroll_guard=True)
        topics = [(c.args[0] if c.args else "") for c in m_log.call_args_list]
        self.assertIn("destination_descendant_snapshot_dirty_flushed", topics)
        flushed = next(
            c for c in m_log.call_args_list if c.args and c.args[0] == "destination_descendant_snapshot_dirty_flushed"
        )
        self.assertIn("allocation_descendant_graph_overlay", str(flushed.kwargs.get("reason_counts", {})))

    def test_apply_complete_flushes_pending_batch(self):
        w = _stub_snapshot_coalesce_host()
        w._destination_descendant_apply_tick_running = True
        w._destination_descendant_apply_state = {"graph_walk": True}
        w._destination_descendant_apply_queue = [1]
        MainWindow._mark_destination_tree_snapshot_dirty_after_injection(w, reason="allocation_descendant_bind_segment")
        self.assertTrue(w._destination_descendant_snapshot_dirty_pending)
        with patch.object(w.__class__, "_destination_flush_descendant_snapshot_dirty_batch") as m_flush:
            MainWindow._destination_maybe_flush_descendant_snapshot_batch_after_apply_job_complete(w)
        m_flush.assert_called_once()
        # Class-patched target receives no bound self in MagicMock.call_args[0].
        self.assertEqual(m_flush.call_args[0][0], "after_descendant_apply_complete")

    def test_shutdown_prep_calls_flush(self):
        w = _stub_snapshot_coalesce_host()
        w._destination_descendant_apply_tick_running = True
        w._destination_descendant_apply_state = {"graph_walk": True}
        w._destination_descendant_apply_queue = [1]
        MainWindow._mark_destination_tree_snapshot_dirty_after_injection(w, reason="allocation_descendant_bind_segment")
        with (
            patch.object(w.__class__, "_destination_finalize_inflight_descendant_apply_for_snapshot_capture", MagicMock()),
            patch.object(w.__class__, "_destination_flush_descendant_snapshot_dirty_batch") as m_flush,
        ):
            MainWindow._destination_prepare_destination_snapshot_for_shutdown_save(w)
        self.assertTrue(any("before_shutdown" in str(c) for c in m_flush.call_args_list))

    def test_planning_mutation_bypasses_coalesce_when_burst_active(self):
        w = _stub_snapshot_coalesce_host()
        w._destination_descendant_apply_tick_running = True
        w._destination_descendant_apply_state = {"graph_walk": True}
        w._destination_descendant_apply_queue = [1]
        with patch.object(w.__class__, "_destination_maybe_schedule_descendant_snapshot_max_delay_timer", MagicMock()):
            MainWindow._mark_destination_tree_snapshot_dirty_after_injection(
                w, reason="planning_mutation:test"
            )
        self.assertTrue(w._post_tick_snapshot_refresh_pending)
        self.assertFalse(getattr(w, "_destination_descendant_snapshot_dirty_pending", False))

    def test_scroll_defer_preserves_batch(self):
        w = _stub_snapshot_coalesce_host()
        w._destination_descendant_apply_tick_running = True
        w._destination_descendant_apply_state = {"graph_walk": True}
        w._destination_descendant_apply_queue = [1]
        w._destination_user_scroll_interaction_active = MagicMock(return_value=True)
        MainWindow._mark_destination_tree_snapshot_dirty_after_injection(
            w, reason="allocation_descendant_graph_overlay"
        )
        ok = MainWindow._destination_flush_descendant_snapshot_dirty_batch(
            w, "chunk_threshold", force=False, skip_scroll_guard=False
        )
        self.assertFalse(ok)
        self.assertTrue(w._destination_descendant_snapshot_dirty_flush_pending_after_scroll)
        self.assertTrue(w._destination_descendant_snapshot_dirty_pending)

    def test_export_preflight_flushes_batched_dirty(self):
        w = _stub_snapshot_coalesce_host()
        w._destination_descendant_apply_tick_running = True
        w._destination_descendant_apply_state = {"graph_walk": True}
        w._destination_descendant_apply_queue = [1]
        MainWindow._mark_destination_tree_snapshot_dirty_after_injection(w, reason="allocation_descendant_bind_segment")
        w._memory_restore_complete = True
        w.memory_manager = MagicMock()
        with (
            patch.object(w.__class__, "_save_draft_shell", MagicMock(return_value=True)),
            patch.object(w.__class__, "_persist_workspace_snapshot_file", MagicMock()),
            patch.object(w.__class__, "_destination_flush_descendant_snapshot_dirty_batch") as m_flush,
            patch("ozlink_console.main_window.QMessageBox"),
            patch("ozlink_console.main_window.QFileDialog", MagicMock()),
        ):
            MainWindow._handle_export_draft(w)
        self.assertTrue(m_flush.called)

    def test_flush_calls_promotion_for_live_rows(self):
        w = _stub_snapshot_coalesce_host()
        w._destination_descendant_apply_tick_running = False
        w._destination_descendant_apply_state = {"graph_walk": True}
        w._destination_descendant_apply_queue = [1]
        MainWindow._mark_destination_tree_snapshot_dirty_after_injection(
            w, reason="allocation_descendant_graph_overlay"
        )
        with patch.object(w.__class__, "_destination_run_post_tick_descendant_deferred") as m_def:
            MainWindow._destination_flush_descendant_snapshot_dirty_batch(
                w, "unit", force=True, skip_scroll_guard=True
            )
        m_def.assert_called()


if __name__ == "__main__":
    unittest.main()
