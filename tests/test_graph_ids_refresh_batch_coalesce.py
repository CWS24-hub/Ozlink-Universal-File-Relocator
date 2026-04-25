"""Unit tests: coalesced graph-id → deferred planning refresh batching (scroll/hydration storm mitigation)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


def _bare_mw_for_graph_batch():
    w = MainWindow.__new__(MainWindow)
    w._application_shutting_down = False
    w._deferred_planning_refresh_running = False
    w._graph_ids_refresh_batch_paths = set()
    w._graph_ids_refresh_batch_merge_events = 0
    w._graph_ids_refresh_batch_notify_saved = False
    w._graph_ids_refresh_batch_clear_source_lookups = False
    w._graph_ids_refresh_followup_due = False
    w._graph_ids_last_coalesced_merge_events = 0
    w._canonical_source_projection_path = lambda p: str(p or "").strip() or None
    w._evict_source_path_lookup_cache_entries = MagicMock()
    w._invalidate_projection_lookup_caches = MagicMock()
    w._log_restore_exception = MagicMock()
    w.update_progress_summaries = MagicMock()
    w._notify_planning_mutation_destination_snapshot_dirty = MagicMock()
    w.source_tree_widget = None
    w.destination_tree_widget = None
    w._save_draft_shell = MagicMock()
    return w


class GraphIdsRefreshBatchCoalesceTests(unittest.TestCase):
    def test_burst_enqueues_single_timer_restart_coalesces_paths(self):
        w = _bare_mw_for_graph_batch()
        tmr = MagicMock()
        w._graph_ids_refresh_batch_timer = tmr
        starts: list[int] = []

        def _capture_start(ms):
            starts.append(int(ms))

        tmr.stop = MagicMock()
        tmr.start = MagicMock(side_effect=_capture_start)

        MainWindow._graph_ids_enqueue_refresh_batch(w, {"a\\x"}, notify_saved=False, clear_source_path_lookups=False)
        MainWindow._graph_ids_enqueue_refresh_batch(w, {"b\\y"}, notify_saved=False, clear_source_path_lookups=False)
        MainWindow._graph_ids_enqueue_refresh_batch(w, {"a\\x"}, notify_saved=False, clear_source_path_lookups=False)

        self.assertEqual(w._graph_ids_refresh_batch_merge_events, 3)
        self.assertEqual(w._graph_ids_refresh_batch_paths, {"a\\x", "b\\y"})
        self.assertEqual(len(starts), 3, "each enqueue resets single-shot coalesce window")
        tmr.stop.assert_called()

    def test_flush_unions_paths_one_queue_deferred_call(self):
        w = _bare_mw_for_graph_batch()
        w._graph_ids_refresh_batch_timer = MagicMock()
        queued: list[tuple] = []

        def _q(reason, *, source_projection_paths=None, delay_ms=None, notify_saved=True):
            queued.append((reason, set(source_projection_paths or []), delay_ms, notify_saved))

        w._queue_deferred_planning_refresh = _q

        with patch("ozlink_console.main_window.QTimer.singleShot", side_effect=lambda _ms, fn: fn()):
            w._graph_ids_refresh_batch_paths.update(["p1", "p2"])
            w._graph_ids_refresh_batch_merge_events = 5
            MainWindow._flush_graph_ids_refresh_batch(w)

        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0][0], "graph_ids_resolved_from_sharepoint_paths")
        self.assertEqual(queued[0][1], {"p1", "p2"})
        self.assertEqual(queued[0][2], 120)
        w._notify_planning_mutation_destination_snapshot_dirty.assert_called()
        w.update_progress_summaries.assert_called()

    def test_while_deferred_running_enqueue_merges_no_timer_start(self):
        w = _bare_mw_for_graph_batch()
        w._deferred_planning_refresh_running = True
        tmr = MagicMock()
        w._graph_ids_refresh_batch_timer = tmr
        MainWindow._graph_ids_enqueue_refresh_batch(w, {"only"}, notify_saved=True, clear_source_path_lookups=True)
        tmr.start.assert_not_called()
        self.assertTrue(w._graph_ids_refresh_followup_due)
        self.assertEqual(w._graph_ids_refresh_batch_paths, {"only"})
        tmr.stop.assert_called_once()

    def test_flush_while_deferred_running_defers_and_preserves_paths(self):
        w = _bare_mw_for_graph_batch()
        w._deferred_planning_refresh_running = True
        w._graph_ids_refresh_batch_paths = {"keep"}
        tmr = MagicMock()
        w._graph_ids_refresh_batch_timer = tmr
        w._queue_deferred_planning_refresh = MagicMock()
        MainWindow._flush_graph_ids_refresh_batch(w)
        w._queue_deferred_planning_refresh.assert_not_called()
        self.assertTrue(w._graph_ids_refresh_followup_due)
        self.assertEqual(w._graph_ids_refresh_batch_paths, {"keep"})
        tmr.stop.assert_called_once()

    def test_drain_after_deferred_flushes_once_with_accumulated_paths(self):
        w = _bare_mw_for_graph_batch()
        w._graph_ids_refresh_followup_due = True
        w._graph_ids_refresh_batch_paths = {"z"}
        flushed: list[int] = []

        def _flush(**_kwargs):
            flushed.append(1)

        w._flush_graph_ids_refresh_batch = _flush
        MainWindow._graph_ids_drain_followup_after_deferred_refresh(w)
        self.assertEqual(flushed, [1])
        self.assertFalse(w._graph_ids_refresh_followup_due)

    def test_coalesced_paths_match_union_of_individual_triggers(self):
        """Contract: one flush with union(paths) matches what N separate queues would have unioned."""
        batches = [{"a"}, {"b"}, {"a", "c"}]
        union_direct: set[str] = set()
        for b in batches:
            union_direct |= b
        w = _bare_mw_for_graph_batch()
        w._graph_ids_refresh_batch_timer = MagicMock()
        captured: set[str] = set()

        def _q(_reason, *, source_projection_paths=None, **_k):
            captured.update(source_projection_paths or [])

        w._queue_deferred_planning_refresh = _q
        for b in batches:
            MainWindow._graph_ids_enqueue_refresh_batch(w, b, notify_saved=False, clear_source_path_lookups=False)
        with patch("ozlink_console.main_window.QTimer.singleShot", side_effect=lambda _ms, fn: fn()):
            MainWindow._flush_graph_ids_refresh_batch(w)
        self.assertEqual(captured, union_direct)

    def test_empty_canonical_paths_still_flushes_when_merge_events_nonzero(self):
        w = _bare_mw_for_graph_batch()
        w._graph_ids_refresh_batch_timer = MagicMock()
        w._graph_ids_refresh_batch_paths.clear()
        w._graph_ids_refresh_batch_merge_events = 2
        queued = []

        def _q(reason, *, source_projection_paths=None, **_k):
            queued.append((reason, set(source_projection_paths or [])))

        w._queue_deferred_planning_refresh = _q
        with patch("ozlink_console.main_window.QTimer.singleShot", side_effect=lambda _ms, fn: fn()):
            MainWindow._flush_graph_ids_refresh_batch(w)
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0][1], set())


if __name__ == "__main__":
    unittest.main()
