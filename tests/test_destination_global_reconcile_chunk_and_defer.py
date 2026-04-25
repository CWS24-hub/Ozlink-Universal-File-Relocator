"""Global destination reconcile: overlay-queue deferral, per-tick parent cap, and path-normalization cache."""

from __future__ import annotations

from collections import deque
import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


class _GlobalReconcileHost:
    """Minimal host for :meth:`MainWindow._destination_reconcile_all_planned_parents_after_graph_update`."""

    def __init__(self) -> None:
        self.destination_planning_model = object()
        self.chunk_calls = 0
        self._destination_global_planned_reconcile_active = False
        self._destination_global_reconcile_chunk_state = None
        self._destination_overlay_active_materialize_reason = ""
        self._planning_cache_generation = 1
        self._application_shutting_down = False
        self.unresolved_proposed_by_parent_path: dict = {}
        self.unresolved_allocations_by_parent_path: dict = {}
        self.pending_folder_loads = {"source": set(), "destination": set()}
        self.folder_load_workers: dict = {}
        self.root_load_workers = {"source": {}, "destination": {}}
        self._destination_full_tree_worker = None
        self.destination_incremental_refresh_worker = None
        self._destination_restore_materialization_queue: list = []
        self._destination_planning_overlay_gui_chunk_state = None
        self._destination_fixpoint_slice_incomplete = False
        self._destination_snapshot_capture_drain_active = False
        self._destination_descendant_apply_queue: deque = deque()
        self._destination_descendant_apply_state = None
        self._destination_descendant_apply_paused_for_finalize_alloc = False
        self._destination_reconcile_in_progress = False
        self._destination_reconcile_last_completed_plan_gen = -1

    def _planning_browse_mode(self, key: str) -> str:
        return "sharepoint" if key == "destination" else "local"

    def _destination_reconcile_global_planned_parents_process_chunk(self) -> None:
        self.chunk_calls += 1

    def _destination_reconcile_global_gate_eval(self):
        return True, {"reason": "test"}

    def _destination_collect_planned_reconcile_parent_canonical_paths(self):
        return ["p1", "p2"]

    def _safe_invoke(self, _tag, fn):
        fn()

    def _unresolved_proposed_queue_size(self) -> int:
        return sum(len(entries) for entries in (self.unresolved_proposed_by_parent_path or {}).values())

    def _unresolved_allocation_queue_size(self) -> int:
        return sum(len(entries) for entries in (self.unresolved_allocations_by_parent_path or {}).values())

    def _destination_find_planning_index_for_overlay_target_path(self, *_a, **_k):
        class _Inv:
            def isValid(self) -> bool:
                return False

        return _Inv()

    def _log_restore_exception(self, *_a, **_k) -> None:
        return None

    def _destination_true_idle_diagnostics(self):
        return MainWindow._destination_true_idle_diagnostics(self)

    def _destination_is_truly_idle(self) -> bool:
        return MainWindow._destination_is_truly_idle(self)


class GlobalReconcileDeferAndChunkTests(unittest.TestCase):
    def test_defers_when_graph_ids_overlay_and_queues_nonempty(self) -> None:
        h = _GlobalReconcileHost()
        h._destination_overlay_active_materialize_reason = "deferred_graph_ids_resolved_from_sharepoint_paths"
        h.unresolved_proposed_by_parent_path = {"x": {"k": object()}}
        h._destination_schedule_global_reconcile_after_overlay_queue_drain = MagicMock()

        done = MainWindow._destination_reconcile_all_planned_parents_after_graph_update(
            h,
            gate_already_verified=True,
            _request_overlay_terminal_completion_mark=True,
        )
        self.assertFalse(done)
        h._destination_schedule_global_reconcile_after_overlay_queue_drain.assert_called_once()
        self.assertEqual(h.chunk_calls, 0)
        self.assertTrue(getattr(h, "_destination_overlay_terminal_reconcile_completion_pending", False))

    def test_skip_defer_flag_runs_reconcile_despite_queues(self) -> None:
        h = _GlobalReconcileHost()
        h._destination_overlay_active_materialize_reason = "deferred_graph_ids_resolved_from_sharepoint_paths"
        h.unresolved_proposed_by_parent_path = {"x": {"k": object()}}
        h._destination_schedule_global_reconcile_after_overlay_queue_drain = MagicMock()

        done = MainWindow._destination_reconcile_all_planned_parents_after_graph_update(
            h,
            gate_already_verified=True,
            _skip_overlay_queue_defer=True,
            _bypass_true_idle_gate=True,
        )
        self.assertTrue(done)
        h._destination_schedule_global_reconcile_after_overlay_queue_drain.assert_not_called()
        self.assertEqual(h.chunk_calls, 1)

    def test_max_parents_per_chunk_one_when_graph_overlay_reason(self) -> None:
        h = _GlobalReconcileHost()
        h._destination_overlay_active_materialize_reason = "deferred_graph_ids_resolved_from_sharepoint_paths"
        h.unresolved_proposed_by_parent_path = {}
        captured: list[int] = []

        def _capture_then_reset() -> None:
            st = getattr(h, "_destination_global_reconcile_chunk_state", None)
            if isinstance(st, dict):
                captured.append(int(st.get("max_parents_per_chunk", 0) or 0))
            h._destination_global_planned_reconcile_active = False
            h._destination_global_reconcile_chunk_state = None

        h._destination_reconcile_global_planned_parents_process_chunk = _capture_then_reset

        MainWindow._destination_reconcile_all_planned_parents_after_graph_update(h, gate_already_verified=True)
        self.assertEqual(captured, [1])

    def test_flush_deferred_invokes_reconcile_with_skip_defer(self) -> None:
        h = _GlobalReconcileHost()
        h._destination_global_reconcile_overlay_deferred = {
            "pending": True,
            "run_eta": True,
            "ticks": 0,
            "gate_already_verified": True,
            "_skip_overlay_queue_defer": True,
            "_request_overlay_terminal_completion_mark": False,
            "_bypass_true_idle_gate": False,
        }
        rec = MagicMock(return_value=True)
        h._destination_reconcile_all_planned_parents_after_graph_update = rec

        MainWindow._destination_flush_deferred_global_reconcile_if_overlay_idle(h)
        rec.assert_called_once()
        kw = rec.call_args.kwargs
        self.assertTrue(kw.get("_skip_overlay_queue_defer"))
        self.assertIsNone(getattr(h, "_destination_global_reconcile_overlay_deferred", None))


class NormalizeCanonCacheTests(unittest.TestCase):
    def test_second_identical_path_hits_cache_same_generation(self) -> None:
        w = MainWindow.__new__(MainWindow)
        w._destination_graph_root_name = "Root"
        w._planning_cache_generation = 3
        w._destination_graph_canon_norm_cache = {}
        w._destination_graph_canon_norm_cache_gen = -1
        w._destination_norm_cache_hits_pass = 0
        w._strip_legacy_snapshot_wrapper_segment = lambda p: str(p or "")
        w._planning_path_key_backslash = lambda p: str(p or "").replace("/", "\\")

        with patch("ozlink_console.main_window.log_info"):
            MainWindow._normalize_to_graph_canonical_path(w, "Finance\\Sub")
            MainWindow._normalize_to_graph_canonical_path(w, "Finance\\Sub")

        self.assertEqual(int(getattr(w, "_destination_norm_cache_hits_pass", 0) or 0), 1)


class ReconcileStatsTests(unittest.TestCase):
    def test_invoke_planned_reconcile_returns_no_op_counter_key(self) -> None:
        w = MainWindow.__new__(MainWindow)
        w.destination_planning_model = None
        out = MainWindow._destination_invoke_planned_workspace_reconcile_after_graph_folder_load(
            w,
            MagicMock(),
            [],
            allow_reappend=True,
        )
        self.assertIn("no_op", out)
        self.assertEqual(out["no_op"], 0)


if __name__ == "__main__":
    unittest.main()
