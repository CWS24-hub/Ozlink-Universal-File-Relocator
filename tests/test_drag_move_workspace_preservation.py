"""Regression: manual drag follow-up must not run broad destination overlay / collapse visible planned rows."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow, _INCREMENTAL_DEFERRED_PLANNING_REFRESH_REASONS
from ozlink_console.planning_interaction_contract import is_narrow_planned_item_move_overlay_reason


def _qapp():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        QApplication([])


class DragMoveWorkspacePreservationTests(unittest.TestCase):
    def test_coalesced_incremental_reasons_skip_full_destination_under_overlay_backlog(self):
        """Merged ``planning_change_lightweight`` + manual drag must not force full destination finalize."""
        w = MainWindow.__new__(MainWindow)
        w._destination_require_deferred_full_materialize_once = False
        w.unresolved_proposed_by_parent_path = {"Root\\P": {"one": object()}}
        w.unresolved_allocations_by_parent_path = {"Root\\A": {"two": object()}}
        w.destination_tree_widget = object()
        w._destination_steady_state_full_materialize_redundant = lambda: False
        dec = MainWindow._deferred_planning_refresh_compute_skip_full_and_overlay_decision(
            w,
            ["planning_change_lightweight", "planned_item_moved_manual_drag"],
            "planning_change_lightweight__planned_item_moved_manual_drag",
        )
        self.assertTrue(dec["skip_full_destination_future_model"])
        self.assertFalse(dec["_run_dest_overlay"])

    def test_combined_deferred_materialize_reason_is_narrow_for_drag_move(self):
        self.assertTrue(
            is_narrow_planned_item_move_overlay_reason("deferred_planning_change_lightweight__planned_item_moved_manual_drag")
        )

    def test_all_incremental_reasons_in_set(self):
        self.assertIn("planning_change_lightweight", _INCREMENTAL_DEFERRED_PLANNING_REFRESH_REASONS)
        self.assertIn("planned_item_moved_manual_drag", _INCREMENTAL_DEFERRED_PLANNING_REFRESH_REASONS)

    def test_local_first_drag_preserve_check_bumps_fingerprint_when_stable(self):
        _qapp()
        w = MainWindow.__new__(MainWindow)
        w._destination_bind_scope_paths = {"Root\\A", "Root\\B"}
        w._destination_planned_chain_overlay_relax = False
        w._cancel_destination_future_async_projection = lambda _r: None
        w._destination_planning_overlay_replay_persisted_only = lambda _r: 2
        w._destination_enumerate_visible_planned_paths_and_all_visible = lambda: (["x"] * 120, ["x"] * 120)
        bump = MagicMock()
        w._bump_destination_materialized_overlay_fingerprint = bump
        with patch("ozlink_console.main_window.log_info"):
            with patch("ozlink_console.main_window.log_error") as err:
                MainWindow._apply_destination_planning_overlays_body_local_first_edit(
                    w, "local_first_edit_planned_item_moved_manual_drag"
                )
        bump.assert_called_once()
        err.assert_not_called()

    def test_local_first_drag_collapse_skips_fingerprint_bump(self):
        _qapp()
        w = MainWindow.__new__(MainWindow)
        w._destination_bind_scope_paths = {"Root\\X"}
        w._destination_planned_chain_overlay_relax = False
        w._cancel_destination_future_async_projection = lambda _r: None
        w._destination_planning_overlay_replay_persisted_only = lambda _r: 0

        _pass = [0]

        def _collapse_enum():
            _pass[0] += 1
            if _pass[0] == 1:
                return (["x"] * 500, [])
            return (["x"] * 50, [])

        w._destination_enumerate_visible_planned_paths_and_all_visible = _collapse_enum
        bump = MagicMock()
        w._bump_destination_materialized_overlay_fingerprint = bump
        with patch("ozlink_console.main_window.log_info"):
            with patch("ozlink_console.main_window.log_error"):
                MainWindow._apply_destination_planning_overlays_body_local_first_edit(
                    w, "local_first_edit_planned_item_moved_manual_drag"
                )
        bump.assert_not_called()

    def test_deferred_inner_skips_destination_overlay_for_coalesced_incremental(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_require_deferred_full_materialize_once = False
        w.unresolved_proposed_by_parent_path = {}
        w.unresolved_allocations_by_parent_path = {}
        w.destination_tree_widget = object()
        w._destination_steady_state_full_materialize_redundant = lambda: False
        overlay_calls = []

        def _no_overlay(*_a, **_k):
            overlay_calls.append(1)
            return 0

        w._apply_destination_planning_overlays = _no_overlay
        w.update_progress_summaries = lambda: None
        w._set_window_title_status = lambda: None
        w._log_restore_exception = lambda *a, **k: None
        w._destination_lifecycle_trace_TEMP = lambda **k: None
        w.destination_planning_model = object()
        w._destination_enumerate_visible_planned_paths_and_all_visible = lambda: ([], [])
        MainWindow._run_deferred_planning_refresh_inner(
            w,
            ["planning_change_lightweight", "planned_item_moved_manual_drag"],
            "planning_change_lightweight__planned_item_moved_manual_drag",
            set(),
        )
        self.assertEqual(overlay_calls, [])

    def test_manual_drag_twice_preserves_quick_path(self):
        _qapp()
        mw = MainWindow.__new__(MainWindow)
        move = {
            "source_name": "Doc.xlsx",
            "target_name": "Doc.xlsx",
            "source_path": "Src\\Doc.xlsx",
            "destination_path": "Root\\B",
            "destination_id": "d1",
            "destination_name": "B",
            "destination": {
                "id": "d1",
                "name": "B",
                "display_path": "Root\\B",
                "item_path": "Root\\B",
            },
            "source": {"name": "Doc.xlsx"},
            "status": "Draft",
        }
        mw.planned_moves = [move]
        mw._resolve_planned_move_for_destination_node = lambda node: (0, move, None)
        mw._is_move_submitted = lambda m: False
        mw._destination_row_semantic_path = lambda n: str(n.get("display_path") or n.get("item_path") or "")
        mw._find_visible_destination_item_by_path = lambda path: None
        mw._quick_remove_planned_move_from_destination_tree = lambda old: 0
        mw._reset_unresolved_allocation_queue = lambda: None
        mw._allocation_parent_candidates_for_touch_paths = lambda _t: {"Root\\B", "Root\\C"}
        mw._reapply_allocation_overlays_for_paste_touch_paths = lambda o, n, t: (1, {"Root\\C": 1})
        mw._paths_equivalent = lambda a, b, role: str(a).replace("/", "\\") == str(b).replace("/", "\\")
        scheduled = []
        mw._schedule_deferred_destination_materialization = lambda r, delay_ms=180: scheduled.append((r, delay_ms))
        lightweight = []
        mw._persist_planning_change_lightweight = lambda **kw: lightweight.append(kw)
        mw._persist_planning_change = lambda *a, **k: (_ for _ in ()).throw(AssertionError("heavy persist"))
        mw.refresh_planned_moves_table = lambda: None
        mw.planned_moves_status = MagicMock()
        mw.destination_tree_status = MagicMock()
        mw._rewrite_nonprimary_planned_moves_destination_prefix = lambda *a, **k: []
        mw._allocation_projection_path = lambda m: (
            str(m.get("destination_path") or "").rstrip("\\") + "\\Doc.xlsx"
        ).replace("/", "\\")
        mw._allocation_parent_path = lambda m: str(m.get("destination_path") or "").replace("/", "\\")
        mw._destination_target_snapshot = lambda t, p: {"id": "dx", "name": str(p).split("\\")[-1] if p else ""}
        mw._destination_planned_drag_name_taken_by_different_item = lambda *a, **k: False
        mw._bump_destination_materialized_overlay_fingerprint = lambda **kw: None

        src_b = {"name": "Doc.xlsx", "display_path": "Root\\B\\Doc.xlsx", "item_path": "Root\\B\\Doc.xlsx"}
        tgt_c = {"name": "C", "display_path": "Root\\C", "item_path": "Root\\C", "is_folder": True}
        assert mw._move_planned_destination_node(src_b, tgt_c, from_manual_planning_drag=True) is True
        assert move["destination_path"] == "Root\\C"
        src_c = {"name": "Doc.xlsx", "display_path": "Root\\C\\Doc.xlsx", "item_path": "Root\\C\\Doc.xlsx"}
        tgt_b = {"name": "B", "display_path": "Root\\B", "item_path": "Root\\B", "is_folder": True}
        assert mw._move_planned_destination_node(src_c, tgt_b, from_manual_planning_drag=True) is True
        assert move["destination_path"] == "Root\\B"
        self.assertEqual(len(lightweight), 2)
        self.assertFalse(scheduled)


if __name__ == "__main__":
    unittest.main()
