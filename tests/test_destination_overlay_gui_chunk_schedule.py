"""GUI-chunked destination overlay pass for graph-id deferred materialize (cooperative yield)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow


class DestinationOverlayGuiChunkScheduleTests(unittest.TestCase):
    def test_begin_gui_chunked_pass_starts_timer_and_sets_state(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        before = int(getattr(w, "_destination_planning_overlay_gui_chunk_gen", 0) or 0)
        tm = MagicMock()
        w._destination_planning_overlay_gui_chunk_timer = tm
        with patch("ozlink_console.main_window.log_info"):
            ret = MainWindow._destination_planning_overlay_begin_gui_chunked_pass(
                w,
                "deferred_graph_ids_resolved_from_sharepoint_paths__x",
                allow_defer=True,
                prefer_chunked_projection=True,
                narrow_restore_real_snapshot=False,
            )
        self.assertEqual(ret, 0)
        self.assertEqual(int(getattr(w, "_destination_planning_overlay_gui_chunk_gen", 0)), before + 1)
        st = getattr(w, "_destination_planning_overlay_gui_chunk_state", None)
        self.assertIsInstance(st, dict)
        self.assertEqual(st.get("phase"), 0)
        self.assertEqual(st.get("reason"), "deferred_graph_ids_resolved_from_sharepoint_paths__x")
        tm.start.assert_called_once_with(0)

    def test_chunk_tick_advances_phase_zero_to_one(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = False
        w._destination_planning_overlay_gui_chunk_gen = 1
        w._destination_planning_overlay_gui_chunk_state = {
            "gen": 1,
            "phase": 0,
            "reason": "deferred_graph_ids_resolved_test",
            "chunk_seq": 0,
            "t_pass0": 0.0,
        }
        tm = MagicMock()
        w._destination_planning_overlay_gui_chunk_timer = tm
        w._destination_materialize_profile_start_cycle = MagicMock()
        w._destination_expanded_paths_for_planning_bind = MagicMock(return_value={"p"})
        w._destination_selected_path_for_planning_bind = MagicMock(return_value="sel")
        w._replay_unresolved_proposed_overlay = MagicMock(return_value=2)
        w._replay_unresolved_allocation_overlay = MagicMock(return_value=3)
        w._destination_finalize_pass_log_reason = MagicMock(return_value=False)
        with patch(
            "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
            return_value=False,
        ):
            with patch("ozlink_console.main_window.log_info"):
                MainWindow._destination_planning_overlay_body_gui_chunk_tick(w)
        self.assertEqual(w._destination_planning_overlay_gui_chunk_state.get("phase"), 1)
        self.assertEqual(w._destination_planning_overlay_gui_chunk_state.get("n_prop"), 2)
        self.assertEqual(w._destination_planning_overlay_gui_chunk_state.get("n_alloc"), 3)
        tm.start.assert_called_once_with(0)

    def test_sync_body_does_not_use_chunk_for_non_graph_reason_with_prefer_chunked(self):
        """prefer_chunked alone must not trigger GUI chunking (only graph deferred reasons)."""
        w = MainWindow.__new__(MainWindow)
        w._destination_planning_overlay_begin_gui_chunked_pass = MagicMock(return_value=0)
        # Force entry past early returns in body — minimal stubs
        w._schedule_proactive_graph_parent_chains_for_unresolved_overlays = MagicMock()
        w._try_skip_redundant_destination_future_model_materialize = MagicMock(return_value=None)
        w._cancel_destination_future_async_projection = MagicMock()
        w._destination_root_prime_pending = False
        w._destination_full_tree_ready = MagicMock(return_value=True)
        w._destination_sharepoint_planning_destination_active = MagicMock(return_value=False)
        w._destination_future_model_blocked_by_source_restore = MagicMock(return_value=False)
        w._destination_snapshot_chunked_restore_active = False
        w._should_defer_destination_materialization = MagicMock(return_value=False)
        w._log_restore_phase = MagicMock()
        w._startup_post_snapshot_trace_event = MagicMock()
        w._destination_materialize_profile_start_cycle = MagicMock()
        w._destination_expanded_paths_for_planning_bind = MagicMock(return_value=set())
        w._destination_selected_path_for_planning_bind = MagicMock(return_value="")
        w._destination_full_tree_idle_light_overlay = False
        w._destination_finalize_pass_log_reason = MagicMock(return_value=False)
        w._replay_unresolved_proposed_overlay = MagicMock(return_value=0)
        w._replay_unresolved_allocation_overlay = MagicMock(return_value=0)
        w._apply_visible_destination_allocation_descendants = MagicMock(return_value=0)
        w._hydrate_destination_allocations_for_expanded_paths_model = MagicMock()
        w._hydrate_destination_prefix_chain_for_path_model = MagicMock()
        w._schedule_refresh_destination_tree_indicators = MagicMock()
        w._restore_expanded_tree_paths = MagicMock()
        w._restore_selected_tree_path = MagicMock()
        w._refresh_expand_all_button_for_panel = MagicMock()
        w._reconcile_destination_semantic_duplicates = MagicMock()
        w._current_destination_full_overlay_fingerprint = MagicMock(return_value="fp")
        w._bump_destination_materialized_overlay_fingerprint = MagicMock()
        w._set_tree_status_message = MagicMock()
        w._destination_on_authoritative_tree_bind_committed = MagicMock()
        w._destination_audit_overlay_placement_after_pass = MagicMock()
        w._destination_planning_overlay_run_terminal_reconcile_if_needed = MagicMock()
        w._destination_planning_overlay_emit_finalize_finished_log = MagicMock()
        with patch(
            "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
            return_value=False,
        ):
            MainWindow._apply_destination_planning_overlays_body(
                w,
                "folder_worker_success",
                allow_defer=True,
                prefer_chunked_projection=True,
                narrow_restore_real_snapshot=False,
                force_authoritative_bind=False,
            )
        w._destination_planning_overlay_begin_gui_chunked_pass.assert_not_called()


if __name__ == "__main__":
    unittest.main()
