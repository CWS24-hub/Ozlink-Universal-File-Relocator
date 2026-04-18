"""DestinationTreeSnapshot must track live injected allocation descendants (dirty-flag + shutdown prep)."""

from __future__ import annotations

import unittest
from collections import OrderedDict, deque
from unittest.mock import MagicMock, patch

from ozlink_console.main_window import MainWindow
from ozlink_console.models import SessionState


class DestinationSnapshotInjectionPersistTests(unittest.TestCase):
    def test_build_draft_shell_refreshes_destination_when_dirty_and_not_tick_running(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_tree_snapshot_dirty_for_persist = True
        w._destination_descendant_apply_tick_running = False
        w._draft_shell_state = SessionState()
        w._draft_shell_state.DestinationTreeSnapshot = [{"text": "stale"}]
        w.active_draft_session_id = "DRAFT-UNIT"
        w.current_session_context = {"user_role": "user", "operator_upn": "", "tenant_domain": ""}
        w._plan_leaf_exclusions = set()
        w._needs_review_dismissed_inherited_paths = set()
        w.source_tree_widget = MagicMock()
        w._planning_browse_mode = lambda panel: "browse"

        def _cap(panel: str):
            if panel == "destination":
                return [{"text": "fresh"}]
            return []

        w._capture_tree_items_snapshot = _cap
        st = MainWindow._build_current_draft_shell_state(w, include_workspace_ui=False)
        self.assertEqual(st.DestinationTreeSnapshot, [{"text": "fresh"}])
        self.assertFalse(w._destination_tree_snapshot_dirty_for_persist)

    def test_build_draft_shell_keeps_dirty_when_tick_running_partial_refresh(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_tree_snapshot_dirty_for_persist = True
        w._destination_descendant_apply_tick_running = True
        w._draft_shell_state = SessionState()
        w._draft_shell_state.DestinationTreeSnapshot = [{"text": "stale"}]
        w.active_draft_session_id = "DRAFT-UNIT"
        w.current_session_context = {"user_role": "user"}
        w._plan_leaf_exclusions = set()
        w._needs_review_dismissed_inherited_paths = set()
        w.source_tree_widget = MagicMock()
        w._planning_browse_mode = lambda panel: "browse"
        w._capture_tree_items_snapshot = MagicMock()
        with patch("ozlink_console.main_window.log_info"):
            st = MainWindow._build_current_draft_shell_state(w, include_workspace_ui=False)
        self.assertEqual(st.DestinationTreeSnapshot, [{"text": "stale"}])
        self.assertTrue(w._destination_tree_snapshot_dirty_for_persist)
        w._capture_tree_items_snapshot.assert_not_called()

    def test_shutdown_prepare_writes_runtime_destination_snapshot(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._destination_descendant_apply_tick_running = False
        w._destination_tree_snapshot_dirty_for_persist = True
        w._runtime_session_tree_snapshots = {"destination": [{"text": "old"}]}
        w._destination_finalize_inflight_descendant_apply_for_snapshot_capture = MagicMock()

        def _cap(panel: str):
            self.assertEqual(panel, "destination")
            return [{"text": "shutdown_fresh"}]

        w._capture_tree_items_snapshot = _cap
        with patch("ozlink_console.main_window.log_info"):
            MainWindow._destination_prepare_destination_snapshot_for_shutdown_save(w)
        self.assertEqual(w._runtime_session_tree_snapshots["destination"], [{"text": "shutdown_fresh"}])
        self.assertFalse(w._destination_tree_snapshot_dirty_for_persist)

    def test_invalidate_allocation_root_evicts_subtree_cache_keys(self):
        w = MainWindow.__new__(MainWindow)
        od = OrderedDict()
        src = {"drive_id": "d1", "id": "i1", "item_path": "/a/b", "is_folder": True}
        sk = MainWindow._source_projection_descendants_cache_key_graph_subtree_stable(w, src)
        lk = MainWindow._source_projection_descendants_cache_key_for_root(w, src)
        od[sk] = [{"name": "cached"}]
        od[lk] = [{"name": "legacy"}]
        w._source_projection_descendants_cache = od
        with patch("ozlink_console.main_window.log_info"):
            MainWindow._invalidate_source_projection_descendants_cache_for_allocation_root(
                w, src, reason="test", caller="test"
            )
        self.assertNotIn(sk, od)
        self.assertNotIn(lk, od)

    def test_graph_exact_move_equivalence_case_insensitive(self):
        """Same terminal with different casing must not skip inherited projection."""
        w = MainWindow.__new__(MainWindow)
        w._tree_item_path = lambda d: str(d.get("item_path", "") or "")
        w._canonical_source_projection_path = lambda s: str(s or "")

        def _unified(p):
            return "UNIFIED"

        w._canonical_destination_projection_path = _unified
        w.normalize_memory_path = lambda p: str(p or "")
        w._canonical_destination_path_with_visible_library_anchor = lambda p: "UNIFIED"
        w._canonical_planned_memory_path_for_graph_match = lambda p: "UNIFIED"
        w._move_target_name = lambda m: "File.pdf"
        w._allocation_move_key = lambda m: "alloc-key"
        w._find_exact_planned_move_for_source_path = MagicMock(
            return_value={"destination_path": "Root\\X", "request_id": "r1"}
        )
        st = {
            "desc_index": 0,
            "descendants": [{"name": "File.pdf", "is_folder": False, "item_path": "s"}],
            "parent_ix": MagicMock(isValid=lambda: True),
            "parent_data": {},
            "allocation_destination_path": "Root\\Alloc",
            "source_root_path": "src\\root",
            "model": MagicMock(is_index_live=lambda ix: True),
            "graph_auth": True,
            "move": {"allocation_id": "a1"},
        }
        st["descendants"][0]["item_path"] = "src\\root\\sub\\FILE.PDF"
        w._destination_graph_descendant_apply_try_rebind_parent_ix = lambda s: "ok"
        with patch.object(
            MainWindow,
            "_allocation_projection_relative_source_segments",
            return_value=["sub", "FILE.PDF"],
        ), patch.object(
            MainWindow,
            "_source_tree_payload_implies_file_leaf",
            return_value=True,
        ), patch.object(MainWindow, "_is_leaf_path_excluded_for_plan", return_value=False):
            out = MainWindow._destination_graph_descendant_apply_prepare_next_descendant(w, st)
        self.assertEqual(out, "walk")
        self.assertEqual(st["desc_index"], 0)

    def test_graph_exact_move_different_destination_still_skips(self):
        w = MainWindow.__new__(MainWindow)
        w._tree_item_path = lambda d: str(d.get("item_path", "") or "")
        w._canonical_source_projection_path = lambda s: str(s or "")

        def _canon(p):
            s = str(p)
            if "Elsewhere" in s:
                return "EXACT_ONLY"
            return "INHERIT_ONLY"

        w._canonical_destination_projection_path = _canon
        w.normalize_memory_path = lambda p: str(p or "")
        w._canonical_destination_path_with_visible_library_anchor = lambda p: p
        w._canonical_planned_memory_path_for_graph_match = lambda p: p
        w._move_target_name = lambda m: "Other.pdf"
        w._allocation_move_key = lambda m: "k"
        w._find_exact_planned_move_for_source_path = MagicMock(
            return_value={"destination_path": "Root\\Elsewhere", "request_id": "r2"}
        )
        st = {
            "desc_index": 0,
            "descendants": [{"name": "Leaf.pdf", "is_folder": False}],
            "parent_ix": MagicMock(isValid=lambda: True),
            "parent_data": {},
            "allocation_destination_path": "Root\\Alloc",
            "source_root_path": "src\\root",
            "model": MagicMock(is_index_live=lambda ix: True),
            "graph_auth": True,
            "move": {"allocation_id": "a1"},
        }
        st["descendants"][0]["item_path"] = "src\\root\\sub\\Leaf.pdf"
        w._destination_graph_descendant_apply_try_rebind_parent_ix = lambda s: "ok"
        with patch.object(
            MainWindow,
            "_allocation_projection_relative_source_segments",
            return_value=["sub", "Leaf.pdf"],
        ), patch.object(
            MainWindow,
            "_source_tree_payload_implies_file_leaf",
            return_value=True,
        ), patch.object(MainWindow, "_is_leaf_path_excluded_for_plan", return_value=False), patch(
            "ozlink_console.main_window.log_info"
        ) as m_log:
            MainWindow._destination_graph_descendant_apply_prepare_next_descendant(w, st)
        self.assertEqual(st["desc_index"], 1)
        topics = [c.args[0] for c in m_log.call_args_list if c.args]
        self.assertIn("destination_exact_move_skips_allocation_descendant_projection", topics)


class DestinationStructuralPathAndShutdownTests(unittest.TestCase):
    def test_structural_invalid_file_segment_before_terminal_contractor_bank(self):
        w = MainWindow.__new__(MainWindow)
        bad = (
            r"Root3\HR\Employee Files\Contractor Resumes\Contractor bank.docx"
            r"\Amal Karunasena Commercial Cleaning Resume.pdf"
        )
        inv, reason = MainWindow._destination_structural_path_chain_invalid_for_container_parent(w, bad)
        self.assertTrue(inv)
        self.assertEqual(reason, "file_segment_before_terminal")

    def test_structural_valid_only_folders_before_terminal(self):
        w = MainWindow.__new__(MainWindow)
        ok = r"Root3\HR\Employee Files\Contractor Resumes\Amal Karunasena Commercial Cleaning Resume.pdf"
        inv, _ = MainWindow._destination_structural_path_chain_invalid_for_container_parent(w, ok)
        self.assertFalse(inv)

    def test_shutdown_overlay_flush_suppresses_shutdown_skip_for_apply_overlays(self):
        w = MainWindow.__new__(MainWindow)
        w._application_shutting_down = True
        w._destination_shutdown_pre_save_overlay_flush = True
        self.assertFalse(MainWindow._if_shutdown_skip_mutation(w, "_apply_destination_planning_overlays"))

    def test_payload_cannot_parent_when_file_row(self):
        w = MainWindow.__new__(MainWindow)
        self.assertTrue(
            MainWindow._destination_payload_cannot_serve_as_folder_parent(
                w, {"is_folder": False, "row_kind": "planned_file"}
            )
        )
        self.assertFalse(
            MainWindow._destination_payload_cannot_serve_as_folder_parent(
                w, {"is_folder": True, "row_kind": "planned_folder"}
            )
        )

    def test_graph_exact_move_invalid_file_parent_allows_inherited_projection(self):
        """Contractor bank.docx must not act as a folder parent in exact-move terminal; inherited walk proceeds."""
        w = MainWindow.__new__(MainWindow)
        w._tree_item_path = lambda d: str(d.get("item_path", "") or "")
        w._canonical_source_projection_path = lambda s: str(s or "")
        w._canonical_destination_projection_path = lambda p: p
        w.normalize_memory_path = lambda p: str(p or "")
        w._canonical_destination_path_with_visible_library_anchor = lambda p: p
        w._canonical_planned_memory_path_for_graph_match = lambda p: p
        w._move_target_name = lambda m: "Amal Karunasena Commercial Cleaning Resume.pdf"
        w._allocation_move_key = lambda m: "k"
        w._find_exact_planned_move_for_source_path = MagicMock(
            return_value={
                "destination_path": r"Root3\HR\Employee Files\Contractor Resumes\Contractor bank.docx",
                "request_id": "r-amal",
            }
        )
        st = {
            "desc_index": 0,
            "descendants": [{"name": "Amal.pdf", "is_folder": False, "item_path": "x"}],
            "parent_ix": MagicMock(isValid=lambda: True),
            "parent_data": {},
            "allocation_destination_path": r"Root3\HR\Employee Files\Contractor Resumes",
            "source_root_path": "src\\root",
            "model": MagicMock(is_index_live=lambda ix: True),
            "graph_auth": True,
            "move": {"allocation_id": "a1"},
        }
        st["descendants"][0]["item_path"] = r"src\root\Amal Karunasena Commercial Cleaning Resume.pdf"
        w._destination_graph_descendant_apply_try_rebind_parent_ix = lambda s: "ok"
        with patch.object(
            MainWindow,
            "_allocation_projection_relative_source_segments",
            return_value=["Amal Karunasena Commercial Cleaning Resume.pdf"],
        ), patch.object(
            MainWindow,
            "_source_tree_payload_implies_file_leaf",
            return_value=True,
        ), patch.object(MainWindow, "_is_leaf_path_excluded_for_plan", return_value=False), patch(
            "ozlink_console.main_window.log_info"
        ) as m_log:
            out = MainWindow._destination_graph_descendant_apply_prepare_next_descendant(w, st)
        self.assertEqual(out, "walk")
        self.assertEqual(st["desc_index"], 0)
        topics = [c.args[0] for c in m_log.call_args_list if c.args]
        self.assertIn("destination_exact_move_invalid_terminal_ignored", topics)


class DestinationUnresolvedParentPruneTests(unittest.TestCase):
    def test_unresolved_parent_terminal_file_like_is_invalid(self):
        w = MainWindow.__new__(MainWindow)
        bad = r"Root\HR\Files\Contractor bank.docx"
        inv, rsn = MainWindow._destination_unresolved_replay_parent_path_structurally_invalid(w, bad)
        self.assertTrue(inv)
        self.assertEqual(rsn, "parent_terminal_segment_file_like")

    def test_unresolved_parent_valid_folder_container_not_invalid(self):
        w = MainWindow.__new__(MainWindow)
        ok = r"Root\HR\Employee Files\Contractor Resumes"
        inv, _ = MainWindow._destination_unresolved_replay_parent_path_structurally_invalid(w, ok)
        self.assertFalse(inv)

    def test_prune_removes_invalid_allocation_parent_from_queue(self):
        w = MainWindow.__new__(MainWindow)
        w.unresolved_allocations_by_parent_path = {
            r"Root\X\bad.docx": {"m1": {"allocation_id": "a1"}},
        }
        w.unresolved_proposed_by_parent_path = {}
        w._sync_restore_destination_overlay_pending_from_unresolved_queues = MagicMock()
        w._canonical_planned_memory_path_for_graph_match = lambda p: str(p or "")
        w.normalize_memory_path = lambda p: str(p or "")
        with patch("ozlink_console.main_window.log_info") as m_log:
            out = MainWindow._destination_prune_invalid_unresolved_replay_parent_paths(w, context="unit")
        self.assertEqual(w.unresolved_allocations_by_parent_path, {})
        self.assertGreaterEqual(int(out.get("pruned_entry_total", 0) or 0), 1)
        topics = [c.args[0] for c in m_log.call_args_list if c.args]
        self.assertIn("destination_unresolved_parent_pruned_invalid", topics)

    def test_collect_unresolved_overlay_targets_skips_invalid_parents(self):
        w = MainWindow.__new__(MainWindow)
        w.unresolved_allocations_by_parent_path = {
            r"Root\X\bad.docx": {"m1": {"allocation_id": "a1"}},
        }
        w.unresolved_proposed_by_parent_path = {}
        w._allocation_parent_path = lambda m: r"Root\X\bad.docx"
        w._canonical_destination_projection_path = lambda p: str(p or "")
        w.normalize_memory_path = lambda p: str(p or "")
        paths = MainWindow._destination_collect_unresolved_overlay_target_canonical_paths(w)
        self.assertEqual(paths, [])

    def test_queue_allocation_skips_file_like_parent(self):
        w = MainWindow.__new__(MainWindow)
        w.unresolved_allocations_by_parent_path = {}
        w._allocation_parent_path = lambda m: r"Root\Sub\file.docx"
        w._allocation_move_key = lambda m: "k"
        w._log_restore_phase = MagicMock()
        w._canonical_planned_memory_path_for_graph_match = lambda p: str(p or "")
        w.normalize_memory_path = lambda p: str(p or "")
        with patch("ozlink_console.main_window.log_info"):
            MainWindow._queue_unresolved_allocation(w, {"allocation_id": "z"}, "unit")
        self.assertEqual(w.unresolved_allocations_by_parent_path, {})

    def test_shutdown_prepare_logs_bounded_flush_without_full_overlay(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_descendant_apply_tick_running = False
        w._destination_tree_snapshot_dirty_for_persist = True
        w._runtime_session_tree_snapshots = {"destination": [{"text": "old"}]}
        w._destination_finalize_inflight_descendant_apply_for_snapshot_capture = MagicMock()
        w.unresolved_allocations_by_parent_path = {}
        w.unresolved_proposed_by_parent_path = {}
        w._destination_prune_invalid_unresolved_replay_parent_paths = MagicMock(
            return_value={"pruned_entry_total": 0}
        )
        w._unresolved_allocation_queue_size = MagicMock(return_value=0)
        w._unresolved_proposed_queue_size = MagicMock(return_value=0)

        def _cap(panel: str):
            return [{"text": "shutdown_fresh"}]

        w._capture_tree_items_snapshot = _cap
        w._count_destination_model_non_placeholder_nodes = MagicMock(return_value=1)
        w._count_tree_snapshot_nodes = MagicMock(return_value=1)
        with patch("ozlink_console.main_window.log_info") as m_log:
            MainWindow._destination_prepare_destination_snapshot_for_shutdown_save(w)
        topics = [c.args[0] for c in m_log.call_args_list if c.args]
        self.assertIn("destination_shutdown_bounded_flush_start", topics)
        self.assertIn("destination_shutdown_final_snapshot_capture", topics)


class DestinationDraftSaveRuntimeSnapshotTests(unittest.TestCase):
    def test_force_persist_noop_when_not_in_save_pipeline(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_save_in_progress = False
        w._application_shutting_down = False
        w._runtime_session_tree_snapshots = {"destination": [{"stale": True}], "source": []}
        with patch("ozlink_console.main_window.log_info"):
            out = MainWindow._destination_force_live_destination_snapshot_for_session_persist(w, reason="unit")
        self.assertEqual(out, [{"stale": True}])

    def test_force_persist_updates_runtime_session_destination(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_save_in_progress = True
        w._application_shutting_down = False
        w._runtime_session_tree_snapshots = {"destination": [], "source": []}
        w._capture_tree_items_snapshot = lambda p: [{"text": "live", "data": {}, "children": []}]
        w._count_tree_snapshot_nodes = lambda s: MainWindow._count_tree_snapshot_nodes(w, s)
        w._count_destination_model_non_placeholder_nodes = MagicMock(return_value=3)
        with patch("ozlink_console.main_window.log_info"):
            out = MainWindow._destination_force_live_destination_snapshot_for_session_persist(w, reason="unit")
        self.assertEqual(len(out), 1)
        self.assertEqual(w._runtime_session_tree_snapshots["destination"], out)

    def test_build_prefers_save_override_for_destination_tree(self):
        w = MainWindow.__new__(MainWindow)
        w.active_draft_session_id = "DRAFT-UNIT"
        w._draft_shell_state = SessionState()
        w._draft_shell_state.DraftId = "DRAFT-UNIT"
        w._destination_draft_save_destination_snapshot_override = [{"text": "from_save", "data": {}, "children": []}]
        w.planning_inputs = {}
        w.source_tree_widget = MagicMock()
        w._capture_workspace_tree_state = MagicMock(
            return_value={
                "source_expanded_paths": set(),
                "destination_expanded_paths": set(),
                "source_selected_path": "",
                "destination_selected_path": "",
            }
        )
        w._panel_is_expanded_all = MagicMock(return_value=False)
        w._planning_browse_mode = lambda panel: "browse"
        w.current_session_context = {"user_role": "user", "operator_upn": "", "tenant_domain": ""}
        with patch.object(MainWindow, "_capture_tree_items_snapshot", return_value=[{"text": "src"}]):
            st = MainWindow._build_current_draft_shell_state(w, include_workspace_ui=True)
        self.assertEqual(list(st.DestinationTreeSnapshot or []), [{"text": "from_save", "data": {}, "children": []}])


class DestinationStartupSnapshotAuthorityHandoffTests(unittest.TestCase):
    def test_startup_promote_updates_runtime_snapshot_via_refresh(self):
        w = MainWindow.__new__(MainWindow)
        # Force-live walks are save/shutdown-only; promotion uses refresh without blocking the UI thread.
        w._destination_descendant_apply_tick_running = False
        w._runtime_session_tree_snapshots = {"destination": [], "source": []}
        w._destination_tree_snapshot_dirty_for_persist = True
        w._destination_flush_descendant_apply_resume_to_model_payloads = MagicMock()
        snap = [{"text": "Row", "data": {"item_path": r"Root\Lib\Branch"}, "children": []}]

        def _cap(panel: str):
            self.assertEqual(panel, "destination")
            return list(snap)

        w._capture_tree_items_snapshot = _cap
        w._count_tree_snapshot_nodes = lambda snaps: MainWindow._count_tree_snapshot_nodes(w, snaps)
        with patch("ozlink_console.main_window.log_info"):
            MainWindow._destination_startup_promote_runtime_snapshot_after_graph_bind(
                w, branch_path_excerpt=r"Root\Lib\Branch"
            )
        self.assertEqual(w._runtime_session_tree_snapshots.get("destination"), snap)
        self.assertIn(r"root\lib\branch".casefold(), w._destination_startup_promoted_semantic_paths)

    def test_handoff_before_authority_merges_dirty_snapshot(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_tree_snapshot_dirty_for_persist = True
        w._destination_descendant_apply_tick_running = False
        w._runtime_session_tree_snapshots = {"destination": [], "source": []}

        def _cap(panel: str):
            return [{"text": "x", "data": {"item_path": "P"}, "children": []}]

        w._capture_tree_items_snapshot = _cap
        with patch("ozlink_console.main_window.log_info"):
            MainWindow._destination_startup_snapshot_handoff_before_authority_full_walk(w)
        self.assertEqual(len(w._runtime_session_tree_snapshots.get("destination") or []), 1)

    def test_promoted_paths_tracked_for_merge_are_bounded(self):
        w = MainWindow.__new__(MainWindow)
        w.normalize_memory_path = lambda p: str(p or "").strip()
        w._destination_startup_promoted_semantic_paths_cap = 4
        MainWindow._destination_startup_promoted_semantic_paths_note(
            w, [f"Path\\N{i}" for i in range(20)]
        )
        self.assertLessEqual(len(w._destination_startup_promoted_semantic_paths), 4)

    def test_resume_after_authority_schedules_descendant_tick(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_startup_descendant_queue_paused_for_authority = True
        w._schedule_destination_descendant_apply_tick = MagicMock()
        with patch("ozlink_console.main_window.log_info"):
            MainWindow._destination_resume_startup_descendant_queue_after_authority_if_paused(w, phase="unit")
        self.assertFalse(w._destination_startup_descendant_queue_paused_for_authority)
        w._schedule_destination_descendant_apply_tick.assert_called_once()

    def test_startup_queue_completed_log_when_idle_after_injection(self):
        w = MainWindow.__new__(MainWindow)
        w._destination_startup_descendant_injection_active = True
        w._destination_descendant_apply_queue = deque()
        w._destination_descendant_apply_state = None
        with patch("ozlink_console.main_window.log_info") as m_log:
            MainWindow._destination_maybe_log_startup_descendant_queue_completed(w)
        topics = [c.args[0] for c in m_log.call_args_list if c.args]
        self.assertIn("destination_startup_descendant_queue_completed", topics)
        self.assertTrue(w._destination_startup_descendant_queue_completed_logged)


if __name__ == "__main__":
    unittest.main()
