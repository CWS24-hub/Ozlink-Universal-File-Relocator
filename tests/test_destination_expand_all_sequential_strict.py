"""Strict sequential destination expand-all: expansion-scope merge and materialization gates."""

import inspect
import os
import unittest
from collections import deque
from unittest.mock import MagicMock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, QPersistentModelIndex, Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel

from ozlink_console.main_window import MainWindow


def _bare_main_window_for_seq_tests():
    mw = MainWindow.__new__(MainWindow)
    mw.pending_folder_loads = {"destination": set()}
    mw.pending_root_drive_ids = {"destination": ""}
    mw.root_load_workers = {}
    mw._destination_descendant_apply_state = None
    mw._destination_descendant_apply_queue = deque()
    mw._destination_expand_user_deferred_queue = deque()
    mw._expand_all_pending = {"destination": True}
    mw._destination_expand_all_sequential_mode = True
    mw._destination_incremental_merge_session = None
    mw._destination_restore_materialization_queue = []
    mw._destination_root_prime_pending = False
    mw._restore_destination_overlay_pending = False
    mw._destination_structure_blocked_reason = ""
    mw._destination_full_tree_ready = lambda: True
    mw._destination_snapshot_chunked_restore_active = False
    mw._destination_bind_scope_paths = None
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    mw._log_restore_phase = lambda *a, **k: None
    mw.destination_tree_widget = MagicMock()
    mw._destination_tree_uses_model_view = lambda: True
    mw._planning_tree_top_level_count = lambda _t: 0
    mw._flush_pending_destination_library_root_if_any = lambda **_k: None
    mw.planned_moves = []
    mw.proposed_folders = []
    return mw


def _model_folder_with_child_payloads(folder_pl: dict, child_payloads: list):
    m = QStandardItemModel()
    root = m.invisibleRootItem()
    parent_it = QStandardItem("folder")
    parent_it.setData(folder_pl, Qt.UserRole)
    for pl in child_payloads:
        ch = QStandardItem("c")
        ch.setData(pl, Qt.UserRole)
        parent_it.appendRow(ch)
    root.appendRow(parent_it)
    return m, m.indexFromItem(parent_it)


class DestinationExpandAllSequentialStrictTests(unittest.TestCase):
    def test_root_normalization_reparents_root_children_from_top_level(self):
        mw = _bare_main_window_for_seq_tests()
        nodes = {
            "Root": {"parent_semantic_path": "", "node_state": "real", "name": "Root", "data": {"is_folder": True}},
            "Root\\Finance": {"parent_semantic_path": "", "node_state": "real", "name": "Finance", "data": {"is_folder": True}},
            "Root\\HR": {"parent_semantic_path": "", "node_state": "real", "name": "HR", "data": {"is_folder": True}},
        }
        out = MainWindow._normalize_destination_future_root_topology(mw, nodes)
        self.assertTrue(out["root_present"])
        self.assertEqual(out["reparented"], 2)
        self.assertEqual(nodes["Root\\Finance"]["parent_semantic_path"], "Root")
        self.assertEqual(nodes["Root\\HR"]["parent_semantic_path"], "Root")

    def test_package_model_has_single_root_top_level(self):
        mw = _bare_main_window_for_seq_tests()
        mw._log_restore_phase = lambda *a, **k: None
        nodes = {
            "Root": {"semantic_path": "Root", "parent_semantic_path": "", "node_state": "real", "name": "Root", "data": {"is_folder": True}},
            "Root\\Finance": {"semantic_path": "Root\\Finance", "parent_semantic_path": "", "node_state": "real", "name": "Finance", "data": {"is_folder": True}},
            "Root\\HR": {"semantic_path": "Root\\HR", "parent_semantic_path": "", "node_state": "real", "name": "HR", "data": {"is_folder": True}},
        }
        out = MainWindow._package_destination_future_model(
            mw,
            nodes,
            {},
            total_real_nodes=3,
            total_proposed_nodes=0,
            total_allocation_nodes=0,
        )
        self.assertEqual(out["top_level_paths"], ["Root"])
        self.assertIn("Root\\Finance", out["nodes"]["Root"]["children"])
        self.assertIn("Root\\HR", out["nodes"]["Root"]["children"])

    def test_no_mixed_root_and_child_top_level_structure(self):
        mw = _bare_main_window_for_seq_tests()
        mw._log_restore_phase = lambda *a, **k: None
        nodes = {
            "Root": {"semantic_path": "Root", "parent_semantic_path": "", "node_state": "real", "name": "Root", "data": {"is_folder": True}},
            "Finance": {"semantic_path": "Finance", "parent_semantic_path": "", "node_state": "real", "name": "Finance", "data": {"is_folder": True}},
            "Root\\Sales": {"semantic_path": "Root\\Sales", "parent_semantic_path": "", "node_state": "real", "name": "Sales", "data": {"is_folder": True}},
        }
        out = MainWindow._package_destination_future_model(
            mw,
            nodes,
            {},
            total_real_nodes=3,
            total_proposed_nodes=0,
            total_allocation_nodes=0,
        )
        # Coalesce + normalization eliminate mixed Root/child top-level shape.
        self.assertEqual(out["top_level_paths"], ["Root"])
        root_children = out["nodes"]["Root"]["children"]
        self.assertIn("Root\\Finance", root_children)
        self.assertIn("Root\\Sales", root_children)
        self.assertNotIn("Root\\Sales", out["top_level_paths"])
        self.assertNotIn("Root\\Finance", out["top_level_paths"])

    def test_when_root_present_all_parentless_nodes_become_root_children(self):
        mw = _bare_main_window_for_seq_tests()
        nodes = {
            "Root": {"parent_semantic_path": "", "node_state": "real", "name": "Root", "data": {"is_folder": True}},
            "RandomA": {"parent_semantic_path": "", "node_state": "real", "name": "RandomA", "data": {"is_folder": True}},
            "Root\\Dept": {"parent_semantic_path": "", "node_state": "real", "name": "Dept", "data": {"is_folder": True}},
        }
        out = MainWindow._normalize_destination_future_root_topology(mw, nodes)
        self.assertEqual(out["reparented"], 2)
        self.assertEqual(nodes["RandomA"]["parent_semantic_path"], "Root")
        self.assertEqual(nodes["Root\\Dept"]["parent_semantic_path"], "Root")

    def test_bind_uses_only_top_level_paths_root_only_when_present(self):
        mw = _bare_main_window_for_seq_tests()
        model = {
            "nodes": {
                "Root": {"parent_semantic_path": "", "children": [], "data": {"is_folder": True}},
                "Root\\Finance": {"parent_semantic_path": "Root", "children": [], "data": {"is_folder": True}},
            },
            "top_level_paths": ["Root", "Root\\Finance"],
        }
        out = MainWindow._destination_bind_resolved_top_level_paths(mw, model)
        self.assertEqual(out, ["Root"])

    def test_bind_widget_rejects_root_append_outside_top_level_paths(self):
        mw = _bare_main_window_for_seq_tests()
        tree = MagicMock()
        mw._build_destination_tree_item_from_future_model = MagicMock(return_value=object())
        MainWindow._destination_bind_add_top_level_item_widget(
            mw,
            tree,
            {"Root\\Finance": {"data": {"is_folder": True}}},
            "Root\\Finance",
            {"Root"},
            bind_tick_context="unit_test",
        )
        tree.addTopLevelItem.assert_not_called()

    def test_bind_widget_allows_root_append_inside_top_level_paths(self):
        mw = _bare_main_window_for_seq_tests()
        tree = MagicMock()
        built_item = object()
        mw._build_destination_tree_item_from_future_model = MagicMock(return_value=built_item)
        MainWindow._destination_bind_add_top_level_item_widget(
            mw,
            tree,
            {"Root": {"data": {"is_folder": True}}},
            "Root",
            {"Root"},
            bind_tick_context="unit_test",
        )
        tree.addTopLevelItem.assert_called_once_with(built_item)

    def test_projection_path_widget_blocks_top_level_insert_when_root_missing(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_tree_uses_model_view = lambda: False
        mw.destination_tree_widget = MagicMock()
        mw._find_visible_destination_item_by_path = lambda _p: None
        mw._find_destination_child_by_path = lambda _parent, _path: None
        mw._build_projected_destination_folder_node = MagicMock()
        out = MainWindow._ensure_destination_projection_path(mw, r"Root\Finance\Payroll")
        self.assertIsNone(out)
        mw.destination_tree_widget.addTopLevelItem.assert_not_called()

    def test_projection_path_widget_creates_intermediate_under_root_only(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_tree_uses_model_view = lambda: False
        tree = MagicMock()
        mw.destination_tree_widget = tree
        mw._refresh_destination_item_visibility = lambda *a, **k: None
        root_item = MagicMock()
        root_item.data.return_value = {"item_path": "Root", "name": "Root", "is_folder": True}
        mw._find_visible_destination_item_by_path = lambda p: root_item if p == "Root" else None
        mw._find_destination_child_by_path = lambda _parent, _path: None
        child_item = MagicMock()
        child_item.data.return_value = {"item_path": r"Root\Finance", "name": "Finance", "is_folder": True}
        mw._build_projected_destination_folder_node = MagicMock(return_value=child_item)
        out = MainWindow._ensure_destination_projection_path(mw, r"Root\Finance")
        self.assertIsNotNone(out)
        root_item.addChild.assert_called_once_with(child_item)
        tree.addTopLevelItem.assert_not_called()

    def test_projection_path_model_never_inserts_with_invalid_root_parent(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_tree_uses_model_view = lambda: True
        mw.destination_tree_widget = MagicMock()
        mw._refresh_destination_item_visibility_index = lambda *a, **k: None

        class _FakeModel(QStandardItemModel):
            def __init__(self):
                super().__init__()
                self.append_parent_valid_flags = []

            def append_child_payloads(self, parent, payloads):
                self.append_parent_valid_flags.append(bool(parent.isValid()))
                parent_item = self.itemFromIndex(parent) if parent.isValid() else self.invisibleRootItem()
                for pl in payloads:
                    it = QStandardItem(str(pl.get("name") or "x"))
                    it.setData(dict(pl), Qt.UserRole)
                    parent_item.appendRow(it)

        dm = _FakeModel()
        root = QStandardItem("Root")
        root.setData({"item_path": "Root", "name": "Root", "is_folder": True}, Qt.UserRole)
        dm.invisibleRootItem().appendRow(root)
        mw.destination_planning_model = dm
        root_ix = dm.index(0, 0, QModelIndex())
        mw._find_visible_destination_item_by_path = lambda p: root_ix if p == "Root" else None
        mw._find_destination_child_by_path = lambda _parent, _path: None
        out = MainWindow._ensure_destination_projection_path_model(mw, r"Root\HR", r"Root\HR")
        self.assertIsNotNone(out)
        self.assertTrue(dm.append_parent_valid_flags)
        self.assertTrue(all(dm.append_parent_valid_flags))

    def test_destination_structure_ready_false_when_blocked_reason_set(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_structure_blocked_reason = "source_descendants_missing_graph_ids"
        self.assertFalse(MainWindow._destination_structure_ready(mw, "unit", log_waiting=False))

    def test_destination_structure_ready_true_when_full_tree_and_not_blocked(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_structure_blocked_reason = ""
        self.assertTrue(MainWindow._destination_structure_ready(mw, "unit", log_waiting=False))

    def test_merge_session_unrelated_does_not_gate(self):
        mw = _bare_main_window_for_seq_tests()
        folder_sp = r"Root\TeamA\ClientB"
        sess = {
            "phase": "attach",
            "parent_order": [r"Root\OtherZ"],
            "parent_i": 0,
            "root_j": 0,
            "by_parent": {r"Root\OtherZ": [r"Root\OtherZ\Leaf"]},
            "entry_roots": [r"Root\OtherZ\Leaf"],
            "fill_stack": [],
            "current_merge_root_path": r"Root\OtherZ\Leaf",
        }
        pend, why = MainWindow._destination_expand_all_seq_merge_remaining_for_subtree(mw, folder_sp, sess)
        self.assertFalse(pend, why)

    def test_merge_session_subtree_root_pending_gates(self):
        mw = _bare_main_window_for_seq_tests()
        folder_sp = r"Root\TeamA\ClientB"
        sess = {
            "phase": "attach",
            "parent_order": [r"Root\TeamA"],
            "parent_i": 0,
            "root_j": 0,
            "by_parent": {r"Root\TeamA": [r"Root\TeamA\ClientB", r"Root\TeamA\ClientC"]},
            "entry_roots": [r"Root\TeamA\ClientB"],
            "fill_stack": [],
            "current_merge_root_path": r"Root\TeamA\ClientB",
        }
        pend, why = MainWindow._destination_expand_all_seq_merge_remaining_for_subtree(mw, folder_sp, sess)
        self.assertTrue(pend, why)

    def test_merge_deep_attach_path_does_not_gate_root(self):
        """Deep merge targets (two+ segments below folder) must not block Root completion."""
        mw = _bare_main_window_for_seq_tests()
        folder_sp = r"Root"
        sess = {
            "phase": "attach",
            "parent_order": [r"Root\Libraries"],
            "parent_i": 0,
            "root_j": 0,
            "by_parent": {r"Root\Libraries": [r"Root\Libraries\Deep\Leaf"]},
            "entry_roots": [r"Root\Libraries\Deep\Leaf"],
            "fill_stack": [],
            "current_merge_root_path": r"Root\Libraries\Deep\Leaf",
        }
        pend, why = MainWindow._destination_expand_all_seq_merge_remaining_for_subtree(mw, folder_sp, sess)
        self.assertFalse(pend, why)

    def test_merge_direct_child_of_root_pending_gates(self):
        mw = _bare_main_window_for_seq_tests()
        folder_sp = r"Root"
        sess = {
            "phase": "attach",
            "parent_order": [r"Root"],
            "parent_i": 0,
            "root_j": 0,
            "by_parent": {r"Root": [r"Root\Libraries"]},
            "entry_roots": [r"Root\Libraries"],
            "fill_stack": [],
            "current_merge_root_path": r"Root\Libraries",
        }
        pend, why = MainWindow._destination_expand_all_seq_merge_remaining_for_subtree(mw, folder_sp, sess)
        self.assertTrue(pend, why)

    def test_descendant_apply_exact_folder_pending_blocks(self):
        mw = _bare_main_window_for_seq_tests()
        pix = MagicMock()
        pix.isValid.return_value = True
        pix.data.return_value = {"is_folder": True, "destination_path": r"Root\Alloc\Sub"}

        mw._destination_semantic_path = lambda pl: str(pl.get("destination_path") or "")
        mw._tree_item_path = lambda pl: ""

        mw._destination_descendant_apply_queue.append((pix, None, None))
        self.assertTrue(mw._destination_expand_all_seq_descendant_apply_pending_for_active_folder(r"Root\Alloc\Sub"))

    def test_descendant_apply_deeper_path_blocks_parent_until_subtree_applied(self):
        """Branch-complete: descendant apply for a child path under the active folder gates the parent row."""
        mw = _bare_main_window_for_seq_tests()
        pix = MagicMock()
        pix.isValid.return_value = True
        pix.data.return_value = {"is_folder": True, "destination_path": r"Root\Alloc\Sub"}

        mw._destination_semantic_path = lambda pl: str(pl.get("destination_path") or "")
        mw._tree_item_path = lambda pl: ""

        mw._destination_descendant_apply_queue.append((pix, None, None))
        self.assertTrue(mw._destination_expand_all_seq_descendant_apply_pending_for_active_folder(r"Root\Alloc"))

    def test_projection_pending_on_direct_row_blocks_branch_materialization(self):
        mw = _bare_main_window_for_seq_tests()
        model, parent_ix = _model_folder_with_child_payloads(
            {"is_folder": True},
            [{"placeholder": True, "placeholder_role": "projection_pending", "is_folder": False}],
        )
        mp, why = mw._destination_expand_all_seq_model_branch_materialization_pending(
            model, parent_ix, r"Root\X"
        )
        self.assertTrue(mp, why)
        self.assertIn("projection_pending", why)

    def test_deep_grandchild_projection_pending_blocks_branch_until_resolved(self):
        """DFS branch scan includes descendants; nested projection_pending must gate parent folder completion."""
        mw = _bare_main_window_for_seq_tests()
        m = QStandardItemModel()
        inv = m.invisibleRootItem()
        parent_it = QStandardItem("folder")
        parent_it.setData({"is_folder": True}, Qt.UserRole)
        mid = QStandardItem("mid")
        mid.setData({"is_folder": True}, Qt.UserRole)
        deep = QStandardItem("deep")
        deep.setData(
            {"placeholder": True, "placeholder_role": "projection_pending", "is_folder": False},
            Qt.UserRole,
        )
        mid.appendRow(deep)
        parent_it.appendRow(mid)
        inv.appendRow(parent_it)
        parent_ix = m.indexFromItem(parent_it)
        mp, why = mw._destination_expand_all_seq_model_branch_materialization_pending(
            m, parent_ix, r"Root\X"
        )
        self.assertTrue(mp, why)
        self.assertIn("projection_pending", why)

    def test_non_root_folder_waits_for_branch_loading_rows(self):
        mw = _bare_main_window_for_seq_tests()
        model, parent_ix = _model_folder_with_child_payloads(
            {"is_folder": True},
            [{"placeholder": True, "placeholder_role": "loading_in_progress", "is_folder": False}],
        )
        mp, why = mw._destination_expand_all_seq_model_branch_materialization_pending(
            model, parent_ix, r"Root\Nested"
        )
        self.assertTrue(mp, why)
        self.assertIn("loading_in_progress", why)

    def test_terminal_empty_not_materialization_pending(self):
        mw = _bare_main_window_for_seq_tests()
        model, parent_ix = _model_folder_with_child_payloads(
            {"is_folder": True},
            [{"placeholder": True, "placeholder_role": "terminal_empty", "is_folder": False}],
        )
        mp, _ = mw._destination_expand_all_seq_model_branch_materialization_pending(
            model, parent_ix, r"Root\X"
        )
        self.assertFalse(mp)

    def test_terminal_empty_requires_settled_folder_load(self):
        mw = _bare_main_window_for_seq_tests()
        model, parent_ix = _model_folder_with_child_payloads(
            {"is_folder": True, "children_loaded": False, "load_failed": False},
            [{"placeholder": True, "placeholder_role": "terminal_empty", "is_folder": False}],
        )
        self.assertFalse(mw._destination_expand_all_seq_is_terminal_empty_folder(model, parent_ix))

    def test_terminal_empty_true_when_children_settled(self):
        mw = _bare_main_window_for_seq_tests()
        model, parent_ix = _model_folder_with_child_payloads(
            {"is_folder": True, "children_loaded": True, "load_failed": False},
            [{"placeholder": True, "placeholder_role": "terminal_empty", "is_folder": False}],
        )
        self.assertTrue(mw._destination_expand_all_seq_is_terminal_empty_folder(model, parent_ix))

    def test_folder_complete_source_has_no_timeout_or_patience(self):
        src = inspect.getsource(MainWindow._destination_expand_all_seq_folder_complete)
        low = src.lower()
        self.assertNotIn("timeout", low)
        self.assertNotIn("patience", low)

    def test_sequential_tick_source_has_no_timeout_forced_advance(self):
        src = inspect.getsource(MainWindow._destination_model_expand_all_tick_sequential)
        self.assertNotIn("folder_timeout", src)
        self.assertNotIn("timed_out", src)

    def test_merge_sibling_root_remaining_does_not_gate(self):
        mw = _bare_main_window_for_seq_tests()
        folder_sp = r"Root\TeamA\ClientB"
        sess = {
            "phase": "attach",
            "parent_order": [r"Root\TeamA"],
            "parent_i": 0,
            "root_j": 1,
            "by_parent": {r"Root\TeamA": [r"Root\TeamA\ClientB", r"Root\TeamA\ClientC"]},
            "entry_roots": [r"Root\TeamA\ClientB", r"Root\TeamA\ClientC"],
            "fill_stack": [],
            "current_merge_root_path": "",
        }
        pend, why = MainWindow._destination_expand_all_seq_merge_remaining_for_subtree(mw, folder_sp, sess)
        self.assertFalse(pend, why)

    def test_post_attach_sort_immediate_parent_pending_gates(self):
        mw = _bare_main_window_for_seq_tests()
        folder_sp = r"Root\TeamA\ClientB"
        sess = {
            "phase": "post_attach_sort",
            "parents_sort_queue": [r"Root\TeamA", r"Root\Other"],
            "parent_sort_i": 0,
            "by_parent": {r"Root\TeamA": [folder_sp]},
            "entry_roots": [folder_sp],
        }
        pend, why = MainWindow._destination_expand_all_seq_merge_remaining_for_subtree(mw, folder_sp, sess)
        self.assertTrue(pend, why)

    def test_post_attach_sort_distant_ancestor_does_not_gate(self):
        """Sort work queued only under a far ancestor must not block a deep folder's completion."""
        mw = _bare_main_window_for_seq_tests()
        folder_sp = r"Root\TeamA\ClientB"
        sess = {
            "phase": "post_attach_sort",
            "parents_sort_queue": [r"Root", r"Root\Other"],
            "parent_sort_i": 0,
            "by_parent": {},
            "entry_roots": [],
        }
        pend, why = MainWindow._destination_expand_all_seq_merge_remaining_for_subtree(mw, folder_sp, sess)
        self.assertFalse(pend, why)

    def test_folder_not_complete_while_branch_model_work_remains(self):
        """Branch-complete: placeholder under the folder subtree blocks completion (strict, no timeout)."""
        mw = _bare_main_window_for_seq_tests()
        model, parent_ix = _model_folder_with_child_payloads(
            {"is_folder": True, "children_loaded": True},
            [{"placeholder": True, "placeholder_role": "projection_pending", "is_folder": False}],
        )
        pl = parent_ix.data(Qt.UserRole) or {}
        done, why = mw._destination_expand_all_seq_folder_complete(model, parent_ix, pl, r"Root\First")
        self.assertFalse(done)
        self.assertIn("model_branch", why)

    def test_restore_finalize_not_blocked_by_destination_folder_loads_during_strict_expand_all(self):
        mw = _bare_main_window_for_seq_tests()
        mw.pending_folder_loads = {"destination": {"d:1"}}
        mw._memory_restore_in_progress = True
        mw._memory_restore_complete = False
        mw._suppress_autosave = True
        mw._restore_abort_active = lambda: False
        mw._unresolved_proposed_queue_size = lambda: 0
        mw._unresolved_allocation_queue_size = lambda: 0
        mw._memory_restore_apply_finalize_success = MagicMock()
        self.assertTrue(MainWindow._finalize_memory_restore_if_ready(mw, "unit_test"))
        mw._memory_restore_apply_finalize_success.assert_called_once_with("unit_test")

    def test_restore_finalize_still_blocked_by_folder_loads_when_not_strict_expand_all(self):
        mw = _bare_main_window_for_seq_tests()
        mw._expand_all_pending["destination"] = False
        mw.pending_folder_loads = {"destination": {"d:1"}}
        mw._memory_restore_in_progress = True
        mw._memory_restore_complete = False
        mw._suppress_autosave = True
        mw._restore_abort_active = lambda: False
        mw._unresolved_proposed_queue_size = lambda: 0
        mw._unresolved_allocation_queue_size = lambda: 0
        mw._memory_restore_apply_finalize_success = MagicMock()
        mw._log_restore_phase = MagicMock()
        mw._suppress_selector_change_handlers = False
        self.assertFalse(MainWindow._finalize_memory_restore_if_ready(mw, "unit_test"))
        mw._memory_restore_apply_finalize_success.assert_not_called()

    def test_burst_schedule_no_queue_when_strict_sequential_expand_all(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_expand_burst_queue = []
        mw._destination_expand_burst_timer = MagicMock()
        mw._destination_bump_interactive_expand_idle_full_tree_lockout = MagicMock()
        MainWindow._destination_expand_burst_schedule(
            mw, {"trigger_path_captured": r"Root\A"}
        )
        self.assertEqual(mw._destination_expand_burst_queue, [])

    def test_sequential_tick_finishes_when_dfs_idle_and_no_pending_loads(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_expand_all_dfs_roots_deque = deque()
        mw._destination_expand_all_dfs_stack = []
        mw.destination_tree_widget = MagicMock()
        mw.destination_planning_model = MagicMock()
        mw._destination_model_expand_all_tick_sequential = MagicMock()
        mw._finish_destination_model_expand_all = MagicMock()
        mw._safe_invoke = lambda _n, fn, *a, **k: fn(*a, **k)
        MainWindow._destination_model_expand_all_tick(mw)
        mw._destination_model_expand_all_tick_sequential.assert_called_once()
        mw._finish_destination_model_expand_all.assert_called_once()

    def test_run_burst_pipeline_discards_when_strict_sequential_expand_all(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_expand_burst_ctx = None
        mw._destination_expand_burst_queue = [{"trigger_path_captured": "Root"}]
        MainWindow._run_destination_expand_burst_pipeline(mw)
        self.assertEqual(mw._destination_expand_burst_queue, [])
        self.assertIsNone(mw._destination_expand_burst_ctx)

    def test_reseed_collapsed_skipped_during_strict_sequential(self):
        mw = _bare_main_window_for_seq_tests()
        self.assertEqual(MainWindow._destination_expand_all_reseed_collapsed_folders(mw, 112), 0)

    def test_ensure_tree_item_load_started_destination_model_no_structural_mutation(self):
        mw = _bare_main_window_for_seq_tests()
        model = QStandardItemModel()
        root = model.invisibleRootItem()
        it = QStandardItem("RootA")
        it.setData({"is_folder": True, "children_loaded": False, "destination_path": r"Root\A"}, Qt.UserRole)
        root.appendRow(it)
        ix = model.indexFromItem(it)
        mw._destination_semantic_path = lambda pl: str(pl.get("destination_path") or "")
        mw._tree_item_path = lambda pl: str(pl.get("destination_path") or "")
        self.assertFalse(MainWindow._ensure_tree_item_load_started(mw, "destination", ix))

    def test_bind_deferred_when_structure_incomplete(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_structure_ready = lambda _r="", log_waiting=False: False
        mw._log_restore_phase = MagicMock()
        mw._safe_invoke = lambda _n, fn, *a, **k: fn(*a, **k)
        out = MainWindow._bind_destination_tree_from_future_state_model(mw, {"nodes": {}, "root_path": "Root"})
        self.assertIsNone(out)
        mw._log_restore_phase.assert_called()

    def test_bind_starts_when_structure_complete(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_structure_ready = lambda _r="", log_waiting=False: True
        mw._destination_snapshot_chunked_restore_active = False
        mw._destination_future_bind_should_chunk_async = lambda _m, on_complete=None: False
        mw._bind_destination_future_model_sync = lambda _m: (5, 5)
        out = MainWindow._bind_destination_tree_from_future_state_model(
            mw,
            {"nodes": {"Root": {}}, "root_path": "Root"},
        )
        self.assertEqual(out, (5, 5))

    def test_expand_all_does_not_start_until_structure_ready(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_structure_ready = lambda _r="", log_waiting=False: False
        mw.destination_tree_widget = MagicMock()
        mw.destination_planning_model = MagicMock()
        mw.destination_planning_model.rowCount.return_value = 0
        mw._expand_all_pending = {"destination": False}
        mw._safe_invoke = lambda _n, fn, *a, **k: fn(*a, **k)
        MainWindow._begin_destination_model_expand_all(mw)
        self.assertFalse(mw._expand_all_pending["destination"])

    def test_collect_source_descendants_prefers_authoritative_graph(self):
        mw = _bare_main_window_for_seq_tests()
        mw.graph = MagicMock()
        mw.graph.list_drive_subtree_items_normalized.return_value = [
            {"is_folder": True, "item_path": r"Root\A\B", "name": "B"}
        ]
        mw._find_source_item_for_planned_move = lambda _m: object()
        mw._destination_projection_diag_payload = lambda *_a, **_k: {}
        mw._log_destination_projection_collect_result = MagicMock()
        src = {"is_folder": True, "drive_id": "d1", "id": "i1", "item_path": r"Root\A"}
        out = MainWindow._collect_source_descendants_for_projection(
            mw, src, {"source": src, "source_path": r"Root\A"}
        )
        self.assertEqual(len(out), 1)
        mw.graph.list_drive_subtree_items_normalized.assert_called_once()

    def test_dynamic_child_added_after_initial_iteration_is_still_visited(self):
        mw = _bare_main_window_for_seq_tests()
        mw._destination_expand_all_dfs_stack = []
        mw._destination_expand_all_seq_run_id = 77
        mw._destination_expand_all_seq_active_sp = ""
        mw._destination_expand_all_seq_expand_dispatched = False
        mw._destination_expand_all_seq_wait_started = None
        mw._destination_expand_all_seq_last_wait_log_key = None
        mw._destination_model_expand_all_max_inflight_loads = lambda: 1
        mw._ensure_tree_item_load_started = lambda _p, _ix: False
        mw._destination_semantic_path = lambda pl: str(pl.get("destination_path") or "")
        mw._tree_item_path = lambda pl: ""
        mw._destination_model_expand_all_key = lambda ix: str((ix.data(Qt.UserRole) or {}).get("destination_path") or "")
        mw._destination_expand_all_seq_folder_complete = lambda _m, _ix, _pl, _sp: (True, "ok")

        events = []
        completed = []
        added = {"done": False}
        mw._destination_expand_all_seq_log = lambda event, **fields: events.append((event, fields))

        model = QStandardItemModel()
        inv = model.invisibleRootItem()
        root_item = QStandardItem("Root")
        root_item.setData(
            {"is_folder": True, "children_loaded": True, "destination_path": r"Root"},
            Qt.UserRole,
        )
        child_a = QStandardItem("A")
        child_a.setData(
            {"is_folder": True, "children_loaded": True, "destination_path": r"Root\A"},
            Qt.UserRole,
        )
        root_item.appendRow(child_a)
        inv.appendRow(root_item)

        def _note_complete(_model, _index, _reason, semantic_excerpt):
            completed.append(str(semantic_excerpt))
            if semantic_excerpt == r"Root\A" and not added["done"]:
                added["done"] = True
                child_b = QStandardItem("B")
                child_b.setData(
                    {"is_folder": True, "children_loaded": True, "destination_path": r"Root\B"},
                    Qt.UserRole,
                )
                root_item.appendRow(child_b)

        mw._destination_expand_all_seq_dfs_note_folder_complete = _note_complete
        mw._expand_all_pending = {"destination": True}
        mw.pending_folder_loads = {"destination": set()}
        mw._destination_expand_all_dfs_roots_deque = deque([QPersistentModelIndex(model.indexFromItem(root_item))])

        class _Tree:
            @staticmethod
            def expand(_index):
                return None

        tree = _Tree()
        for _ in range(30):
            MainWindow._destination_model_expand_all_tick_sequential(mw, tree, model)
            if not mw._destination_expand_all_dfs_stack and not mw._destination_expand_all_dfs_roots_deque:
                break

        self.assertIn(r"Root\A", completed)
        self.assertIn(r"Root\B", completed)
        self.assertIn("folder_reopened_due_to_new_children", [e[0] for e in events])
        self.assertIn("folder_completion_confirmed_stable", [e[0] for e in events])


if __name__ == "__main__":
    unittest.main()
