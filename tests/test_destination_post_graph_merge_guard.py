"""Post-Graph destination merge: reuse real SPO rows by canonical path (no duplicate siblings)."""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.destination_tree_contract import EMPTY_LIBRARY_MESSAGE
from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _qapp():
    return QApplication.instance() or QApplication([])


def _bare_mw():
    mw = MainWindow.__new__(MainWindow)
    mw.pending_root_drive_ids = {"destination": "drive-dest"}
    mw._destination_tree_model_view = True
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    return mw


class DestinationOverlaySkeletonContractTests(unittest.TestCase):
    """Lazy overlay contract: future-model repaint must not add new library-top structural rows."""

    def test_overlay_does_not_insert_rows_for_unanchored_future_nodes(self):
        _qapp()
        mw = _bare_mw()
        mw._sharepoint_lazy_mode = True
        mw.planned_moves = []
        mw.proposed_folders = []
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        mw.destination_planning_model = model
        mw._destination_tree_uses_model_view = lambda: True  # type: ignore[method-assign]

        real_items = [
            {
                "name": "Finance",
                "is_folder": True,
                "drive_id": "d1",
                "id": "f1",
                "item_path": "/Finance",
                "node_origin": "sharepoint",
            }
        ]
        sorted_items = sorted(
            real_items, key=lambda v: (not v.get("is_folder", False), v.get("name", "").lower())
        )
        payloads = [MainWindow._destination_payload_from_graph_item(mw, it) for it in sorted_items]
        model.reset_root_payloads(payloads)
        lib = QModelIndex()
        self.assertEqual(model.rowCount(lib), 1)

        fin_key = None
        for cand in ("/Finance", "Finance"):
            if model.find_indices_for_canonical_destination_path(mw._canonical_destination_projection_path(cand) or cand):
                fin_key = cand
                break
        self.assertIsNotNone(fin_key)

        future_model = {
            "nodes": {
                fin_key: {
                    "data": {"name": "Finance", "is_folder": True, "id": "f1"},
                    "parent_semantic_path": "",
                    "node_state": "proposed",
                    "name": "Finance",
                    "children": [],
                },
                "ProposedOnlyTop": {
                    "data": {"name": "ProposedOnlyTop", "is_folder": True},
                    "parent_semantic_path": "",
                    "node_state": "proposed",
                    "name": "ProposedOnlyTop",
                    "children": [],
                },
            },
            "root_path": "",
        }
        attached, unresolved = MainWindow._destination_apply_future_model_overlay_to_planning_skeleton(mw, future_model)
        self.assertEqual(model.rowCount(lib), 1)
        self.assertGreaterEqual(attached, 1)
        self.assertGreaterEqual(unresolved, 1)


class DestinationIncrementalBindOverlayContractTests(unittest.TestCase):
    def test_incremental_append_overlay_contract_skips_reset_nested(self):
        """Chunked/sync incremental bind uses overlay repaint under lazy mode — never nuclear reset_nested."""
        _qapp()
        mw = _bare_mw()
        mw._sharepoint_lazy_mode = True
        mw._destination_refresh_pipeline_log = MagicMock()
        mw._destination_log_bind_visible_vs_canonical_top_level = lambda *a, **k: None  # type: ignore[method-assign]
        mw._reconcile_destination_root_child_top_level_leaks_planning_model = lambda _r: 0  # type: ignore[method-assign, assignment]

        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        mw.destination_planning_model = model

        real_items = [
            {
                "name": "Finance",
                "is_folder": True,
                "drive_id": "d1",
                "id": "f1",
                "item_path": "/Finance",
                "node_origin": "sharepoint",
            }
        ]
        sorted_items = sorted(
            real_items, key=lambda v: (not v.get("is_folder", False), v.get("name", "").lower())
        )
        payloads = [MainWindow._destination_payload_from_graph_item(mw, it) for it in sorted_items]
        model.reset_root_payloads(payloads)
        lib = QModelIndex()
        self.assertEqual(model.rowCount(lib), 1)

        fin_key = None
        for cand in ("/Finance", "Finance"):
            ck = mw._canonical_destination_projection_path(cand) or cand
            if model.find_indices_for_canonical_destination_path(ck):
                fin_key = cand
                break
        self.assertIsNotNone(fin_key)

        future_model = {
            "nodes": {
                fin_key: {
                    "data": {"name": "Finance", "is_folder": True, "id": "f1"},
                    "parent_semantic_path": "",
                    "node_state": "proposed",
                    "name": "Finance",
                    "children": [],
                },
                "ProposedOnlyTop": {
                    "data": {"name": "ProposedOnlyTop", "is_folder": True},
                    "parent_semantic_path": "",
                    "node_state": "proposed",
                    "name": "ProposedOnlyTop",
                    "children": [],
                },
            },
            "root_path": "",
        }
        top_paths = MainWindow._destination_bind_resolved_top_level_paths(mw, future_model)

        reset_calls: list = []
        inner_reset = model.reset_nested

        def _track_reset(roots):
            reset_calls.append(roots)
            return inner_reset(roots)

        model.reset_nested = _track_reset  # type: ignore[method-assign]

        MainWindow._destination_bind_run_incremental_append_model(
            mw,
            model,
            future_model["nodes"],
            top_paths,
            full_model=future_model,
        )
        self.assertEqual(
            reset_calls,
            [],
            "overlay-contract incremental bind must not clear/rebuild the planning model",
        )
        self.assertEqual(model.rowCount(lib), 1)


class DestinationPostGraphMergeGuardTests(unittest.TestCase):
    def test_replace_all_children_empty_library_message(self):
        _qapp()
        model = DestinationPlanningTreeModel()
        model.reset_root_payloads(
            [
                {
                    "base_display_label": "Folder: Lib",
                    "name": "Lib",
                    "is_folder": True,
                    "display_path": "Lib",
                    "item_path": "Lib",
                    "tree_role": "destination",
                    "drive_id": "d1",
                }
            ]
        )
        lib_ix = model.index(0, 0, QModelIndex())
        model.replace_all_children(lib_ix, [], zero_children_mode="empty_library_message")
        self.assertEqual(model.rowCount(lib_ix), 1)
        pl = model.index(0, 0, lib_ix).data(Qt.UserRole) or {}
        self.assertTrue(pl.get("placeholder"))
        self.assertEqual(pl.get("placeholder_role"), "empty_library_message")
        self.assertEqual(pl.get("base_display_label"), EMPTY_LIBRARY_MESSAGE)

    def test_replace_all_children_empty_library_silent_none(self):
        _qapp()
        model = DestinationPlanningTreeModel()
        model.reset_root_payloads(
            [
                {
                    "base_display_label": "Folder: Lib",
                    "name": "Lib",
                    "is_folder": True,
                    "display_path": "Lib",
                    "item_path": "Lib",
                    "tree_role": "destination",
                }
            ]
        )
        lib_ix = model.index(0, 0, QModelIndex())
        model.replace_all_children(lib_ix, [], zero_children_mode="none")
        self.assertEqual(model.rowCount(lib_ix), 0)

    def test_restore_future_state_reuses_real_row_same_path_casefold(self):
        _qapp()
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        mw.destination_planning_model = model
        model.reset_root_payloads(
            [
                {
                    "base_display_label": "Folder: Lib",
                    "name": "Lib",
                    "is_folder": True,
                    "display_path": "Lib",
                    "item_path": "Lib",
                    "tree_role": "destination",
                    "drive_id": "d1",
                }
            ]
        )
        lib_ix = model.index(0, 0, QModelIndex())
        real_root = {
            "name": "Root",
            "real_name": "Root",
            "is_folder": True,
            "display_path": r"Lib\Root",
            "item_path": r"Lib\Root",
            "tree_role": "destination",
            "drive_id": "d1",
            "id": "spo-root",
            "node_origin": "Real",
        }
        model.replace_all_children(lib_ix, [real_root])
        self.assertEqual(model.rowCount(lib_ix), 1)
        overlay_pl = {
            "name": "Root",
            "real_name": "Root",
            "is_folder": True,
            "display_path": r"lib\root",
            "item_path": r"lib\root",
            "tree_role": "destination",
            "drive_id": "d1",
            "node_origin": "Proposed",
            "proposed": True,
        }
        moved = mw._restore_destination_future_state_children_model(lib_ix, [(overlay_pl, [])])
        self.assertEqual(moved, 0)
        self.assertEqual(model.rowCount(lib_ix), 1)

    def test_merge_nested_spec_reuses_real_child_by_canonical_path(self):
        _qapp()
        mw = _bare_mw()
        mw._current_destination_context_segments = lambda: []
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        mw.destination_planning_model = model
        model.reset_root_payloads(
            [
                {
                    "base_display_label": "Folder: Lib",
                    "name": "Lib",
                    "is_folder": True,
                    "display_path": "Lib",
                    "item_path": "Lib",
                    "tree_role": "destination",
                }
            ]
        )
        lib_ix = model.index(0, 0, QModelIndex())
        real_root = {
            "name": "Root",
            "real_name": "Root",
            "is_folder": True,
            "display_path": r"Lib\Root",
            "item_path": r"Lib\Root",
            "tree_role": "destination",
            "id": "r1",
            "node_origin": "Real",
        }
        model.replace_all_children(lib_ix, [real_root])
        root_ix = model.index(0, 0, lib_ix)
        nested_pl = {
            "name": "Child",
            "is_folder": True,
            "display_path": r"Lib\Root\Child",
            "item_path": r"Lib\Root\Child",
            "tree_role": "destination",
            "node_origin": "Proposed",
        }
        with patch.object(mw, "_log_restore_phase", lambda *a, **k: None):
            n = mw._merge_nested_spec_into_parent_index(root_ix, (nested_pl, []))
        self.assertEqual(n, 1)
        self.assertEqual(model.rowCount(root_ix), 1)
        nested_pl2 = {
            "name": "Child",
            "is_folder": True,
            "display_path": r"LIB\ROOT\CHILD",
            "item_path": r"LIB\ROOT\CHILD",
            "tree_role": "destination",
            "node_origin": "Proposed",
        }
        n2 = mw._merge_nested_spec_into_parent_index(root_ix, (nested_pl2, []))
        self.assertEqual(n2, 0)
        self.assertEqual(model.rowCount(root_ix), 1)


if __name__ == "__main__":
    unittest.main()
