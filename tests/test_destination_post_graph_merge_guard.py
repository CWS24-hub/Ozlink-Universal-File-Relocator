"""Post-Graph destination merge: reuse real SPO rows by canonical path (no duplicate siblings)."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

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
