"""Destination tree binds Graph library root children as real top-level rows (no synthetic Root)."""

import os
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _bare_mw():
    mw = MainWindow.__new__(MainWindow)
    mw.pending_root_drive_ids = {"destination": "drive-1"}
    mw._log_restore_phase = lambda *a, **k: None
    mw._memory_restore_in_progress = False
    mw._suppress_selector_change_handlers = False
    return mw


class DestinationRealRootBindTests(unittest.TestCase):
    def test_graph_root_load_top_level_rows_are_library_children(self):
        mw = _bare_mw()
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        items = [
            {"name": "Finance", "is_folder": True, "drive_id": "d1", "id": "f1", "item_path": "/Finance"},
            {"name": "HR", "is_folder": True, "drive_id": "d1", "id": "h1", "item_path": "/HR"},
        ]
        sorted_items = sorted(items, key=lambda v: (not v.get("is_folder", False), v.get("name", "").lower()))
        payloads = [MainWindow._destination_payload_from_graph_item(mw, it) for it in sorted_items]
        model.reset_root_payloads(payloads)
        self.assertEqual(model.rowCount(QModelIndex()), 2)
        for r, name in enumerate(["Finance", "HR"]):
            pl = model.index(r, 0, QModelIndex()).data(Qt.UserRole) or {}
            self.assertEqual(pl.get("name"), name)
            self.assertNotEqual(pl.get("id"), "synthetic::destination_root")
            self.assertFalse(bool(pl.get("synthetic_destination_root")))

    def test_no_path_rewrite_to_root_backslash_name_at_bind(self):
        """Graph root child paths must not be rewritten to Root\\<name> in the row payload."""
        mw = _bare_mw()
        items = [
            {"name": "Finance", "is_folder": True, "drive_id": "d1", "id": "f1", "item_path": "/Finance"},
        ]
        pl = MainWindow._destination_payload_from_graph_item(mw, items[0])
        self.assertEqual(pl.get("item_path"), "/Finance")
        self.assertNotEqual(pl.get("item_path"), "Root\\Finance")

    def test_real_folder_named_root_is_ordinary_row(self):
        mw = _bare_mw()
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        items = [
            {"name": "Root", "is_folder": True, "drive_id": "d1", "id": "real-root", "item_path": "/Root"},
            {"name": "Zed", "is_folder": True, "drive_id": "d1", "id": "z1", "item_path": "/Zed"},
        ]
        sorted_items = sorted(items, key=lambda v: (not v.get("is_folder", False), v.get("name", "").lower()))
        payloads = [MainWindow._destination_payload_from_graph_item(mw, it) for it in sorted_items]
        model.reset_root_payloads(payloads)
        self.assertEqual(model.rowCount(QModelIndex()), 2)
        names = []
        for r in range(2):
            pl = model.index(r, 0, QModelIndex()).data(Qt.UserRole) or {}
            names.append(pl.get("name"))
            self.assertNotEqual(pl.get("id"), "synthetic::destination_root")
        self.assertEqual(set(names), {"Root", "Zed"})

    def test_empty_library_zero_top_level_nodes(self):
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        model.clear()
        self.assertEqual(model.rowCount(QModelIndex()), 0)

    def test_empty_library_not_using_placeholder_row(self):
        """Structural empty-library placeholder rows must not be used (UI shows status only)."""
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        model.clear()
        self.assertEqual(model.rowCount(QModelIndex()), 0)
        # If set_empty_library_message were used, one placeholder row would appear.
        model.set_empty_library_message("legacy")
        self.assertEqual(model.rowCount(QModelIndex()), 1)


if __name__ == "__main__":
    unittest.main()
