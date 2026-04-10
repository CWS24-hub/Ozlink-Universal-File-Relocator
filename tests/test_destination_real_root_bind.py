"""Destination tree binds Graph library root children as real top-level rows (no synthetic Root)."""

import os
import unittest
from unittest.mock import MagicMock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.destination_tree_contract import EMPTY_LIBRARY_MESSAGE
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

    def test_graph_root_bind_empty_library_contract_placeholder(self):
        """Empty Graph root bind: contract placeholder row + status (see destination_tree_refresh_contract)."""
        mw = _bare_mw()
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        tree = MagicMock()
        status = MagicMock()
        mw.destination_tree_widget = tree
        mw.destination_tree_status = status
        mw.destination_planning_model = model
        status_msgs: list[tuple] = []

        def _capture_status(pk, msg, loading=False):
            status_msgs.append((pk, msg, loading))

        mw._get_tree_and_status = lambda panel_key: (tree, status)  # type: ignore[method-assign]
        mw._set_tree_status_message = _capture_status  # type: ignore[method-assign]
        mw._mark_destination_real_tree_snapshot_stale = lambda: None  # type: ignore[method-assign]
        MainWindow._apply_root_payload_to_destination_model_view(mw, "destination", [])
        self.assertEqual(model.rowCount(QModelIndex()), 1)
        pl = model.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
        self.assertTrue(pl.get("placeholder"))
        self.assertEqual(pl.get("placeholder_role"), "empty_library_message")
        self.assertEqual(pl.get("base_display_label"), EMPTY_LIBRARY_MESSAGE)
        self.assertTrue(any(m[0] == "destination" and m[1] == EMPTY_LIBRARY_MESSAGE for m in status_msgs))

    def test_set_empty_library_message_uses_contract_constant(self):
        model = DestinationPlanningTreeModel(destination_index_key_fn=lambda pl: str(pl.get("item_path") or ""))
        model.clear()
        model.set_empty_library_message(EMPTY_LIBRARY_MESSAGE)
        self.assertEqual(model.rowCount(QModelIndex()), 1)
        pl = model.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
        self.assertEqual(pl.get("base_display_label"), EMPTY_LIBRARY_MESSAGE)

    def test_apply_root_payload_preserves_graph_count_order_and_no_extra_placeholders(self):
        """DESTINATION READ INVARIANT (skeleton): one row per Graph item, sorted like load path; no stray placeholders."""
        mw = _bare_mw()
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        tree = MagicMock()
        status = MagicMock()
        mw.destination_tree_widget = tree
        mw.destination_tree_status = status
        mw.destination_planning_model = model
        mw._get_tree_and_status = lambda panel_key: (tree, status)  # type: ignore[method-assign]
        mw._set_tree_status_message = lambda *a, **k: None  # type: ignore[method-assign]
        mw._mark_destination_real_tree_snapshot_stale = lambda: None  # type: ignore[method-assign]

        items = [
            {"name": "Zed", "is_folder": True, "drive_id": "d1", "id": "z1", "item_path": "/Zed"},
            {"name": "afile", "is_folder": False, "drive_id": "d1", "id": "a1", "item_path": "/afile"},
            {"name": "Alpha", "is_folder": True, "drive_id": "d1", "id": "al1", "item_path": "/Alpha"},
        ]
        MainWindow._apply_root_payload_to_destination_model_view(mw, "destination", items)
        lib = QModelIndex()
        self.assertEqual(model.rowCount(lib), 3)
        expected_order = sorted(items, key=lambda v: (not v.get("is_folder", False), v.get("name", "").lower()))
        for r, src in enumerate(expected_order):
            pl = model.index(r, 0, lib).data(Qt.UserRole) or {}
            self.assertFalse(bool(pl.get("placeholder")))
            self.assertEqual(pl.get("name"), src["name"])
            self.assertEqual(pl.get("id"), src["id"])
            self.assertEqual(pl.get("item_path"), src["item_path"])

    def test_graph_root_bind_id_poor_items_still_materialize_rows(self):
        """Legacy-friendly: missing Graph id still yields one structural row per item (path/name bind)."""
        mw = _bare_mw()
        model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
        tree = MagicMock()
        status = MagicMock()
        mw.destination_tree_widget = tree
        mw.destination_tree_status = status
        mw.destination_planning_model = model
        mw._get_tree_and_status = lambda panel_key: (tree, status)  # type: ignore[method-assign]
        mw._set_tree_status_message = lambda *a, **k: None  # type: ignore[method-assign]
        mw._mark_destination_real_tree_snapshot_stale = lambda: None  # type: ignore[method-assign]

        items = [
            {"name": "LegacyA", "is_folder": True, "drive_id": "d1", "item_path": "/LegacyA"},
            {"name": "LegacyB", "is_folder": True, "drive_id": "d1", "item_path": "/LegacyB"},
        ]
        MainWindow._apply_root_payload_to_destination_model_view(mw, "destination", items)
        lib = QModelIndex()
        self.assertEqual(model.rowCount(lib), 2)
        for r in range(2):
            pl = model.index(r, 0, lib).data(Qt.UserRole) or {}
            self.assertFalse(bool(pl.get("placeholder")))
            self.assertIn("Legacy", str(pl.get("name") or ""))


if __name__ == "__main__":
    unittest.main()
