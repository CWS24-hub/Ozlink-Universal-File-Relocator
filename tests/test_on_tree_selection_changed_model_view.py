"""Regression: on_tree_selection_changed must support destination QTreeView (no selectedItems())."""

from __future__ import annotations

from unittest.mock import MagicMock

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _qapp():
    return QApplication.instance() or QApplication([])


def test_on_tree_selection_changed_destination_sharepoint_qtreeview_uses_current_index():
    _qapp()
    tree = QTreeView()
    model = QStandardItemModel()
    row = QStandardItem("folder")
    payload = {
        "name": "folder",
        "is_folder": True,
        "tree_role": "destination",
        "id": "1",
        "drive_id": "d",
    }
    row.setData(payload, Qt.UserRole)
    model.appendRow(row)
    tree.setModel(model)
    ix = model.index(0, 0, QModelIndex())
    tree.setCurrentIndex(ix)

    mw = MainWindow.__new__(MainWindow)
    mw._root_tree_bind_in_progress = False
    mw._planning_browse_mode = lambda _k: "sharepoint"
    mw.destination_tree_widget = tree
    mw.source_tree_widget = MagicMock()
    mw._destination_tree_uses_model_view = lambda: True
    mw._source_tree_uses_model_view = lambda: False
    mw._full_trace_enabled = lambda: False
    ctx = {
        "panel_key": "destination",
        "node_data": dict(payload),
        "traceability": {"traceable_to_source": False},
        "planning_state": {},
        "metadata": {
            "item_name": "folder",
            "item_path": "",
            "item_type": "folder",
            "node_origin": "",
            "item_area": "destination",
        },
        "notes_preview": {"body_text": "", "notes_text": "", "preview_text": ""},
        "actions": {},
    }
    mw._resolve_selected_item_context = MagicMock(return_value=ctx)
    mw._update_selection_details = MagicMock()
    mw._set_tree_selection_summary = MagicMock()
    mw.update_details_action_state = MagicMock()

    MainWindow.on_tree_selection_changed(mw, "destination")

    mw._resolve_selected_item_context.assert_called_once()
    call_args = mw._resolve_selected_item_context.call_args[0]
    assert call_args[0] == "destination"
    assert call_args[1].get("id") == "1"
    mw._update_selection_details.assert_called_once_with(ctx)


def test_on_tree_selection_changed_destination_nonzero_column_uses_userrole_via_helper():
    """Regression: currentIndex may sit on Size/Type/Modified; UserRole exists only on column 0."""
    _qapp()
    tree = QTreeView()
    model = DestinationPlanningTreeModel(column_labels=["Name", "Size", "Type", "Modified"])
    payload = {
        "name": "leaf.txt",
        "is_folder": False,
        "tree_role": "destination",
        "id": "id-1",
        "drive_id": "d-1",
        "source_path": "Root\\\\Sub\\\\leaf.txt",
        "base_display_label": "leaf.txt",
    }
    model.reset_root_payloads([payload])
    tree.setModel(model)
    root = QModelIndex()
    ix_type_col = model.index(0, 2, root)
    assert ix_type_col.data(Qt.UserRole) is None
    tree.setCurrentIndex(ix_type_col)

    mw = MainWindow.__new__(MainWindow)
    mw._root_tree_bind_in_progress = False
    mw._planning_browse_mode = lambda _k: "sharepoint"
    mw.destination_tree_widget = tree
    mw.source_tree_widget = MagicMock()
    mw._destination_tree_uses_model_view = lambda: True
    mw._source_tree_uses_model_view = lambda: False
    mw._full_trace_enabled = lambda: False
    captured = {}
    ctx = {
        "panel_key": "destination",
        "node_data": {},
        "traceability": {"traceable_to_source": True, "source_path": "Root\\\\Sub\\\\leaf.txt"},
        "planning_state": {},
        "metadata": {},
        "notes_preview": {"body_text": "", "notes_text": "", "preview_text": ""},
        "actions": {},
    }

    def _capture_ctx(panel_key, node_data):
        captured["node_data"] = dict(node_data)
        ctx["node_data"] = dict(node_data)
        return ctx

    mw._resolve_selected_item_context = _capture_ctx
    mw._update_selection_details = MagicMock()
    mw._set_tree_selection_summary = MagicMock()
    mw.update_details_action_state = MagicMock()
    mw.clear_selection_details = MagicMock()

    MainWindow.on_tree_selection_changed(mw, "destination")

    assert captured["node_data"].get("id") == "id-1"
    assert captured["node_data"].get("source_path") == "Root\\\\Sub\\\\leaf.txt"
    mw.clear_selection_details.assert_not_called()
    mw._update_selection_details.assert_called_once_with(ctx)


def test_on_tree_selection_changed_source_nonzero_column_uses_userrole_via_helper():
    """Source QTreeView: focus on non-name column must still resolve payload (details / planned moves context)."""
    _qapp()
    tree = QTreeView()
    model = DestinationPlanningTreeModel(column_labels=["Name", "Size", "Type", "Modified"])
    payload = {
        "name": "doc.txt",
        "is_folder": False,
        "tree_role": "source",
        "id": "src-1",
        "drive_id": "sd",
        "base_display_label": "doc.txt",
        "item_path": "Lib\\\\Folder\\\\doc.txt",
    }
    model.reset_root_payloads([payload])
    tree.setModel(model)
    root = QModelIndex()
    ix_size_col = model.index(0, 1, root)
    assert ix_size_col.data(Qt.UserRole) is None
    tree.setCurrentIndex(ix_size_col)

    mw = MainWindow.__new__(MainWindow)
    mw._root_tree_bind_in_progress = False
    mw._planning_browse_mode = lambda _k: "sharepoint"
    mw.source_tree_widget = tree
    mw.destination_tree_widget = MagicMock()
    mw._source_tree_uses_model_view = lambda: True
    mw._destination_tree_uses_model_view = lambda: False
    mw._full_trace_enabled = lambda: False
    captured = {}
    ctx = {
        "panel_key": "source",
        "node_data": {},
        "traceability": {"traceable_to_source": False},
        "planning_state": {},
        "metadata": {},
        "notes_preview": {"body_text": "", "notes_text": "", "preview_text": ""},
        "actions": {},
    }

    def _capture_ctx(panel_key, node_data):
        captured["node_data"] = dict(node_data)
        ctx["node_data"] = dict(node_data)
        return ctx

    mw._resolve_selected_item_context = _capture_ctx
    mw._update_selection_details = MagicMock()
    mw._set_tree_selection_summary = MagicMock()
    mw.update_details_action_state = MagicMock()
    mw.clear_selection_details = MagicMock()

    MainWindow.on_tree_selection_changed(mw, "source")

    assert captured["node_data"].get("id") == "src-1"
    assert captured["node_data"].get("tree_role") == "source"
    mw.clear_selection_details.assert_not_called()
    mw._update_selection_details.assert_called_once_with(ctx)
