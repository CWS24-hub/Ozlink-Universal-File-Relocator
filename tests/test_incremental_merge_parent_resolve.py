"""Incremental merge parent resolution when path index and _tree_item_path disagree."""

from __future__ import annotations

from unittest.mock import MagicMock

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _qapp():
    return QApplication.instance() or QApplication([])


def test_resolve_incremental_merge_parent_aligns_index_key_with_tree_item_path():
    """Regression: index buckets use destination_path before source_path; _tree_item_path does not."""
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._destination_tree_model_view = True
    model = DestinationPlanningTreeModel(destination_index_key_fn=mw._destination_payload_index_key)
    mw.destination_planning_model = model

    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
                "tree_role": "destination",
            }
        ]
    )
    root_ix = model.index(0, 0, QModelIndex())
    alloc_folder = {
        "base_display_label": "Folder: Email attachments",
        "name": "Email attachments",
        "is_folder": True,
        "source_path": r"FTBMRoot\Documents\Email attachments",
        "destination_path": r"Root\Management\Email attachments",
        "tree_role": "destination",
    }
    model.append_child_payloads(root_ix, [alloc_folder])
    parent_ix = model.index(0, 0, root_ix)
    assert parent_ix.isValid()

    semantic_parent = r"Root\Management\Email attachments"
    assert mw._destination_payload_index_key(parent_ix.data(Qt.UserRole) or {}) == mw._canonical_destination_projection_path(
        semantic_parent
    )
    assert not mw._destination_parent_match_details(semantic_parent, mw._tree_item_path(alloc_folder)).get("exact_match")

    mw._find_visible_destination_item_by_path = MagicMock(return_value=None)
    resolved = mw._resolve_incremental_merge_parent_item(semantic_parent)
    assert resolved is not None
    assert resolved == parent_ix
