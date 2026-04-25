"""Regression: projected allocation descendant rows must keep source file vs folder typing."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel
from ozlink_console.tree_models.explorer_columns import explorer_type_label


def _app():
    return QApplication.instance() or QApplication([])


def _stub_mw(monkeypatch):
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    dm = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_planning_model = dm
    mw.destination_tree_widget = tw
    for n in (
        "_canonical_destination_projection_path",
        "_tree_item_path",
        "_tree_name_column_label",
        "_apply_tree_item_visual_state",
        "_destination_model_index_user_role_dict",
        "_destination_payload_index_key",
    ):
        setattr(mw, n, getattr(MainWindow, n).__get__(mw, MainWindow))
    return mw, dm


def test_allocation_descendant_overlay_file_replaces_stale_folder_flag(monkeypatch):
    """After teardown/repair, a row may still carry is_folder=True; file overlay must reset type."""
    mw, dm = _stub_mw(monkeypatch)
    root = {
        "name": "Root3",
        "id": "r1",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    parent_ix = dm.index(0, 0, QModelIndex())
    # Stale state: same leaf was treated as a folder (e.g. prior projection segment).
    stale_leaf = {
        "name": "Contractor bank.docx",
        "real_name": "Contractor bank.docx",
        "is_folder": True,
        "row_kind": "planned_folder",
        "item_path": "Root3\\HR\\Contractor bank.docx",
        "destination_path": "Root3\\HR\\Contractor bank.docx",
        "display_path": "Root3\\HR\\Contractor bank.docx",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
        "node_origin": "ProjectedAllocationDescendant",
        "planned_allocation_descendant": True,
        "id": "",
        "graph_item_id": "",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, stale_leaf)
    dm.append_child_payloads(parent_ix, [stale_leaf])
    leaf_ix = dm.index(0, 0, parent_ix)
    par_pl = dict(parent_ix.data(Qt.UserRole) or {})
    source_file = {
        "name": "Contractor bank.docx",
        "real_name": "Contractor bank.docx",
        "is_folder": False,
        "item_path": "FTBMRoot\\HR\\Contractor bank.docx",
    }
    dest_full = "Root3\\HR\\Contractor bank.docx"
    MainWindow._apply_allocation_descendant_overlay_to_existing_model_index(
        mw, leaf_ix, source_file, dest_full, par_pl
    )
    pl = dict(leaf_ix.data(Qt.UserRole) or {})
    assert pl.get("is_folder") is False
    assert pl.get("row_kind") == "planned_file"
    assert pl.get("planned_allocation_descendant") is True


def test_allocation_descendant_overlay_preserves_live_graph_folder_type(monkeypatch):
    """Live Graph rows keep Graph is_folder; overlay must not force file from source."""
    mw, dm = _stub_mw(monkeypatch)
    root = {
        "name": "Root3",
        "id": "r1",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    parent_ix = dm.index(0, 0, QModelIndex())
    live_folder = {
        "name": "Reports",
        "is_folder": True,
        "id": "graph-folder-id",
        "graph_item_id": "graph-folder-id",
        "item_path": "Root3\\Reports",
        "destination_path": "Root3\\Reports",
        "tree_role": "destination",
        "workspace_row_state": "live_confirmed",
        "children_loaded": False,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, live_folder)
    dm.append_child_payloads(parent_ix, [live_folder])
    leaf_ix = dm.index(0, 0, parent_ix)
    par_pl = dict(parent_ix.data(Qt.UserRole) or {})
    # Source says file — must not downgrade a live Graph folder row.
    source_mismatch_file = {
        "name": "Reports",
        "is_folder": False,
        "item_path": "src\\Reports",
    }
    MainWindow._apply_allocation_descendant_overlay_to_existing_model_index(
        mw, leaf_ix, source_mismatch_file, "Root3\\Reports", par_pl
    )
    pl = dict(leaf_ix.data(Qt.UserRole) or {})
    assert pl.get("is_folder") is True


def test_explorer_type_projected_allocation_descendant_file_not_folder_label():
    pl = {
        "name": "Contractor bank.docx",
        "node_origin": "ProjectedAllocationDescendant",
        "is_folder": False,
        "placeholder": False,
    }
    label = explorer_type_label(pl)
    assert "Word" in label or "docx" in label.lower()


def test_build_destination_allocation_descendant_file_row_kind():
    mw = MainWindow.__new__(MainWindow)
    mw._source_tree_payload_implies_file_leaf = MainWindow._source_tree_payload_implies_file_leaf.__get__(mw, MainWindow)
    src = {"name": "a.pdf", "is_folder": False, "item_path": "S\\a.pdf"}
    parent = {"drive_id": "d1", "library_id": "d1", "site_id": "", "site_name": "", "library_name": "", "web_url": ""}
    nd = MainWindow._build_destination_allocation_descendant_node_data(mw, src, "Root3\\a.pdf", parent)
    assert nd["is_folder"] is False
    assert nd["row_kind"] == "planned_file"


def test_build_destination_allocation_descendant_docx_without_is_folder_flag():
    mw = MainWindow.__new__(MainWindow)
    mw._source_tree_payload_implies_file_leaf = MainWindow._source_tree_payload_implies_file_leaf.__get__(mw, MainWindow)
    src = {"name": "Contractor bank.docx", "item_path": r"S\HR\Contractor bank.docx"}
    parent = {"drive_id": "d1", "library_id": "d1", "site_id": "", "site_name": "", "library_name": "", "web_url": ""}
    nd = MainWindow._build_destination_allocation_descendant_node_data(mw, src, r"Root3\HR\Contractor bank.docx", parent)
    assert nd["is_folder"] is False
    assert nd["row_kind"] == "planned_file"
