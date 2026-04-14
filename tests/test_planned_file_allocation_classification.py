"""Planned workspace rows: file leaves vs folders (allocation / explorer / model)."""

from __future__ import annotations


def _minimal_mw_for_planned_bind():
    from PySide6.QtWidgets import QApplication

    from ozlink_console.main_window import MainWindow
    from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel

    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.destination_planning_model = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)
    mw._tree_item_path = MainWindow._tree_item_path.__get__(mw, MainWindow)
    mw._destination_parent_match_details = MainWindow._destination_parent_match_details.__get__(mw, MainWindow)
    mw._destination_row_raw_path_for_path_lookup_match = MainWindow._destination_row_raw_path_for_path_lookup_match.__get__(
        mw, MainWindow
    )
    mw._destination_semantic_path = MainWindow._destination_semantic_path.__get__(mw, MainWindow)
    mw._destination_resolution_rank = MainWindow._destination_resolution_rank.__get__(mw, MainWindow)
    mw._select_canonical_destination_item = MainWindow._select_canonical_destination_item.__get__(mw, MainWindow)
    mw._destination_sibling_folder_dedup_key = MainWindow._destination_sibling_folder_dedup_key.__get__(mw, MainWindow)
    mw._destination_sibling_folder_collision_key = MainWindow._destination_sibling_folder_collision_key.__get__(mw, MainWindow)
    mw._destination_find_child_by_last_segment_folder_name = MainWindow._destination_find_child_by_last_segment_folder_name.__get__(
        mw, MainWindow
    )
    mw._destination_has_future_descendants = lambda _nd: False
    mw._log_restore_phase = lambda *a, **k: None
    mw._refresh_destination_item_visibility_index = lambda *a, **k: None
    mw._tree_name_column_label = MainWindow._tree_name_column_label.__get__(mw, MainWindow)
    return mw, app


def test_allocated_xlsx_leaf_bind_is_planned_file_not_folder():
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.main_window import MainWindow
    from ozlink_console.tree_models.explorer_columns import explorer_type_label

    mw, app = _minimal_mw_for_planned_bind()
    dm = mw.destination_planning_model
    hub = {
        "name": "Hub",
        "id": "live-hub",
        "is_folder": True,
        "item_path": "Hub",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    leaf = MainWindow._sharepoint_bind_planned_segment_chain(
        mw,
        hub_ix,
        ["Reports", "Salary Increases.xlsx"],
        bind_kind="allocation",
        allocation_id="req-1",
        expected_parent_canonical="Hub",
        projection_target_canonical="Hub\\Reports\\Salary Increases.xlsx",
        terminal_is_file=True,
    )
    assert leaf is not None and leaf.isValid()
    pl = leaf.data(Qt.UserRole) or {}
    assert pl.get("row_kind") == "planned_file"
    assert pl.get("is_folder") is False
    assert pl.get("verification_state") == "planned_only"
    assert not dm.hasChildren(leaf)
    assert dm.rowCount(leaf) == 0
    overlay = dict(pl)
    overlay["planned_allocation"] = True
    overlay["node_origin"] = "PlannedAllocation"
    assert explorer_type_label(overlay) == "Allocated file"
    _ = app


def test_allocated_pdf_leaf_bind_is_planned_file():
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.main_window import MainWindow
    from ozlink_console.tree_models.explorer_columns import explorer_type_label

    mw, app = _minimal_mw_for_planned_bind()
    dm = mw.destination_planning_model
    hub = {
        "name": "Hub",
        "id": "live-hub",
        "is_folder": True,
        "item_path": "Hub",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    leaf = MainWindow._sharepoint_bind_planned_segment_chain(
        mw,
        hub_ix,
        ["SWMS 18 St Bernards Lynbrook pdf.pdf"],
        bind_kind="allocation",
        expected_parent_canonical="Hub",
        projection_target_canonical="Hub\\SWMS 18 St Bernards Lynbrook pdf.pdf",
        terminal_is_file=True,
    )
    assert leaf is not None and leaf.isValid()
    pl = leaf.data(Qt.UserRole) or {}
    assert pl.get("row_kind") == "planned_file"
    assert pl.get("is_folder") is False
    assert not dm.hasChildren(leaf)
    overlay = dict(pl)
    overlay["planned_allocation"] = True
    overlay["node_origin"] = "PlannedAllocation"
    assert explorer_type_label(overlay) == "Allocated file"
    _ = app


def test_proposed_folder_bind_last_segment_stays_planned_folder():
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.main_window import MainWindow
    from ozlink_console.tree_models.explorer_columns import explorer_type_label

    mw, app = _minimal_mw_for_planned_bind()
    dm = mw.destination_planning_model
    hub = {
        "name": "Hub",
        "id": "live-hub",
        "is_folder": True,
        "item_path": "Hub",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    leaf = MainWindow._sharepoint_bind_planned_segment_chain(
        mw,
        hub_ix,
        ["NewDept", "Inbox"],
        bind_kind="proposed_folder",
        expected_parent_canonical="Hub",
        projection_target_canonical="Hub\\NewDept\\Inbox",
        terminal_is_file=False,
    )
    assert leaf is not None and leaf.isValid()
    pl = leaf.data(Qt.UserRole) or {}
    assert pl.get("row_kind") == "planned_folder"
    assert pl.get("is_folder") is True
    assert explorer_type_label(pl) == "Planned folder"
    _ = app


def test_update_payload_to_file_strips_children_and_disables_expand():
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.main_window import MainWindow

    mw, app = _minimal_mw_for_planned_bind()
    dm = mw.destination_planning_model
    hub = {
        "name": "Hub",
        "id": "live-hub",
        "is_folder": True,
        "item_path": "Hub",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    wrong_folder = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "planning_uuid": "u1",
        "id": "",
        "graph_item_id": "",
        "name": "wrong.xls",
        "item_path": "Hub\\wrong.xls",
        "display_path": "Hub\\wrong.xls",
        "destination_path": "Hub\\wrong.xls",
        "tree_role": "destination",
        "is_folder": True,
        "children_loaded": True,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, wrong_folder)
    child_pl = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "stale",
        "item_path": "Hub\\wrong.xls\\stale",
        "display_path": "Hub\\wrong.xls\\stale",
        "destination_path": "Hub\\wrong.xls\\stale",
        "tree_role": "destination",
        "is_folder": True,
        "children_loaded": False,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, child_pl)
    dm.append_child_payloads(hub_ix, [wrong_folder])
    parent_row = dm.index(0, 0, hub_ix)
    dm.append_child_payloads(parent_row, [child_pl])
    assert dm.rowCount(parent_row) == 1

    def _fix(p):
        p["is_folder"] = False
        p["row_kind"] = "planned_file"
        p["children_loaded"] = True

    dm.update_payload_for_index(parent_row, _fix)
    assert dm.rowCount(parent_row) == 0
    assert not dm.hasChildren(parent_row)
    fixed = parent_row.data(Qt.UserRole) or {}
    assert fixed.get("row_kind") == "planned_file"
    _ = app
