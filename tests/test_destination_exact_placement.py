"""Graph-authority destination placement: strict child resolution, terminal_empty, reconcile."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _minimal_mw_graph_destination():
    from PySide6.QtWidgets import QApplication

    from ozlink_console.main_window import MainWindow
    from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel

    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.destination_planning_model = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    for name in (
        "normalize_memory_path",
        "_canonical_destination_projection_path",
        "_path_segments",
        "_tree_item_path",
        "_destination_parent_match_details",
        "_destination_row_raw_path_for_path_lookup_match",
        "_destination_semantic_path",
        "_destination_resolution_rank",
        "_select_canonical_destination_item",
        "_destination_sibling_folder_dedup_key",
        "_destination_sibling_folder_collision_key",
        "_destination_find_child_by_last_segment_folder_name",
        "_canonical_destination_path_with_visible_library_anchor",
        "_allocation_projection_path",
        "_proposed_destination_path",
    ):
        setattr(mw, name, getattr(MainWindow, name).__get__(mw, MainWindow))
    mw._destination_visible_library_anchor_canonical_path = lambda: ""
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
    mw.proposed_folders = []
    mw._destination_has_future_descendants = lambda _nd: False
    mw._log_restore_phase = lambda *a, **k: None
    mw._refresh_destination_item_visibility_index = lambda *a, **k: None
    mw._tree_name_column_label = MainWindow._tree_name_column_label.__get__(mw, MainWindow)
    mw._apply_tree_item_visual_state = lambda *a, **k: MainWindow._apply_tree_item_visual_state(mw, *a, **k)
    return mw, app


def test_find_destination_child_strict_skips_wrong_path_same_folder_name():
    """Under Graph authority, do not resolve via last-segment name when full path differs."""
    from PySide6.QtCore import QModelIndex

    from ozlink_console.main_window import MainWindow

    mw, app = _minimal_mw_graph_destination()
    dm = mw.destination_planning_model
    finance = {
        "name": "Finance",
        "id": "live-fin",
        "is_folder": True,
        "item_path": "Root\\Finance",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, finance)
    dm.reset_root_payloads([finance])
    fin_ix = dm.index(0, 0, QModelIndex())
    wrong_payroll = {
        "name": "Payroll",
        "id": "live-p1",
        "is_folder": True,
        "item_path": "Root\\Finance\\Docs\\Payroll",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, wrong_payroll)
    dm.append_child_payloads(fin_ix, [wrong_payroll])
    payroll_ix = dm.index(0, 0, fin_ix)
    strict = MainWindow._find_destination_child_by_path(
        mw, fin_ix, "Root\\Finance\\Payroll", overlay_path_strict=True
    )
    loose = MainWindow._find_destination_child_by_path(
        mw, fin_ix, "Root\\Finance\\Payroll", overlay_path_strict=False
    )
    assert strict is None
    assert loose is not None and loose == payroll_ix
    _ = app


def test_terminal_empty_removed_when_planned_move_targets_descendant():
    from PySide6.QtCore import QModelIndex

    from ozlink_console.main_window import MainWindow

    mw, app = _minimal_mw_graph_destination()
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
    dm.replace_all_children(hub_ix, [])
    assert dm.rowCount(hub_ix) == 1
    from PySide6.QtCore import Qt

    only = dm.index(0, 0, hub_ix).data(Qt.UserRole)
    assert isinstance(only, dict)
    assert only.get("placeholder_role") == "terminal_empty"

    mw.planned_moves = [
        {
            "destination_path": "Hub\\Alpha",
            "target_name": "file.txt",
            "source": {"name": "src.txt", "is_folder": False},
        }
    ]
    MainWindow._destination_strip_terminal_empty_when_planned_descendants_expected(
        mw, hub_ix, folder_semantic_path="Hub"
    )
    assert dm.rowCount(hub_ix) == 0
    _ = app


def test_bind_planned_chain_matches_projection_target_canonical():
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.main_window import MainWindow
    from test_planned_file_allocation_classification import _minimal_mw_for_planned_bind

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
    target = "Hub\\Finance\\Payroll\\Follow up"
    leaf = MainWindow._sharepoint_bind_planned_segment_chain(
        mw,
        hub_ix,
        ["Finance", "Payroll", "Follow up"],
        bind_kind="proposed_folder",
        bind_context_excerpt="test",
        expected_parent_canonical="Hub",
        projection_target_canonical=target,
        terminal_is_file=False,
    )
    assert leaf is not None and leaf.isValid()
    pl = leaf.data(Qt.UserRole) or {}
    ip = str(pl.get("item_path") or "")
    assert ip.replace("/", "\\").casefold() == target.replace("/", "\\").casefold()
    _ = app


def _minimal_mw_for_planned_bind():
    from tests.test_planned_file_allocation_classification import _minimal_mw_for_planned_bind as _f

    return _f()
