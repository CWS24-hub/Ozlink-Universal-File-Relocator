"""Graph authority: folder-move destination preview projects loaded source subtree (no Graph children wait)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel
from ozlink_console.tree_models.sharepoint_source_model import SharePointSourceTreeModel


def _app():
    return QApplication.instance() or QApplication([])


def _dest_child_names(dm, pix):
    out = []
    for r in range(dm.rowCount(pix)):
        pl = dm.index(r, 0, pix).data(Qt.UserRole) or {}
        if pl.get("placeholder"):
            continue
        out.append(str(pl.get("name") or ""))
    return sorted(out)


def _source_key(pl):
    return str(pl.get("item_path", "") or "").replace("/", "\\").strip()


def _mw_graph_source_dest(monkeypatch):
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw._memory_restore_in_progress = False
    mw._memory_restore_background_trees = False
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._planning_cache_generation = 0
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
    mw._root_tree_bind_in_progress = False

    st = QTreeView()
    sm = SharePointSourceTreeModel(parent=st, column_labels=["N", "S", "T", "D"], source_index_key_fn=_source_key)
    st.setModel(sm)
    mw.source_sharepoint_model = sm
    mw.source_tree_widget = st

    dt = QTreeView()
    dm = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    dt.setModel(dm)
    mw.destination_planning_model = dm
    mw.destination_tree_widget = dt

    names = (
        "normalize_memory_path",
        "_canonical_source_projection_path",
        "_canonical_destination_projection_path",
        "_canonical_planned_memory_path_for_graph_match",
        "_canonical_destination_path_with_visible_library_anchor",
        "_path_segments",
        "_tree_item_path",
        "_destination_parent_match_details",
        "_destination_row_raw_path_for_path_lookup_match",
        "_paths_equivalent",
        "_path_is_descendant",
        "_find_destination_child_by_path",
        "_find_visible_destination_item_by_path",
        "_destination_model_index_user_role_dict",
        "_destination_row_is_live_graph_structure",
        "_sharepoint_bind_planned_segment_chain",
        "_sharepoint_canonical_path_segments_under_parent",
        "_build_sharepoint_planned_folder_payload",
        "_build_sharepoint_planned_file_payload",
        "_allocation_projection_path",
        "_allocation_projection_relative_source_segments",
        "_sort_descendants_for_allocation_apply",
        "_collect_source_descendants_for_projection",
        "_find_exact_planned_move_for_source_path",
        "_move_target_name",
        "_is_leaf_path_excluded_for_plan",
        "_find_source_item_for_planned_move",
        "_source_tree_row_payload",
        "_source_subtree_fully_loaded_in_tree",
        "_iter_source_tree_subtree_rows",
        "_iter_source_visible_rows",
        "build_node_key",
        "_apply_allocation_descendant_overlay_to_existing_model_index",
        "_decorate_destination_graph_subtree_for_allocation_move",
        "_load_destination_projected_descendants_index",
        "_invalidate_stale_destination_allocation_projection_index",
        "_mark_allocation_descendants_applied_on_allocation_folder_model_index",
        "_stamp_allocation_projection_cache_metadata_index",
        "_allocation_projection_children_signature_from_index",
        "_destination_allocation_folder_shows_materialized_children_index",
        "_remove_placeholder_children",
        "_destination_remove_deferred_file_summary_children_index",
        "_refresh_destination_item_visibility_index",
        "_apply_tree_item_visual_state",
        "_destination_row_allows_structural_folder_child_load",
        "_destination_row_allows_sharepoint_projection_traversal",
        "_resolve_tree_item_drive_id",
        "node_is_planned_allocation",
        "node_is_proposed",
        "_destination_semantic_path",
        "_invalidate_stale_destination_allocation_projection_index",
        "_find_planned_move_for_destination_node",
        "_tree_name_column_label",
        "_destination_semantic_path",
        "_destination_payload_index_key",
        "_source_payload_index_key",
        "_enrich_source_root_for_projection_graph_lookup",
        "_destination_projection_diag_payload",
        "_log_destination_projection_collect_result",
        "_source_projection_descendants_cache_key_for_root",
        "_source_projection_descendants_cache_get",
        "_source_projection_descendants_cache_put",
        "_destination_forensic_planned_item_materialization_logging_enabled",
        "_destination_planning_dfs_next_preorder_index",
        "_enforce_destination_overlay_source_projection_invariant",
        "_enforce_overlay_projection_invariants",
        "_destination_refresh_folder_move_projection_after_source_paths_loaded",
    )
    for n in names:
        setattr(mw, n, getattr(MainWindow, n).__get__(mw, MainWindow))

    mw._request_graph_destination_children_load = lambda *a, **k: None
    mw._log_restore_phase = lambda *a, **k: None
    mw._full_trace_enabled = lambda: False
    mw._ui_trace = lambda *a, **k: None
    mw._destination_visible_library_anchor_canonical_path = lambda: "Root3"
    mw._log_sharepoint_overlay_destination_anchor_normalized = lambda **_k: None
    mw._ozlink_destination_graph_overlay_mode = MainWindow._ozlink_destination_graph_overlay_mode.__get__(
        mw, MainWindow
    )
    # Partial MainWindow: descendant apply may mark snapshot dirty; skip full coalesce invariants in tests.
    mw._mark_destination_tree_snapshot_dirty_after_injection = lambda *a, **k: None
    return mw, sm, dm


def _base_destination_root():
    return {
        "name": "Root3",
        "id": "id-root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }


def test_load_projects_nested_source_under_allocated_folder_without_graph_children(monkeypatch):
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    hr = {
        "name": "HR",
        "is_folder": True,
        "item_path": "HR",
        "drive_id": "sd1",
        "id": "id-hr",
        "children_loaded": True,
        "tree_role": "source",
    }
    con = {
        "name": "Contractor Resumes",
        "is_folder": True,
        "item_path": "HR\\Contractor Resumes",
        "drive_id": "sd1",
        "id": "id-con",
        "children_loaded": True,
        "tree_role": "source",
    }
    inner = {
        "name": "InnerDoc.pdf",
        "is_folder": False,
        "item_path": "HR\\Contractor Resumes\\InnerDoc.pdf",
        "drive_id": "sd1",
        "id": "id-pdf",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([hr])
    hr_ix = sm.index(0, 0, QModelIndex())
    sm.replace_all_children(hr_ix, [con])
    con_ix = sm.index(0, 0, hr_ix)
    sm.replace_all_children(con_ix, [inner])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "Contractor Resumes",
        "item_path": "Root3\\HR\\Employee Files\\Contractor Resumes",
        "destination_path": "Root3\\HR\\Employee Files\\Contractor Resumes",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "overlay_state": "PlannedAllocation",
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)

    mw.planned_moves = [
        {
            "source_path": r"HR\Contractor Resumes",
            "destination_path": r"Root3\HR\Employee Files\Contractor Resumes",
            "source": {"is_folder": True, "name": "Contractor Resumes", "drive_id": "sd1", "id": "id-con"},
            "request_id": "alloc-src-proj",
        }
    ]
    MainWindow._load_destination_projected_descendants_index(mw, tgt_ix)
    assert "InnerDoc.pdf" in _dest_child_names(dm, tgt_ix)


def test_load_projects_under_planned_workspace_folder_target(monkeypatch):
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    fld = {
        "name": "Email",
        "is_folder": True,
        "item_path": "Mgmt\\Email",
        "drive_id": "sd1",
        "id": "e1",
        "children_loaded": True,
        "tree_role": "source",
    }
    subf = {
        "name": "Sub",
        "is_folder": True,
        "item_path": "Mgmt\\Email\\Sub",
        "drive_id": "sd1",
        "id": "e2",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([fld])
    fld_ix = sm.index(0, 0, QModelIndex())
    sm.replace_all_children(fld_ix, [subf])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "Email attachments",
        "item_path": "Root3\\Management\\Email attachments",
        "destination_path": "Root3\\Management\\Email attachments",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)

    mw.planned_moves = [
        {
            "source_path": r"Mgmt\Email",
            "destination_path": r"Root3\Management\Email attachments",
            "target_name": "Email attachments",
            "source": {"is_folder": True, "name": "Email", "drive_id": "sd1", "id": "e1"},
            "request_id": "plan-ws-proj",
        }
    ]
    MainWindow._load_destination_projected_descendants_index(mw, tgt_ix)
    assert "Sub" in _dest_child_names(dm, tgt_ix)


def test_projection_not_applied_under_unrelated_destination_sibling(monkeypatch):
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    a = {
        "name": "A",
        "is_folder": True,
        "item_path": "OnlyA",
        "drive_id": "sd1",
        "id": "a1",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([a])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    tgt = {
        "name": "Target",
        "item_path": "Root3\\Target",
        "destination_path": "Root3\\Target",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    other = {
        "name": "Other",
        "item_path": "Root3\\Other",
        "destination_path": "Root3\\Other",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, tgt)
    MainWindow._apply_tree_item_visual_state(mw, None, other)
    dm.append_child_payloads(r_ix, [tgt, other])
    tgt_ix = dm.index(0, 0, r_ix)
    other_ix = dm.index(1, 0, r_ix)

    mw.planned_moves = [
        {
            "source_path": r"OnlyA",
            "destination_path": r"Root3\Target",
            "source": {"is_folder": True, "name": "A", "drive_id": "sd1", "id": "a1"},
            "request_id": "m1",
        }
    ]
    MainWindow._load_destination_projected_descendants_index(mw, tgt_ix)
    MainWindow._load_destination_projected_descendants_index(mw, other_ix)
    assert dm.rowCount(other_ix) == 0


def test_refresh_after_source_subtree_extends_destination_projection(monkeypatch):
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    root = {
        "name": "FRoot",
        "is_folder": True,
        "item_path": "FRoot",
        "drive_id": "sd1",
        "id": "fr",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([root])
    root_ix = sm.index(0, 0, QModelIndex())

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "FRoot",
        "item_path": "Root3\\FRootDest",
        "destination_path": "Root3\\FRootDest",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)

    mw.planned_moves = [
        {
            "source_path": r"FRoot",
            "destination_path": r"Root3\FRootDest",
            "target_name": "FRootDest",
            "source": {"is_folder": True, "name": "FRoot", "drive_id": "sd1", "id": "fr"},
            "request_id": "refr",
        }
    ]
    MainWindow._load_destination_projected_descendants_index(mw, tgt_ix)
    assert _dest_child_names(dm, tgt_ix) == []

    f2 = {
        "name": "Second.pdf",
        "is_folder": False,
        "item_path": "FRoot\\Second.pdf",
        "drive_id": "sd1",
        "id": "f2",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.replace_all_children(root_ix, [f2])

    MainWindow._destination_refresh_folder_move_projection_after_source_paths_loaded(mw, ["FRoot"])
    assert "Second.pdf" in _dest_child_names(dm, tgt_ix)


def test_invariant_pass_projects_without_explicit_load(monkeypatch):
    """State-driven enforcement: no _load_destination_projected_descendants_index call."""
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    hr = {
        "name": "HR",
        "is_folder": True,
        "item_path": "HR",
        "drive_id": "sd1",
        "id": "id-hr",
        "children_loaded": True,
        "tree_role": "source",
    }
    con = {
        "name": "Contractor Resumes",
        "is_folder": True,
        "item_path": "HR\\Contractor Resumes",
        "drive_id": "sd1",
        "id": "id-con",
        "children_loaded": True,
        "tree_role": "source",
    }
    inner = {
        "name": "InnerDoc.pdf",
        "is_folder": False,
        "item_path": "HR\\Contractor Resumes\\InnerDoc.pdf",
        "drive_id": "sd1",
        "id": "id-pdf",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([hr])
    hr_ix = sm.index(0, 0, QModelIndex())
    sm.replace_all_children(hr_ix, [con])
    con_ix = sm.index(0, 0, hr_ix)
    sm.replace_all_children(con_ix, [inner])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "Contractor Resumes",
        "item_path": "Root3\\HR\\Employee Files\\Contractor Resumes",
        "destination_path": "Root3\\HR\\Employee Files\\Contractor Resumes",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "overlay_state": "PlannedAllocation",
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)

    mw.planned_moves = [
        {
            "source_path": r"HR\Contractor Resumes",
            "destination_path": r"Root3\HR\Employee Files\Contractor Resumes",
            "source": {"is_folder": True, "name": "Contractor Resumes", "drive_id": "sd1", "id": "id-con"},
            "request_id": "inv-1",
        }
    ]
    MainWindow._enforce_overlay_projection_invariants(mw)
    assert "InnerDoc.pdf" in _dest_child_names(dm, tgt_ix)


def test_invariant_pass_idempotent_and_order_independent(monkeypatch):
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    fld = {
        "name": "Email",
        "is_folder": True,
        "item_path": "Mgmt\\Email",
        "drive_id": "sd1",
        "id": "e1",
        "children_loaded": True,
        "tree_role": "source",
    }
    subf = {
        "name": "Sub",
        "is_folder": True,
        "item_path": "Mgmt\\Email\\Sub",
        "drive_id": "sd1",
        "id": "e2",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([fld])
    fld_ix = sm.index(0, 0, QModelIndex())
    sm.replace_all_children(fld_ix, [subf])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "Email attachments",
        "item_path": "Root3\\Management\\Email attachments",
        "destination_path": "Root3\\Management\\Email attachments",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)

    mw.planned_moves = [
        {
            "source_path": r"Mgmt\Email",
            "destination_path": r"Root3\Management\Email attachments",
            "target_name": "Email attachments",
            "source": {"is_folder": True, "name": "Email", "drive_id": "sd1", "id": "e1"},
            "request_id": "inv-2",
        }
    ]
    # Reactive mutation hook runs invariant synchronously on assign; tree is already correct.
    assert "Sub" in _dest_child_names(dm, tgt_ix)
    assert MainWindow._enforce_destination_overlay_source_projection_invariant(mw, "a") == 0
    assert MainWindow._enforce_destination_overlay_source_projection_invariant(mw, "b") == 0
    MainWindow._load_destination_projected_descendants_index(mw, tgt_ix)
    assert MainWindow._enforce_destination_overlay_source_projection_invariant(mw, "c") == 0


def test_invariant_pass_repairs_after_source_change_without_refresh_hook(monkeypatch):
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    root = {
        "name": "FRoot",
        "is_folder": True,
        "item_path": "FRoot",
        "drive_id": "sd1",
        "id": "fr",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([root])
    root_ix = sm.index(0, 0, QModelIndex())

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "FRoot",
        "item_path": "Root3\\FRootDest",
        "destination_path": "Root3\\FRootDest",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)

    mw.planned_moves = [
        {
            "source_path": r"FRoot",
            "destination_path": r"Root3\FRootDest",
            "target_name": "FRootDest",
            "source": {"is_folder": True, "name": "FRoot", "drive_id": "sd1", "id": "fr"},
            "request_id": "inv-3",
        }
    ]
    MainWindow._enforce_overlay_projection_invariants(mw)
    assert _dest_child_names(dm, tgt_ix) == []

    f2 = {
        "name": "Second.pdf",
        "is_folder": False,
        "item_path": "FRoot\\Second.pdf",
        "drive_id": "sd1",
        "id": "f2",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.replace_all_children(root_ix, [f2])
    MainWindow._enforce_overlay_projection_invariants(mw)
    assert "Second.pdf" in _dest_child_names(dm, tgt_ix)


def test_enforce_overlay_idempotent_no_duplicate_rows(monkeypatch):
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    hr = {
        "name": "HR",
        "is_folder": True,
        "item_path": "HR",
        "drive_id": "sd1",
        "id": "id-hr",
        "children_loaded": True,
        "tree_role": "source",
    }
    con = {
        "name": "Contractor Resumes",
        "is_folder": True,
        "item_path": "HR\\Contractor Resumes",
        "drive_id": "sd1",
        "id": "id-con",
        "children_loaded": True,
        "tree_role": "source",
    }
    inner = {
        "name": "InnerDoc.pdf",
        "is_folder": False,
        "item_path": "HR\\Contractor Resumes\\InnerDoc.pdf",
        "drive_id": "sd1",
        "id": "id-pdf",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([hr])
    hr_ix = sm.index(0, 0, QModelIndex())
    sm.replace_all_children(hr_ix, [con])
    con_ix = sm.index(0, 0, hr_ix)
    sm.replace_all_children(con_ix, [inner])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "Contractor Resumes",
        "item_path": "Root3\\HR\\Employee Files\\Contractor Resumes",
        "destination_path": "Root3\\HR\\Employee Files\\Contractor Resumes",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "overlay_state": "PlannedAllocation",
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)
    mw.planned_moves = [
        {
            "source_path": r"HR\Contractor Resumes",
            "destination_path": r"Root3\HR\Employee Files\Contractor Resumes",
            "source": {"is_folder": True, "name": "Contractor Resumes", "drive_id": "sd1", "id": "id-con"},
            "request_id": "idem",
        }
    ]
    MainWindow._enforce_overlay_projection_invariants(mw)
    c1 = dm.rowCount(tgt_ix)
    n1 = _dest_child_names(dm, tgt_ix)
    MainWindow._enforce_overlay_projection_invariants(mw)
    assert dm.rowCount(tgt_ix) == c1
    assert _dest_child_names(dm, tgt_ix) == n1
    assert c1 == 1


def test_enforce_overlay_after_destination_before_source_subtree_complete(monkeypatch):
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    hr = {
        "name": "HR",
        "is_folder": True,
        "item_path": "HR",
        "drive_id": "sd1",
        "id": "id-hr",
        "children_loaded": True,
        "tree_role": "source",
    }
    con = {
        "name": "Contractor Resumes",
        "is_folder": True,
        "item_path": "HR\\Contractor Resumes",
        "drive_id": "sd1",
        "id": "id-con",
        "children_loaded": False,
        "tree_role": "source",
    }
    sm.reset_root_payloads([hr])
    hr_ix = sm.index(0, 0, QModelIndex())
    sm.replace_all_children(hr_ix, [con])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "Contractor Resumes",
        "item_path": "Root3\\HR\\Employee Files\\Contractor Resumes",
        "destination_path": "Root3\\HR\\Employee Files\\Contractor Resumes",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)
    mw.planned_moves = [
        {
            "source_path": r"HR\Contractor Resumes",
            "destination_path": r"Root3\HR\Employee Files\Contractor Resumes",
            "source": {"is_folder": True, "name": "Contractor Resumes", "drive_id": "sd1", "id": "id-con"},
            "request_id": "order",
        }
    ]
    MainWindow._enforce_overlay_projection_invariants(mw)
    assert _dest_child_names(dm, tgt_ix) == []

    inner = {
        "name": "InnerDoc.pdf",
        "is_folder": False,
        "item_path": "HR\\Contractor Resumes\\InnerDoc.pdf",
        "drive_id": "sd1",
        "id": "id-pdf",
        "children_loaded": True,
        "tree_role": "source",
    }
    con_loaded = dict(con)
    con_loaded["children_loaded"] = True
    sm.replace_all_children(hr_ix, [con_loaded])
    con_ix2 = sm.index(0, 0, hr_ix)
    sm.replace_all_children(con_ix2, [inner])

    MainWindow._enforce_overlay_projection_invariants(mw)
    assert "InnerDoc.pdf" in _dest_child_names(dm, tgt_ix)


@pytest.mark.parametrize(
    "target_extra",
    [
        {"planned_allocation": True, "node_origin": "PlannedAllocation", "overlay_state": "PlannedAllocation"},
        {
            "verification_state": "planned_only",
            "workspace_planned_row": True,
        },
        {"proposed": True, "node_origin": "Proposed"},
    ],
)
def test_enforce_overlay_same_subtree_independent_of_overlay_metadata(monkeypatch, target_extra):
    mw, sm, dm = _mw_graph_source_dest(monkeypatch)
    fld = {
        "name": "Email",
        "is_folder": True,
        "item_path": "Mgmt\\Email",
        "drive_id": "sd1",
        "id": "e1",
        "children_loaded": True,
        "tree_role": "source",
    }
    subf = {
        "name": "Sub",
        "is_folder": True,
        "item_path": "Mgmt\\Email\\Sub",
        "drive_id": "sd1",
        "id": "e2",
        "children_loaded": True,
        "tree_role": "source",
    }
    sm.reset_root_payloads([fld])
    fld_ix = sm.index(0, 0, QModelIndex())
    sm.replace_all_children(fld_ix, [subf])

    root3 = dict(_base_destination_root())
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    target = {
        "name": "Email attachments",
        "item_path": "Root3\\Management\\Email attachments",
        "destination_path": "Root3\\Management\\Email attachments",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "is_folder": True,
        "children_loaded": False,
        "id": "",
        "drive_id": "d1",
        "library_id": "d1",
    }
    target.update(target_extra)
    MainWindow._apply_tree_item_visual_state(mw, None, target)
    dm.append_child_payloads(r_ix, [target])
    tgt_ix = dm.index(0, 0, r_ix)
    mw.planned_moves = [
        {
            "source_path": r"Mgmt\Email",
            "destination_path": r"Root3\Management\Email attachments",
            "target_name": "Email attachments",
            "source": {"is_folder": True, "name": "Email", "drive_id": "sd1", "id": "e1"},
            "request_id": "meta",
        }
    ]
    MainWindow._enforce_overlay_projection_invariants(mw)
    assert _dest_child_names(dm, tgt_ix) == ["Sub"]
