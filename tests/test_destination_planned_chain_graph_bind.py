"""SharePoint Graph mode: planned workspace chain under visible parent when children are missing."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.graph import GraphClient
from ozlink_console.main_window import MainWindow
from ozlink_console.models import ProposedFolder
from ozlink_console.sharepoint_destination_overlay_attach import (
    destination_payload_is_live_graph_row,
    destination_payload_is_planned_workspace_row,
)
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _app():
    return QApplication.instance() or QApplication([])


def _graph_mw_with_sales_branch(monkeypatch):
    """Single library hub Root3 with live Graph folder Sales (children_loaded, no Follow Up child)."""
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_tree_widget = tw
    mw._memory_restore_in_progress = False
    mw._memory_restore_background_trees = False

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
        "_allocation_parent_path",
        "_get_unresolved_allocation_candidates_for_parent",
        "_paths_equivalent",
        "_destination_row_semantic_path",
        "_unresolved_overlay_target_paths_equivalent",
        "_find_destination_child_by_path",
        "_find_visible_destination_item_by_path",
        "_destination_model_index_user_role_dict",
        "_destination_row_is_live_graph_structure",
        "_sharepoint_bind_planned_segment_chain",
        "_build_sharepoint_planned_folder_payload",
        "_build_sharepoint_planned_file_payload",
        "_tree_name_column_label",
        "_apply_tree_item_visual_state",
        "_ensure_planned_allocation_overlay_on_real_model_index",
        "_mark_allocation_resolved",
        "_refresh_destination_item_visibility_index",
        "_destination_visible_path_lookup_canonical_keys_ex",
    ):
        setattr(mw, name, getattr(MainWindow, name).__get__(mw, MainWindow))

    mw._log_restore_phase = lambda *a, **k: None
    mw._destination_visible_library_anchor_canonical_path = lambda: "Root3"
    mw._request_graph_destination_children_load = lambda *_a, **_k: None
    mw._log_sharepoint_overlay_destination_anchor_normalized = lambda **_k: None
    mw._sync_restore_destination_overlay_pending_from_unresolved_queues = lambda: None
    mw._unresolved_overlay_pass_audit_touch_illegal = lambda: None
    mw._unresolved_overlay_pass_audit_touch_removal = lambda *_a, **_k: None
    mw._log_unresolved_queue_removal_event = lambda **_k: None

    root3 = {
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
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    sales = {
        "name": "Sales",
        "id": "id-sales",
        "is_folder": True,
        "item_path": "Root3\\Sales",
        "destination_path": "Root3\\Sales",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, sales)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(r_ix, [sales])

    mw._destination_row_is_live_graph_structure = lambda pl: destination_payload_is_live_graph_row(pl)
    mw.unresolved_allocations_by_parent_path = {}
    mw.planned_moves = []
    mw.proposed_folders = []
    return mw, dm


def _graph_mw_for_nested_proposed(monkeypatch):
    """Sales live folder + unresolved nested ProposedFolder rows (parent under planned [Planned] row)."""
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_tree_widget = tw
    mw._memory_restore_in_progress = False
    mw._memory_restore_background_trees = False
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
    mw.planned_moves = []
    mw.proposed_folders = []

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
        "_allocation_parent_path",
        "_get_unresolved_allocation_candidates_for_parent",
        "_paths_equivalent",
        "_destination_row_semantic_path",
        "_unresolved_overlay_target_paths_equivalent",
        "_find_destination_child_by_path",
        "_find_visible_destination_item_by_path",
        "_destination_model_index_user_role_dict",
        "_destination_row_is_live_graph_structure",
        "_sharepoint_bind_planned_segment_chain",
        "_sharepoint_canonical_path_segments_under_parent",
        "_build_sharepoint_planned_folder_payload",
        "_build_sharepoint_planned_file_payload",
        "_tree_name_column_label",
        "_apply_tree_item_visual_state",
        "_ensure_planned_allocation_overlay_on_real_model_index",
        "_mark_allocation_resolved",
        "_refresh_destination_item_visibility_index",
        "_destination_visible_path_lookup_canonical_keys_ex",
        "_proposed_parent_path",
        "_proposed_destination_path",
        "_proposed_folder_key",
        "_ensure_proposed_folder_stable_key",
        "_build_proposed_payload",
        "_apply_proposed_children_to_model_index",
        "_materialize_planned_move_file_rows_under_parent",
        "_materialize_planned_workspace_proposed_descendants_fixpoint",
        "_load_destination_projected_descendants_index",
        "_find_planned_move_for_destination_node",
        "node_is_proposed",
        "node_is_planned_allocation",
    ):
        setattr(mw, name, getattr(MainWindow, name).__get__(mw, MainWindow))

    mw._log_restore_phase = lambda *a, **k: None
    mw._destination_visible_library_anchor_canonical_path = lambda: "Root3"
    mw._request_graph_destination_children_load = lambda *_a, **_k: None
    mw._log_sharepoint_overlay_destination_anchor_normalized = lambda **_k: None
    mw._sync_restore_destination_overlay_pending_from_unresolved_queues = lambda: None
    mw._unresolved_overlay_pass_audit_touch_illegal = lambda: None
    mw._unresolved_overlay_pass_audit_touch_removal = lambda *_a, **_k: None
    mw._log_unresolved_queue_removal_event = lambda **_k: None
    mw._mark_proposed_folder_resolved = MainWindow._mark_proposed_folder_resolved.__get__(mw, MainWindow)
    mw._unresolved_overlay_verify_proposed_outcome = MainWindow._unresolved_overlay_verify_proposed_outcome.__get__(
        mw, MainWindow
    )
    mw._apply_proposed_overlay_to_existing_graph_row_model_index = (
        MainWindow._apply_proposed_overlay_to_existing_graph_row_model_index.__get__(mw, MainWindow)
    )
    mw._ensure_destination_projection_path_sharepoint_graph_only = (
        MainWindow._ensure_destination_projection_path_sharepoint_graph_only.__get__(mw, MainWindow)
    )

    root3 = {
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
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    sales = {
        "name": "Sales",
        "id": "id-sales",
        "is_folder": True,
        "item_path": "Root3\\Sales",
        "destination_path": "Root3\\Sales",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, sales)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(r_ix, [sales])

    mw._destination_row_is_live_graph_structure = lambda pl: destination_payload_is_live_graph_row(pl)
    return mw, dm


def _child_names(dm, pix):
    out = []
    for r in range(dm.rowCount(pix)):
        pl = dm.index(r, 0, pix).data(Qt.UserRole) or {}
        out.append(str(pl.get("name") or ""))
    return out


def test_ensure_sharepoint_graph_only_creates_planned_chain_under_visible_parent(monkeypatch):
    mw, dm = _graph_mw_with_sales_branch(monkeypatch)
    leaf = MainWindow._ensure_destination_projection_path_sharepoint_graph_only(
        mw,
        "Root3\\Sales\\Follow Up\\Salary Increases.xlsx",
        leaf_is_file=True,
    )
    assert leaf is not None and leaf.isValid()
    pl = leaf.data(Qt.UserRole) or {}
    assert destination_payload_is_planned_workspace_row(pl)
    assert "Salary" in str(pl.get("name") or "")
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    names = _child_names(dm, sales_ix)
    assert "Follow Up" in names


def test_allocation_overlay_after_planned_chain_under_graph_authority(monkeypatch):
    mw, dm = _graph_mw_with_sales_branch(monkeypatch)
    move = {
        "destination_path": "Root3\\Sales\\Follow Up\\Salary Increases.xlsx",
        "source": {"is_folder": False, "name": "Salary Increases.xlsx"},
        "request_id": "talloc1",
    }
    pp = MainWindow._allocation_parent_path(mw, move)
    mk = MainWindow._allocation_move_key(mw, move)
    mw.unresolved_allocations_by_parent_path = {pp: {mk: move}}

    fol_ix = MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, pp, leaf_is_file=False)
    assert fol_ix is not None and fol_ix.isValid()

    n = MainWindow._apply_allocation_children_to_model_index(mw, fol_ix)
    assert n == 1
    assert mw._unresolved_allocation_queue_size() == 0
    # Leaf row should be visible planned allocation under Follow Up
    fu_ix = None
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        pl = ix.data(Qt.UserRole) or {}
        if str(pl.get("name") or "") == "Follow Up":
            fu_ix = ix
            break
    assert fu_ix is not None
    leaf_names = _child_names(dm, fu_ix)
    assert any("Salary" in n for n in leaf_names)


def test_nested_proposed_folders_materialize_in_single_apply_pass(monkeypatch):
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    pf_hr = ProposedFolder(
        FolderName="HR",
        DestinationPath=r"Root3\Sales\HR",
        ParentPath=r"Root3\Sales",
        StableKey="sk-hr",
    )
    pf_emp = ProposedFolder(
        FolderName="Employee Files",
        DestinationPath=r"Root3\Sales\HR\Employee Files",
        ParentPath=r"Root3\Sales\HR",
        StableKey="sk-emp",
    )
    k1 = MainWindow._proposed_folder_key(mw, pf_hr)
    k2 = MainWindow._proposed_folder_key(mw, pf_emp)
    mw.unresolved_proposed_by_parent_path = {
        r"Root3\Sales": {k1: pf_hr},
        r"Root3\Sales\HR": {k2: pf_emp},
    }
    n = MainWindow._apply_proposed_children_to_model_index(mw, sales_ix)
    assert n >= 1
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        pl = ix.data(Qt.UserRole) or {}
        if str(pl.get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    emp_names = _child_names(dm, hr_ix)
    assert "Employee Files" in emp_names


def test_expand_planned_workspace_folder_hydrates_nested_proposed_descendants(monkeypatch):
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    pf_hr = ProposedFolder(
        FolderName="HR",
        DestinationPath=r"Root3\Sales\HR",
        ParentPath=r"Root3\Sales",
        StableKey="sk-hr2",
    )
    pf_emp = ProposedFolder(
        FolderName="Employee Files",
        DestinationPath=r"Root3\Sales\HR\Employee Files",
        ParentPath=r"Root3\Sales\HR",
        StableKey="sk-emp2",
    )
    k1 = MainWindow._proposed_folder_key(mw, pf_hr)
    k2 = MainWindow._proposed_folder_key(mw, pf_emp)
    mw.unresolved_proposed_by_parent_path = {
        r"Root3\Sales": {k1: pf_hr},
        r"Root3\Sales\HR": {k2: pf_emp},
    }
    MainWindow._apply_proposed_children_to_model_index(mw, sales_ix)
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        pl = ix.data(Qt.UserRole) or {}
        if str(pl.get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    pl_hr = dict(hr_ix.data(Qt.UserRole) or {})
    assert destination_payload_is_planned_workspace_row(pl_hr)
    # Simulate fresh expand: nested queue still had parent; row was left unloaded before fix.
    mw.unresolved_proposed_by_parent_path = {r"Root3\Sales\HR": {k2: pf_emp}}
    def _mut_unload(p):
        p["children_loaded"] = False

    dm.update_payload_for_index(hr_ix, _mut_unload)
    MainWindow._load_destination_projected_descendants_index(mw, hr_ix)
    assert "Employee Files" in _child_names(dm, hr_ix)


def test_repeated_materialize_pass_idempotent_for_nested_proposed(monkeypatch):
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    pf_hr = ProposedFolder(
        FolderName="HR",
        DestinationPath=r"Root3\Sales\HR",
        ParentPath=r"Root3\Sales",
        StableKey="sk-hr3",
    )
    pf_emp = ProposedFolder(
        FolderName="Employee Files",
        DestinationPath=r"Root3\Sales\HR\Employee Files",
        ParentPath=r"Root3\Sales\HR",
        StableKey="sk-emp3",
    )
    k1 = MainWindow._proposed_folder_key(mw, pf_hr)
    k2 = MainWindow._proposed_folder_key(mw, pf_emp)
    mw.unresolved_proposed_by_parent_path = {
        r"Root3\Sales": {k1: pf_hr},
        r"Root3\Sales\HR": {k2: pf_emp},
    }
    MainWindow._apply_proposed_children_to_model_index(mw, sales_ix)
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        pl = ix.data(Qt.UserRole) or {}
        if str(pl.get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    before = _child_names(dm, hr_ix)
    mw.unresolved_proposed_by_parent_path = {}
    MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    assert _child_names(dm, hr_ix) == before


def test_three_level_proposed_folder_chain_single_pass(monkeypatch):
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    pf_a = ProposedFolder(
        FolderName="HR",
        DestinationPath=r"Root3\Sales\HR",
        ParentPath=r"Root3\Sales",
        StableKey="sk-a",
    )
    pf_b = ProposedFolder(
        FolderName="Employee Files",
        DestinationPath=r"Root3\Sales\HR\Employee Files",
        ParentPath=r"Root3\Sales\HR",
        StableKey="sk-b",
    )
    pf_c = ProposedFolder(
        FolderName="Contractor Resumes",
        DestinationPath=r"Root3\Sales\HR\Employee Files\Contractor Resumes",
        ParentPath=r"Root3\Sales\HR\Employee Files",
        StableKey="sk-c",
    )
    buckets = {
        r"Root3\Sales": {MainWindow._proposed_folder_key(mw, pf_a): pf_a},
        r"Root3\Sales\HR": {MainWindow._proposed_folder_key(mw, pf_b): pf_b},
        r"Root3\Sales\HR\Employee Files": {MainWindow._proposed_folder_key(mw, pf_c): pf_c},
    }
    mw.unresolved_proposed_by_parent_path = buckets
    MainWindow._apply_proposed_children_to_model_index(mw, sales_ix)
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    emp_ix = None
    for r in range(dm.rowCount(hr_ix)):
        ix = dm.index(r, 0, hr_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "Employee Files":
            emp_ix = ix
            break
    assert emp_ix is not None
    assert "Contractor Resumes" in _child_names(dm, emp_ix)


def test_fixpoint_materializes_planned_move_files_under_multilevel_chain_one_pass(monkeypatch):
    """Proposed folders + planned_moves file leaves attach in one fixpoint (no separate replay)."""
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    # Fixpoint only runs proposed+file attach on planned_workspace rows; live Sales is traversed but skipped.
    MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, r"Root3\Sales\HR", leaf_is_file=False)
    pf_emp = ProposedFolder(
        FolderName="Employee Files",
        DestinationPath=r"Root3\Sales\HR\Employee Files",
        ParentPath=r"Root3\Sales\HR",
        StableKey="sk-emp-file",
    )
    mw.unresolved_proposed_by_parent_path = {
        r"Root3\Sales\HR": {MainWindow._proposed_folder_key(mw, pf_emp): pf_emp},
    }
    mw.planned_moves = [
        {
            "destination_path": r"Root3\Sales\HR\Employee Files\Quarterly.docx",
            "source": {"is_folder": False, "name": "Quarterly.docx"},
            "request_id": "pm-fix-1",
        }
    ]
    n = MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    assert n >= 1
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    emp_ix = None
    for r in range(dm.rowCount(hr_ix)):
        ix = dm.index(r, 0, hr_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "Employee Files":
            emp_ix = ix
            break
    assert emp_ix is not None
    assert any("Quarterly" in x for x in _child_names(dm, emp_ix))


def test_fixpoint_idempotent_including_planned_move_files(monkeypatch):
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, r"Root3\Sales\HR", leaf_is_file=False)
    pf_emp = ProposedFolder(
        FolderName="Employee Files",
        DestinationPath=r"Root3\Sales\HR\Employee Files",
        ParentPath=r"Root3\Sales\HR",
        StableKey="sk-emp-idem",
    )
    mw.unresolved_proposed_by_parent_path = {
        r"Root3\Sales\HR": {MainWindow._proposed_folder_key(mw, pf_emp): pf_emp},
    }
    mw.planned_moves = [
        {
            "destination_path": r"Root3\Sales\HR\Employee Files\Stable.pdf",
            "source": {"is_folder": False, "name": "Stable.pdf"},
            "request_id": "pm-idem",
        }
    ]
    n1 = MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    assert n1 >= 1
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    emp_ix = None
    for r in range(dm.rowCount(hr_ix)):
        ix = dm.index(r, 0, hr_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "Employee Files":
            emp_ix = ix
            break
    assert emp_ix is not None
    after_first = _child_names(dm, emp_ix)
    n2 = MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    assert n2 == 0
    assert _child_names(dm, emp_ix) == after_first


def test_planned_move_file_materialization_does_not_call_graph_create_child_folder(monkeypatch):
    def _forbidden(*_a, **_k):
        raise AssertionError("GraphClient.create_child_folder must not run for projection-only materialization")

    monkeypatch.setattr(GraphClient, "create_child_folder", _forbidden)
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, r"Root3\Sales\HR", leaf_is_file=False)
    pf_emp = ProposedFolder(
        FolderName="Employee Files",
        DestinationPath=r"Root3\Sales\HR\Employee Files",
        ParentPath=r"Root3\Sales\HR",
        StableKey="sk-emp-graph",
    )
    mw.unresolved_proposed_by_parent_path = {
        r"Root3\Sales\HR": {MainWindow._proposed_folder_key(mw, pf_emp): pf_emp},
    }
    mw.planned_moves = [
        {
            "destination_path": r"Root3\Sales\HR\Employee Files\ReadOnly.xlsx",
            "source": {"is_folder": False, "name": "ReadOnly.xlsx"},
            "request_id": "pm-graph",
        }
    ]
    MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    emp_ix = None
    for r in range(dm.rowCount(hr_ix)):
        ix = dm.index(r, 0, hr_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "Employee Files":
            emp_ix = ix
            break
    assert emp_ix is not None
    assert any("ReadOnly" in x for x in _child_names(dm, emp_ix))


def test_planned_move_file_not_attached_when_parent_ix_mismatches_destination_parent(monkeypatch):
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, r"Root3\Sales\HR", leaf_is_file=False)
    pf_emp = ProposedFolder(
        FolderName="Employee Files",
        DestinationPath=r"Root3\Sales\HR\Employee Files",
        ParentPath=r"Root3\Sales\HR",
        StableKey="sk-emp-mis",
    )
    mw.unresolved_proposed_by_parent_path = {
        r"Root3\Sales\HR": {MainWindow._proposed_folder_key(mw, pf_emp): pf_emp},
    }
    mw.planned_moves = [
        {
            "destination_path": r"Root3\Sales\HR\Employee Files\OnlyHere.msg",
            "source": {"is_folder": False, "name": "OnlyHere.msg"},
            "request_id": "pm-mis",
        }
    ]
    MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    emp_ix = None
    for r in range(dm.rowCount(hr_ix)):
        ix = dm.index(r, 0, hr_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "Employee Files":
            emp_ix = ix
            break
    assert emp_ix is not None
    # Immediate parent of the file is Employee Files, not Sales.
    assert MainWindow._materialize_planned_move_file_rows_under_parent(mw, sales_ix) == 0


def test_expand_planned_workspace_load_runs_fixpoint_for_planned_move_files(monkeypatch):
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    pf_hr = ProposedFolder(
        FolderName="HR",
        DestinationPath=r"Root3\Sales\HR",
        ParentPath=r"Root3\Sales",
        StableKey="sk-hr-ld",
    )
    pf_emp = ProposedFolder(
        FolderName="Employee Files",
        DestinationPath=r"Root3\Sales\HR\Employee Files",
        ParentPath=r"Root3\Sales\HR",
        StableKey="sk-emp-ld",
    )
    k1 = MainWindow._proposed_folder_key(mw, pf_hr)
    k2 = MainWindow._proposed_folder_key(mw, pf_emp)
    mw.unresolved_proposed_by_parent_path = {
        r"Root3\Sales": {k1: pf_hr},
        r"Root3\Sales\HR": {k2: pf_emp},
    }
    mw.planned_moves = [
        {
            "destination_path": r"Root3\Sales\HR\Employee Files\ExpandMe.txt",
            "source": {"is_folder": False, "name": "ExpandMe.txt"},
            "request_id": "pm-ld",
        }
    ]
    MainWindow._apply_proposed_children_to_model_index(mw, sales_ix)
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    mw.unresolved_proposed_by_parent_path = {r"Root3\Sales\HR": {k2: pf_emp}}

    def _mut_unload(p):
        p["children_loaded"] = False

    dm.update_payload_for_index(hr_ix, _mut_unload)
    MainWindow._load_destination_projected_descendants_index(mw, hr_ix)
    emp_ix = None
    for r in range(dm.rowCount(hr_ix)):
        ix = dm.index(r, 0, hr_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "Employee Files":
            emp_ix = ix
            break
    assert emp_ix is not None
    assert any("ExpandMe" in x for x in _child_names(dm, emp_ix))


def test_planned_move_file_under_unmaterialized_subfolder_not_attached_at_grandparent(monkeypatch):
    """Destination parent must match the planned folder row (not a shallower prefix)."""
    mw, dm = _graph_mw_for_nested_proposed(monkeypatch)
    sales_ix = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, r"Root3\Sales\HR", leaf_is_file=False)
    mw.unresolved_proposed_by_parent_path = {}
    mw.planned_moves = [
        {
            "destination_path": r"Root3\Sales\HR\NotInModel\orphan.pdf",
            "source": {"is_folder": False, "name": "orphan.pdf"},
            "request_id": "pm-orph",
        }
    ]
    MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    hr_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            hr_ix = ix
            break
    assert hr_ix is not None
    for nm in _child_names(dm, hr_ix):
        assert "orphan" not in nm
