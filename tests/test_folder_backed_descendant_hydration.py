"""Graph authority: fixpoint hydrates folder-backed descendants (planned_moves folders, allocations, proposed)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.models import ProposedFolder
from ozlink_console.sharepoint_destination_overlay_attach import destination_payload_is_live_graph_row
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _app():
    return QApplication.instance() or QApplication([])


def _child_folder_names(dm, pix):
    out = []
    for r in range(dm.rowCount(pix)):
        pl = dm.index(r, 0, pix).data(Qt.UserRole) or {}
        if pl.get("placeholder"):
            continue
        if not bool(pl.get("is_folder", True)):
            continue
        out.append(str(pl.get("name") or ""))
    return out


def _bind_graph_mw(monkeypatch, *, names: tuple[str, ...]):
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
    for name in names:
        setattr(mw, name, getattr(MainWindow, name).__get__(mw, MainWindow))
    mw._log_restore_phase = lambda *a, **k: None
    mw._destination_visible_library_anchor_canonical_path = lambda: "Root3"
    mw._request_graph_destination_children_load = lambda *_a, **_k: None
    mw._log_sharepoint_overlay_destination_anchor_normalized = lambda **_k: None
    mw._sync_restore_destination_overlay_pending_from_unresolved_queues = lambda: None
    mw._unresolved_overlay_pass_audit_touch_illegal = lambda: None
    mw._unresolved_overlay_pass_audit_touch_removal = lambda *_a, **_k: None
    mw._log_unresolved_queue_removal_event = lambda **_k: None
    mw._destination_row_is_live_graph_structure = lambda pl: destination_payload_is_live_graph_row(pl)
    return mw, dm


def test_planned_workspace_folder_move_hydrates_nested_planned_folder(monkeypatch):
    """planned_moves with folder source bind under [Planned] workspace folder (same fixpoint pass)."""
    mw, dm = _bind_graph_mw(
        monkeypatch,
        names=(
            "normalize_memory_path",
            "_canonical_destination_projection_path",
            "_canonical_planned_memory_path_for_graph_match",
            "_canonical_destination_path_with_visible_library_anchor",
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
            "_paths_equivalent",
            "_destination_row_semantic_path",
            "_unresolved_overlay_target_paths_equivalent",
            "_find_destination_child_by_path",
            "_find_visible_destination_item_by_path",
            "_destination_model_index_user_role_dict",
            "_sharepoint_bind_planned_segment_chain",
            "_sharepoint_canonical_path_segments_under_parent",
            "_build_sharepoint_planned_folder_payload",
            "_build_sharepoint_planned_file_payload",
            "_tree_name_column_label",
            "_apply_tree_item_visual_state",
            "_refresh_destination_item_visibility_index",
            "_destination_visible_path_lookup_canonical_keys_ex",
            "_ensure_destination_projection_path_sharepoint_graph_only",
            "_forensic_planned_file_destination_paths_under_folder",
            "_forensic_planned_folder_move_destination_paths_under_folder",
            "_materialize_planned_move_file_rows_under_parent",
            "_materialize_planned_move_folder_rows_under_parent",
            "_materialize_planned_workspace_proposed_descendants_fixpoint",
            "_get_unresolved_candidates_for_parent",
            "_apply_proposed_children_to_model_index",
        ),
    )
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
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(r_ix, [sales])
    sales_ix = dm.index(0, 0, r_ix)
    MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, r"Root3\Sales\HR\Employee Files", leaf_is_file=False)
    mw.planned_moves = [
        {
            "destination_path": r"Root3\Sales\HR\Employee Files\Contractor Resumes",
            "source": {"is_folder": True, "name": "Contractor Resumes"},
            "request_id": "pfold1",
        }
    ]
    n = MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    assert n >= 1
    emp_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            for r2 in range(dm.rowCount(ix)):
                ix2 = dm.index(r2, 0, ix)
                if str((ix2.data(Qt.UserRole) or {}).get("name") or "") == "Employee Files":
                    emp_ix = ix2
                    break
            break
    assert emp_ix is not None
    assert "Contractor Resumes" in _child_folder_names(dm, emp_ix)


def test_allocated_folder_parent_hydrates_nested_allocation(monkeypatch):
    """[Allocated] folder rows receive :meth:`_apply_allocation_children_to_model_index` during fixpoint BFS."""
    mw, dm = _bind_graph_mw(
        monkeypatch,
        names=(
            "normalize_memory_path",
            "_canonical_destination_projection_path",
            "_canonical_planned_memory_path_for_graph_match",
            "_canonical_destination_path_with_visible_library_anchor",
            "_path_segments",
            "_tree_item_path",
            "_destination_parent_match_details",
            "_destination_row_raw_path_for_path_lookup_match",
            "_destination_semantic_path",
            "_paths_equivalent",
            "_destination_row_semantic_path",
            "_find_destination_child_by_path",
            "_find_visible_destination_item_by_path",
            "_destination_model_index_user_role_dict",
            "_sharepoint_bind_planned_segment_chain",
            "_sharepoint_canonical_path_segments_under_parent",
            "_build_sharepoint_planned_folder_payload",
            "_build_sharepoint_planned_file_payload",
            "_tree_name_column_label",
            "_apply_tree_item_visual_state",
            "_refresh_destination_item_visibility_index",
            "_destination_visible_path_lookup_canonical_keys_ex",
            "_ensure_destination_projection_path_sharepoint_graph_only",
            "_ensure_planned_allocation_overlay_on_real_model_index",
            "_mark_allocation_resolved",
            "_allocation_projection_path",
            "_allocation_move_key",
            "_apply_allocation_children_to_model_index",
            "_materialize_planned_workspace_proposed_descendants_fixpoint",
            "_get_unresolved_allocation_candidates_for_parent",
            "_apply_proposed_children_to_model_index",
            "_forensic_planned_file_destination_paths_under_folder",
            "_forensic_planned_folder_move_destination_paths_under_folder",
            "_materialize_planned_move_file_rows_under_parent",
            "_materialize_planned_move_folder_rows_under_parent",
            "_find_destination_child_by_allocation_exact_semantic_path",
        ),
    )
    root3 = {
        "name": "Root3",
        "id": "id-r",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    mgmt = {
        "name": "Management",
        "id": "id-m",
        "is_folder": True,
        "item_path": "Root3\\Management",
        "destination_path": "Root3\\Management",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, mgmt)
    email = {
        "name": "Email attachments",
        "id": "",
        "is_folder": True,
        "item_path": "Root3\\Management\\Email attachments",
        "destination_path": "Root3\\Management\\Email attachments",
        "tree_role": "destination",
        "row_kind": "planned_folder",
        "planned_allocation": True,
        "node_origin": "PlannedAllocation",
        "overlay_state": "PlannedAllocation",
        "children_loaded": False,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, email)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(r_ix, [mgmt])
    m_ix = dm.index(0, 0, r_ix)
    dm.append_child_payloads(m_ix, [email])
    move = {
        "destination_path": r"Root3\Management\Email attachments\Follow up",
        "source": {"is_folder": True, "name": "Follow up"},
        "request_id": "alloc-nested",
    }
    mk = MainWindow._allocation_move_key(mw, move)
    mw.unresolved_allocations_by_parent_path = {r"Root3\Management\Email attachments": {mk: move}}
    MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, r_ix)
    email_ix = dm.index(0, 0, m_ix)
    assert "Follow up" in _child_folder_names(dm, email_ix)


def test_proposed_folder_parent_hydrates_nested_proposed(monkeypatch):
    """[Proposed] folder (including under live Graph parent) pulls ``unresolved_proposed`` in fixpoint."""
    mw, dm = _bind_graph_mw(
        monkeypatch,
        names=(
            "normalize_memory_path",
            "_canonical_destination_projection_path",
            "_canonical_planned_memory_path_for_graph_match",
            "_canonical_destination_path_with_visible_library_anchor",
            "_path_segments",
            "_tree_item_path",
            "_destination_parent_match_details",
            "_destination_row_raw_path_for_path_lookup_match",
            "_destination_semantic_path",
            "_paths_equivalent",
            "_destination_row_semantic_path",
            "_find_destination_child_by_path",
            "_find_visible_destination_item_by_path",
            "_destination_model_index_user_role_dict",
            "_sharepoint_bind_planned_segment_chain",
            "_sharepoint_canonical_path_segments_under_parent",
            "_build_sharepoint_planned_folder_payload",
            "_build_sharepoint_planned_file_payload",
            "_tree_name_column_label",
            "_apply_tree_item_visual_state",
            "_refresh_destination_item_visibility_index",
            "_destination_visible_path_lookup_canonical_keys_ex",
            "_ensure_destination_projection_path_sharepoint_graph_only",
            "_proposed_parent_path",
            "_proposed_destination_path",
            "_proposed_folder_key",
            "_ensure_proposed_folder_stable_key",
            "_build_proposed_payload",
            "_apply_proposed_children_to_model_index",
            "_mark_proposed_folder_resolved",
            "_apply_proposed_overlay_to_existing_graph_row_model_index",
            "_materialize_planned_workspace_proposed_descendants_fixpoint",
            "_get_unresolved_candidates_for_parent",
            "_forensic_planned_file_destination_paths_under_folder",
            "_forensic_planned_folder_move_destination_paths_under_folder",
            "_materialize_planned_move_file_rows_under_parent",
            "_materialize_planned_move_folder_rows_under_parent",
        ),
    )
    mw._unresolved_overlay_verify_proposed_outcome = MainWindow._unresolved_overlay_verify_proposed_outcome.__get__(mw, MainWindow)
    root3 = {
        "name": "Root3",
        "id": "id-r2",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    newf = {
        "name": "New Folder",
        "id": "live-nf",
        "is_folder": True,
        "item_path": "Root3\\Management\\New Folder",
        "destination_path": "Root3\\Management\\New Folder",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
        "proposed": True,
        "node_origin": "Proposed",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, newf)
    mgmt = {
        "name": "Management",
        "id": "id-m2",
        "is_folder": True,
        "item_path": "Root3\\Management",
        "destination_path": "Root3\\Management",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, mgmt)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(r_ix, [mgmt])
    m_ix = dm.index(0, 0, r_ix)
    dm.append_child_payloads(m_ix, [newf])
    pf = ProposedFolder(
        FolderName="Deep",
        DestinationPath=r"Root3\Management\New Folder\Deep",
        ParentPath=r"Root3\Management\New Folder",
        StableKey="sk-deep",
    )
    k = MainWindow._proposed_folder_key(mw, pf)
    mw.unresolved_proposed_by_parent_path = {r"Root3\Management\New Folder": {k: pf}}
    nf_ix = dm.index(0, 0, m_ix)
    MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, r_ix)
    assert "Deep" in _child_folder_names(dm, nf_ix)


def test_mixed_proposed_queue_and_planned_folder_move_same_round(monkeypatch):
    mw, dm = _bind_graph_mw(
        monkeypatch,
        names=(
            "normalize_memory_path",
            "_canonical_destination_projection_path",
            "_canonical_planned_memory_path_for_graph_match",
            "_canonical_destination_path_with_visible_library_anchor",
            "_path_segments",
            "_tree_item_path",
            "_destination_parent_match_details",
            "_destination_row_raw_path_for_path_lookup_match",
            "_destination_semantic_path",
            "_paths_equivalent",
            "_destination_row_semantic_path",
            "_find_destination_child_by_path",
            "_find_visible_destination_item_by_path",
            "_destination_model_index_user_role_dict",
            "_sharepoint_bind_planned_segment_chain",
            "_sharepoint_canonical_path_segments_under_parent",
            "_build_sharepoint_planned_folder_payload",
            "_build_sharepoint_planned_file_payload",
            "_tree_name_column_label",
            "_apply_tree_item_visual_state",
            "_refresh_destination_item_visibility_index",
            "_destination_visible_path_lookup_canonical_keys_ex",
            "_ensure_destination_projection_path_sharepoint_graph_only",
            "_proposed_parent_path",
            "_proposed_destination_path",
            "_proposed_folder_key",
            "_ensure_proposed_folder_stable_key",
            "_build_proposed_payload",
            "_apply_proposed_children_to_model_index",
            "_mark_proposed_folder_resolved",
            "_apply_proposed_overlay_to_existing_graph_row_model_index",
            "_materialize_planned_workspace_proposed_descendants_fixpoint",
            "_get_unresolved_candidates_for_parent",
            "_forensic_planned_file_destination_paths_under_folder",
            "_forensic_planned_folder_move_destination_paths_under_folder",
            "_materialize_planned_move_file_rows_under_parent",
            "_materialize_planned_move_folder_rows_under_parent",
        ),
    )
    mw._unresolved_overlay_verify_proposed_outcome = MainWindow._unresolved_overlay_verify_proposed_outcome.__get__(mw, MainWindow)
    sales = {
        "name": "Sales",
        "id": "id-s3",
        "is_folder": True,
        "item_path": "Root3\\Sales",
        "destination_path": "Root3\\Sales",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, sales)
    root3 = {
        "name": "Root3",
        "id": "id-r3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(r_ix, [sales])
    sales_ix = dm.index(0, 0, r_ix)
    MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, r"Root3\Sales\HR\Employee Files", leaf_is_file=False)
    pf = ProposedFolder(
        FolderName="FromProposed",
        DestinationPath=r"Root3\Sales\HR\Employee Files\FromProposed",
        ParentPath=r"Root3\Sales\HR\Employee Files",
        StableKey="sk-mix-p",
    )
    mw.unresolved_proposed_by_parent_path = {
        r"Root3\Sales\HR\Employee Files": {MainWindow._proposed_folder_key(mw, pf): pf}
    }
    mw.planned_moves = [
        {
            "destination_path": r"Root3\Sales\HR\Employee Files\FromProposed\FromMove",
            "source": {"is_folder": True, "name": "FromMove"},
            "request_id": "mix-fold",
        }
    ]
    MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    emp_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            for r2 in range(dm.rowCount(ix)):
                ix2 = dm.index(r2, 0, ix)
                if str((ix2.data(Qt.UserRole) or {}).get("name") or "") == "Employee Files":
                    emp_ix = ix2
                    break
            break
    assert emp_ix is not None
    assert "FromProposed" in _child_folder_names(dm, emp_ix)
    fp_ix = None
    for r in range(dm.rowCount(emp_ix)):
        ix = dm.index(r, 0, emp_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "FromProposed":
            fp_ix = ix
            break
    assert fp_ix is not None
    assert "FromMove" in _child_folder_names(dm, fp_ix)


def test_planned_folder_move_not_hydrated_under_unrelated_sibling_parent(monkeypatch):
    mw, dm = _bind_graph_mw(
        monkeypatch,
        names=(
            "normalize_memory_path",
            "_canonical_destination_projection_path",
            "_canonical_planned_memory_path_for_graph_match",
            "_canonical_destination_path_with_visible_library_anchor",
            "_path_segments",
            "_tree_item_path",
            "_destination_parent_match_details",
            "_destination_row_raw_path_for_path_lookup_match",
            "_destination_semantic_path",
            "_paths_equivalent",
            "_destination_row_semantic_path",
            "_find_destination_child_by_path",
            "_find_visible_destination_item_by_path",
            "_destination_model_index_user_role_dict",
            "_sharepoint_bind_planned_segment_chain",
            "_sharepoint_canonical_path_segments_under_parent",
            "_build_sharepoint_planned_folder_payload",
            "_build_sharepoint_planned_file_payload",
            "_tree_name_column_label",
            "_apply_tree_item_visual_state",
            "_refresh_destination_item_visibility_index",
            "_destination_visible_path_lookup_canonical_keys_ex",
            "_ensure_destination_projection_path_sharepoint_graph_only",
            "_get_unresolved_candidates_for_parent",
            "_apply_proposed_children_to_model_index",
            "_forensic_planned_file_destination_paths_under_folder",
            "_forensic_planned_folder_move_destination_paths_under_folder",
            "_materialize_planned_move_file_rows_under_parent",
            "_materialize_planned_move_folder_rows_under_parent",
            "_materialize_planned_workspace_proposed_descendants_fixpoint",
        ),
    )
    sales = {
        "name": "Sales",
        "id": "id-s4",
        "is_folder": True,
        "item_path": "Root3\\Sales",
        "destination_path": "Root3\\Sales",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, sales)
    root3 = {
        "name": "Root3",
        "id": "id-r4",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root3)
    dm.reset_root_payloads([root3])
    r_ix = dm.index(0, 0, QModelIndex())
    dm.append_child_payloads(r_ix, [sales])
    sales_ix = dm.index(0, 0, r_ix)
    MainWindow._ensure_destination_projection_path_sharepoint_graph_only(mw, r"Root3\Sales\HR\Employee Files", leaf_is_file=False)
    mw.planned_moves = [
        {
            "destination_path": r"Root3\Sales\OtherBranch\Nested",
            "source": {"is_folder": True, "name": "Nested"},
            "request_id": "wrong-parent",
        }
    ]
    MainWindow._materialize_planned_workspace_proposed_descendants_fixpoint(mw, sales_ix)
    emp_ix = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        if str((ix.data(Qt.UserRole) or {}).get("name") or "") == "HR":
            for r2 in range(dm.rowCount(ix)):
                ix2 = dm.index(r2, 0, ix)
                if str((ix2.data(Qt.UserRole) or {}).get("name") or "") == "Employee Files":
                    emp_ix = ix2
                    break
            break
    assert emp_ix is not None
    for nm in _child_folder_names(dm, emp_ix):
        assert "Nested" not in nm
