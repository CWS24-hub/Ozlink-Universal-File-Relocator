"""Stamped projection freshness, unresolved backlog, and structural repair gating."""

from __future__ import annotations

import os
from collections import deque
from unittest.mock import MagicMock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _app():
    return QApplication.instance() or QApplication([])


def _bind(mw, names: tuple[str, ...]) -> None:
    for n in names:
        setattr(mw, n, getattr(MainWindow, n).__get__(mw, MainWindow))


@pytest.fixture
def mw_graph(monkeypatch):
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._planning_cache_generation = 42
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
    mw.planned_moves = []
    _bind(
        mw,
        (
            "_allocation_parent_path",
            "_allocation_projection_path",
            "_allocation_move_key",
            "_tree_item_path",
            "_canonical_planned_memory_path_for_graph_match",
            "_normalize_to_graph_canonical_path",
            "_allocation_destination_path_includes_target_leaf",
            "_allocation_destination_path_canonical_segments",
            "_canonical_destination_projection_path",
            "normalize_memory_path",
            "_move_target_name",
            "_destination_overlay_folder_row_needs_source_descendant_reproject",
            "_destination_overlay_unresolved_backlog_snapshot_for_allocation_move",
            "_apply_tree_item_visual_state",
            "_destination_planning_dfs_next_preorder_index",
            "_destination_payload_index_key",
            "_destination_row_allows_structural_folder_child_load",
        ),
    )
    dt = QTreeView()
    dm = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    dt.setModel(dm)
    mw.destination_planning_model = dm
    mw.destination_tree_widget = dt
    return mw, dm


def test_unresolved_backlog_stamped_fresh_structural_parity_does_not_require_reproject(monkeypatch, mw_graph):
    """Unresolved replay backlog alone must not force structural reproject when direct children match source."""
    mw, dm = mw_graph
    move = {
        "destination_path": "Root3\\AllocFolder",
        "target_name": "AllocFolder",
        "source_name": "Src",
        "source_path": "S\\Src",
        "source": {"is_folder": True, "name": "Src", "item_path": "S\\Src", "children_loaded": True},
    }
    proj = MainWindow._allocation_projection_path(mw, move)
    parent = MainWindow._allocation_parent_path(mw, move)
    assert proj
    assert parent
    assert proj.casefold() != parent.casefold(), "fixture expects backlog scope distinct from parent-only key"

    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    pix = dm.index(0, 0, QModelIndex())
    sig = "deadbeefcafe"
    pl = {
        "name": "AllocFolder",
        "is_folder": True,
        "item_path": proj,
        "destination_path": proj,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_children_signature": sig,
        "tree_role": "destination",
    }
    dm.append_child_payloads(pix, [pl])
    ix = dm.index(0, 0, pix)

    mw.unresolved_allocations_by_parent_path = {proj: {MainWindow._allocation_move_key(mw, move): move}}

    monkeypatch.setattr(
        mw,
        "_allocation_projection_children_signature_from_index",
        lambda _ix: sig,
    )
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: object())
    monkeypatch.setattr(mw, "_source_subtree_fully_loaded_in_tree", lambda _it: True)
    monkeypatch.setattr(mw, "_expected_direct_child_identity_tuples_under_move_source", lambda _m: [("a", True)])
    monkeypatch.setattr(mw, "_direct_child_identity_tuple_list_for_model_index", lambda *_a: [("a", True)])
    monkeypatch.setattr(
        mw,
        "_direct_child_identity_tuple_list_for_model_index_plan_parity",
        lambda *_a, **_k: [("a", True)],
    )

    needs = MainWindow._destination_overlay_folder_row_needs_source_descendant_reproject(mw, ix, pl, move)
    assert needs is False


def test_unresolved_backlog_stamped_fresh_structural_mismatch_requires_reproject(monkeypatch, mw_graph):
    """Stamped-fresh + backlog + direct child multiset mismatch still requires structural repair."""
    mw, dm = mw_graph
    move = {
        "destination_path": "Root3\\AllocFolder2",
        "target_name": "AllocFolder2",
        "source_name": "Src",
        "source_path": "S\\Src",
        "source": {"is_folder": True, "name": "Src", "item_path": "S\\Src", "children_loaded": True},
    }
    proj = MainWindow._allocation_projection_path(mw, move)
    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    pix = dm.index(0, 0, QModelIndex())
    sig = "sig-mismatch"
    pl = {
        "name": "AllocFolder2",
        "is_folder": True,
        "item_path": proj,
        "destination_path": proj,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_children_signature": sig,
        "tree_role": "destination",
    }
    dm.append_child_payloads(pix, [pl])
    ix = dm.index(0, 0, pix)
    mw.unresolved_allocations_by_parent_path = {proj: {MainWindow._allocation_move_key(mw, move): move}}
    monkeypatch.setattr(mw, "_allocation_projection_children_signature_from_index", lambda _ix: sig)
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: object())
    monkeypatch.setattr(mw, "_source_subtree_fully_loaded_in_tree", lambda _it: True)
    monkeypatch.setattr(mw, "_expected_direct_child_identity_tuples_under_move_source", lambda _m: [("a", True)])
    monkeypatch.setattr(mw, "_direct_child_identity_tuple_list_for_model_index", lambda *_a: [("b", True)])
    monkeypatch.setattr(
        mw,
        "_direct_child_identity_tuple_list_for_model_index_plan_parity",
        lambda *_a, **_k: [("b", True)],
    )

    needs = MainWindow._destination_overlay_folder_row_needs_source_descendant_reproject(mw, ix, pl, move)
    assert needs is True


def test_genuinely_stamped_fresh_still_skips_when_no_backlog(monkeypatch, mw_graph):
    mw, dm = mw_graph
    move = {
        "destination_path": "Root3\\Leaf",
        "target_name": "Leaf",
        "source_name": "Src",
        "source_path": "S\\Src",
        "source": {"is_folder": True, "name": "Src", "item_path": "S\\Src", "children_loaded": True},
    }
    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    pix = dm.index(0, 0, QModelIndex())
    sig = "stable-sig-1"
    proj = MainWindow._allocation_projection_path(mw, move)
    pl = {
        "name": "Leaf",
        "is_folder": True,
        "item_path": proj,
        "destination_path": proj,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_children_signature": sig,
        "tree_role": "destination",
    }
    dm.append_child_payloads(pix, [pl])
    ix = dm.index(0, 0, pix)
    mw.unresolved_allocations_by_parent_path = {}
    mw.unresolved_proposed_by_parent_path = {}
    monkeypatch.setattr(
        mw,
        "_allocation_projection_children_signature_from_index",
        lambda _ix: sig,
    )
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: object())
    monkeypatch.setattr(mw, "_source_subtree_fully_loaded_in_tree", lambda _it: True)
    monkeypatch.setattr(mw, "_expected_direct_child_identity_tuples_under_move_source", lambda _m: [("a", True)])
    monkeypatch.setattr(mw, "_direct_child_identity_tuple_list_for_model_index", lambda *_a: [("a", True)])

    needs = MainWindow._destination_overlay_folder_row_needs_source_descendant_reproject(mw, ix, pl, move)
    assert needs is False


def test_backlog_snapshot_sees_rebuilt_queue(monkeypatch, mw_graph):
    mw, _dm = mw_graph
    move = {
        "destination_path": "Root3\\Box",
        "target_name": "Box",
        "source": {"is_folder": True, "name": "B"},
    }
    snap0 = MainWindow._destination_overlay_unresolved_backlog_snapshot_for_allocation_move(mw, move, {})
    assert snap0["has_parent_scope_backlog"] is False
    proj = MainWindow._allocation_projection_path(mw, move)
    mw.unresolved_allocations_by_parent_path = {proj: {"k": move}}
    snap1 = MainWindow._destination_overlay_unresolved_backlog_snapshot_for_allocation_move(mw, move, {})
    assert snap1["has_parent_scope_backlog"] is True


def test_overlay_invariant_pass_makes_repair_progress_with_backlog(monkeypatch, mw_graph):
    """Invariant pass repairs when stamped-fresh + backlog + structural mismatch (not backlog alone)."""
    mw, dm = mw_graph
    mw.__dict__["_destination_quiet_startup_overlay_structural_suppress"] = False
    mw.__dict__["_application_shutting_down"] = False
    mw.__dict__["_disable_overlay_invariant_timer_for_test"] = True

    move = {
        "destination_path": "Root3\\ProjF",
        "target_name": "ProjF",
        "source_name": "SrcF",
        "source_path": "lib\\SrcF",
        "source": {
            "is_folder": True,
            "name": "SrcF",
            "item_path": "lib\\SrcF",
            "children_loaded": True,
            "tree_role": "source",
        },
    }
    mw.planned_moves = [move]
    proj = MainWindow._allocation_projection_path(mw, move)

    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    r = dm.index(0, 0, QModelIndex())
    sig = "sig-for-invariant"
    pl = {
        "name": "ProjF",
        "is_folder": True,
        "item_path": proj,
        "destination_path": proj,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_children_signature": sig,
        "tree_role": "destination",
    }
    dm.append_child_payloads(r, [pl])

    mw.unresolved_allocations_by_parent_path = {proj: {MainWindow._allocation_move_key(mw, move): move}}

    monkeypatch.setattr(
        mw,
        "_allocation_projection_children_signature_from_index",
        lambda _ix: sig,
    )
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: object())
    monkeypatch.setattr(mw, "_source_subtree_fully_loaded_in_tree", lambda _it: True)
    monkeypatch.setattr(mw, "_expected_direct_child_identity_tuples_under_move_source", lambda _m: [("x", True)])
    monkeypatch.setattr(mw, "_direct_child_identity_tuple_list_for_model_index", lambda *_a: [("y", True)])
    monkeypatch.setattr(
        mw,
        "_direct_child_identity_tuple_list_for_model_index_plan_parity",
        lambda *_a, **_k: [("y", True)],
    )
    monkeypatch.setattr(mw, "_overlay_projection_invariant_teardown_needed_at_index", lambda *_a, **_k: False)
    monkeypatch.setattr(mw, "_find_exact_planned_move_for_destination_projection_path", lambda _pl: move)
    monkeypatch.setattr(mw, "_destination_descendant_apply_pending_for_parent_index", lambda *_a: False)
    can = MainWindow._canonical_planned_memory_path_for_graph_match(mw, proj)

    def _finder(p):
        if str(p).strip().casefold() == str(can).strip().casefold():
            return dm.index(0, 0, r)
        return QModelIndex()

    monkeypatch.setattr(mw, "_find_visible_destination_item_by_path", _finder)
    applied: list[str] = []

    def _capture(_ix, _mv):
        applied.append("repair")

    monkeypatch.setattr(mw, "_apply_overlay_projection_invariant_repair_to_index", _capture)

    n = MainWindow._run_overlay_projection_invariant_pass(mw, "test_backlog_progress")
    assert n >= 1
    assert applied == ["repair"]


def test_overlay_invariant_pass_counts_stamp_skips_and_overrides(monkeypatch, mw_graph):
    mw, dm = mw_graph
    mw.__dict__["_destination_quiet_startup_overlay_structural_suppress"] = False
    mw.__dict__["_application_shutting_down"] = False
    mw.__dict__["_disable_overlay_invariant_timer_for_test"] = True

    move = {
        "destination_path": "Root3\\OnlyFresh",
        "target_name": "OnlyFresh",
        "source_name": "Z",
        "source_path": "lib\\Z",
        "source": {"is_folder": True, "name": "Z", "item_path": "lib\\Z", "children_loaded": True},
    }
    mw.planned_moves = [move]
    proj = MainWindow._allocation_projection_path(mw, move)
    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    r = dm.index(0, 0, QModelIndex())
    sig = "only-fresh-sig"
    pl = {
        "name": "OnlyFresh",
        "is_folder": True,
        "item_path": proj,
        "destination_path": proj,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_children_signature": sig,
        "tree_role": "destination",
    }
    dm.append_child_payloads(r, [pl])
    mw.unresolved_allocations_by_parent_path = {}
    monkeypatch.setattr(mw, "_allocation_projection_children_signature_from_index", lambda _ix: sig)
    monkeypatch.setattr(mw, "_overlay_projection_invariant_teardown_needed_at_index", lambda *_a, **_k: False)
    monkeypatch.setattr(mw, "_find_exact_planned_move_for_destination_projection_path", lambda _pl: move)
    monkeypatch.setattr(mw, "_destination_descendant_apply_pending_for_parent_index", lambda *_a: False)
    monkeypatch.setattr(mw, "_find_visible_destination_item_by_path", lambda _p: QModelIndex())
    monkeypatch.setattr(mw, "_apply_overlay_projection_invariant_repair_to_index", lambda *_a, **_k: None)
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: None)

    MainWindow._run_overlay_projection_invariant_pass(mw, "test_stamp_stats")
    assert int(getattr(mw, "_overlay_inv_pass_stamp_skip", 0) or 0) >= 1
    assert int(getattr(mw, "_overlay_inv_pass_stamp_force_backlog", 0) or 0) == 0


def test_graph_descendant_finalize_clears_unresolved_allocation_when_verify_passes(monkeypatch, mw_graph):
    mw, dm = mw_graph
    monkeypatch.setattr(mw, "_mark_allocation_descendants_applied_on_allocation_folder_model_index", lambda *a, **k: None)
    monkeypatch.setattr(mw, "_stamp_allocation_projection_cache_metadata_index", lambda *a, **k: {})
    monkeypatch.setattr(mw, "_promote_destination_workspace_snapshot_after_structure_change", lambda: None)
    monkeypatch.setattr(mw, "_log_unresolved_queue_removal_event", lambda **k: None)
    monkeypatch.setattr(mw, "_sync_restore_destination_overlay_pending_from_unresolved_queues", lambda: None)
    monkeypatch.setattr(mw, "_unresolved_overlay_pass_audit_touch_removal", lambda *_a, **_k: None)
    monkeypatch.setattr(mw, "_unresolved_overlay_pass_audit_touch_illegal", lambda: None)
    monkeypatch.setattr(
        mw,
        "_canonical_destination_path_with_visible_library_anchor",
        lambda p: str(p or "").strip(),
    )
    monkeypatch.setattr(mw, "_unresolved_overlay_verify_allocation_outcome", lambda *a, **k: True)

    move = {
        "destination_path": "Root3\\QAlloc",
        "target_name": "QAlloc",
        "source": {"is_folder": True, "name": "S"},
    }
    pp = MainWindow._allocation_parent_path(mw, move)
    mk = MainWindow._allocation_move_key(mw, move)
    mw.unresolved_allocations_by_parent_path = {pp: {mk: move}}

    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    r = dm.index(0, 0, QModelIndex())
    pl = {
        "name": "QAlloc",
        "is_folder": True,
        "item_path": "Root3\\QAlloc",
        "planned_allocation": True,
        "tree_role": "destination",
    }
    dm.append_child_payloads(r, [pl])
    ix = dm.index(0, 0, r)
    proj = MainWindow._allocation_projection_path(mw, move)
    st = {
        "graph_walk": True,
        "move": move,
        "parent_ix": ix,
        "model": dm,
        "allocation_destination_path": proj,
        "overlay_count": 0,
        "on_complete": None,
    }
    MainWindow._destination_descendant_apply_finalize_job(mw, st)
    assert not mw.unresolved_allocations_by_parent_path


def test_enqueue_descendant_apply_deduplicates_across_column_indices(monkeypatch, mw_graph):
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: False,
    )
    mw, dm = mw_graph
    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    r = dm.index(0, 0, QModelIndex())
    pl = {
        "name": "Child",
        "is_folder": True,
        "item_path": "Root3\\Child",
        "tree_role": "destination",
    }
    dm.append_child_payloads(r, [pl])
    mw._destination_descendant_apply_queue = deque()
    mw._destination_descendant_apply_state = None
    monkeypatch.setattr(mw, "_schedule_destination_descendant_apply_tick", lambda: None)
    move = {"destination_path": "Root3", "target_name": "Child"}
    ix0 = dm.index(0, 0, r)
    ix1 = dm.index(0, 1, r)
    assert ix0.isValid() and ix1.isValid()
    assert MainWindow._enqueue_destination_descendant_apply_to_model(mw, ix0, move, enqueue_reason="test")
    assert MainWindow._enqueue_destination_descendant_apply_to_model(mw, ix1, move, enqueue_reason="test")
    assert len(mw._destination_descendant_apply_queue) == 1


def test_plan_parity_filters_excluded_file_like_expected(monkeypatch, mw_graph):
    mw, _dm = mw_graph
    sp_ex = "sp_share\\deferred_leaf.txt"
    canon = MainWindow._canonical_source_projection_path(mw, sp_ex) or sp_ex
    mw._plan_leaf_exclusions = {canon}
    move = {"destination_path": "Root3\\Folder"}
    child_ix = MagicMock()
    child_ix.data = MagicMock(
        side_effect=lambda role, *a, **k: {
            "name": "deferred_leaf.txt",
            "is_folder": False,
            "source_path": sp_ex,
        }
        if role == Qt.UserRole
        else None
    )
    parent_ix = MagicMock()
    parent_ix.isValid = MagicMock(return_value=True)
    mm = MagicMock()
    mm.rowCount = MagicMock(return_value=1)
    mm.index = MagicMock(return_value=child_ix)
    raw = MainWindow._direct_child_identity_tuple_list_for_model_index(mw, mm, parent_ix)
    pr = MainWindow._direct_child_identity_tuple_list_for_model_index_plan_parity(mw, mm, parent_ix, move)
    assert raw == [("deferred_leaf.txt", False)]
    assert pr == []


def test_stamped_fresh_sibling_only_backlog_skips_tuple_compare(monkeypatch, mw_graph):
    mw, dm = mw_graph
    move = {
        "destination_path": "Root3\\AllocFolder",
        "target_name": "AllocFolder",
        "source_name": "Src",
        "source_path": "S\\Src",
        "source": {"is_folder": True, "name": "Src", "item_path": "S\\Src", "children_loaded": True},
    }
    proj = MainWindow._allocation_projection_path(mw, move)
    parent = MainWindow._allocation_parent_path(mw, move)
    assert parent
    other = {"allocation_id": "other-move", "destination_path": "Root3\\Other"}
    mw.unresolved_allocations_by_parent_path = {
        parent: {MainWindow._allocation_move_key(mw, other): other},
    }
    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    pix = dm.index(0, 0, QModelIndex())
    sig = "deadbeefcafe"
    pl = {
        "name": "AllocFolder",
        "is_folder": True,
        "item_path": proj,
        "destination_path": proj,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_children_signature": sig,
        "tree_role": "destination",
    }
    dm.append_child_payloads(pix, [pl])
    ix = dm.index(0, 0, pix)

    def _boom(_m):
        raise AssertionError("tuple compare should be skipped for sibling-only backlog")

    monkeypatch.setattr(mw, "_allocation_projection_children_signature_from_index", lambda _ix: sig)
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: object())
    monkeypatch.setattr(mw, "_source_subtree_fully_loaded_in_tree", lambda _it: True)
    monkeypatch.setattr(mw, "_expected_direct_child_identity_tuples_under_move_source", _boom)
    needs = MainWindow._destination_overlay_folder_row_needs_source_descendant_reproject(mw, ix, pl, move)
    assert needs is False
    snap = MainWindow._destination_overlay_unresolved_backlog_snapshot_for_allocation_move(mw, move, pl)
    assert snap["has_parent_scope_backlog"] is True
    assert snap["move_key_in_scope_alloc_bucket"] is False


def test_backlog_snapshot_move_key_in_scope_when_bucket_contains_move(monkeypatch, mw_graph):
    mw, _dm = mw_graph
    move = {
        "destination_path": "Root3\\Box",
        "target_name": "Box",
        "allocation_id": "aid1",
        "source": {"is_folder": True, "name": "B"},
    }
    mk = MainWindow._allocation_move_key(mw, move)
    proj = MainWindow._allocation_projection_path(mw, move)
    mw.unresolved_allocations_by_parent_path = {proj: {mk: move}}
    snap = MainWindow._destination_overlay_unresolved_backlog_snapshot_for_allocation_move(mw, move, {})
    assert snap["move_key_in_scope_alloc_bucket"] is True
    assert snap["move_backlog_relevant_to_projection_anchor"] is True


def test_backlog_snapshot_move_key_parent_only_not_projection_relevant(monkeypatch, mw_graph):
    """Move key queued only under allocation parent path must not tighten parity when projection path is distinct."""
    mw, _dm = mw_graph
    move = {
        "destination_path": "Root3\\AllocFolder",
        "target_name": "AllocFolder",
        "source_name": "Src",
        "source_path": "S\\Src",
        "source": {"is_folder": True, "name": "Src", "item_path": "S\\Src", "children_loaded": True},
    }
    mk = MainWindow._allocation_move_key(mw, move)
    parent = MainWindow._allocation_parent_path(mw, move)
    proj = MainWindow._allocation_projection_path(mw, move)
    assert parent and proj and parent.casefold() != proj.casefold()
    mw.unresolved_allocations_by_parent_path = {parent: {mk: move}}
    pl = {
        "name": "AllocFolder",
        "is_folder": True,
        "item_path": proj,
        "destination_path": proj,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_children_signature": "sig",
        "tree_role": "destination",
    }
    snap = MainWindow._destination_overlay_unresolved_backlog_snapshot_for_allocation_move(mw, move, pl)
    assert snap["move_key_in_scope_alloc_bucket"] is True
    assert snap["move_key_in_alloc_bucket_at_parent_only"] is True
    assert snap["move_backlog_relevant_to_projection_anchor"] is False


def test_parity_multiset_prefers_source_path_canonical_for_plan_exclusion(monkeypatch, mw_graph):
    mw, _dm = mw_graph
    canon_sp = "canon/from/source_path"
    canon_tree = "canon/from/item_path"
    mw._plan_leaf_exclusions = {canon_sp}

    def _canon(raw):
        r = str(raw or "").strip()
        if r == "raw_sp":
            return canon_sp
        if r == "raw_tree":
            return canon_tree
        return MainWindow._canonical_source_projection_path(mw, r)

    monkeypatch.setattr(mw, "_canonical_source_projection_path", _canon)
    cd = {
        "name": "leaf.txt",
        "is_folder": False,
        "source_path": "raw_sp",
        "item_path": "raw_tree",
        "tree_role": "destination",
    }
    assert MainWindow._direct_child_parity_tuple_multiset_from_payloads(mw, [cd]) == []


def test_parity_multiset_file_without_source_path_uses_tree_path_for_exclusion(monkeypatch, mw_graph):
    mw, _dm = mw_graph
    canon_tree = "canon/from/tree_only"
    mw._plan_leaf_exclusions = {canon_tree}

    def _canon(raw):
        r = str(raw or "").strip()
        if r == "raw_tree":
            return canon_tree
        return MainWindow._canonical_source_projection_path(mw, r)

    monkeypatch.setattr(mw, "_canonical_source_projection_path", _canon)
    cd = {
        "name": "onlytree.txt",
        "is_folder": False,
        "item_path": "raw_tree",
        "tree_role": "destination",
    }
    assert MainWindow._direct_child_parity_tuple_multiset_from_payloads(mw, [cd]) == []


def test_parity_side_records_empty_key_file(monkeypatch, mw_graph):
    mw, _dm = mw_graph
    rec = MainWindow._direct_child_parity_side_records_from_payloads(
        mw,
        [{"name": "orphan.bin", "is_folder": False, "tree_role": "destination"}],
    )
    assert rec["empty_key_files"]
    assert ("orphan.bin", False) in rec["included"]


def test_contractor_resumes_parent_bucket_noise_skips_strict_compare(monkeypatch, mw_graph):
    """Injected row: signature fresh, move key only under shared parent bucket — no false structural repair."""
    mw, dm = mw_graph
    move = {
        "destination_path": "Root3\\Contractor Resumes",
        "target_name": "Contractor Resumes",
        "source_name": "Src",
        "source_path": "S\\Src",
        "source": {"is_folder": True, "name": "Src", "item_path": "S\\Src", "children_loaded": True},
    }
    proj = MainWindow._allocation_projection_path(mw, move)
    parent = MainWindow._allocation_parent_path(mw, move)
    mk = MainWindow._allocation_move_key(mw, move)
    other = {"destination_path": "Root3\\Other", "allocation_id": "other"}
    omk = MainWindow._allocation_move_key(mw, other)
    mw.unresolved_allocations_by_parent_path = {parent: {omk: other, mk: move}}
    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    pix = dm.index(0, 0, QModelIndex())
    sig = "contractor-sig"
    pl = {
        "name": "Contractor Resumes",
        "is_folder": True,
        "item_path": proj,
        "destination_path": proj,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_children_signature": sig,
        "tree_role": "destination",
    }
    dm.append_child_payloads(pix, [pl])
    ix = dm.index(0, 0, pix)

    def _boom(_m):
        raise AssertionError("strict tuple compare must be skipped for parent-only backlog noise")

    monkeypatch.setattr(mw, "_allocation_projection_children_signature_from_index", lambda _ix: sig)
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: object())
    monkeypatch.setattr(mw, "_source_subtree_fully_loaded_in_tree", lambda _it: True)
    monkeypatch.setattr(mw, "_expected_direct_child_identity_tuples_under_move_source", _boom)
    needs = MainWindow._destination_overlay_folder_row_needs_source_descendant_reproject(mw, ix, pl, move)
    assert needs is False


def test_wrong_is_folder_tuple_still_mismatches_after_parity_alignment(monkeypatch, mw_graph):
    mw, dm = mw_graph
    move = {
        "destination_path": "Root3\\AllocFolder2",
        "target_name": "AllocFolder2",
        "source_name": "Src",
        "source_path": "S\\Src",
        "source": {"is_folder": True, "name": "Src", "item_path": "S\\Src", "children_loaded": True},
    }
    proj = MainWindow._allocation_projection_path(mw, move)
    root = {
        "name": "Root3",
        "is_folder": True,
        "item_path": "Root3",
        "destination_path": "Root3",
        "children_loaded": True,
        "tree_role": "destination",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, root)
    dm.reset_root_payloads([root])
    pix = dm.index(0, 0, QModelIndex())
    sig = "sig-mismatch-type"
    pl = {
        "name": "AllocFolder2",
        "is_folder": True,
        "item_path": proj,
        "destination_path": proj,
        "children_loaded": True,
        "allocation_descendants_applied": True,
        "allocation_projection_children_signature": sig,
        "tree_role": "destination",
    }
    dm.append_child_payloads(pix, [pl])
    ix = dm.index(0, 0, pix)
    mw.unresolved_allocations_by_parent_path = {proj: {MainWindow._allocation_move_key(mw, move): move}}
    monkeypatch.setattr(mw, "_allocation_projection_children_signature_from_index", lambda _ix: sig)
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: object())
    monkeypatch.setattr(mw, "_source_subtree_fully_loaded_in_tree", lambda _it: True)
    monkeypatch.setattr(mw, "_expected_direct_child_identity_tuples_under_move_source", lambda _m: [("x", True)])
    monkeypatch.setattr(
        mw,
        "_direct_child_identity_tuple_list_for_model_index_plan_parity",
        lambda *_a, **_k: [("x", False)],
    )
    monkeypatch.setattr(mw, "_direct_child_identity_tuple_list_for_model_index", lambda *_a: [("x", False)])

    needs = MainWindow._destination_overlay_folder_row_needs_source_descendant_reproject(mw, ix, pl, move)
    assert needs is True
