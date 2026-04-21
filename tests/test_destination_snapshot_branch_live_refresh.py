"""Destination snapshot shell → per-branch Graph verification (no full-tree authority)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from PySide6.QtCore import QModelIndex, Qt

from ozlink_console.main_window import MainWindow
from ozlink_console.sharepoint_destination_overlay_attach import WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _mw_bare():
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw._destination_library_context_unresolved_for_graph_display = lambda: False
    mw._resolve_tree_item_drive_id = lambda panel, pl: str(pl.get("drive_id") or "")
    mw._destination_semantic_path = lambda pl: str(pl.get("item_path") or "")
    return mw


def test_stamp_marks_graph_identity_folders_skips_planned():
    mw = _mw_bare()
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    planned = {
        "name": "[Planned] X",
        "id": "",
        "drive_id": "d1",
        "is_folder": True,
        "item_path": "Planned",
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_row_state": "planned_only",
    }
    graphish = {
        "name": "Root3",
        "id": "r1",
        "drive_id": "d1",
        "library_id": "d1",
        "is_folder": True,
        "item_path": r"Lib\Root3",
        "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
        "children_loaded": True,
    }
    dm.reset_root_payloads([graphish, planned])
    st = MainWindow._destination_stamp_destination_model_rows_for_snapshot_live_refresh(mw, context="unit_stamp")
    assert st["folders_marked"] >= 1
    r0 = dm.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
    assert r0.get("destination_snapshot_cached") is True
    assert r0.get("graph_children_verified") is False
    assert r0.get("needs_live_child_refresh") is True
    r1 = dm.index(1, 0, QModelIndex()).data(Qt.UserRole) or {}
    assert r1.get("workspace_row_state") == "planned_only"


def test_schedule_top_level_unverified_requests_graph_without_expand():
    mw = _mw_bare()
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    tw = MagicMock()
    tw.isExpanded = lambda _ix: False
    mw.destination_tree_widget = tw
    root = {
        "name": "Root3",
        "id": "r1",
        "drive_id": "d1",
        "is_folder": True,
        "item_path": r"Lib\Root3",
        "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
        "destination_snapshot_cached": True,
        "graph_children_verified": False,
        "needs_live_child_refresh": True,
        "children_loaded": True,
    }
    dm.reset_root_payloads([root])
    tw.collapseAll()
    mock_req = MagicMock(return_value=True)
    mw._request_graph_destination_children_load = mock_req
    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        MainWindow._destination_schedule_unverified_snapshot_branches_live_refresh(
            mw, drive_id="d1", reason="unit_test_top_level"
        )
    mock_req.assert_called()
    assert mock_req.call_args.kwargs.get("reason") == "snapshot_branch_live_refresh"


def test_skeleton_skip_delegates_when_top_level_unverified(monkeypatch):
    mw = _mw_bare()
    mw._safe_invoke = lambda _label, fn: fn()
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    mw.destination_tree_widget = MagicMock()
    mw.root_load_workers = {"destination": {"id": 7}}
    mw.pending_root_drive_ids = {"destination": "d1"}
    root = {
        "name": "Root3",
        "id": "r1",
        "drive_id": "d1",
        "is_folder": True,
        "item_path": r"Lib\Root3",
        "workspace_row_state": "",
        "children_loaded": True,
        "destination_snapshot_cached": True,
        "graph_children_verified": False,
        "needs_live_child_refresh": True,
    }
    dm.reset_root_payloads([root])
    sched = MagicMock()
    monkeypatch.setattr(mw, "_destination_schedule_unverified_snapshot_branches_live_refresh", sched)
    def _sync_single_shot(_delay, fn):
        if callable(fn):
            fn()

    monkeypatch.setattr("ozlink_console.main_window.QTimer.singleShot", _sync_single_shot)
    MainWindow._destination_schedule_skeleton_first_level_graph_child_loads(
        mw, "d1", 7, worker_tag="unit"
    )
    sched.assert_called_once()


def test_deferred_expand_drain_not_blocked_by_full_tree_only_gate():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_expand_user_deferred_scheduled = False
    mw._destination_expand_user_deferred_queue = __import__("collections").deque(["Root3\\Finance"])
    mw._destination_expand_user_deferred_seen = set()
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw._destination_future_tree_bind_busy = lambda: False
    mw._destination_incremental_merge_in_progress = False
    mw._destination_incremental_merge_session = None
    mw._destination_future_projection_async_state = None
    mw._destination_sharepoint_planning_destination_active = lambda: True
    mw._destination_full_tree_ready = lambda: False
    mw.pending_folder_loads = {"destination": set(), "source": set()}
    mw._destination_descendant_apply_paused_for_finalize_alloc = False
    mw._destination_descendant_apply_queue = None
    mw._destination_descendant_apply_state = None
    mw._current_selected_destination_drive_id = lambda: "d1"
    mw.pending_root_drive_ids = {"destination": "d1"}
    mw._destination_snapshot_spo_trust_valid = lambda _d: True
    mw._destination_snapshot_light_validation_worker = None
    mw._destination_library_context_unresolved_for_graph_display = lambda: False
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    fin = {
        "name": "Finance",
        "id": "f1",
        "drive_id": "d1",
        "is_folder": True,
        "item_path": r"Lib\Root3\Finance",
        "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
        "children_loaded": True,
    }
    dm.reset_root_payloads(
        [
            {
                "name": "Root3",
                "id": "r1",
                "drive_id": "d1",
                "is_folder": True,
                "item_path": r"Lib\Root3",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "children_loaded": True,
            }
        ]
    )
    r_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(r_ix, [fin])
    _fin_ix = dm.index(0, 0, r_ix)
    tw = MagicMock()
    tw.isExpanded = lambda _ix: False
    tw.expand = MagicMock()
    mw.destination_tree_widget = tw
    dm.find_indices_for_canonical_destination_path = MagicMock(  # type: ignore[method-assign]
        return_value=[_fin_ix],
    )
    mw._on_destination_planning_model_expanded = MagicMock()
    MainWindow._drain_destination_expand_user_deferred_queue(mw)
    mw._on_destination_planning_model_expanded.assert_called_once()
    assert len(mw._destination_expand_user_deferred_queue) == 0


def test_full_tree_worker_not_invoked_by_branch_refresh_schedule():
    mw = _mw_bare()
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    tw = MagicMock()
    tw.isExpanded = lambda _ix: False
    mw.destination_tree_widget = tw
    root = {
        "name": "Root",
        "id": "r1",
        "drive_id": "d1",
        "is_folder": True,
        "item_path": "X",
        "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
        "destination_snapshot_cached": True,
        "graph_children_verified": False,
        "needs_live_child_refresh": True,
        "children_loaded": False,
    }
    dm.reset_root_payloads([root])
    mw._request_graph_destination_children_load = MagicMock(return_value=True)
    ft = MagicMock()
    mw._ensure_sharepoint_destination_full_tree_worker_scheduled = ft
    with patch(
        "ozlink_console.main_window.destination_authority_contract.graph_owns_visible_real_destination_structure",
        return_value=True,
    ):
        MainWindow._destination_schedule_unverified_snapshot_branches_live_refresh(mw, drive_id="d1", reason="unit")
    ft.assert_not_called()
