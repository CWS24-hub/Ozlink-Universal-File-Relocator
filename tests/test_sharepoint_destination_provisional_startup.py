"""Option 3 Phase 1: provisional destination snapshot before Graph root bind."""

from __future__ import annotations

import os

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication, QTreeView

from ozlink_console.main_window import MainWindow
from ozlink_console.models import SessionState
from ozlink_console.sharepoint_destination_overlay_attach import (
    WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
    WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
    destination_payload_is_live_graph_row,
    destination_stamp_snapshot_tree_workspace_state,
)
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def test_provisional_snapshot_rows_are_not_live_graph_rows():
    snap = {
        "text": "Hub",
        "data": {
            "name": "Hub",
            "id": "live-hub",
            "is_folder": True,
            "item_path": "Hub",
            "tree_role": "destination",
            "drive_id": "d1",
        },
        "children": [],
    }
    destination_stamp_snapshot_tree_workspace_state([snap])
    pl = snap["data"]
    assert pl.get("workspace_row_state") == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
    assert not destination_payload_is_live_graph_row(pl)


def test_apply_provisional_sets_status_and_model_rows_without_graph():
    os.environ.pop("OZLINK_PROVISIONAL_DESTINATION_STARTUP", None)
    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw._destination_tree_model_view = True
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_tree_widget = tw
    mw.destination_tree_status = tw
    mw._pending_session_tree_snapshots = {
        "destination": [
            {
                "text": "X",
                "data": {
                    "name": "XLib",
                    "id": "root-1",
                    "is_folder": True,
                    "item_path": "XLib",
                    "tree_role": "destination",
                    "drive_id": "d1",
                },
                "children": [],
            }
        ],
    }
    mw._runtime_session_tree_snapshots = {"source": [], "destination": []}
    mw._draft_shell_state = SessionState(
        SelectedDestinationLibraryId="d1",
        DestinationTreeSnapshotIdentityDriveId="d1",
    )
    mw._set_tree_status_message = lambda *a, **k: None
    mw._schedule_snapshot_branch_refresh = lambda *a, **k: None
    ok = MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test")
    assert ok is True
    assert mw._destination_provisional_startup_applied is True
    assert dm.rowCount(QModelIndex()) == 1
    pl = dm.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
    assert pl.get("workspace_row_state") == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
    _ = app


def test_root_graph_bind_merges_matching_provisional_id_to_live():
    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw._destination_tree_model_view = True
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_tree_widget = tw
    mw.destination_tree_status = tw  # _get_tree_and_status requires non-None status label widget
    mw._pending_session_tree_snapshots = {"destination": []}
    mw._runtime_session_tree_snapshots = {"source": [], "destination": []}
    dm.reset_root_payloads(
        [
            {
                "name": "Same",
                "id": "same-id",
                "is_folder": True,
                "item_path": "Same",
                "tree_role": "destination",
                "drive_id": "d1",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "overlay_keep": "x",
            }
        ]
    )
    mw._destination_provisional_startup_applied = True
    mw.pending_root_drive_ids = {"destination": "d1"}
    mw._current_selected_destination_drive_id = lambda: "d1"
    mw._mark_destination_real_tree_snapshot_stale = lambda: None
    mw._destination_lifecycle_trace_TEMP = lambda **k: None
    mw._log_restore_phase = lambda *a, **k: None
    mw._set_tree_status_message = lambda *a, **k: None
    mw._destination_sharepoint_root_graph_bound_drive_id = ""
    mw._destination_full_library_reconcile_pending = False
    mw._destination_authority_pending_shell = False
    mw._destination_non_authoritative_shell_active = False
    mw._schedule_destination_authority_shell_watchdog = lambda *_a, **_k: None
    mw._ensure_sharepoint_destination_full_tree_worker_scheduled = lambda *_a, **_k: None
    MainWindow._apply_root_payload_to_destination_model_view(
        mw,
        "destination",
        [{"name": "Same", "id": "same-id", "is_folder": True, "drive_id": "d1"}],
    )
    pl = dm.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
    assert pl.get("workspace_row_state") == WORKSPACE_ROW_STATE_LIVE_CONFIRMED
    assert pl.get("overlay_keep") == "x"
    assert mw._destination_provisional_startup_applied is False
    _ = app


def test_provisional_startup_respects_disable_env(monkeypatch):
    monkeypatch.setenv("OZLINK_PROVISIONAL_DESTINATION_STARTUP", "0")
    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint"
    mw._destination_tree_model_view = True
    mw.destination_planning_model = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    mw.destination_tree_widget = QTreeView()
    mw.destination_tree_widget.setModel(mw.destination_planning_model)
    mw._pending_session_tree_snapshots = {"destination": [{"text": "A", "data": {"name": "A", "id": "1", "is_folder": True, "drive_id": "d"}, "children": []}]}
    mw._runtime_session_tree_snapshots = {"source": [], "destination": []}
    assert MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test") is False
    assert mw.destination_planning_model.rowCount(QModelIndex()) == 0
    monkeypatch.delenv("OZLINK_PROVISIONAL_DESTINATION_STARTUP", raising=False)
    _ = app
