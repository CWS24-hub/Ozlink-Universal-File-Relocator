"""Option 3 Phase 1: provisional destination snapshot before Graph root bind."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

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


def _mock_destination_library_selector_resolved(drive_id: str = "d1"):
    """Minimal combo so ``_destination_library_context_unresolved_for_graph_display`` is False in unit tests."""
    lib_sel = MagicMock()
    lib_sel.currentIndex = MagicMock(return_value=0)
    lib_sel.currentData = MagicMock(return_value={"id": drive_id, "drive_id": drive_id})
    return lib_sel


def _provisional_startup_test_mw_with_snapshot():
    """Shared harness: SharePoint destination model-view + one-root cached snapshot + library id."""
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw._destination_tree_model_view = True
    mw.planning_inputs = {"Destination Library": _mock_destination_library_selector_resolved("d1")}
    dm = DestinationPlanningTreeModel(destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow))
    mw.destination_planning_model = dm
    tw = QTreeView()
    tw.setModel(dm)
    mw.destination_tree_widget = tw
    mw.destination_tree_status = tw
    snap = [
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
    ]
    mw._pending_session_tree_snapshots = {"destination": snap}
    mw._runtime_session_tree_snapshots = {"source": [], "destination": []}
    mw._draft_shell_state = SessionState(
        SelectedDestinationLibraryId="d1",
        DestinationTreeSnapshotIdentityDriveId="d1",
    )
    mw._set_tree_status_message = lambda *a, **k: None
    mw._safe_invoke = lambda _name, fn: fn()
    mw._schedule_provisional_startup_hydration_timer = lambda: None
    return mw, dm


def test_cold_start_cached_tree_is_mounted_without_immediate_heavy_hydration():
    """Regression: cached destination tree mounts; expand/hydrate/branch-refresh stay off the provisional stack."""
    os.environ.pop("OZLINK_PROVISIONAL_DESTINATION_STARTUP", None)
    app = QApplication.instance() or QApplication([])
    mw, dm = _provisional_startup_test_mw_with_snapshot()
    mw._restore_expanded_destination_paths = MagicMock()
    mw._hydrate_destination_allocations_for_expanded_paths_model = MagicMock()
    mw._schedule_snapshot_branch_refresh = MagicMock()

    ok = MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test")
    assert ok is True
    assert mw._destination_provisional_startup_applied is True
    assert getattr(mw, "_destination_startup_ui_phase", "") == "cached_only"
    assert dm.rowCount(QModelIndex()) == 1

    mw._restore_expanded_destination_paths.assert_not_called()
    mw._hydrate_destination_allocations_for_expanded_paths_model.assert_not_called()
    mw._schedule_snapshot_branch_refresh.assert_not_called()
    _ = app


def test_provisional_startup_mount_does_not_immediately_trigger_heavy_hydration():
    """Invariant: cached tree paints first; expand/hydrate/branch-refresh are not invoked on that call stack."""
    os.environ.pop("OZLINK_PROVISIONAL_DESTINATION_STARTUP", None)
    app = QApplication.instance() or QApplication([])
    mw, dm = _provisional_startup_test_mw_with_snapshot()
    mw._restore_expanded_destination_paths = MagicMock()
    mw._hydrate_destination_allocations_for_expanded_paths_model = MagicMock()
    mw._schedule_snapshot_branch_refresh = MagicMock()

    ok = MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test")
    assert ok is True
    assert mw._destination_provisional_startup_applied is True
    assert dm.rowCount(QModelIndex()) == 1
    pl = dm.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
    assert pl.get("workspace_row_state") == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL

    mw._restore_expanded_destination_paths.assert_not_called()
    mw._hydrate_destination_allocations_for_expanded_paths_model.assert_not_called()
    mw._schedule_snapshot_branch_refresh.assert_not_called()
    _ = app


def test_startup_hydration_begins_after_cached_only_phase(monkeypatch):
    """Invariant: hydration is deferred from phase-1 paint, then runs when explicitly advanced — not stuck in cached_only."""
    os.environ.pop("OZLINK_PROVISIONAL_DESTINATION_STARTUP", None)
    monkeypatch.setenv("OZLINK_STARTUP_BACKGROUND_HYDRATION_DELAY_MS", "0")
    app = QApplication.instance() or QApplication([])
    mw, _dm = _provisional_startup_test_mw_with_snapshot()
    restore_m = MagicMock()
    hydrate_m = MagicMock()
    branch_m = MagicMock()
    prune_m = MagicMock()
    mw._restore_expanded_destination_paths = restore_m
    mw._hydrate_destination_allocations_for_expanded_paths_model = hydrate_m
    mw._schedule_snapshot_branch_refresh = branch_m
    mw._destination_prune_pending_snapshot_branch_refresh_after_provisional_mount = prune_m
    mw._snapshot_refresh_targets_from_snapshot = MagicMock(return_value={"p1"})

    assert MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test") is True
    assert getattr(mw, "_destination_startup_ui_phase", "") == "cached_only"
    restore_m.assert_not_called()
    hydrate_m.assert_not_called()
    branch_m.assert_not_called()
    prune_m.assert_not_called()

    MainWindow._destination_maybe_begin_provisional_startup_hydration(mw, reason="explicit_unit")

    app.processEvents()
    assert getattr(mw, "_destination_startup_ui_phase", "") == "background_hydration"
    restore_m.assert_called_once()
    hydrate_m.assert_called_once()
    prune_m.assert_called_once()
    branch_m.assert_called_once()
    _ = app


def test_apply_provisional_sets_status_and_model_rows_without_graph():
    os.environ.pop("OZLINK_PROVISIONAL_DESTINATION_STARTUP", None)
    app = QApplication.instance() or QApplication([])
    mw, dm = _provisional_startup_test_mw_with_snapshot()
    mw._schedule_snapshot_branch_refresh = MagicMock()
    ok = MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test")
    assert ok is True
    assert getattr(mw, "_destination_startup_ui_phase", "") == "cached_only"
    mw._schedule_snapshot_branch_refresh.assert_not_called()
    assert mw._destination_provisional_startup_applied is True
    assert dm.rowCount(QModelIndex()) == 1
    pl = dm.index(0, 0, QModelIndex()).data(Qt.UserRole) or {}
    assert pl.get("workspace_row_state") == WORKSPACE_ROW_STATE_CACHED_PROVISIONAL
    _ = app


def test_provisional_startup_phase2_runs_expand_hydrate_and_branch_refresh():
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
    snap = [
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
    ]
    mw._pending_session_tree_snapshots = {"destination": snap}
    mw._runtime_session_tree_snapshots = {"source": [], "destination": snap}
    mw._destination_startup_ui_phase = "cached_only"
    mw._restore_expanded_destination_paths = MagicMock()
    mw._hydrate_destination_allocations_for_expanded_paths_model = MagicMock()
    mw._destination_prune_pending_snapshot_branch_refresh_after_provisional_mount = MagicMock()
    mw._schedule_snapshot_branch_refresh = MagicMock()
    mw._snapshot_refresh_targets_from_snapshot = MagicMock(return_value={"p1"})
    MainWindow._destination_run_provisional_startup_hydration_body(mw)
    mw._restore_expanded_destination_paths.assert_called_once()
    mw._hydrate_destination_allocations_for_expanded_paths_model.assert_called_once()
    mw._destination_prune_pending_snapshot_branch_refresh_after_provisional_mount.assert_called_once()
    mw._schedule_snapshot_branch_refresh.assert_called_once()
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


def test_startup_heavy_destination_work_is_gated_while_cached_only():
    """Regression: overlay materialize + indicator scheduling are deferred while cached_only / hydrating."""
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw._destination_tree_model_view = True
    mw.destination_planning_model = MagicMock()
    mw.destination_tree_widget = QTreeView()
    mw._destination_startup_ui_phase = "cached_only"
    mw._destination_startup_deferred_overlay_reasons = []
    mw._destination_startup_indicator_refresh_pending_after_cached = False
    mw._destination_shutdown_pre_save_overlay_flush = False

    with patch.object(MainWindow, "_destination_materialize_requires_authoritative_hard_flush", return_value=False):
        with patch.object(MainWindow, "_on_destination_state_mutation", MagicMock()) as mut:
            with patch.object(MainWindow, "_apply_destination_planning_overlays_body", MagicMock()) as body:
                r = MainWindow._apply_destination_planning_overlays(mw, "unit_test_overlay_gate")
                assert r == 0
                body.assert_not_called()
                mut.assert_not_called()
    assert mw._destination_startup_deferred_overlay_reasons
    assert "unit_test_overlay_gate" in mw._destination_startup_deferred_overlay_reasons[0]

    mw._destination_indicator_refresh_timer = MagicMock()
    MainWindow._schedule_refresh_destination_tree_indicators(mw, delay_ms=50)
    mw._destination_indicator_refresh_timer.start.assert_not_called()
    assert mw._destination_startup_indicator_refresh_pending_after_cached is True


def test_startup_hydration_can_begin_later_without_losing_state(monkeypatch):
    """Explicit hydration after cached_only: rows preserved, phase reaches background_hydration, deferred work flushes.

    Snapshot bind emits ``startup_memory_visible_tree_ready``; deferred overlay flush can be held behind
    ``_startup_post_visible_heavy_work_blocked`` (post-visible grace). A single ``processEvents()`` does not
    run a later retry timer, so grace is set to zero here to exercise the flush path without implying
    synchronous overlay on the real hot path.
    """
    os.environ.pop("OZLINK_PROVISIONAL_DESTINATION_STARTUP", None)
    monkeypatch.setenv("OZLINK_STARTUP_BACKGROUND_HYDRATION_DELAY_MS", "0")
    monkeypatch.setenv("OZLINK_STARTUP_POST_VISIBLE_HEAVY_WORK_GRACE_SEC", "0")
    app = QApplication.instance() or QApplication([])
    mw, dm = _provisional_startup_test_mw_with_snapshot()
    mw._restore_expanded_destination_paths = MagicMock()
    mw._hydrate_destination_allocations_for_expanded_paths_model = MagicMock()
    mw._destination_prune_pending_snapshot_branch_refresh_after_provisional_mount = MagicMock()
    mw._schedule_snapshot_branch_refresh = MagicMock()
    mw._snapshot_refresh_targets_from_snapshot = MagicMock(return_value={"p1"})
    mw._destination_startup_deferred_overlay_reasons = ["prior_deferred"]
    mw._destination_startup_indicator_refresh_pending_after_cached = True

    with patch.object(MainWindow, "_apply_destination_planning_overlays", MagicMock(return_value=0)) as ov:
        assert MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test") is True
        assert getattr(mw, "_destination_startup_ui_phase", "") == "cached_only"
        MainWindow._destination_maybe_begin_provisional_startup_hydration(mw, reason="explicit_unit")
        app.processEvents()
        assert getattr(mw, "_destination_startup_ui_phase", "") == "background_hydration"
        assert dm.rowCount(QModelIndex()) == 1
        mw._restore_expanded_destination_paths.assert_called_once()
        mw._hydrate_destination_allocations_for_expanded_paths_model.assert_called_once()
        ov.assert_called()
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
