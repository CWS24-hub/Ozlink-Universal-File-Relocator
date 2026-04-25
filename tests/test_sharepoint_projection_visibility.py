"""Projection/overlay visibility ordering and Graph-authority guards (narrow)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from ozlink_console.main_window import MainWindow


def test_apply_destination_planning_overlays_body_runs_proactive_before_try_skip():
    """Proactive parent-chain scheduling must not be skipped when redundant-skip returns early."""
    order: list[str] = []

    class Host:
        _destination_future_model_last_blocked_source_restore = False
        _destination_suppress_steady_materialize_skip_once = False
        _destination_incremental_merge_in_progress = False

        def _destination_user_scroll_interaction_active(self):
            return False

        def _schedule_proactive_graph_parent_chains_for_unresolved_overlays(self, *, reason: str) -> int:
            order.append("proactive")
            return 0

        def _try_skip_redundant_destination_future_model_materialize(self, reason):
            order.append("try_skip")
            return 0

    h = Host()
    out = MainWindow._apply_destination_planning_overlays_body(h, "idle_destination_materialize")
    assert out == 0
    assert order == ["proactive", "try_skip"]


def test_drfws_deferred_schedule_short_circuit_still_invokes_proactive(monkeypatch):
    from unittest.mock import MagicMock

    from ozlink_console import destination_authority_contract as dac

    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _h: False)
    calls: list[str] = []

    class Win:
        planned_moves = []
        proposed_folders = []
        _destination_chunked_bind_state = None
        _destination_future_projection_async_state = None
        _destination_idle_materialize_timer = None
        _destination_idle_materialize_pending_reason = ""
        _destination_drfws_affected_paths = set()

        def _should_park_deferred_materialize_for_full_tree(self, _r):
            return False

        def _try_skip_redundant_destination_future_model_materialize(self, _r):
            return 1

        def _schedule_proactive_graph_parent_chains_for_unresolved_overlays(self, *, reason: str) -> int:
            calls.append(reason)
            return 0

        def _destination_drfws_pending_work_signature(self):
            return ""

    w = Win()
    w._destination_idle_materialize_timer = MagicMock()
    w._destination_idle_materialize_timer.isActive.return_value = False

    MainWindow._schedule_deferred_destination_materialization(
        w, "deferred_reconcile_folder_worker_success", delay_ms=180
    )
    assert calls == ["deferred_reconcile_folder_worker_success"]


def test_planned_workspace_predicate_and_not_live_graph():
    from ozlink_console.sharepoint_destination_overlay_attach import (
        destination_payload_is_live_graph_row,
        destination_payload_is_planned_workspace_row,
    )

    planned = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "is_folder": True,
        "placeholder": False,
    }
    assert destination_payload_is_planned_workspace_row(planned)
    assert not destination_payload_is_live_graph_row(planned)
    assert not destination_payload_is_planned_workspace_row({"row_kind": "live_folder", "verification_state": "live_confirmed"})


def test_sharepoint_canonical_path_segments_under_parent():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)
    segs = mw._sharepoint_canonical_path_segments_under_parent("Hub", "Hub\\A\\B")
    assert segs == ["A", "B"]
    _ = app


def test_sharepoint_bind_planned_segment_chain_appends_planned_rows():
    from PySide6.QtCore import QModelIndex, Qt
    from PySide6.QtWidgets import QApplication

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
    dm = mw.destination_planning_model
    hub = {
        "name": "Hub",
        "id": "live-hub",
        "is_folder": True,
        "item_path": "/Hub",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    leaf = MainWindow._sharepoint_bind_planned_segment_chain(mw, hub_ix, ["DeptA", "DeptB"], bind_kind="test")
    assert leaf is not None and leaf.isValid()
    pl = leaf.data(Qt.UserRole) or {}
    assert pl.get("verification_state") == "planned_only"
    assert pl.get("row_kind") == "planned_folder"
    assert not str(pl.get("id") or "").strip()
    assert pl.get("item_path") == "Hub\\DeptA\\DeptB"
    assert pl.get("planned_parent_canonical_path") == "Hub\\DeptA"
    mid = dm.index(0, 0, dm.index(0, 0, QModelIndex()))
    mid_pl = mid.data(Qt.UserRole) or {}
    assert mid_pl.get("item_path") == "Hub\\DeptA"
    assert mid_pl.get("planned_parent_canonical_path") == "Hub"
    _ = app


def test_sharepoint_planned_three_segment_chain_canonical_paths():
    from PySide6.QtCore import QModelIndex, Qt
    from PySide6.QtWidgets import QApplication

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
        ["Payroll", "FY26", "Drafts"],
        bind_kind="test",
        expected_parent_canonical="Hub",
        projection_target_canonical="Hub\\Payroll\\FY26\\Drafts",
    )
    assert leaf is not None and leaf.isValid()
    pl = leaf.data(Qt.UserRole) or {}
    assert pl.get("item_path") == "Hub\\Payroll\\FY26\\Drafts"
    assert pl.get("planning_uuid")
    _ = app


def test_sharepoint_planned_bind_rejects_expected_parent_mismatch():
    from PySide6.QtCore import QModelIndex
    from PySide6.QtWidgets import QApplication

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
    out = MainWindow._sharepoint_bind_planned_segment_chain(
        mw,
        hub_ix,
        ["X"],
        bind_kind="test",
        expected_parent_canonical="OtherLibrary\\Wrong",
    )
    assert out is None
    assert dm.rowCount(hub_ix) == 0
    _ = app


def test_planned_workspace_never_live_graph_even_with_stray_id():
    from ozlink_console.sharepoint_destination_overlay_attach import destination_payload_is_live_graph_row

    planned = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "is_folder": True,
        "placeholder": False,
        "id": "should-not-make-this-live",
        "non_graph_structural_authority": False,
    }
    assert not destination_payload_is_live_graph_row(planned)


def _make_reconcile_mw():
    from PySide6.QtWidgets import QApplication

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
    mw._destination_model_index_user_role_dict = MainWindow._destination_model_index_user_role_dict.__get__(mw, MainWindow)
    mw._log_restore_phase = lambda *a, **k: None
    mw._refresh_destination_item_visibility_index = lambda *a, **k: None
    mw._apply_tree_item_visual_state = MainWindow._apply_tree_item_visual_state.__get__(mw, MainWindow)
    return mw, app


def _reconcile_planning_memory_init(mw):
    """``MainWindow.__new__`` skips ``__init__``; reconcile reads ``planned_moves`` / ``proposed_folders``."""
    mw.planned_moves = []
    mw.proposed_folders = []


def test_reconcile_planned_to_live_by_canonical_path():
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.models import ProposedFolder

    mw, app = _make_reconcile_mw()
    _reconcile_planning_memory_init(mw)
    mw.proposed_folders = [
        ProposedFolder(
            FolderName="Target",
            DestinationPath="Hub\\Target",
            ParentPath="Hub",
            StableKey="sk-reconcile-merge-path",
        )
    ]
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
    live_child = {
        "name": "Target",
        "id": "live-target",
        "is_folder": True,
        "item_path": "Hub\\Target",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, live_child)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [live_child])
    snap = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "Target",
        "item_path": "Hub\\Target",
        "destination_path": "Hub\\Target",
        "planning_uuid": "pu-merge-path",
        "placeholder": False,
        "is_folder": True,
    }
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, hub_ix, [snap], allow_reappend=True)
    child_ix = dm.index(0, 0, hub_ix)
    pl = child_ix.data(Qt.UserRole) or {}
    assert pl.get("verification_state") == "live_confirmed"
    assert pl.get("id") == "live-target"
    assert pl.get("planning_uuid") == "pu-merge-path"
    assert dm.rowCount(hub_ix) == 1
    _ = app


def test_reconcile_under_graph_authority_requires_path_match_not_name_fallback():
    """Graph-owned tree: stale snapshot paths are not planning-backed, so no merge and no phantom row."""
    from PySide6.QtCore import QModelIndex, Qt

    mw, app = _make_reconcile_mw()
    _reconcile_planning_memory_init(mw)
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
    live_child = {
        "name": "Reports",
        "id": "live-reports",
        "is_folder": True,
        "item_path": "Hub\\Reports",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, live_child)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [live_child])
    snap = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "Reports",
        "item_path": "Hub\\NotThisPath",
        "destination_path": "Hub\\NotThisPath",
        "planning_uuid": "pu-name-fallback",
        "placeholder": False,
        "is_folder": True,
    }
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, hub_ix, [snap], allow_reappend=True)
    assert dm.rowCount(hub_ix) == 1
    rows = [dm.index(r, 0, hub_ix).data(Qt.UserRole) or {} for r in range(dm.rowCount(hub_ix))]
    live_rows = [x for x in rows if str(x.get("id") or "") == "live-reports"]
    planned_rows = [x for x in rows if str(x.get("planning_uuid") or "") == "pu-name-fallback"]
    assert len(live_rows) == 1
    assert live_rows[0].get("verification_state") == "live_confirmed"
    assert live_rows[0].get("planning_uuid") in (None, "")
    assert len(planned_rows) == 0
    _ = app


def test_reconcile_intended_path_prefers_proposed_stable_key_over_stale_snapshot():
    """Stale snapshot path/name must not win when ``proposed_folder_stable_id`` maps to ``ProposedFolder`` memory."""
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.models import ProposedFolder

    mw, app = _make_reconcile_mw()
    mw.proposed_folders = [
        ProposedFolder(
            FolderName="RealTarget",
            DestinationPath=r"Hub\RealTarget",
            DestinationId="",
            ParentPath=r"Hub",
            StableKey="sk-stable-reconcile-test",
        )
    ]
    mw.planned_moves = []
    mw._proposed_destination_path = MainWindow._proposed_destination_path.__get__(mw, MainWindow)
    mw._proposed_parent_path = MainWindow._proposed_parent_path.__get__(mw, MainWindow)
    mw._destination_planned_snapshot_paths_exact_canonical = MainWindow._destination_planned_snapshot_paths_exact_canonical.__get__(
        mw, MainWindow
    )
    mw._destination_folder_index_canonical_path = MainWindow._destination_folder_index_canonical_path.__get__(
        mw, MainWindow
    )
    mw._canonical_destination_path_with_visible_library_anchor = (
        MainWindow._canonical_destination_path_with_visible_library_anchor.__get__(mw, MainWindow)
    )
    mw._destination_visible_library_anchor_canonical_path = lambda: ""
    mw._destination_semantic_path = MainWindow._destination_semantic_path.__get__(mw, MainWindow)
    mw._destination_reconcile_trace_enabled = lambda: False
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
    live_child = {
        "name": "RealTarget",
        "id": "live-real",
        "is_folder": True,
        "item_path": r"Hub\RealTarget",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, live_child)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [live_child])
    snap = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "StaleWrongName",
        "item_path": r"Hub\StaleWrongPath",
        "destination_path": r"Hub\StaleWrongPath",
        "planning_uuid": "pu-stale-snap-stable-key",
        "proposed_folder_stable_id": "sk-stable-reconcile-test",
        "placeholder": False,
        "is_folder": True,
    }
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, hub_ix, [snap], allow_reappend=True)
    assert dm.rowCount(hub_ix) == 1
    pl = dm.index(0, 0, hub_ix).data(Qt.UserRole) or {}
    assert pl.get("verification_state") == "live_confirmed"
    assert pl.get("id") == "live-real"
    assert pl.get("planning_uuid") == "pu-stale-snap-stable-key"
    assert pl.get("proposed_folder_stable_id") == "sk-stable-reconcile-test"
    _ = app


def test_reconcile_name_fallback_when_single_live_sibling_matches_local_destination():
    """Local disk destination: single name match may still merge when path snapshot differs."""
    from PySide6.QtCore import QModelIndex, Qt

    mw, app = _make_reconcile_mw()
    mw._planning_browse_mode = lambda k: "local" if k == "destination" else "local"
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
    live_child = {
        "name": "Reports",
        "id": "live-reports",
        "is_folder": True,
        "item_path": "Hub\\Reports",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, live_child)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [live_child])
    snap = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "Reports",
        "item_path": "Hub\\NotThisPath",
        "destination_path": "Hub\\NotThisPath",
        "planning_uuid": "pu-name-fallback-local",
        "placeholder": False,
        "is_folder": True,
    }
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, hub_ix, [snap], allow_reappend=True)
    assert dm.rowCount(hub_ix) == 1
    pl = dm.index(0, 0, hub_ix).data(Qt.UserRole) or {}
    assert pl.get("id") == "live-reports"
    assert pl.get("planning_uuid") == "pu-name-fallback-local"
    assert pl.get("verification_state") == "live_confirmed"
    _ = app


def test_reconcile_name_fallback_requires_unique_sibling_name():
    from PySide6.QtCore import QModelIndex, Qt

    mw, app = _make_reconcile_mw()
    _reconcile_planning_memory_init(mw)
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
    a = {
        "name": "Dup",
        "id": "id-a",
        "is_folder": True,
        "item_path": "Hub\\a\\Dup",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    b = {
        "name": "Dup",
        "id": "id-b",
        "is_folder": True,
        "item_path": "Hub\\b\\Dup",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, a)
    MainWindow._apply_tree_item_visual_state(mw, None, b)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [a, b])
    snap = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "Dup",
        "item_path": "Hub\\no-such-path",
        "destination_path": "Hub\\no-such-path",
        "planning_uuid": "pu-ambig",
        "placeholder": False,
        "is_folder": True,
    }
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, hub_ix, [snap], allow_reappend=True)
    assert dm.rowCount(hub_ix) == 2
    ids = []
    for r in range(dm.rowCount(hub_ix)):
        pl = dm.index(r, 0, hub_ix).data(Qt.UserRole) or {}
        ids.append(str(pl.get("id") or ""))
    assert "id-a" in ids and "id-b" in ids
    planned_rows = sum(
        1
        for r in range(dm.rowCount(hub_ix))
        if (dm.index(r, 0, hub_ix).data(Qt.UserRole) or {}).get("verification_state") == "planned_only"
    )
    assert planned_rows == 0
    _ = app


def test_reconcile_dedupes_snapshots_by_planning_uuid():
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.models import ProposedFolder

    mw, app = _make_reconcile_mw()
    _reconcile_planning_memory_init(mw)
    mw.proposed_folders = [
        ProposedFolder(
            FolderName="T",
            DestinationPath="Hub\\T",
            ParentPath="Hub",
            StableKey="sk-reconcile-dedupe-uuid",
        )
    ]
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
    live_child = {
        "name": "T",
        "id": "live-t",
        "is_folder": True,
        "item_path": "Hub\\T",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, live_child)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [live_child])
    snap = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "T",
        "item_path": "Hub\\T",
        "planning_uuid": "same-uuid",
        "placeholder": False,
        "is_folder": True,
    }
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, hub_ix, [snap, dict(snap)], allow_reappend=True)
    assert dm.rowCount(hub_ix) == 1
    pl = dm.index(0, 0, hub_ix).data(Qt.UserRole) or {}
    assert pl.get("planning_uuid") == "same-uuid"
    _ = app


def test_reconcile_shallow_load_skips_reappend_without_duplicating_planned():
    from PySide6.QtCore import QModelIndex

    from ozlink_console.destination_overlay_layer import overlay_row_marker

    mw, app = _make_reconcile_mw()
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
    planned = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "Orphan",
        "item_path": "Hub\\Orphan",
        "planning_uuid": "pu-orphan",
        "placeholder": False,
        "is_folder": True,
        "id": "",
        **overlay_row_marker("planned_workspace"),
    }
    MainWindow._apply_tree_item_visual_state(mw, None, planned)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [planned])
    snap = dict(planned)
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, hub_ix, [snap], allow_reappend=False)
    assert dm.rowCount(hub_ix) == 1
    _ = app


def test_reconcile_reparents_misplaced_planned_row_by_identity():
    """Planned row under wrong folder must move when planning memory resolves intended parent path."""
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.destination_overlay_layer import overlay_row_marker
    from ozlink_console.models import ProposedFolder

    mw, app = _make_reconcile_mw()
    _reconcile_planning_memory_init(mw)
    mw.proposed_folders = [
        ProposedFolder(
            FolderName="Leaf",
            DestinationPath="Hub\\A\\Leaf",
            ParentPath="Hub\\A",
            StableKey="sk-reconcile-reparent-leaf",
        )
    ]
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
    folder_a = {
        "name": "A",
        "id": "live-a",
        "is_folder": True,
        "item_path": "Hub\\A",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    folder_b = {
        "name": "B",
        "id": "live-b",
        "is_folder": True,
        "item_path": "Hub\\B",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
    }
    leaf = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "Leaf",
        "item_path": "Hub\\A\\Leaf",
        "destination_path": "Hub\\A\\Leaf",
        "planning_uuid": "pu-misplaced-leaf",
        "placeholder": False,
        "is_folder": True,
        "tree_role": "destination",
        **overlay_row_marker("planned_workspace"),
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    MainWindow._apply_tree_item_visual_state(mw, None, folder_a)
    MainWindow._apply_tree_item_visual_state(mw, None, folder_b)
    MainWindow._apply_tree_item_visual_state(mw, None, leaf)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [folder_a, folder_b])
    a_ix = dm.index(0, 0, hub_ix)
    b_ix = dm.index(1, 0, hub_ix)
    dm.append_child_payloads(b_ix, [dict(leaf)])
    assert dm.rowCount(b_ix) == 1
    assert dm.rowCount(a_ix) == 0
    snap = dict(leaf)
    stats = MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, a_ix, [snap], allow_reappend=True)
    assert int(stats.get("reattached", 0)) == 1
    assert dm.rowCount(a_ix) == 1
    assert dm.rowCount(b_ix) == 0
    moved = dm.index(0, 0, a_ix).data(Qt.UserRole) or {}
    assert moved.get("planning_uuid") == "pu-misplaced-leaf"
    assert str(moved.get("item_path") or "").endswith("Leaf")
    _ = app


def test_reconcile_corrects_row_paths_from_planned_moves_not_snapshot():
    """Stale snapshot paths must be replaced by ``_allocation_projection_path`` for matching ``allocation_id``."""
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.destination_overlay_layer import overlay_row_marker

    mw, app = _make_reconcile_mw()
    mw.planned_moves = [
        {
            "request_id": "req-snap-truth",
            "allocation_id": "req-snap-truth",
            "destination_path": "Hub",
            "target_name": "Leaf",
            "source_name": "Leaf",
            "source_path": "S:\\Old\\Leaf.txt",
            "source": {"name": "Leaf.txt", "is_folder": False},
        }
    ]
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
    wrong_snap = {
        "row_kind": "planned_file",
        "verification_state": "planned_only",
        "name": "Leaf.txt",
        "item_path": "Hub\\WrongPlace\\Leaf.txt",
        "destination_path": "Hub\\WrongPlace\\Leaf.txt",
        "allocation_id": "req-snap-truth",
        "placeholder": False,
        "is_folder": False,
        "tree_role": "destination",
        **overlay_row_marker("planned_workspace"),
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    MainWindow._apply_tree_item_visual_state(mw, None, wrong_snap)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    dm.replace_all_children(hub_ix, [])
    dm.append_child_payloads(hub_ix, [dict(wrong_snap)])
    stats = MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(
        mw, hub_ix, [dict(wrong_snap)], allow_reappend=True
    )
    assert int(stats.get("reattached", 0)) == 1
    fixed = {}
    for r in range(dm.rowCount(hub_ix)):
        pl = dm.index(r, 0, hub_ix).data(Qt.UserRole) or {}
        if pl.get("placeholder"):
            continue
        if str(pl.get("allocation_id") or "") == "req-snap-truth":
            fixed = pl
            break
    ip = str(fixed.get("item_path") or "")
    assert "WrongPlace" not in ip
    assert ip.rstrip("\\").endswith("Leaf")
    _ = app


def test_reconcile_skips_second_planned_with_same_uuid():
    from PySide6.QtCore import QModelIndex, Qt

    mw, app = _make_reconcile_mw()
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
    snap = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "name": "Only",
        "item_path": "Hub\\Only",
        "planning_uuid": "pu-twice",
        "placeholder": False,
        "is_folder": True,
    }
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, hub_ix, [snap], allow_reappend=True)
    assert dm.rowCount(hub_ix) == 1
    MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(mw, hub_ix, [snap], allow_reappend=True)
    assert dm.rowCount(hub_ix) == 1
    _ = app


def test_nested_planned_workspace_preserved_after_graph_child_replace():
    """Deep snapshot + reconcile: nested planned file under planned folder survives replace_all_children."""
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.destination_overlay_layer import overlay_row_marker
    from ozlink_console.models import ProposedFolder
    from ozlink_console.sharepoint_destination_overlay_attach import destination_payload_is_planned_workspace_row

    mw, app = _make_reconcile_mw()
    _reconcile_planning_memory_init(mw)
    mw.proposed_folders = [
        ProposedFolder(
            FolderName="Follow Up",
            DestinationPath="Root3\\Sales\\Follow Up",
            ParentPath="Root3\\Sales",
            StableKey="sk-nested-follow-up",
        )
    ]
    mw.planned_moves = [
        {
            "request_id": "req-doc-nested",
            "allocation_id": "req-doc-nested",
            "destination_path": "Root3\\Sales\\Follow Up",
            "target_name": "Doc.xlsx",
            "source_name": "Doc.xlsx",
            "source_path": "S:\\Doc.xlsx",
            "source": {"name": "Doc.xlsx", "is_folder": False},
        }
    ]
    for name in (
        "_destination_count_planned_snapshot_tree_nodes",
        "_destination_collect_planned_workspace_children_under_model",
        "_destination_planned_snapshot_paths_exact_canonical",
        "_destination_reconcile_planned_rows_after_graph_folder_load",
    ):
        setattr(mw, name, getattr(MainWindow, name).__get__(mw, MainWindow))
    mw._destination_reconcile_trace_enabled = lambda: False

    dm = mw.destination_planning_model
    sales = {
        "name": "Sales",
        "id": "id-sales",
        "is_folder": True,
        "item_path": "Root3\\Sales",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, sales)
    dm.reset_root_payloads([sales])
    sales_ix = dm.index(0, 0, QModelIndex())
    fu = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "name": "Follow Up",
        "item_path": "Root3\\Sales\\Follow Up",
        "destination_path": "Root3\\Sales\\Follow Up",
        "is_folder": True,
        "placeholder": False,
        "planning_uuid": "pu-fu-nested",
        "tree_role": "destination",
        **overlay_row_marker("planned_workspace"),
    }
    MainWindow._apply_tree_item_visual_state(mw, None, fu)
    dm.append_child_payloads(sales_ix, [fu])
    fu_ix = dm.index(0, 0, sales_ix)
    doc = {
        "row_kind": "planned_file",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "name": "Doc.xlsx",
        "item_path": "Root3\\Sales\\Follow Up\\Doc.xlsx",
        "destination_path": "Root3\\Sales\\Follow Up\\Doc.xlsx",
        "is_folder": False,
        "children_loaded": True,
        "placeholder": False,
        "planning_uuid": "pu-doc-nested",
        "allocation_id": "req-doc-nested",
        "tree_role": "destination",
        **overlay_row_marker("planned_workspace"),
    }
    MainWindow._apply_tree_item_visual_state(mw, None, doc)
    dm.append_child_payloads(fu_ix, [doc])

    snap_tree = MainWindow._destination_collect_planned_workspace_children_under_model(mw, sales_ix)
    assert mw._destination_count_planned_snapshot_tree_nodes(snap_tree) == 2

    live_other = {
        "name": "OtherLive",
        "id": "id-other",
        "is_folder": True,
        "item_path": "Root3\\Sales\\OtherLive",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, live_other)
    dm.replace_all_children(sales_ix, [live_other])

    stats = MainWindow._destination_reconcile_planned_rows_after_graph_folder_load(
        mw, sales_ix, snap_tree, allow_reappend=True
    )
    assert stats.get("lost", 0) == 0
    assert stats.get("reattached", 0) >= 1

    names = []
    for r in range(dm.rowCount(sales_ix)):
        pl = dm.index(r, 0, sales_ix).data(Qt.UserRole) or {}
        names.append(str(pl.get("name") or ""))
    assert "Follow Up" in names
    assert "OtherLive" in names
    fu_ix2 = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        pl = ix.data(Qt.UserRole) or {}
        if str(pl.get("name") or "") == "Follow Up" and destination_payload_is_planned_workspace_row(pl):
            fu_ix2 = ix
            break
    assert fu_ix2 is not None
    doc_names = [
        str((dm.index(r, 0, fu_ix2).data(Qt.UserRole) or {}).get("name") or "")
        for r in range(dm.rowCount(fu_ix2))
    ]
    assert "Doc.xlsx" in doc_names
    _ = app


def test_invoke_planned_reconcile_after_snapshot_and_replace_restores_rows():
    """Folder-load pipeline: snapshot → replace_all_children → invoke reconcile (not deferred-only)."""
    from PySide6.QtCore import QModelIndex, Qt

    from ozlink_console.destination_overlay_layer import overlay_row_marker
    from ozlink_console.models import ProposedFolder
    from ozlink_console.sharepoint_destination_overlay_attach import destination_payload_is_planned_workspace_row

    mw, app = _make_reconcile_mw()
    _reconcile_planning_memory_init(mw)
    mw.proposed_folders = [
        ProposedFolder(
            FolderName="Follow Up",
            DestinationPath="Root3\\Sales\\Follow Up",
            ParentPath="Root3\\Sales",
            StableKey="sk-invoke-follow-up",
        )
    ]
    mw.planned_moves = [
        {
            "request_id": "req-doc-invoke",
            "allocation_id": "req-doc-invoke",
            "destination_path": "Root3\\Sales\\Follow Up",
            "target_name": "Doc.xlsx",
            "source_name": "Doc.xlsx",
            "source_path": "S:\\Doc.xlsx",
            "source": {"name": "Doc.xlsx", "is_folder": False},
        }
    ]
    for name in (
        "_destination_count_planned_snapshot_tree_nodes",
        "_destination_collect_planned_workspace_children_under_model",
        "_destination_planned_snapshot_paths_exact_canonical",
        "_destination_reconcile_planned_rows_after_graph_folder_load",
        "_destination_invoke_planned_workspace_reconcile_after_graph_folder_load",
        "_destination_semantic_path",
    ):
        setattr(mw, name, getattr(MainWindow, name).__get__(mw, MainWindow))
    mw._destination_reconcile_trace_enabled = lambda: False

    dm = mw.destination_planning_model
    sales = {
        "name": "Sales",
        "id": "id-sales",
        "is_folder": True,
        "item_path": "Root3\\Sales",
        "tree_role": "destination",
        "children_loaded": True,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, sales)
    dm.reset_root_payloads([sales])
    sales_ix = dm.index(0, 0, QModelIndex())
    fu = {
        "row_kind": "planned_folder",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "name": "Follow Up",
        "item_path": "Root3\\Sales\\Follow Up",
        "destination_path": "Root3\\Sales\\Follow Up",
        "is_folder": True,
        "placeholder": False,
        "planning_uuid": "pu-fu-invoke",
        "tree_role": "destination",
        **overlay_row_marker("planned_workspace"),
    }
    MainWindow._apply_tree_item_visual_state(mw, None, fu)
    dm.append_child_payloads(sales_ix, [fu])
    fu_ix = dm.index(0, 0, sales_ix)
    doc = {
        "row_kind": "planned_file",
        "verification_state": "planned_only",
        "workspace_planned_row": True,
        "name": "Doc.xlsx",
        "item_path": "Root3\\Sales\\Follow Up\\Doc.xlsx",
        "destination_path": "Root3\\Sales\\Follow Up\\Doc.xlsx",
        "is_folder": False,
        "children_loaded": True,
        "placeholder": False,
        "planning_uuid": "pu-doc-invoke",
        "allocation_id": "req-doc-invoke",
        "tree_role": "destination",
        **overlay_row_marker("planned_workspace"),
    }
    MainWindow._apply_tree_item_visual_state(mw, None, doc)
    dm.append_child_payloads(fu_ix, [doc])

    snap_tree = MainWindow._destination_collect_planned_workspace_children_under_model(mw, sales_ix)
    reconcile_calls = 0
    _inner = MainWindow._destination_reconcile_planned_rows_after_graph_folder_load.__get__(mw, MainWindow)

    def _counting_reconcile(*args, **kwargs):
        nonlocal reconcile_calls
        reconcile_calls += 1
        return _inner(*args, **kwargs)

    mw._destination_reconcile_planned_rows_after_graph_folder_load = _counting_reconcile

    live_other = {
        "name": "OtherLive",
        "id": "id-other",
        "is_folder": True,
        "item_path": "Root3\\Sales\\OtherLive",
        "tree_role": "destination",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "drive_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, live_other)
    dm.replace_all_children(sales_ix, [live_other])

    stats = MainWindow._destination_invoke_planned_workspace_reconcile_after_graph_folder_load(
        mw, sales_ix, snap_tree, allow_reappend=True
    )
    assert reconcile_calls == 1
    assert stats.get("lost", 0) == 0
    assert stats.get("reattached", 0) >= 1

    names = []
    for r in range(dm.rowCount(sales_ix)):
        pl = dm.index(r, 0, sales_ix).data(Qt.UserRole) or {}
        names.append(str(pl.get("name") or ""))
    assert "Follow Up" in names
    assert "OtherLive" in names
    fu_ix2 = None
    for r in range(dm.rowCount(sales_ix)):
        ix = dm.index(r, 0, sales_ix)
        pl = ix.data(Qt.UserRole) or {}
        if str(pl.get("name") or "") == "Follow Up" and destination_payload_is_planned_workspace_row(pl):
            fu_ix2 = ix
            break
    assert fu_ix2 is not None
    doc_names = [
        str((dm.index(r, 0, fu_ix2).data(Qt.UserRole) or {}).get("name") or "")
        for r in range(dm.rowCount(fu_ix2))
    ]
    assert "Doc.xlsx" in doc_names
    _ = app


def _main_window_library_anchor_test_harness():
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.normalize_memory_path = MainWindow.normalize_memory_path.__get__(mw, MainWindow)
    mw._canonical_destination_projection_path = MainWindow._canonical_destination_projection_path.__get__(mw, MainWindow)
    mw._path_segments = MainWindow._path_segments.__get__(mw, MainWindow)
    mw._destination_projection_segments = MainWindow._destination_projection_segments.__get__(mw, MainWindow)
    mw._current_destination_context_segments = MainWindow._current_destination_context_segments.__get__(mw, MainWindow)
    mw._destination_semantic_alias_path = MainWindow._destination_semantic_alias_path.__get__(mw, MainWindow)
    mw._destination_visible_library_anchor_canonical_path = lambda: "Root3"
    return mw, app


def test_library_anchor_destination_matches_anchored_parent_for_proposed_allocation_suffix(monkeypatch):
    from ozlink_console import destination_authority_contract as dac

    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _h: True)
    mw, app = _main_window_library_anchor_test_harness()
    cases = [
        ("Finance\\Follow up", "Root3\\Finance", ["Follow up"]),
        ("Finance\\Payroll\\file.xlsx", "Root3\\Finance", ["Payroll", "file.xlsx"]),
        ("Management\\Follow up\\file.txt", "Root3\\Management", ["Follow up", "file.txt"]),
        ("Projects\\Completed Projects\\leaf", "Root3\\Projects", ["Completed Projects", "leaf"]),
    ]
    for bare, parent, expected_rel in cases:
        dcanon = mw._canonical_destination_path_with_visible_library_anchor(bare)
        pcanon = mw._canonical_destination_projection_path(parent) or mw.normalize_memory_path(parent)
        rel = mw._sharepoint_canonical_path_segments_under_parent(pcanon, dcanon)
        assert rel == expected_rel, (bare, parent, pcanon, dcanon, rel)
    _ = app


def test_library_anchor_idempotent_for_path_already_prefixed_with_hub(monkeypatch):
    from ozlink_console import destination_authority_contract as dac

    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _h: True)
    mw, app = _main_window_library_anchor_test_harness()
    already = "Root3\\Finance\\Follow up"
    anchored = mw._canonical_destination_path_with_visible_library_anchor(already)
    pcanon = mw._canonical_destination_projection_path("Root3\\Finance") or mw.normalize_memory_path("Root3\\Finance")
    assert mw._sharepoint_canonical_path_segments_under_parent(pcanon, anchored) == ["Follow up"]
    _ = app
