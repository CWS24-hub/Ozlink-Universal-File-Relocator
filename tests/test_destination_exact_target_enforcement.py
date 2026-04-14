"""Exact-target enforcement pass: intended paths vs visible planned rows (narrow harness)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console import destination_authority_contract as dac
from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _bind_chain_helpers(mw):
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
        "_find_destination_child_by_path",
        "_destination_visible_path_lookup_canonical_keys_ex",
    ):
        setattr(mw, name, getattr(MainWindow, name).__get__(mw, MainWindow))
    mw._destination_has_future_descendants = lambda _nd: False
    mw._log_restore_phase = lambda *a, **k: None
    mw._refresh_destination_item_visibility_index = lambda *a, **k: None


def test_exact_target_enforcement_creates_missing_planned_chain(monkeypatch):
    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _h: True)
    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.destination_planning_model = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    _bind_chain_helpers(mw)
    mw._destination_visible_library_anchor_canonical_path = lambda: ""
    mw._canonical_destination_path_with_visible_library_anchor = (
        lambda p: mw._canonical_destination_projection_path(p) or mw.normalize_memory_path(p)
    )
    mw.proposed_folders = []
    mw._log_restore_exception = lambda *a, **k: None
    dm = mw.destination_planning_model
    hub = {
        "name": "Root3",
        "id": "live-root",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "is_folder": True,
        "item_path": "Root3",
        "tree_role": "destination",
        "children_loaded": True,
        "load_failed": False,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    mw.planned_moves = [
        {
            "destination_path": "Root3\\Sales\\Pictures",
            "source": {"name": "Pictures", "is_folder": True},
        }
    ]
    MainWindow._destination_run_exact_target_enforcement_pass(mw, "unit_test")
    vp, _ = MainWindow._destination_enumerate_visible_planned_paths_and_all_visible(mw)
    assert any(p.casefold() == "root3\\sales\\pictures".casefold() for p in vp)
    _ = app


def test_exact_target_enforcement_removes_misplaced_planned_then_recreates(monkeypatch):
    monkeypatch.setattr(dac, "graph_owns_visible_real_destination_structure", lambda _h: True)
    app = QApplication.instance() or QApplication([])
    mw = MainWindow.__new__(MainWindow)
    mw._planning_browse_mode = lambda k: "sharepoint" if k == "destination" else "local"
    mw.destination_planning_model = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    _bind_chain_helpers(mw)
    mw._destination_visible_library_anchor_canonical_path = lambda: ""
    mw._canonical_destination_path_with_visible_library_anchor = (
        lambda p: mw._canonical_destination_projection_path(p) or mw.normalize_memory_path(p)
    )
    mw.proposed_folders = []
    mw._log_restore_exception = lambda *a, **k: None
    dm = mw.destination_planning_model
    hub = {
        "name": "Root3",
        "id": "live-root",
        "row_kind": "live_folder",
        "verification_state": "live_confirmed",
        "is_folder": True,
        "item_path": "Root3",
        "tree_role": "destination",
        "children_loaded": True,
        "load_failed": False,
        "drive_id": "d1",
        "library_id": "d1",
    }
    MainWindow._apply_tree_item_visual_state(mw, None, hub)
    dm.reset_root_payloads([hub])
    hub_ix = dm.index(0, 0, QModelIndex())
    MainWindow._sharepoint_bind_planned_segment_chain(
        mw, hub_ix, ["WrongBranch", "Orphan"], bind_kind="test_misplaced"
    )
    mw.planned_moves = [
        {
            "destination_path": "Root3\\Sales\\Follow Up",
            "source": {"name": "Follow Up", "is_folder": True},
        }
    ]
    MainWindow._destination_run_exact_target_enforcement_pass(mw, "unit_test_misplaced")
    vp, _ = MainWindow._destination_enumerate_visible_planned_paths_and_all_visible(mw)
    assert any("sales" in p.casefold() and "follow up" in p.casefold() for p in vp)
    assert not any("wrongbranch" in p.casefold() for p in vp)
    _ = app
