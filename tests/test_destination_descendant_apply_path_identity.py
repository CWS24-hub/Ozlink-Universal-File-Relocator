"""Graph descendant-apply path identity, rebind, yield vs abort, and drain fingerprint."""

from __future__ import annotations

import os

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
    mw.planned_moves = []
    mw._destination_descendant_apply_state = None
    mw._destination_descendant_apply_queue = None
    _bind(
        mw,
        (
            "_allocation_move_key",
            "_apply_tree_item_visual_state",
            "_destination_payload_index_key",
            "_destination_graph_descendant_apply_try_rebind_parent_ix",
            "_destination_graph_descendant_apply_process_one_segment",
            "_destination_snapshot_capture_drain_progress_fingerprint",
            "_destination_graph_descendant_apply_move_still_planned",
            "_destination_descendant_apply_pending_graph_walk_for_dest_lookup",
            "_destination_graph_descendant_expected_allocation_folder_anchor_path",
            "_destination_descendant_apply_active_graph_walk_blocks_dest_lookup",
            "_destination_descendant_apply_graph_dest_lookup_matches_folder_paths",
            "_unresolved_overlay_target_paths_equivalent",
        ),
    )
    dt = QTreeView()
    dm = DestinationPlanningTreeModel(
        destination_index_key_fn=MainWindow._destination_payload_index_key.__get__(mw, MainWindow)
    )
    dt.setModel(dm)
    mw.destination_planning_model = dm
    return mw, dm


def test_try_rebind_parent_updates_state_when_path_resolves(monkeypatch, mw_graph):
    mw, dm = mw_graph
    move = {"destination_path": "Root3\\Box", "target_name": "Box", "allocation_id": "a1"}
    mw.planned_moves = [move]
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
    fresh = dm.index(0, 0, QModelIndex())
    st: dict = {
        "graph_walk": True,
        "move": move,
        "model": dm,
        "allocation_destination_path": "Root3\\Box",
        "parent_ix": QModelIndex(),
        "parent_data": {},
    }
    monkeypatch.setattr(
        mw,
        "_destination_graph_descendant_resolve_model_index_for_path",
        lambda _m, _p: fresh,
    )
    assert MainWindow._destination_graph_descendant_apply_try_rebind_parent_ix(mw, st) == "ok"
    assert st["parent_ix"] == fresh
    assert isinstance(st.get("parent_data"), dict)


def test_try_rebind_terminal_when_move_removed_from_planned(monkeypatch, mw_graph):
    mw, dm = mw_graph
    move = {"destination_path": "Root3\\Gone", "target_name": "Gone", "allocation_id": "gone1"}
    mw.planned_moves = []
    st = {
        "graph_walk": True,
        "move": move,
        "model": dm,
        "allocation_destination_path": "Root3\\Gone",
        "parent_ix": QModelIndex(),
        "parent_data": {},
    }
    assert MainWindow._destination_graph_descendant_apply_try_rebind_parent_ix(mw, st) == "terminal"


def test_graph_bind_in_progress_yields_not_stale(monkeypatch, mw_graph):
    mw, _dm = mw_graph
    mw._destination_descendant_apply_graph_bind_depth = 1
    st = {"graph_walk": True}
    r = MainWindow._destination_graph_descendant_apply_process_one_segment(mw, st)
    assert r == "yield"
    mw._destination_descendant_apply_graph_bind_depth = 0


def test_walk_cursor_unresolved_yields_when_resolve_fails(monkeypatch, mw_graph):
    mw, dm = mw_graph
    move = {"destination_path": "Root3\\W", "target_name": "W", "allocation_id": "w1"}
    mw.planned_moves = [move]
    monkeypatch.setattr(mw, "_destination_graph_descendant_apply_try_rebind_parent_ix", lambda _st: "ok")
    monkeypatch.setattr(
        mw,
        "_destination_graph_descendant_resolve_model_index_for_path",
        lambda _dm, _p: None,
    )
    st = {
        "graph_walk": True,
        "move": move,
        "model": dm,
        "allocation_destination_path": "Root3\\W",
        "parent_ix": QModelIndex(),
        "parent_data": {},
        "rel_clean": ["sub"],
        "descendant_data": {"name": "f", "is_folder": False, "item_path": "s\\f"},
        "base_parts": ["Root3", "W"],
        "base_canon": "Root3\\W",
        "projection_terminal": "Root3\\W\\sub\\f",
        "graph_auth": True,
        "descendant_source_path": "s\\f",
        "seg_index": 0,
        "child_map_cache": {},
    }
    r = MainWindow._destination_graph_descendant_apply_process_one_segment(mw, st)
    assert r == "yield"


def test_drain_fingerprint_includes_seg_index_overlay_and_generation(monkeypatch, mw_graph):
    mw, dm = mw_graph
    mw._destination_descendant_apply_queue = None
    mw._destination_descendant_apply_state = {
        "graph_walk": True,
        "walk_phase": "walk",
        "added_count": 1,
        "desc_index": 2,
        "seg_index": 3,
        "overlay_count": 4,
        "model": dm,
        "pending_leaf_batches": {},
    }
    fp = MainWindow._destination_snapshot_capture_drain_progress_fingerprint(mw)
    assert fp[0] == 0
    assert fp[1] == "walk"
    assert fp[2] is True
    assert fp[3] == 1
    assert fp[4] == 2
    assert fp[5] == 3
    assert fp[6] == 4
    assert fp[7] == 0
    assert isinstance(fp[8], int)
    assert isinstance(fp[9], str)
    assert isinstance(fp[10], int)


def test_pending_graph_walk_matches_queued_move_anchor(monkeypatch, mw_graph):
    mw, _dm = mw_graph
    from collections import deque

    move = {"destination_path": "Root3\\HR\\Folder", "target_name": "Folder", "allocation_id": "q1"}
    mw.planned_moves = [move]

    def anchor(m):
        return "Root3\\HR\\Folder"

    monkeypatch.setattr(mw, "_destination_graph_descendant_expected_allocation_folder_anchor_path", anchor)
    monkeypatch.setattr(mw, "_unresolved_overlay_target_paths_equivalent", lambda a, b: str(a) == str(b))
    mw._destination_descendant_apply_state = None
    mw._destination_descendant_apply_queue = deque(
        [(QModelIndex(), move, None, "t", "c")]
    )
    assert MainWindow._destination_descendant_apply_pending_graph_walk_for_dest_lookup(mw, "Root3\\HR\\Folder", move)


def test_active_graph_walk_blocks_dest_lookup(monkeypatch, mw_graph):
    mw, _dm = mw_graph
    mw._destination_descendant_apply_state = {
        "graph_walk": True,
        "allocation_destination_path": "Root3\\HR\\X",
    }
    monkeypatch.setattr(mw, "_unresolved_overlay_target_paths_equivalent", lambda a, b: False)
    assert MainWindow._destination_descendant_apply_active_graph_walk_blocks_dest_lookup(mw, "Root3\\HR\\X")
