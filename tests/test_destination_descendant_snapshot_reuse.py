"""Tests for destination allocation descendant snapshot reuse audit (startup / enqueue / repair gates)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from PySide6.QtCore import Qt

from ozlink_console import destination_authority_contract
from ozlink_console.main_window import MainWindow


def _minimal_dest_model(parent_payload: dict):
    """Stand-in QModelIndex for allocation folder row."""
    m = MagicMock()

    def _data(role, _col=0):
        if role == Qt.UserRole:
            return parent_payload
        return None

    m.data = _data
    m.rowCount = MagicMock(return_value=0)
    m.siblingAtColumn = lambda c: m
    return m


def _mw_with_dm_and_index(mw, ix):
    dm = MagicMock()
    dm.is_index_live = MagicMock(return_value=True)
    mw.destination_planning_model = dm
    return dm


def test_snapshot_reuse_assess_complete_when_counts_and_signatures_align(monkeypatch):
    mw = MainWindow.__new__(MainWindow)
    mw._allocation_move_key = lambda mv: "mk1"
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._canonical_source_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._tree_item_path = lambda d: str((d or {}).get("item_path") or "")
    mw._sort_descendants_for_allocation_apply = lambda rows: list(rows)
    mw.node_is_planned_allocation = lambda pl: bool(pl.get("planned_alloc"))
    monkeypatch.setattr(
        destination_authority_contract,
        "graph_owns_visible_real_destination_structure",
        lambda _host: True,
    )
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: None)
    monkeypatch.setattr(
        mw,
        "_collect_source_descendants_for_projection",
        lambda _src, _mv, collect_reason="": [{"item_path": f"d{i}", "is_folder": False} for i in range(3)],
    )
    monkeypatch.setattr(mw, "_destination_collect_planned_workspace_children_under_model", lambda _ix: [{"x": 1}])
    monkeypatch.setattr(mw, "_destination_count_planned_snapshot_tree_nodes", lambda nodes: 3 if nodes else 0)
    monkeypatch.setattr(mw, "_allocation_projection_children_signature_from_index", lambda _ix: "abcdabcdabcdabcdabcdabcdabcdabcd12")
    move = {"source": {"is_folder": True}, "source_path": "S\\A", "projection_path": "D\\T"}
    parent_pl = {
        "is_folder": True,
        "planned_alloc": True,
        "item_path": "D\\T",
        "allocation_projection_destination_path_saved": "D\\T",
        "allocation_projection_children_signature": "abcdabcdabcdabcdabcdabcdabcdabcd12",
        "allocation_projection_resume_source_token": "",
    }
    ix = _minimal_dest_model(parent_pl)
    ix.isValid = MagicMock(return_value=True)
    ix.column = MagicMock(return_value=0)
    _mw_with_dm_and_index(mw, ix)
    monkeypatch.setattr(mw, "_allocation_projection_path", lambda _m: "D\\T")
    monkeypatch.setattr(mw, "_destination_allocation_projection_resume_token", lambda _m, _s: "toktok")

    out = mw._destination_descendant_snapshot_reuse_assess(ix, move, audit_context="test")
    assert out["outcome"] == "complete"
    assert out["overlay_descendant_count"] == 3
    assert out["expected_descendant_count"] == 3


def test_snapshot_reuse_assess_signature_mismatch(monkeypatch):
    mw = MainWindow.__new__(MainWindow)
    mw._allocation_move_key = lambda mv: "mk1"
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._canonical_source_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._tree_item_path = lambda d: str((d or {}).get("item_path") or "")
    mw._sort_descendants_for_allocation_apply = lambda rows: list(rows)
    mw.node_is_planned_allocation = lambda pl: bool(pl.get("planned_alloc"))
    monkeypatch.setattr(
        destination_authority_contract,
        "graph_owns_visible_real_destination_structure",
        lambda _host: True,
    )
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: None)
    monkeypatch.setattr(
        mw,
        "_collect_source_descendants_for_projection",
        lambda _src, _mv, collect_reason="": [{"item_path": "x", "is_folder": False}],
    )
    monkeypatch.setattr(mw, "_destination_collect_planned_workspace_children_under_model", lambda _ix: [{"x": 1}])
    monkeypatch.setattr(mw, "_destination_count_planned_snapshot_tree_nodes", lambda nodes: 1 if nodes else 0)
    monkeypatch.setattr(mw, "_allocation_projection_children_signature_from_index", lambda _ix: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    move = {"source": {"is_folder": True}}
    parent_pl = {
        "is_folder": True,
        "planned_alloc": True,
        "item_path": "D\\T",
        "allocation_projection_destination_path_saved": "D\\T",
        "allocation_projection_children_signature": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    }
    ix = _minimal_dest_model(parent_pl)
    ix.isValid = MagicMock(return_value=True)
    ix.column = MagicMock(return_value=0)
    _mw_with_dm_and_index(mw, ix)
    monkeypatch.setattr(mw, "_allocation_projection_path", lambda _m: "D\\T")
    monkeypatch.setattr(mw, "_destination_allocation_projection_resume_token", lambda _m, _s: "tok")

    out = mw._destination_descendant_snapshot_reuse_assess(ix, move, audit_context="test")
    assert out["outcome"] == "signature_mismatch"


def test_snapshot_reuse_assess_partial_subtree(monkeypatch):
    mw = MainWindow.__new__(MainWindow)
    mw._allocation_move_key = lambda mv: "mk1"
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._canonical_source_projection_path = lambda p: str(p or "").replace("/", "\\").strip()
    mw._tree_item_path = lambda d: str((d or {}).get("item_path") or "")
    mw._sort_descendants_for_allocation_apply = lambda rows: list(rows)
    mw.node_is_planned_allocation = lambda pl: bool(pl.get("planned_alloc"))
    monkeypatch.setattr(
        destination_authority_contract,
        "graph_owns_visible_real_destination_structure",
        lambda _host: True,
    )
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: None)
    monkeypatch.setattr(
        mw,
        "_collect_source_descendants_for_projection",
        lambda _src, _mv, collect_reason="": [{}, {}, {}],
    )
    monkeypatch.setattr(mw, "_destination_collect_planned_workspace_children_under_model", lambda _ix: [{"x": 1}])
    monkeypatch.setattr(mw, "_destination_count_planned_snapshot_tree_nodes", lambda nodes: 1 if nodes else 0)
    monkeypatch.setattr(mw, "_allocation_projection_children_signature_from_index", lambda _ix: "abcdabcdabcdabcdabcdabcdabcdabcd12")
    move = {"source": {"is_folder": True}}
    parent_pl = {
        "is_folder": True,
        "planned_alloc": True,
        "item_path": "D\\T",
        "allocation_projection_destination_path_saved": "D\\T",
        "allocation_projection_children_signature": "abcdabcdabcdabcdabcdabcdabcdabcd12",
    }
    ix = _minimal_dest_model(parent_pl)
    ix.isValid = MagicMock(return_value=True)
    ix.column = MagicMock(return_value=0)
    _mw_with_dm_and_index(mw, ix)
    monkeypatch.setattr(mw, "_allocation_projection_path", lambda _m: "D\\T")
    monkeypatch.setattr(mw, "_destination_allocation_projection_resume_token", lambda _m, _s: "tok")

    out = mw._destination_descendant_snapshot_reuse_assess(ix, move, audit_context="test")
    assert out["outcome"] == "partial"


@pytest.mark.parametrize(
    "prev,cur,want_metric_hint",
    [
        (658, 570, False),
        (100, 99, True),
    ],
)
def test_snapshot_node_transition_log_smoke(prev, cur, want_metric_hint, monkeypatch):
    mw = MainWindow.__new__(MainWindow)
    mw._count_tree_snapshot_nodes = lambda _sn: cur
    mw._destination_snapshot_prev_recursive_node_count = prev
    log_calls: list[dict] = []

    def _fake_log_info(name, **kw):
        log_calls.append({"name": name, **kw})

    monkeypatch.setattr("ozlink_console.main_window.log_info", _fake_log_info)
    monkeypatch.setattr(mw, "_capture_tree_items_snapshot", lambda _pk: [])
    runtime_snapshots = {"source": [], "destination": []}
    mw._runtime_session_tree_snapshots = runtime_snapshots
    mw._destination_tree_snapshot_dirty_for_persist = False
    mw._refresh_runtime_tree_snapshot("destination")
    names = {c["name"] for c in log_calls}
    assert "destination_snapshot_node_count_transition" in names
    if want_metric_hint:
        assert "destination_snapshot_node_drop_metric_difference" in names
    assert mw._destination_snapshot_prev_recursive_node_count == cur
