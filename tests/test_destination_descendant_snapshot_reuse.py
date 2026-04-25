"""Tests for destination allocation descendant snapshot reuse audit (startup / enqueue / repair gates)."""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock

import pytest
from PySide6.QtCore import Qt

from ozlink_console import destination_authority_contract
from ozlink_console import main_window as main_window_mod
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


def test_allocation_parent_resolve_prefers_index_with_larger_overlay_subtree(monkeypatch):
    """When multiple model rows match an allocation path, prefer the one that already holds overlay children."""
    mw = MainWindow.__new__(MainWindow)
    monkeypatch.setattr(
        destination_authority_contract,
        "graph_owns_visible_real_destination_structure",
        lambda _host: True,
    )
    monkeypatch.setattr(mw, "_current_selected_destination_drive_id", lambda: "d1")
    monkeypatch.setattr(mw, "_allocation_effective_destination_allocation_root_path", lambda _m: r"Root3\Sales\Pictures")
    monkeypatch.setattr(mw, "_destination_graph_descendant_model_index_keys_for_lookup", lambda _ap: [r"Root3\Sales\Pictures"])
    monkeypatch.setattr(
        mw,
        "_destination_parent_match_details",
        lambda _ap, _vis: {"exact_match": True},
    )
    monkeypatch.setattr(mw, "_tree_item_path", lambda pl: str((pl or {}).get("item_path") or ""))
    monkeypatch.setattr(
        mw,
        "_canonical_planned_memory_path_for_graph_match",
        lambda p: str(p or "").replace("/", "\\").strip(),
    )
    monkeypatch.setattr(
        mw,
        "_canonical_destination_projection_path",
        lambda p: str(p or "").replace("/", "\\").strip(),
    )

    pl_empty = {
        "is_folder": True,
        "drive_id": "d1",
        "item_path": r"Root3\Sales\Pictures",
        "workspace_row_state": "live_confirmed",
        "verification_state": "live_confirmed",
        "row_kind": "live_folder",
    }
    pl_rich = {
        "is_folder": True,
        "drive_id": "d1",
        "item_path": r"Root3\Sales\Pictures",
        "workspace_row_state": "planned_only",
        "verification_state": "planned_only",
        "row_kind": "planned_folder",
        "planned_allocation": True,
    }

    ix_empty = MagicMock()
    ix_empty.isValid = MagicMock(return_value=True)
    ix_empty.column = MagicMock(return_value=0)
    ix_empty.siblingAtColumn = lambda _c=0: ix_empty
    ix_empty.data = lambda role, _pl=pl_empty: _pl if role == Qt.UserRole else None

    ix_rich = MagicMock()
    ix_rich.isValid = MagicMock(return_value=True)
    ix_rich.column = MagicMock(return_value=0)
    ix_rich.siblingAtColumn = lambda _c=0: ix_rich
    ix_rich.data = lambda role, _pl=pl_rich: _pl if role == Qt.UserRole else None

    dm = MagicMock()
    dm.find_indices_for_canonical_destination_path = MagicMock(return_value=[ix_empty, ix_rich])
    dm.is_index_live = MagicMock(return_value=True)
    mw.destination_planning_model = dm

    def _collect(ix):
        if ix is ix_rich:
            return [{"payload": {"verification_state": "planned_only", "row_kind": "planned_file"}, "children": []}]
        return []

    monkeypatch.setattr(mw, "_destination_collect_planned_workspace_children_under_model", _collect)
    monkeypatch.setattr(mw, "_destination_count_planned_snapshot_tree_nodes", lambda nodes: len(nodes or []))

    chosen, reason = mw._find_destination_allocation_descendant_parent_index({"source": {"is_folder": True}})
    assert reason == "ok"
    assert chosen is ix_rich


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


def _assess_mw_common(monkeypatch, *, expected_n: int, overlay_nodes: int):
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
        lambda _src, _mv, collect_reason="": [{"item_path": f"d{i}", "is_folder": False} for i in range(expected_n)],
    )
    sig = "abcdabcdabcdabcdabcdabcdabcdabcd12"

    def _collect(_ix):
        p = {
            "verification_state": "planned_only",
            "row_kind": "planned_folder",
            "workspace_row_state": "planned_only",
            "is_folder": True,
            "item_path": r"D\T\child",
        }
        return [{"payload": dict(p), "children": []} for _ in range(overlay_nodes)]

    monkeypatch.setattr(mw, "_destination_collect_planned_workspace_children_under_model", _collect)
    monkeypatch.setattr(mw, "_allocation_projection_children_signature_from_index", lambda _ix: sig)
    move = {"source": {"is_folder": True}, "source_path": "S\\A", "projection_path": "D\\T"}
    parent_pl = {
        "is_folder": True,
        "planned_alloc": True,
        "item_path": "D\\T",
        "allocation_projection_destination_path_saved": "D\\T",
        "allocation_projection_children_signature": sig,
        "allocation_projection_resume_source_token": "",
    }
    ix = _minimal_dest_model(parent_pl)
    ix.isValid = MagicMock(return_value=True)
    ix.column = MagicMock(return_value=0)
    _mw_with_dm_and_index(mw, ix)
    monkeypatch.setattr(mw, "_allocation_projection_path", lambda _m: "D\\T")
    monkeypatch.setattr(mw, "_destination_allocation_projection_resume_token", lambda _m, _s: "toktok")
    return mw, ix, move


@pytest.mark.parametrize("expected_n", [9, 75])
def test_snapshot_reuse_assess_complete_contractor_and_email_attachment_sizes(expected_n, monkeypatch):
    """FIX 3A/B: when overlay snapshot count covers expected descendants, outcome is complete."""
    mw, ix, move = _assess_mw_common(monkeypatch, expected_n=expected_n, overlay_nodes=expected_n)
    out = mw._destination_descendant_snapshot_reuse_assess(ix, move, audit_context="test")
    assert out["outcome"] == "complete"
    assert out["overlay_descendant_count"] == expected_n
    assert out["expected_descendant_count"] == expected_n


def test_snapshot_reuse_assess_rejected_when_overlay_empty_but_expected_positive(monkeypatch):
    """FIX 3C: zero overlay rows with positive expected → rejected (replay still required)."""
    mw, ix, move = _assess_mw_common(monkeypatch, expected_n=9, overlay_nodes=0)
    out = mw._destination_descendant_snapshot_reuse_assess(ix, move, audit_context="test")
    assert out["outcome"] == "rejected"
    assert out["overlay_descendant_count"] == 0
    assert out["expected_descendant_count"] == 9


def test_snapshot_reuse_assess_partial_pictures_style_underfill(monkeypatch):
    """FIX 4A: large expected count with smaller overlay → partial, not complete."""
    mw, ix, move = _assess_mw_common(monkeypatch, expected_n=3624, overlay_nodes=166)
    out = mw._destination_descendant_snapshot_reuse_assess(ix, move, audit_context="test")
    assert out["outcome"] == "partial"
    assert out["overlay_descendant_count"] == 166
    assert out["expected_descendant_count"] == 3624


def test_enqueue_skips_descendant_queue_when_snapshot_reuse_complete(monkeypatch):
    """FIX 3: complete reuse short-circuit must not append to destination_descendant_apply_queue."""
    monkeypatch.setattr(main_window_mod, "_shutdown_mutation_skip_for_host", lambda *a, **k: False)
    monkeypatch.setattr(
        destination_authority_contract,
        "graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    mw = MainWindow.__new__(MainWindow)
    monkeypatch.setattr(mw, "_find_destination_allocation_descendant_parent_index", lambda _m: (None, "none"))
    monkeypatch.setattr(
        mw,
        "_destination_descendant_snapshot_reuse_assess",
        lambda *_a, **_k: {
            "outcome": "complete",
            "overlay_descendant_count": 9,
            "expected_descendant_count": 9,
        },
    )
    called: list[str] = []

    def _mark(*_a, **_k):
        called.append("mark")

    monkeypatch.setattr(mw, "_destination_descendant_snapshot_reuse_mark_allocation_applied_index", _mark)
    monkeypatch.setattr(mw, "_refresh_destination_item_visibility_index", lambda *_a: None)
    monkeypatch.setattr(mw, "_apply_tree_item_visual_state", lambda *_a, **_k: None)
    monkeypatch.setattr(mw, "_find_source_item_for_planned_move", lambda _m: None)
    monkeypatch.setattr(mw, "_destination_allocation_projection_resume_token", lambda _m, _s: "")
    mw._destination_descendant_apply_queue = deque()
    dm = MagicMock()
    dm.is_index_live = MagicMock(return_value=True)
    mw.destination_planning_model = dm
    parent_pl = {"is_folder": True, "planned_alloc": True, "item_path": "D\\T"}
    ix = _minimal_dest_model(parent_pl)
    ix.isValid = MagicMock(return_value=True)
    ix.column = MagicMock(return_value=0)
    move = {"source": {"is_folder": True}}
    ok = MainWindow._enqueue_destination_descendant_apply_to_model(mw, ix, move, enqueue_reason="test")
    assert ok is True
    assert len(mw._destination_descendant_apply_queue) == 0
    assert called == ["mark"]


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
