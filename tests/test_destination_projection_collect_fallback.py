"""Regression: projection collection must not silently return empty when Graph can enumerate."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from unittest.mock import MagicMock

from PySide6.QtCore import QModelIndex
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel


def _app():
    return QApplication.instance() or QApplication([])


def test_destination_real_snapshot_direct_root_child_detection():
    mw = MainWindow.__new__(MainWindow)
    assert mw._destination_real_snapshot_node_is_direct_root_child(
        {"semantic_path": "Root\\IT", "parent_semantic_path": "Root"}
    )
    assert mw._destination_real_snapshot_node_is_direct_root_child(
        {"semantic_path": "Libraries", "parent_semantic_path": ""}
    )
    assert not mw._destination_real_snapshot_node_is_direct_root_child(
        {"semantic_path": "Root\\IT\\Sub", "parent_semantic_path": "Root\\IT"}
    )


def test_collect_source_descendants_graph_fallback_when_partial_subtree_empty():
    """Defer Graph while source row exists but subtree is partial; empty walk + ids => Graph."""
    mw = MainWindow.__new__(MainWindow)
    src_item = MagicMock()
    src_item.childCount = MagicMock(return_value=0)
    graph_rows = [{"name": "Nested", "is_folder": True, "id": "n1"}]

    g = MagicMock()
    g.list_drive_subtree_items_normalized = MagicMock(return_value=graph_rows)
    mw.graph = g

    mw._enrich_source_root_for_projection_graph_lookup = lambda d, _m: d  # type: ignore[method-assign]
    mw._canonical_source_projection_path = lambda p: str(p or "").strip()  # type: ignore[method-assign]
    mw._tree_item_path = lambda d: str((d or {}).get("item_path", "") or "").strip()  # type: ignore[method-assign]
    mw._find_source_item_for_planned_move = lambda _m: src_item  # type: ignore[method-assign]
    mw._source_subtree_fully_loaded_in_tree = lambda _item: False  # type: ignore[method-assign]
    mw._iter_source_tree_subtree_rows = lambda item: [item]  # type: ignore[method-assign]
    mw._source_tree_row_payload = lambda _item: {"placeholder": False, "children_loaded": False}  # type: ignore[method-assign]
    mw._log_restore_phase = lambda *_a, **_k: None  # type: ignore[method-assign]
    mw._log_restore_exception = lambda *_a, **_k: None  # type: ignore[method-assign]

    logged: list[dict] = []

    def _log_collect(descendants, *, branch, graph_attempted, **kwargs):
        logged.append({"branch": branch, "graph_attempted": graph_attempted, "n": len(descendants or [])})

    mw._log_destination_projection_collect_result = _log_collect  # type: ignore[method-assign]

    root = {
        "drive_id": "drive-1",
        "id": "item-root",
        "item_path": "/",
        "is_folder": True,
    }
    move = {"source": root, "source_path": "/alloc"}
    out = mw._collect_source_descendants_for_projection(root, move=move)

    assert out == graph_rows
    g.list_drive_subtree_items_normalized.assert_called_once()
    assert any(e.get("branch") == "graph_fallback_empty_partial_walk" for e in logged)
    assert any(e.get("graph_attempted") is True for e in logged)


def test_ensure_visible_root_children_includes_non_planning_relevant_siblings():
    """Narrow snapshot must still copy every real Root child from the planning model into the overlay."""
    _app()
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
                "destination_path": "Root",
            },
        ]
    )
    root_ix = model.index(0, 0, QModelIndex())
    model.replace_all_children(
        root_ix,
        [
            {
                "base_display_label": "Folder: IT",
                "name": "IT",
                "is_folder": True,
                "item_path": r"Root\IT",
                "destination_path": r"Root\IT",
            },
        ],
    )
    mw = MainWindow.__new__(MainWindow)
    mw._destination_browse_mode = "local"
    mw.proposed_folders = []
    mw.planned_moves = []
    mw.destination_planning_model = model
    model_nodes: dict = {}
    # Only paths unrelated to IT — IT must still be preserved as a direct Root child.
    narrow = {"Root", r"Root\Finance"}
    n = mw._ensure_visible_destination_root_children_in_model(
        model_nodes, planning_relevant_paths=narrow
    )
    assert n == 1
    assert r"Root\IT" in model_nodes


def test_real_snapshot_refresh_merges_entire_full_tree_when_ready():
    """Completed Graph full-tree walk for the drive merges all live paths into the real snapshot."""
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw.destination_tree_widget = object()
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
                "destination_path": "Root",
            },
        ]
    )
    mw.destination_planning_model = model
    mw._destination_real_tree_snapshot_stale = True
    mw.pending_root_drive_ids = {"source": "", "destination": "d1"}
    mw._current_selected_destination_drive_id = lambda: "d1"  # type: ignore[method-assign]
    mw._destination_full_tree_completed_drive_id = "d1"
    mw._destination_full_tree_snapshot = [
        {
            "semantic_path": "Root\\Marketing",
            "parent_semantic_path": "Root",
            "data": {"name": "Marketing"},
        },
        {
            "semantic_path": "Root\\Marketing\\Deep",
            "parent_semantic_path": "Root\\Marketing",
            "data": {"name": "Deep"},
        },
    ]
    mw._log_restore_phase = lambda *_a, **_k: None  # type: ignore[method-assign]
    mw._refresh_destination_real_tree_snapshot(force=True)
    paths = {e["semantic_path"] for e in mw._destination_real_tree_snapshot}
    assert "Root\\Marketing" in paths
    assert "Root\\Marketing\\Deep" in paths


def test_lazy_deferred_files_summary_does_not_add_visible_model_node():
    mw = MainWindow.__new__(MainWindow)
    model_nodes = {
        "Root": {"semantic_path": "Root", "node_state": "real", "data": {}, "children": []},
        "Root\\Alloc": {
            "semantic_path": "Root\\Alloc",
            "node_state": "allocated",
            "data": {},
            "children": [],
        },
    }
    chunk = {
        "defer_files_mode": True,
        "deferred_file_count": 3570,
        "allocation_destination_path": "Root\\Alloc",
        "allocation_node": model_nodes["Root\\Alloc"],
    }
    MainWindow._destination_add_lazy_files_summary_to_future_model(mw, chunk, model_nodes)
    assert not any(
        str(k).endswith(".ozlink_deferred_files_summary") for k in model_nodes
    )
    assert len(model_nodes) == 2


def test_try_deferred_expand_all_waits_until_incremental_merge_finishes():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_expand_all_start_pending = True
    mw._destination_incremental_merge_in_progress = True
    mw._destination_future_projection_async_state = None
    mw._destination_chunked_bind_state = None
    mw._destination_future_bind_sync_active = False
    mw._destination_incremental_merge_session = None
    ran: list[str] = []

    def _no_timer(_ms, _cb):
        return None

    mw._fast_expand_all_loaded_tree = lambda _pk: ran.append("fast")  # type: ignore[method-assign]
    mw._begin_destination_model_expand_all = lambda: ran.append("begin")  # type: ignore[method-assign]
    mw._can_fast_bulk_expand = lambda _pk: True  # type: ignore[method-assign]
    mw._safe_invoke = lambda _name, fn: fn()  # type: ignore[method-assign]

    from unittest.mock import patch

    with patch("ozlink_console.main_window.QTimer.singleShot", _no_timer):
        MainWindow._try_deferred_destination_expand_all_start(mw)
    assert mw._destination_expand_all_start_pending is True
    assert ran == []


def test_try_deferred_expand_all_runs_fast_expand_when_idle():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_expand_all_start_pending = True
    mw._destination_incremental_merge_in_progress = False
    mw._destination_future_projection_async_state = None
    mw._destination_chunked_bind_state = None
    mw._destination_future_bind_sync_active = False
    mw._destination_incremental_merge_session = None
    mw._log_restore_phase = lambda *_a, **_k: None  # type: ignore[method-assign]
    ran: list[str] = []
    mw._fast_expand_all_loaded_tree = lambda _pk: ran.append("fast")  # type: ignore[method-assign]
    mw._begin_destination_model_expand_all = lambda: ran.append("begin")  # type: ignore[method-assign]
    mw._can_fast_bulk_expand = lambda _pk: True  # type: ignore[method-assign]
    mw._safe_invoke = lambda _name, fn: fn()  # type: ignore[method-assign]
    MainWindow._try_deferred_destination_expand_all_start(mw)
    assert mw._destination_expand_all_start_pending is False
    assert ran == ["fast"]
