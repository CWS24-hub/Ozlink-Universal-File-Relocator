"""Tests for destination UX performance pass: async graph projection, bind phasing, merge ordering."""

from collections import OrderedDict
from unittest.mock import MagicMock, patch

from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow, _PROJECTION_COLLECT_ASYNC_PENDING


def _qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_collect_source_descendants_graph_async_returns_pending_and_starts_worker():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.graph = MagicMock()
    mw._planned_moves = []
    mw._graph_subtree_projection_workers = {}
    mw._graph_subtree_projection_pending_keys = set()
    mw._graph_subtree_projection_serial_by_key = {}
    mw._force_sync_graph_subtree_projection = False
    mw._find_source_item_for_planned_move = MagicMock(return_value=object())
    mw._source_subtree_fully_loaded_in_tree = MagicMock(return_value=False)
    mw._enrich_source_root_for_projection_graph_lookup = lambda d, m: dict(d or {})
    mw._destination_projection_diag_payload = lambda s, m, si: {}
    mw._log_destination_projection_collect_result = MagicMock()
    mw._log_restore_phase = MagicMock()
    mw._log_restore_exception = MagicMock()
    mw._safe_invoke = lambda _name, fn, *a, **k: fn(*a, **k)

    root = {"drive_id": "drv1", "id": "id1", "item_path": "/root", "is_folder": True}
    move = {"source": root, "source_path": "Root\\A"}

    with patch.object(mw, "_schedule_graph_subtree_projection_worker") as sched:
        out = mw._collect_source_descendants_for_projection(root, move)
    assert out is _PROJECTION_COLLECT_ASYNC_PENDING
    sched.assert_called_once()


def test_graph_subtree_worker_success_ignores_stale_serial():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = []
    mw._graph_subtree_projection_serial_by_key = {("sig", "d", "i", "p"): 2}
    mw._graph_subtree_projection_pending_keys = {("sig", "d", "i", "p")}
    mw._source_projection_descendants_cache = OrderedDict()
    mw._source_projection_descendants_cache_limit = 96
    mw._log_restore_phase = MagicMock()
    mw._log_destination_projection_collect_result = MagicMock()
    mw._schedule_coalesced_materialize_after_graph_subtree = MagicMock()
    mw._resume_destination_future_projection_after_graph_subtree_if_needed = MagicMock()
    planned_sig = MainWindow._projection_descendants_planned_signature_digest(mw)
    payload = {
        "context": {
            "cache_key": ("sig", "d", "i", "p"),
            "serial": 1,
            "planned_sig": planned_sig,
            "defer_graph_until_subtree_ready": True,
            "source_root_data": {},
            "move_for_fallback": {"source": {}, "source_path": ""},
        },
        "descendants": [{"name": "x"}],
    }
    mw._on_source_subtree_projection_worker_success(payload)
    mw._schedule_coalesced_materialize_after_graph_subtree.assert_not_called()
    assert not mw._source_projection_descendants_cache


def test_sort_incremental_merge_roots_visible_first():
    _qapp()
    mw = MainWindow.__new__(MainWindow)

    def _prio(p, exp, sel):
        ps = str(p).lower()
        return 3 if "vis" in ps else 0

    mw._destination_visible_subtree_merge_priority = _prio
    mw._path_segments = lambda p: [s for s in str(p).split("\\") if s]
    out = mw._sort_incremental_merge_roots_visible_first(
        ["Root\\later\\a", "Root\\vis\\b"],
        {"Root\\vis"},
        "",
    )
    assert out[0] == "Root\\vis\\b"


def test_start_allocation_descendant_projection_chunk_returns_pending_marker():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = []
    mw._graph_subtree_projection_workers = {}
    mw._graph_subtree_projection_pending_keys = set()
    mw._graph_subtree_projection_serial_by_key = {}
    mw._force_sync_graph_subtree_projection = False
    mw._find_source_item_for_planned_move = MagicMock(return_value=None)
    mw._canonical_destination_projection_path = lambda p: p or ""
    mw._allocation_projection_path = lambda m: "Root\\Alloc"
    mw._canonical_source_projection_path = lambda p: p or ""
    mw._tree_item_path = lambda d: d.get("item_path", "")
    mw._log_restore_phase = MagicMock()

    move = {
        "source": {"drive_id": "d", "id": "i", "item_path": "/x", "is_folder": True},
        "source_path": "Root\\S",
    }
    model_nodes = {
        "Root\\Alloc": {"data": {}},
    }
    with patch.object(mw, "_collect_source_descendants_for_projection", return_value=_PROJECTION_COLLECT_ASYNC_PENDING):
        chunk = mw._start_allocation_descendant_projection_chunk(move, "Root\\Alloc", model_nodes)
    assert chunk == {"__async_graph_subtree_pending__": True}
