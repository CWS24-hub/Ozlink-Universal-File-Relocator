"""Phase 1: JSON-safe tree snapshots and deferred workspace UI persist during restore/materialize."""

from __future__ import annotations

import json

from PySide6.QtCore import QByteArray, QTimer, Qt
from PySide6.QtGui import QBrush, QColor, QFont
from PySide6.QtWidgets import QApplication, QTreeWidgetItem

from ozlink_console.main_window import MainWindow


def _qapp():
    return QApplication.instance() or QApplication([])


def test_sanitize_value_replaces_qcolor_qbrush_for_json():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._JSON_PERSIST_MAX_DEPTH = 48
    d = {
        "_model_background": QColor("#1B2942"),
        "nested": {"brush": QBrush(QColor("#FF0000"))},
        "keep": 1,
    }
    out = mw._sanitize_value_for_json_persist(d, 0)
    assert isinstance(out, dict)
    assert out["keep"] == 1
    assert isinstance(out["_model_background"], str) and out["_model_background"].startswith("#")
    assert isinstance(out["nested"]["brush"], str)
    json.dumps(out)


def test_sanitize_tree_snapshot_branch_nested_children():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    snap = {
        "text": "x",
        "data": {"a": QColor("#00FF00")},
        "expanded": False,
        "children": [{"text": "c", "data": {"b": QFont()}, "expanded": False, "children": []}],
    }
    MainWindow._sanitize_tree_snapshot_branch_for_persist(mw, snap)
    assert isinstance(snap["data"]["a"], str)
    assert isinstance(snap["children"][0]["data"]["b"], str)
    json.dumps(snap)


def test_capture_tree_items_snapshot_output_is_json_serializable():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._JSON_PERSIST_MAX_DEPTH = 48
    tree = type("T", (), {})()
    tree.topLevelItemCount = lambda: 1
    item = QTreeWidgetItem(["row"])
    item.setData(0, Qt.UserRole, {"name": "n", "_model_background": QColor("#112233")})
    tree.topLevelItem = lambda _i=0: item
    mw._source_tree_uses_model_view = lambda: False
    mw._destination_tree_uses_model_view = lambda: False
    mw._get_tree_and_status = lambda pk: (tree, None)
    mw._normalize_destination_snapshot_tree_for_persist = lambda _s: None
    out = MainWindow._capture_tree_items_snapshot(mw, "source")
    assert out
    json.dumps(out)


def test_persist_workspace_ui_deferred_when_restore_in_progress():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._workspace_ui_snapshot_dirty_panels = set()
    mw._memory_restore_in_progress = True
    mw._restore_finalization_deferred_active = False
    mw._destination_chunked_bind_state = None
    mw._destination_future_bind_sync_active = False
    mw._destination_future_projection_async_state = None
    mw._destination_snapshot_chunked_restore_active = False
    mw.pending_folder_loads = {"source": set(), "destination": set()}
    mw._root_tree_bind_in_progress = False
    mw._expand_all_pending = {"source": False, "destination": False}
    mw._lazy_destination_projection_pending_reason = ""
    starts: list[int] = []
    t = QTimer()
    t.start = lambda ms: starts.append(int(ms))
    mw._workspace_ui_persist_timer = t
    calls: list = []
    mw._save_draft_shell = lambda **kwargs: calls.append(kwargs)
    MainWindow._persist_workspace_ui_state_safely(mw)
    assert calls == []
    assert "source" in mw._workspace_ui_snapshot_dirty_panels
    assert starts and starts[0] == 2000


def test_workspace_ui_persist_timer_reschedules_when_chunked_bind_active():
    _qapp()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = False
    mw._restore_finalization_deferred_active = False
    mw._destination_chunked_bind_state = {"gen": 1}
    mw._destination_future_bind_sync_active = False
    mw._destination_future_projection_async_state = None
    mw._destination_snapshot_chunked_restore_active = False
    mw.pending_folder_loads = {"source": set(), "destination": set()}
    mw._root_tree_bind_in_progress = False
    mw._expand_all_pending = {"source": False, "destination": False}
    mw._lazy_destination_projection_pending_reason = ""
    starts: list[int] = []
    mw._workspace_ui_persist_timer = type(
        "T",
        (),
        {"start": lambda self, ms: starts.append(int(ms))},
    )()
    mw._workspace_ui_snapshot_dirty_panels = set()
    mw._refresh_runtime_tree_snapshot = lambda _p: []
    persist_calls = []
    mw._persist_workspace_ui_state_safely = lambda: persist_calls.append(1)
    MainWindow._on_workspace_ui_persist_timer(mw)
    assert starts == [2000]
    assert persist_calls == []
