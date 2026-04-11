"""Regression: preserved future-state handoff must attach under QTreeView planning model (not only widget tree)."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel, NestedSpec


def _app():
    return QApplication.instance() or QApplication([])


def test_restore_future_state_children_model_appends_missing_child_under_model_view():
    """Handoff specs must call append_nested_child when no semantic row exists (model-view path)."""
    _app()
    model = DestinationPlanningTreeModel()
    model.reset_root_payloads(
        [
            {
                "base_display_label": "Folder: Root",
                "name": "Root",
                "is_folder": True,
                "item_path": "Root",
            }
        ]
    )
    root_ix = model.index(0, 0, QModelIndex())
    assert root_ix.isValid()

    mw = MainWindow.__new__(MainWindow)
    mw.destination_planning_model = model
    mw.destination_tree_widget = None
    mw._destination_tree_uses_model_view = lambda: True  # type: ignore[method-assign]

    child_pl = {
        "base_display_label": "Folder: HandoffChild",
        "name": "HandoffChild",
        "is_folder": True,
        "item_path": r"Root\HandoffChild",
        "node_origin": "projectedDestination",
    }
    spec: NestedSpec = (child_pl, [])
    moved = mw._restore_destination_future_state_children_model(root_ix, [spec])
    assert moved == 1
    assert model.rowCount(root_ix) == 1
    ch = model.index(0, 0, root_ix)
    assert ch.isValid()
    pl = ch.data(Qt.UserRole) or {}
    assert pl.get("name") == "HandoffChild"


def test_start_destination_restore_materialization_skips_when_queue_already_populated():
    """Avoid wiping/replacing an in-flight materialization queue on duplicate start calls."""
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = []
    mw.proposed_folders = []
    mw._destination_restore_completed_once = False
    mw._destination_restore_materialization_user_paused = False
    mw._destination_restore_materialization_queue = [r"Root\Existing"]
    mw._destination_restore_materialization_seen = {r"Root\Existing"}
    rebuilt: list[int] = []

    def _log(*_a, **_k):
        return None

    def _sched(*_a, **_k):
        return None

    def _build():
        rebuilt.append(1)
        return []

    mw._log_restore_phase = _log  # type: ignore[method-assign]
    mw._schedule_destination_restore_materialization_queue = _sched  # type: ignore[method-assign]
    mw._build_destination_materialization_paths = _build  # type: ignore[method-assign]

    mw._start_destination_restore_materialization()
    assert rebuilt == []
    assert mw._destination_restore_materialization_queue == [r"Root\Existing"]


def test_find_visible_destination_item_by_path_returns_none_during_root_bind():
    """Path lookups should not treat the tree as stable while destination root bind is mutating structure."""
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._root_tree_bind_in_progress = True
    from unittest.mock import MagicMock

    tree = MagicMock()
    mw.destination_tree_widget = tree
    assert mw._find_visible_destination_item_by_path("Root") is None
