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


def test_destination_expanded_paths_for_planning_bind_unions_restore_intent():
    """Bind-time gating must see saved expanded paths before those rows exist in the view."""
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._destination_restore_session_expanded_paths_intent = {r"Root\A", r"Root\B"}

    def _collect(pk: str):
        return {r"Root\A"} if pk == "destination" else set()

    mw._collect_expanded_tree_paths = _collect  # type: ignore[method-assign]
    ep = mw._destination_expanded_paths_for_planning_bind()
    assert ep == {r"Root\A", r"Root\B"}


def test_destination_selected_path_for_planning_bind_prefers_intent_during_restore():
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = True
    mw._destination_restore_session_selected_path_intent = r"Root\Deep\Item"

    def _sel(pk: str) -> str:
        return "" if pk == "destination" else ""

    mw._collect_selected_tree_path = _sel  # type: ignore[method-assign]
    assert mw._destination_selected_path_for_planning_bind() == r"Root\Deep\Item"


def test_destination_selected_path_for_planning_bind_uses_live_when_not_restoring():
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = False
    mw._destination_restore_session_selected_path_intent = r"Root\Stale"

    def _sel(pk: str) -> str:
        return r"Root\Live" if pk == "destination" else ""

    mw._collect_selected_tree_path = _sel  # type: ignore[method-assign]
    assert mw._destination_selected_path_for_planning_bind() == r"Root\Live"


def test_destination_path_effective_expanded_true_when_expand_all_pending():
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._expand_all_pending = {"source": False, "destination": True}
    from unittest.mock import MagicMock

    tree = MagicMock()
    tree.isExpanded.return_value = False
    ix = MagicMock()
    ix.isValid.return_value = True
    assert mw._destination_path_effective_expanded_for_hydrate(tree, ix, r"Root\Any") is True


def test_destination_restore_path_under_session_expanded_intent_uses_explicit_paths():
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = True
    mw._destination_restore_session_expanded_paths_intent = {r"Root\Library\Plans"}
    child = r"Root\Library\Plans\Q1"
    assert mw._destination_restore_path_under_session_expanded_intent(child) is True


def test_destination_restore_path_under_session_expanded_intent_false_without_restore():
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw._memory_restore_in_progress = False
    mw._destination_restore_session_expanded_paths_intent = {r"Root\Library\Plans"}
    assert mw._destination_restore_path_under_session_expanded_intent(r"Root\Library\Plans\Q1") is False


def test_destination_live_refresh_still_blocked_during_incremental_merge():
    _app()
    mw = MainWindow.__new__(MainWindow)
    mw.unresolved_proposed_by_parent_path = {}
    mw.unresolved_allocations_by_parent_path = {}
    mw._memory_restore_in_progress = False
    mw._restore_destination_overlay_pending = False
    mw._destination_restore_materialization_queue = []
    mw._destination_idle_materialize_pending_reason = ""
    mw._destination_idle_materialize_timer = None
    mw._destination_incremental_merge_in_progress = True
    assert mw._destination_live_refresh_still_blocked() is True
