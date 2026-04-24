"""First slice: hybrid destination preview (browse-first startup + badges)."""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest
from PySide6.QtCore import QCoreApplication, QModelIndex, Qt
from PySide6.QtWidgets import QApplication

from ozlink_console.hybrid_destination_preview import (
    destination_hybrid_preview_badge_text,
    hybrid_destination_preview_browse_first_enabled,
)
from ozlink_console.main_window import MainWindow
from ozlink_console.sharepoint_destination_overlay_attach import (
    WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
    WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
)
from ozlink_console.tree_models.destination_planning_model import DestinationPlanningTreeModel

from test_sharepoint_destination_provisional_startup import _provisional_startup_test_mw_with_snapshot


def test_hybrid_badge_live_planned_from_graph_vs_planned() -> None:
    pl = {"graph_vs_planned": "live_planned", "workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED}
    assert destination_hybrid_preview_badge_text(pl) == "[Live + Planned]"


def test_hybrid_badge_not_refreshed_cached_provisional() -> None:
    pl = {"workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL}
    assert destination_hybrid_preview_badge_text(pl) == "[Not refreshed]"


def test_hybrid_badge_live_confirmed() -> None:
    pl = {"workspace_row_state": WORKSPACE_ROW_STATE_LIVE_CONFIRMED, "row_kind": "live_folder"}
    assert destination_hybrid_preview_badge_text(pl) == "[Live]"


def test_hybrid_browse_first_skips_provisional_phase2_expand_hydrate(monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ.pop("OZLINK_PROVISIONAL_DESTINATION_STARTUP", None)
    monkeypatch.setenv("OZLINK_HYBRID_DESTINATION_PREVIEW", "1")
    app = QApplication.instance() or QApplication([])
    mw, _dm = _provisional_startup_test_mw_with_snapshot()
    mw._destination_on_startup_replay_guard_idle_ready = lambda *a, **k: None

    restore_m = MagicMock()
    hydrate_m = MagicMock()
    branch_m = MagicMock()
    prune_m = MagicMock()
    mw._restore_expanded_destination_paths = restore_m
    mw._hydrate_destination_allocations_for_expanded_paths_model = hydrate_m
    mw._schedule_snapshot_branch_refresh = branch_m
    mw._destination_prune_pending_snapshot_branch_refresh_after_provisional_mount = prune_m
    mw._snapshot_refresh_targets_from_snapshot = MagicMock(return_value={"p1"})

    assert MainWindow._destination_apply_provisional_session_snapshot_if_eligible(mw, phase="test") is True
    assert getattr(mw, "_destination_startup_ui_phase", "") == "cached_only"

    MainWindow._destination_maybe_begin_provisional_startup_hydration(mw, reason="explicit_hybrid_test")
    assert getattr(mw, "_destination_startup_ui_phase", "") == "restore_minimal_complete"
    QCoreApplication.processEvents()
    assert getattr(mw, "_destination_startup_ui_phase", "") == "inactive"

    restore_m.assert_not_called()
    hydrate_m.assert_not_called()
    branch_m.assert_not_called()
    prune_m.assert_not_called()
    _ = app


def test_destination_name_column_shows_badge_when_hybrid_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OZLINK_HYBRID_DESTINATION_PREVIEW", "1")
    assert hybrid_destination_preview_browse_first_enabled() is True
    m = DestinationPlanningTreeModel(
        destination_index_key_fn=lambda pl: str((pl or {}).get("name") or "")
    )
    m.append_child_payloads(
        QModelIndex(),
        [
            {
                "name": "R",
                "base_display_label": "R",
                "tree_role": "destination",
                "workspace_row_state": WORKSPACE_ROW_STATE_CACHED_PROVISIONAL,
                "is_folder": True,
            }
        ],
    )
    ix = m.index(0, 0, QModelIndex())
    s = str(ix.data(Qt.DisplayRole) or "")
    assert "[Not refreshed]" in s
    assert "R" in s
