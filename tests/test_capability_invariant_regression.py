"""
Capability → invariant regression suite (P0 implemented here).

This module locks non-negotiable behaviors described in the whole-app audit so that
performance/scheduling changes cannot silently violate authority, descendant progress,
scroll/reconcile policy, or shutdown ordering.

Phase 1–2 summary (see also project docs)
----------------------------------------
Matrix columns: capability | invariant | why | evidence | existing tests | gap | proposed test

P0 (implemented below): graph authority triplet; descendant scroll defer + coalesce pairing;
preview idle blocked by descendant queue; semantic reconcile pended on scroll for overlay pass;
structure coalesce on tick path; delegate fast-path without exclusions; transfer manifest contract;
shutdown overlay skip (reference existing suite).

P1: snapshot identity parity tests; cache_refresh dedup scroll guard; graph_ids chunk coalesce;
full source merge integration.

P2: end-to-end scroll→idle injection progress; GPU/compositor gap diagnostics (non-automated).
"""

from __future__ import annotations

import time
from collections import deque
from unittest.mock import MagicMock, patch

import pytest
from PySide6.QtCore import QModelIndex, QRect, Qt
from PySide6.QtGui import QPainter
from PySide6.QtWidgets import QStyleOptionViewItem, QApplication

from ozlink_console import destination_authority_contract as dac
from ozlink_console.main_window import DestinationPlanningTreeDelegate, MainWindow
from ozlink_console.tree_models.destination_planning_model import (
    DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE,
    DestinationPlanningTreeModel,
)
from ozlink_console.transfer_job_runner import SUPPORTED_MANIFEST_VERSIONS


# --- Phase 1: capability / invariant registry (documentation + pytest markers) ---

CAPABILITY_INVARIANTS = [
    {
        "capability": "Destination Graph authority",
        "invariant": "When SharePoint planning tree is active, full_tree_snapshot_may_author_visible_real_rows is False",
        "test": "test_invariant_full_tree_snapshot_cannot_author_visible_rows_when_graph_owns_structure",
    },
    {
        "capability": "Destination Graph authority (local mode)",
        "invariant": "Local destination planning may insert visible real rows on future model bind",
        "test": "test_invariant_future_bind_may_insert_when_local_destination",
    },
    {
        "capability": "Descendant apply (graph walk)",
        "invariant": "While destination scroll is active, graph_walk tick defers and reschedules without clearing state",
        "test": "test_invariant_graph_descendant_tick_defers_while_scroll_active_and_preserves_state",
    },
    {
        "capability": "Descendant apply (structure batching)",
        "invariant": "Tick body wraps model structure-signal coalesce begin/end when model present",
        "test": "test_invariant_descendant_tick_body_closes_structure_coalesce_on_empty_queue",
    },
    {
        "capability": "Preview pipeline",
        "invariant": "Authoritative preview idle is False while descendant queue or state active",
        "test": "test_invariant_preview_pipeline_blocked_while_descendant_queue_nonempty",
    },
    {
        "capability": "Reconcile / scroll",
        "invariant": "destination_planning_overlay_pass reconcile is pended after scroll, not invoked inline",
        "test": "test_invariant_semantic_reconcile_maybe_deferred_pends_overlay_pass_during_scroll",
    },
    {
        "capability": "Delegate / exclusion",
        "invariant": "Empty plan-leaf exclusions: delegate paint uses super() path without querying exclusion paint role",
        "test": "test_invariant_delegate_paint_skips_exclusion_probe_when_plan_leaf_exclusions_empty",
    },
    {
        "capability": "Execution / transfer",
        "invariant": "Transfer manifest versions supported set is stable and exported",
        "test": "test_invariant_transfer_manifest_supported_versions_frozen",
    },
    {
        "capability": "Worker lifecycle / shutdown",
        "invariant": "During shutdown, descendant ticks are skipped unless snapshot capture drain is active; overlays still skipped",
        "test": "test_invariant_shutdown_skip_descendant_tick_unless_snapshot_drain_active",
    },
]


class _Host:
    def __init__(self, *, dest_mode: str = "local", has_model: bool = True) -> None:
        self._dest_mode = dest_mode
        self.destination_planning_model = object() if has_model else None

    def _planning_browse_mode(self, key: str) -> str:
        return self._dest_mode if key == "destination" else "local"


# --- Graph authority (extends tests/test_destination_authority_contract.py) ---


def test_invariant_full_tree_snapshot_cannot_author_visible_rows_when_graph_owns_structure():
    """Regression: snapshot enumeration must not author visible real rows in Graph-owned mode."""
    h = _Host(dest_mode="sharepoint", has_model=True)
    assert dac.graph_owns_visible_real_destination_structure(h) is True
    assert dac.full_tree_snapshot_may_author_visible_real_rows(h) is False


def test_invariant_future_bind_may_insert_when_local_destination():
    h = _Host(dest_mode="local", has_model=True)
    assert dac.future_model_bind_may_insert_visible_real_rows(h) is True


# --- Descendant apply + structure coalesce ---


def test_invariant_graph_descendant_tick_defers_while_scroll_active_and_preserves_state():
    """Graph-walk descendant work must not run during active scroll; state preserved; follow-up scheduled."""
    mw = MainWindow.__new__(MainWindow)
    st = {"graph_walk": True, "walk_phase": "walk"}
    mw._destination_descendant_apply_state = st
    mw._destination_descendant_apply_queue = deque()
    scheduled: list[str] = []

    def _sched():
        scheduled.append("tick")

    mw._schedule_destination_descendant_apply_tick = _sched
    with patch.object(mw, "_destination_user_scroll_interaction_active", return_value=True):
        with patch("ozlink_console.main_window.log_info"):
            MainWindow._run_destination_descendant_apply_tick_body(mw)
    assert mw._destination_descendant_apply_state is st
    assert scheduled == ["tick"]


def test_invariant_descendant_tick_body_closes_structure_coalesce_on_empty_queue():
    """Even an empty-queue tick must pair begin/end coalesce when model exists (signal batching invariant)."""
    mw = MainWindow.__new__(MainWindow)
    mw._destination_descendant_apply_state = None
    mw._destination_descendant_apply_queue = deque()
    dm = MagicMock()
    dm.begin_coalesce_destination_structure_signal = MagicMock()
    dm.end_coalesce_destination_structure_signal = MagicMock()
    mw.destination_planning_model = dm
    with patch.object(mw, "_destination_user_scroll_interaction_active", return_value=False):
        with patch("ozlink_console.main_window.time.perf_counter", return_value=0.0):
            MainWindow._run_destination_descendant_apply_tick_body(mw)
    dm.begin_coalesce_destination_structure_signal.assert_called_once()
    dm.end_coalesce_destination_structure_signal.assert_called_once()


# --- Preview pipeline ---


def test_invariant_preview_pipeline_blocked_while_descendant_queue_nonempty():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_sharepoint_planning_destination_active = lambda: True  # type: ignore[method-assign]
    mw._destination_tree_shows_authority_pending_shell = lambda: False  # type: ignore[method-assign]
    mw._destination_full_tree_ready = lambda: True  # type: ignore[method-assign]
    mw.pending_folder_loads = {"destination": set(), "source": set()}
    mw._destination_incremental_merge_in_progress = False
    mw._destination_incremental_merge_session = None
    mw._destination_future_projection_async_state = None
    mw._destination_descendant_apply_paused_for_finalize_alloc = False
    mw._destination_descendant_apply_queue = deque([1])
    mw._destination_descendant_apply_state = None
    mw._current_selected_destination_drive_id = lambda: "d"  # type: ignore[method-assign]
    mw.pending_root_drive_ids = {"destination": "d"}
    mw._destination_snapshot_spo_trust_valid = lambda _d: True  # type: ignore[method-assign]
    mw._destination_snapshot_light_validation_worker = None
    assert MainWindow._destination_authoritative_preview_pipeline_idle(mw) is False

    mw._destination_descendant_apply_queue = deque()
    mw._destination_descendant_apply_state = {"k": 1}
    assert MainWindow._destination_authoritative_preview_pipeline_idle(mw) is False


# --- Reconcile + scroll ---


def test_invariant_semantic_reconcile_maybe_deferred_pends_overlay_pass_during_scroll():
    """Heavy reconcile for overlay pass must not run synchronously while user is scrolling."""
    mw = MainWindow.__new__(MainWindow)
    mw._destination_semantic_reconcile_guard_depth = 0
    mw._destination_reconcile_pended_after_scroll = None
    mw._destination_deferred_reconcile_burst_pending = False

    with patch.object(MainWindow, "_destination_sharepoint_planning_destination_active", return_value=True), patch.object(
        MainWindow, "_destination_full_tree_ready", return_value=True
    ), patch.object(MainWindow, "_should_defer_destination_duplicate_reconcile", return_value=False), patch.object(
        MainWindow, "_reconcile_destination_semantic_duplicates"
    ) as direct, patch.object(
        MainWindow, "_destination_user_scroll_interaction_active", return_value=True
    ), patch.object(
        MainWindow, "_destination_note_destination_tree_scroll_activity"
    ) as note_scroll, patch(
        "ozlink_console.main_window.log_info"
    ):
        out = MainWindow._reconcile_destination_semantic_duplicates_maybe_deferred(
            mw, "destination_planning_overlay_pass"
        )
    assert out == 0
    direct.assert_not_called()
    assert mw._destination_reconcile_pended_after_scroll is not None
    assert mw._destination_reconcile_pended_after_scroll[0] == "destination_planning_overlay_pass"
    note_scroll.assert_called_once()


# --- Delegate column-0 / exclusions ---


@pytest.fixture(scope="module")
def _qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_invariant_delegate_paint_skips_exclusion_probe_when_plan_leaf_exclusions_empty(_qapp):
    """When no exclusions configured, delegate must not query DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE."""

    class _CountingDestinationModel(DestinationPlanningTreeModel):
        def __init__(self) -> None:
            super().__init__()
            self.roles_seen: list[int] = []

        def data(self, index, role: int = Qt.DisplayRole):
            self.roles_seen.append(int(role))
            return super().data(index, role)

    win = MagicMock()
    win._plan_leaf_exclusions = None
    win._dest_scroll_profiler = None
    win.get_source_item_display_name = lambda *_a, **_k: "n"
    win._plan_leaf_exclusion_display_active = MagicMock()
    delegate = DestinationPlanningTreeDelegate(win)
    model = _CountingDestinationModel()
    model.reset_root_payloads(
        [
            {
                "name": "n",
                "base_display_label": "n",
                "is_folder": True,
                "item_path": "Root\\n",
                "destination_path": "Root\\n",
            }
        ]
    )
    ix = model.index(0, 0, QModelIndex())
    opt = QStyleOptionViewItem()
    opt.rect = QRect(0, 0, 120, 20)
    painter = QPainter()

    with patch("ozlink_console.main_window.QStyledItemDelegate.paint") as super_paint:
        delegate.paint(painter, opt, ix)

    assert DESTINATION_PLAN_LEAF_EXCLUSION_PAINT_ROLE not in set(model.roles_seen)
    super_paint.assert_called_once()


# --- Transfer / execution contract ---


def test_invariant_transfer_manifest_supported_versions_frozen():
    """Manifest version set is part of execution contract; changes must be intentional."""
    assert 1 in SUPPORTED_MANIFEST_VERSIONS
    assert 2 in SUPPORTED_MANIFEST_VERSIONS
    assert SUPPORTED_MANIFEST_VERSIONS == frozenset({1, 2})


# --- Shutdown / snapshot drain (same contract as test_destination_shutdown_capture_drain) ---


def test_invariant_shutdown_skip_descendant_tick_unless_snapshot_drain_active():
    """During shutdown, descendant ticks are skipped unless snapshot capture drain is active (allows persist)."""
    w = MainWindow.__new__(MainWindow)
    w._application_shutting_down = True
    w._destination_snapshot_capture_drain_active = False
    with patch("ozlink_console.main_window.log_info"):
        assert MainWindow._if_shutdown_skip_mutation(w, "_run_destination_descendant_apply_tick") is True
        w._destination_snapshot_capture_drain_active = True
        assert MainWindow._if_shutdown_skip_mutation(w, "_run_destination_descendant_apply_tick") is False
        assert MainWindow._if_shutdown_skip_mutation(w, "_apply_destination_planning_overlays") is True


def test_phase2_ranking_documented():
    """Anchor test: matrix and P0/P1 list live in module docstring + CAPABILITY_INVARIANTS."""
    assert len(CAPABILITY_INVARIANTS) >= 9
    names = {row["test"] for row in CAPABILITY_INVARIANTS}
    assert "test_invariant_full_tree_snapshot_cannot_author_visible_rows_when_graph_owns_structure" in names
