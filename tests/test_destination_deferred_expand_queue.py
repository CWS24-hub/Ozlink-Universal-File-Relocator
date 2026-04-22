"""Deferred destination user-expand queue: scheduling, re-entry guard, and no QTimer(0) storm."""

from __future__ import annotations

from collections import deque
from unittest.mock import MagicMock

import pytest
from PySide6.QtCore import QTimer

from ozlink_console.main_window import MainWindow


def _fresh_mw():
    mw = MainWindow.__new__(MainWindow)
    mw._destination_expand_user_deferred_queue = deque()
    mw._destination_expand_user_deferred_seen = set()
    mw._destination_expand_user_deferred_scheduled = False
    mw._destination_expand_deferred_block_count = 0
    mw._destination_expand_deferred_path_retries = {}
    mw._destination_expand_deferred_parked_paths = set()
    mw._destination_expand_deferred_drain_running = False
    mw._destination_expand_deferred_drain_reentry_pended = False
    mw._destination_expand_deferred_no_progress_streak = 0
    mw._destination_expand_deferred_consecutive_block_logs = 0
    mw._destination_expand_deferred_last_block_log_mono = 0.0
    mw._destination_expand_deferred_cumulative_dropped = 0
    mw._destination_expand_deferred_cumulative_parked = 0
    mw._destination_expand_deferred_cumulative_summary_drains = 0
    mw._destination_expand_deferred_duplicate_zero_suppressed = 0
    mw._destination_expand_deferred_no_progress_log_streak = 0
    return mw


def test_schedule_duplicate_zero_delay_suppressed_when_timer_pending(monkeypatch):
    """100x schedule(0) while a drain is already scheduled must not stack timers."""
    shot: list[tuple[int, object]] = []

    def _shot(ms, fn):
        shot.append((int(ms), fn))

    monkeypatch.setattr(QTimer, "singleShot", _shot)
    mw = _fresh_mw()
    mw._destination_expand_user_deferred_scheduled = True
    for _ in range(100):
        mw._schedule_destination_expand_user_deferred_drain(delay_ms=0)
    assert len(shot) == 0
    assert int(getattr(mw, "_destination_expand_deferred_duplicate_zero_suppressed", 0) or 0) >= 90


def test_drain_reentry_suppresses_second_drain_and_schedules_coalesce(monkeypatch):
    shot: list[tuple[int, object]] = []

    def _shot(ms, fn):
        shot.append((int(ms), fn))

    monkeypatch.setattr(QTimer, "singleShot", _shot)
    mw = _fresh_mw()
    mw._destination_expand_deferred_drain_running = True
    mw._destination_expand_user_deferred_queue.append("p1")
    mw._drain_destination_expand_user_deferred_queue()
    assert len(shot) == 1
    assert shot[0][0] == int(MainWindow._DEFERRED_EXPAND_REENTRY_COALESCE_MS)


def test_drain_flags_clear_in_finally(monkeypatch):
    """After a no-op drain (empty queue), running guard is clear."""
    monkeypatch.setattr(QTimer, "singleShot", lambda ms, fn: None)
    mw = _fresh_mw()
    mw._drain_destination_expand_user_deferred_queue()
    assert mw._destination_expand_deferred_drain_running is False
    assert mw._destination_expand_user_deferred_scheduled is False


def test_nonempty_queue_uses_nonzero_tail_delay_not_zero_default(monkeypatch):
    """Follow-up schedule after processing one item must use TAIL delay (not 0)."""
    shot: list[int] = []

    def _shot(ms, fn):
        shot.append(int(ms))

    monkeypatch.setattr(QTimer, "singleShot", _shot)
    mw = _fresh_mw()
    mw.destination_tree_widget = MagicMock()
    mw.destination_planning_model = MagicMock()
    mw.destination_planning_model.find_indices_for_canonical_destination_path = MagicMock(
        return_value=[MagicMock(isValid=MagicMock(return_value=True))]
    )
    mw.destination_planning_model.is_index_live = MagicMock(return_value=True)
    mw._on_destination_planning_model_expanded = MagicMock()
    mw._planning_browse_mode = MagicMock(return_value="local")
    mw._destination_deferred_expand_queue_block_reason = MagicMock(return_value=None)
    mw._destination_full_tree_ready = MagicMock(return_value=True)
    mw._destination_deferred_expand_destination_root_ready = MagicMock(return_value=True)
    mw._destination_sharepoint_planning_destination_active = MagicMock(return_value=False)
    tw = mw.destination_tree_widget
    tw.isExpanded = MagicMock(return_value=False)
    tw.expand = MagicMock()
    mw._destination_expand_user_deferred_queue.append("Root3\\A")
    mw._destination_expand_user_deferred_queue.append("Root3\\B")
    mw._destination_expand_user_deferred_seen.add("Root3\\A")
    mw._destination_expand_user_deferred_seen.add("Root3\\B")
    mw._drain_destination_expand_user_deferred_queue()
    assert any(ms == int(MainWindow._DEFERRED_EXPAND_TAIL_DELAY_MS) for ms in shot), shot
