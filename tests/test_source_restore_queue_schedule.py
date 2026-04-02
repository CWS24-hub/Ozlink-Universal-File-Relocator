"""Source restore materialization queue must reschedule on folder_load during restore."""

from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("PyQt6")

import ozlink_console.main_window as main_window_mod
from ozlink_console.main_window import MainWindow


def test_schedule_source_restore_folder_load_schedules_timer_during_restore():
    """Regression: blanket ``folder_load`` early-return left the queue stuck at high queue_size."""
    mw = MainWindow.__new__(MainWindow)
    mw._expand_all_pending = {"source": False, "destination": False}
    mw._memory_restore_in_progress = True
    mw._memory_restore_complete = False
    mw._restore_queue_tick_delay_ms = 99

    calls: list[tuple] = []

    def capture_single_shot(delay, callback):
        calls.append((delay, callback))

    with patch.object(main_window_mod.QTimer, "singleShot", side_effect=capture_single_shot):
        MainWindow._schedule_source_restore_materialization_queue(mw, "folder_load", trigger_path=r"Root\Lib\X")

    assert len(calls) == 1
    assert calls[0][0] == 0


def test_schedule_source_restore_root_bind_uses_tick_delay():
    mw = MainWindow.__new__(MainWindow)
    mw._expand_all_pending = {"source": False, "destination": False}
    mw._memory_restore_in_progress = True
    mw._memory_restore_complete = False
    mw._restore_queue_tick_delay_ms = 47

    calls: list[tuple] = []

    with patch.object(main_window_mod.QTimer, "singleShot", side_effect=lambda d, cb: calls.append((d, cb))):
        MainWindow._schedule_source_restore_materialization_queue(mw, "root_bind", trigger_path="")

    assert len(calls) == 1
    assert calls[0][0] == 47


def test_schedule_source_restore_folder_load_skips_after_memory_restore_complete():
    mw = MainWindow.__new__(MainWindow)
    mw._expand_all_pending = {"source": False, "destination": False}
    mw._memory_restore_in_progress = False
    mw._memory_restore_complete = True

    with patch.object(main_window_mod.QTimer, "singleShot") as m:
        MainWindow._schedule_source_restore_materialization_queue(mw, "folder_load", trigger_path="")
    m.assert_not_called()
