"""Coalesced overlay invariant: many source-driven schedules → one enforcement pass per burst."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow


def _app():
    return QApplication.instance() or QApplication([])


@pytest.fixture
def deferred_single_shot(monkeypatch):
    """Queue QTimer.singleShot(0, cb) and drain in order (matches event-loop coalescing)."""

    pending: list = []

    def _queue(_ms, callback):
        pending.append(callback)

    def _drain():
        for cb in pending:
            cb()
        pending.clear()

    monkeypatch.setattr(QTimer, "singleShot", staticmethod(lambda ms, cb: _queue(ms, cb)))
    yield _drain
    pending.clear()


def test_coalesce_many_schedules_one_enforce(monkeypatch, deferred_single_shot):
    _app()
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    mw = MainWindow.__new__(MainWindow)
    mw._main_window_qobject_ready = True
    mw._disable_overlay_invariant_timer_for_test = False

    calls: list[str] = []

    def _track(reason: str = "", payload=None):
        calls.append(str(reason or ""))

    mw._on_destination_state_mutation = _track

    for i in range(8):
        MainWindow._schedule_coalesced_overlay_invariant(mw, f"src:{i}", source_driven=True)

    deferred_single_shot()

    assert len(calls) == 1
    assert "src:7" in calls[0]
    assert int(getattr(mw, "_overlay_invariant_coalesce_superseded_flushes", 0) or 0) == 7


def test_coalesce_disabled_runs_immediate_each_time(monkeypatch):
    _app()
    monkeypatch.setattr(
        "ozlink_console.destination_authority_contract.graph_owns_visible_real_destination_structure",
        lambda _h: True,
    )
    mw = MainWindow.__new__(MainWindow)
    mw._main_window_qobject_ready = True
    mw._disable_overlay_invariant_timer_for_test = True
    calls: list[int] = []

    def _track(reason: str = "", payload=None):
        calls.append(1)

    mw._on_destination_state_mutation = _track
    MainWindow._schedule_coalesced_overlay_invariant(mw, "a", source_driven=True)
    MainWindow._schedule_coalesced_overlay_invariant(mw, "b", source_driven=True)
    assert len(calls) == 2
