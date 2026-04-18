from __future__ import annotations

import time

import pytest

from PySide6.QtCore import QThread
from PySide6.QtWidgets import QApplication

from ozlink_console.main_window import MainWindow


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_quiesce_joins_running_destination_root_worker(qapp):
    class SlowRoot(QThread):
        def run(self):
            time.sleep(0.06)

    w = SlowRoot()
    w.start()
    mw = MainWindow.__new__(MainWindow)
    mw.root_load_workers = {"destination": {"id": "r1", "worker": w, "panel_key": "destination", "stale": False}}
    mw.root_load_retired_workers = {}
    mw.folder_load_workers = {}
    mw.folder_load_retired_workers = {}
    MainWindow._quiesce_destination_workers_for_rebind(
        mw, reason="test", incoming_signature={"panel_key": "destination"}
    )
    assert not w.isRunning()


def test_on_root_load_success_skips_stale_worker_id():
    payload = {"panel_key": "destination", "drive_id": "d1", "items": []}
    host = type("Host", (), {})()
    host._restore_abort_active = lambda: False
    host.root_load_workers = {"destination": {"id": "new", "worker": None}}
    host.pending_root_drive_ids = {"destination": "d1"}
    host._log_restore_phase = lambda *a, **k: None
    host._log_worker_lifecycle = lambda *a, **k: None
    MainWindow.on_root_load_success(host, payload, "old-worker-id")


def test_cleanup_root_worker_joins_running_thread_before_delete(qapp):
    class SlowRoot(QThread):
        def run(self):
            time.sleep(0.12)

    w = SlowRoot()
    w.start()
    mw = MainWindow.__new__(MainWindow)
    mw.root_load_workers = {"destination": {"id": "w1", "worker": w, "panel_key": "destination"}}
    mw.root_load_retired_workers = {}
    mw.folder_load_workers = {}
    mw.folder_load_retired_workers = {}
    MainWindow._cleanup_root_worker(mw, "destination", "w1")
    assert mw.root_load_workers.get("destination") is None
    for _ in range(80):
        QApplication.processEvents()
        time.sleep(0.005)
        if not w.isRunning():
            break
    assert not w.isRunning()
