"""win32_qt_minmaxinfo: safe imports and no-op paths off-Windows / invalid Qt native payloads."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QByteArray

from ozlink_console import win32_qt_minmaxinfo as m


def test_qt_native_clamp_rejects_wrong_event_type():
    assert m.qt_native_clamp_getminmaxinfo(QByteArray(b"other"), 0) is False


def test_win32_monitor_stub_off_windows_or_non_hwnd():
    assert m.win32_monitor_rc_work_phys(0) is None


def test_install_filter_is_noop_when_not_windows(monkeypatch):
    from PySide6.QtWidgets import QApplication

    monkeypatch.setattr(m.sys, "platform", "linux")
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    m.install_win32_getminmaxinfo_native_filter(app)
    assert getattr(app, "_ozlink_getminmaxinfo_native_filter", None) is None
