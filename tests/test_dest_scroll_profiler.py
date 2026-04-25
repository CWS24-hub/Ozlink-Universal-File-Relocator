"""Tests for destination scroll-window profiler helpers."""

from __future__ import annotations

from PySide6.QtCore import QObject

from ozlink_console.dest_scroll_profiler import DestScrollProfiler


def test_should_record_fine_grained_only_when_capturing_and_active():
    p = DestScrollProfiler(QObject(), enabled_fn=lambda: True)
    assert not p.should_record_fine_grained()
    p.note_scroll("scrollbar_valueChanged")
    assert p.should_record_fine_grained()
    p._flush_window()
    assert not p.should_record_fine_grained()
