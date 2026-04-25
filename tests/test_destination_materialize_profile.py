"""Opt-in destination materialize profiling (OZLINK_DEST_MATERIALIZE_PROFILE)."""

from __future__ import annotations

from ozlink_console.main_window import MainWindow


def test_materialize_profile_record_and_finish_logs_top_steps(monkeypatch):
    monkeypatch.setenv("OZLINK_DEST_MATERIALIZE_PROFILE", "1")
    mw = MainWindow.__new__(MainWindow)
    mw._destination_materialize_profile_start_cycle()
    assert mw._destination_materialize_profile_active is True
    mw._destination_materialize_profile_record("phase_a", 0.02)
    mw._destination_materialize_profile_record("phase_a", 0.01)
    mw._destination_materialize_profile_record("phase_b", 0.05)
    mw._destination_materialize_profile_finish_cycle("test_reason")
    assert mw._destination_materialize_profile_active is False
    assert mw._materialize_cycle_path_canon_cache is None


def test_materialize_cached_norm_when_overlay_pass_active(monkeypatch):
    monkeypatch.delenv("OZLINK_DEST_MATERIALIZE_PROFILE", raising=False)
    mw = MainWindow.__new__(MainWindow)
    mw._canonical_destination_projection_path = lambda p: f"CANON::{p}"
    mw.normalize_memory_path = lambda p: str(p)
    assert mw._materialize_cached_destination_lookup_norm("x") == "CANON::x"
    mw._destination_overlay_materialize_pass_begin()
    assert mw._materialize_cached_destination_lookup_norm("y") == "CANON::y"
    assert mw._materialize_cached_destination_lookup_norm("y") == "CANON::y"
    mw._destination_overlay_materialize_pass_end()
