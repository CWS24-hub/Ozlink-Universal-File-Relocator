"""Per-pass planned-file parent index: O(1) lookup vs full planned_moves scan."""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from ozlink_console.main_window import MainWindow
from tests.test_planned_move_file_leaf_metadata_fallback import _stub_mw


@pytest.fixture
def _no_forensic_probe_noise(monkeypatch):
    monkeypatch.setenv("OZLINK_DESTINATION_FORENSIC_PLANNED_ITEM_BIND", "0")


def test_planned_file_parent_index_matches_full_scan_multi_parent(monkeypatch, _no_forensic_probe_noise):
    mw = _stub_mw(monkeypatch)
    mw.planned_moves = [
        {"destination_path": r"Root\A\B\f1.txt", "source": {"name": "f1.txt"}},
        {"destination_path": r"Root\A\B\f2.txt", "source": {"name": "f2.txt"}},
        {"destination_path": r"Root\A\C\f3.txt", "source": {"name": "f3.txt"}},
    ]
    mw._rebuild_planned_file_parent_index_for_overlay_pass()
    for folder in (r"Root\A\B", r"Root\A\C", r"Root\Z\Missing"):
        truth = MainWindow._forensic_planned_file_destination_paths_under_folder_full_scan(mw, folder.strip())
        got = mw._forensic_planned_file_destination_paths_under_folder(folder)
        assert sorted(truth) == sorted(got), (folder, truth, got)


def test_planned_file_parent_fallback_without_index_matches_full_scan(monkeypatch, _no_forensic_probe_noise):
    mw = _stub_mw(monkeypatch)
    mw.planned_moves = [
        {"destination_path": r"Hub\HR\Contractor bank.docx", "source": {"name": "Contractor bank.docx"}},
    ]
    assert getattr(mw, "_planned_file_children_by_parent_cf", None) is None
    got = mw._forensic_planned_file_destination_paths_under_folder(r"Hub\HR")
    truth = MainWindow._forensic_planned_file_destination_paths_under_folder_full_scan(mw, r"Hub\HR")
    assert sorted(got) == sorted(truth)


def test_rebuild_counts_rows_and_distinct_parents(monkeypatch, _no_forensic_probe_noise):
    mw = _stub_mw(monkeypatch)
    mw.planned_moves = [
        {"destination_path": r"P\a.txt", "source": {"name": "a.txt"}},
        {"destination_path": r"P\b.txt", "source": {"name": "b.txt"}},
        {"destination_path": r"Q\c.txt", "source": {"name": "c.txt"}},
    ]
    mw._rebuild_planned_file_parent_index_for_overlay_pass()
    assert int(getattr(mw, "_planned_file_parent_index_build_row_count", 0) or 0) == 3
    assert int(getattr(mw, "_planned_file_parent_index_build_distinct_parent_keys", 0) or 0) == 2
