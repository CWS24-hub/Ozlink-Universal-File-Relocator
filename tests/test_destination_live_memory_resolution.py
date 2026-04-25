"""Resolution actions for live_duplicate_proposed_folder (planning-only; no Graph writes)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ozlink_console.destination_live_memory_conflicts import REVIEW_TYPE_LIVE_MEMORY_DUPLICATE
from ozlink_console.destination_live_memory_resolution import (
    RESOLVED_BY_ACCEPT_EXISTING_LIVE_FOLDER,
    apply_accept_existing_live_folder_to_proposed_folder,
    filter_runtime_live_memory_rows,
    proposed_folder_accepts_existing_live_folder,
)
from ozlink_console.main_window import MainWindow
from ozlink_console.models import ProposedFolder


def _row():
    return {
        "review_type": REVIEW_TYPE_LIVE_MEMORY_DUPLICATE,
        "live_memory_subtype": "live_duplicate_proposed_folder",
        "source_path": r"Root3\HR",
        "reason": "x",
        "item_name": "y",
        "action": r"Root3\HR",
        "live_graph_path": r"Root3\HR",
        "live_item_id": "live-id-1",
    }


def test_a_accept_marks_proposed_resolved_and_drops_runtime_row() -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw.memory_manager = MagicMock()
    mw.graph = MagicMock()
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    row = _row()
    mw._runtime_live_memory_conflict_rows = [dict(row)]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._refresh_workflow_state_on_demand = lambda: None  # type: ignore[method-assign]
    mw._refresh_proposed_folders_table = lambda: None  # type: ignore[method-assign]
    mw._on_destination_state_mutation = lambda *a, **k: None  # type: ignore[method-assign]
    notify = MagicMock()
    mw._notify_planning_mutation_destination_snapshot_dirty = notify  # type: ignore[method-assign]

    ok = MainWindow._resolve_live_duplicate_proposed_folder_accept_existing(mw, row)
    assert ok is True
    assert len(mw._runtime_live_memory_conflict_rows) == 0
    assert len(mw.proposed_folders) == 1
    pf = mw.proposed_folders[0]
    assert proposed_folder_accepts_existing_live_folder(pf)
    assert pf.LiveMemoryDuplicateLiveItemId == "live-id-1"
    assert pf.LiveMemoryDuplicateLiveItemPath == r"Root3\HR"
    assert pf.LiveMemoryDuplicateResolvedAtUtc
    notify.assert_called_once()
    notify.assert_called_with(
        reason="live_memory_duplicate_accept_existing",
        surface="live_memory_duplicate",
    )


def test_b_accept_does_not_touch_graph() -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw.memory_manager = MagicMock()
    g = MagicMock()
    mw.graph = g
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    row = _row()
    mw._runtime_live_memory_conflict_rows = [dict(row)]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._refresh_workflow_state_on_demand = lambda: None  # type: ignore[method-assign]
    mw._refresh_proposed_folders_table = lambda: None  # type: ignore[method-assign]
    mw._on_destination_state_mutation = lambda *a, **k: None  # type: ignore[method-assign]
    mw._notify_planning_mutation_destination_snapshot_dirty = lambda *a, **k: None  # type: ignore[method-assign]
    MainWindow._resolve_live_duplicate_proposed_folder_accept_existing(mw, row)
    g.assert_not_called()


def test_c_remove_proposed_when_no_dependent_allocations() -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw.memory_manager = MagicMock()
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    mw.planned_moves = []
    row = _row()
    mw._runtime_live_memory_conflict_rows = [dict(row)]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._allocation_parent_path = lambda _m: ""  # type: ignore[method-assign]
    mw._planned_moves_dependent_on_proposed_destination = lambda _dp: []  # type: ignore[method-assign]
    mw._refresh_workflow_state_on_demand = lambda: None  # type: ignore[method-assign]
    mw._refresh_proposed_folders_table = lambda: None  # type: ignore[method-assign]
    mw._on_destination_state_mutation = lambda *a, **k: None  # type: ignore[method-assign]
    notify = MagicMock()
    mw._notify_planning_mutation_destination_snapshot_dirty = notify  # type: ignore[method-assign]

    ok = MainWindow._resolve_live_duplicate_proposed_folder_remove_proposed(mw, row)
    assert ok is True
    assert mw.proposed_folders == []
    assert mw._runtime_live_memory_conflict_rows == []
    notify.assert_called_once()
    notify.assert_called_with(
        reason="live_memory_duplicate_remove_proposed",
        surface="live_memory_duplicate",
    )


def test_d_remove_blocked_when_dependent_allocations(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._application_shutting_down = False
    mw.memory_manager = MagicMock()
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    row = _row()
    mw._runtime_live_memory_conflict_rows = [dict(row)]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._planned_moves_dependent_on_proposed_destination = lambda _dp: [{"destination_path": r"Root3\HR\a.txt"}]  # type: ignore[method-assign]
    warns: list[str] = []

    def _capt(_self, _title, msg) -> None:
        warns.append(str(msg))

    monkeypatch.setattr("ozlink_console.main_window.QMessageBox.warning", _capt)
    ok = MainWindow._resolve_live_duplicate_proposed_folder_remove_proposed(mw, row)
    assert ok is False
    assert len(mw.proposed_folders) == 1
    assert "depend" in (warns[0] if warns else "").lower()


def test_e_rename_logs_requested_then_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._live_memory_retarget_pending_indices = []
    mw._live_memory_retarget_tree_hook = []
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    monkeypatch.setattr("ozlink_console.main_window.QMessageBox.information", lambda *_a, **_k: None)
    monkeypatch.setattr("ozlink_console.main_window.QMessageBox.warning", lambda *_a, **_k: None)
    events: list[str] = []

    def _log(msg: str, **_kw) -> None:
        events.append(msg)

    monkeypatch.setattr("ozlink_console.main_window.log_info", _log)
    monkeypatch.setattr(
        "ozlink_console.main_window.QInputDialog.getText",
        lambda *a, **k: ("", False),
    )
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    MainWindow._live_duplicate_proposed_folder_run_rename(mw, _row())
    assert "destination_live_memory_duplicate_rename_requested" in events
    assert "destination_live_memory_duplicate_rename_cancelled" in events


def test_f_retarget_logs_requested_and_started_with_dependents(monkeypatch: pytest.MonkeyPatch) -> None:
    mw = MainWindow.__new__(MainWindow)
    mw._live_memory_retarget_pending_indices = []
    mw._live_memory_retarget_tree_hook = []
    monkeypatch.setattr("ozlink_console.main_window.QMessageBox.information", lambda *_a, **_k: None)
    events: list[str] = []

    def _log(msg: str, **_kw) -> None:
        events.append(msg)

    monkeypatch.setattr("ozlink_console.main_window.log_info", _log)
    mw.proposed_folders = [ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR")]
    mw.planned_moves = [
        {"source_path": "s1", "destination_path": r"Root3\HR\f.txt", "source": {"is_folder": False}},
    ]
    mw.destination_tree_widget = None
    mw.normalize_memory_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]

    def _deps(_dp):
        return [mw.planned_moves[0]]

    mw._planned_moves_dependent_on_proposed_destination = _deps  # type: ignore[method-assign]
    MainWindow._live_duplicate_proposed_folder_begin_retarget(mw, _row())
    MainWindow._end_live_memory_duplicate_retarget_session(mw, cancelled=True)
    assert "destination_live_memory_duplicate_retarget_requested" in events
    assert "destination_live_memory_duplicate_retarget_started" in events


def test_filter_runtime_rows_removes_one_match() -> None:
    rows = [_row(), {**_row(), "source_path": r"Other\Path"}]
    out, n = filter_runtime_live_memory_rows(
        rows,
        normalized_source_path=r"Root3\HR",
        live_memory_subtype="live_duplicate_proposed_folder",
        normalize_path=lambda p: str(p or "").replace("/", "\\"),
    )
    assert n == 1
    assert len(out) == 1
    assert out[0].get("source_path") == r"Other\Path"


def test_apply_accept_sets_resolution_fields() -> None:
    pf = ProposedFolder(FolderName="A", DestinationPath=r"x\y")
    got = apply_accept_existing_live_folder_to_proposed_folder(
        pf,
        live_item_id="idz",
        live_item_path=r"x\y",
        resolved_at_utc="2026-01-01T00:00:00Z",
    )
    assert got.LiveMemoryDuplicateResolution == RESOLVED_BY_ACCEPT_EXISTING_LIVE_FOLDER
    assert got.LiveMemoryDuplicateLiveItemId == "idz"
    assert got.Status == "AcceptedLive"


def test_g_accepted_proposed_folder_omitted_from_conflict_path_detection() -> None:
    mw = MainWindow.__new__(MainWindow)
    mw.planned_moves = []
    acc = apply_accept_existing_live_folder_to_proposed_folder(
        ProposedFolder(FolderName="HR", DestinationPath=r"Root3\HR"),
        live_item_id="x",
        live_item_path=r"Root3\HR",
        resolved_at_utc="2026-01-01T00:00:00Z",
    )
    mw.proposed_folders = [
        acc,
        ProposedFolder(FolderName="Keep", DestinationPath=r"Other\Branch"),
    ]
    mw._canonical_destination_projection_path = lambda p: str(p or "").replace("/", "\\")  # type: ignore[method-assign]
    proposed, _planned, _alloc = MainWindow._destination_memory_paths_for_live_conflict_detection(mw)
    assert r"Root3\HR" not in proposed
    assert r"Other\Branch" in proposed