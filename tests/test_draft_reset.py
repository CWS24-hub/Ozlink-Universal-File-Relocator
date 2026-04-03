"""Backup-first draft reset (MemoryManager + MainWindow helpers)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ozlink_console.memory import MemoryManager
from ozlink_console.models import AllocationRow, ProposedFolder, SessionState


def test_save_draft_reset_backup_writes_verified_json(tmp_path):
    mm = MemoryManager(tenant_domain="tenant.example", operator_upn="user@tenant.example")
    mm.backups = tmp_path / "Backups"
    mm.backups.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "kind": "draft_reset_backup",
        "created_utc": "2099-01-01T00:00:00",
        "draft_id": "DRAFT-TEST",
        "session": {"DraftId": "DRAFT-TEST"},
        "allocations": [{"RequestId": "r1", "SourcePath": "S\\a"}],
        "proposed_folders": [],
        "planned_moves": [{"source_path": "S\\a"}],
    }
    path = mm.save_draft_reset_backup(payload)
    assert path.is_file()
    assert path.name.startswith("DraftReset_")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["schema_version"] == 1
    assert data["allocations"][0]["RequestId"] == "r1"
    assert data["planned_moves"][0]["source_path"] == r"S\a"


def test_save_draft_reset_backup_raises_typeerror_when_not_dict(tmp_path):
    mm = MemoryManager(tenant_domain="t.ex", operator_upn="u@t.ex")
    mm.backups = tmp_path / "Backups"
    mm.backups.mkdir(parents=True, exist_ok=True)
    with pytest.raises(TypeError):
        mm.save_draft_reset_backup("not-a-dict")  # type: ignore[arg-type]


def test_handle_reset_draft_aborts_when_backup_raises(tmp_path):
    from PySide6.QtWidgets import QApplication

    from ozlink_console.main_window import MainWindow

    _ = QApplication.instance() or QApplication([])

    mm = MemoryManager(tenant_domain="t.ex", operator_upn="u@t.ex")
    mm.backups = tmp_path / "Backups"
    mm.backups.mkdir(parents=True, exist_ok=True)

    mw = MainWindow.__new__(MainWindow)
    mw.memory_manager = mm
    mw.planned_moves = [{"source_path": "S\\x", "status": "Draft"}]
    mw.proposed_folders = []
    mw.current_session_context = {
        "connected": True,
        "operator_upn": "u@t.ex",
        "tenant_domain": "t.ex",
        "operator_display_name": "User",
        "user_role": "user",
    }
    mw._memory_restore_in_progress = False
    mw._ensure_active_draft_session = lambda: True
    mw._build_draft_reset_backup_payload = lambda: {
        "schema_version": 1,
        "kind": "draft_reset_backup",
        "created_utc": "",
        "draft_id": "D1",
        "session": {},
        "allocations": [],
        "proposed_folders": [],
        "planned_moves": [],
    }
    applied: list = []
    mw._apply_draft_reset_after_backup = lambda: applied.append(True)

    reset_btn = object()
    cancel_btn = object()

    mock_dlg = MagicMock()
    mock_dlg.addButton.side_effect = [reset_btn, cancel_btn]
    mock_dlg.exec.return_value = 0
    mock_dlg.clickedButton.return_value = reset_btn

    with patch.object(mm, "save_draft_reset_backup", side_effect=OSError("write failed")):
        with patch("ozlink_console.main_window.QMessageBox", return_value=mock_dlg):
            mw._handle_reset_draft()

    assert applied == []
    assert len(mw.planned_moves) == 1


def test_apply_draft_reset_after_backup_clears_runtime_and_persists_empty():
    from PySide6.QtWidgets import QApplication

    from ozlink_console.main_window import MainWindow

    _ = QApplication.instance() or QApplication([])

    mm = MagicMock()
    mw = MainWindow.__new__(MainWindow)
    mw.memory_manager = mm
    mw.planned_moves = [{"source_path": "S\\f", "status": "Draft"}]
    mw.proposed_folders = [ProposedFolder(FolderName="P", DestinationPath="D\\P")]
    mw.active_draft_session_id = "OLD"
    mw._draft_shell_state = SessionState(DraftId="OLD")
    mw._draft_shell_raw = {}
    mw.current_session_context = {
        "operator_upn": "u@t.ex",
        "tenant_domain": "t.ex",
        "operator_display_name": "User",
    }
    mw._suppress_autosave = True

    def _minimal_clear(*, refresh_ui=True):
        mw.planned_moves = []
        mw.proposed_folders = []
        mw.active_draft_session_id = ""
        mw._draft_shell_state = SessionState()
        mw._draft_shell_raw = {}
        mw.unresolved_proposed_by_parent_path = {}
        mw.unresolved_allocations_by_parent_path = {}

    mw._clear_runtime_draft_state = _minimal_clear
    mw._invalidate_projection_lookup_caches = MagicMock()
    mw._create_new_draft_session_id = lambda: "NEW-DRAFT-ID"

    def _fresh(**_kw):
        return SessionState(
            DraftId="",
            SelectedSourceSite="SiteA",
            SelectedDestinationSite="SiteB",
        )

    mw._build_current_draft_shell_state = _fresh
    mw._rebuild_submission_visual_cache = MagicMock()
    mw.refresh_planned_moves_table = MagicMock()
    mw.clear_selection_details = MagicMock()
    mw.update_progress_summaries = MagicMock()
    mw.planned_moves_status = MagicMock()
    mw.source_tree_widget = None
    mw.destination_tree_widget = None
    mw._refresh_source_projection = MagicMock()
    mw._schedule_deferred_destination_materialization = MagicMock()

    mw._apply_draft_reset_after_backup()

    assert mw.planned_moves == []
    assert mw.proposed_folders == []
    assert mw.active_draft_session_id == "NEW-DRAFT-ID"
    assert mw._suppress_autosave is False
    mm.save_allocations.assert_called_once_with([], allow_empty=True)
    mm.save_proposed.assert_called_once_with([], allow_empty=True)
    mm.save_session.assert_called_once()
    mm.refresh_manifest.assert_called_once()


def test_build_draft_reset_backup_payload_includes_allocations_and_moves():
    from PySide6.QtWidgets import QApplication

    from ozlink_console.main_window import MainWindow

    _ = QApplication.instance() or QApplication([])

    mw = MainWindow.__new__(MainWindow)
    mw.active_draft_session_id = "DRAFT-X"
    mw.planning_inputs = {}
    mw.planned_moves = []
    row = AllocationRow(
        RequestId="1",
        SourceItemName="n",
        SourcePath="S\\n",
        SourceType="file",
        RequestedDestinationPath="D\\n",
        AllocationMethod="Manual",
        RequestedBy="u",
        RequestedDate="",
        Status="Draft",
    )

    def _rows():
        return [row]

    def _sess(**_kw):
        s = SessionState()
        s.DraftId = "DRAFT-X"
        return s

    mw._build_current_draft_shell_state = _sess
    mw._build_memory_allocation_rows = _rows
    mw._build_memory_proposed_folders = lambda: [ProposedFolder(FolderName="F", DestinationPath="D\\F")]
    mw._planned_moves_serializable_for_backup = lambda: [{"source_path": "S\\n"}]

    payload = mw._build_draft_reset_backup_payload()
    assert payload["schema_version"] == 1
    assert payload["kind"] == "draft_reset_backup"
    assert len(payload["allocations"]) == 1
    assert payload["allocations"][0]["SourcePath"] == r"S\n"
    assert len(payload["proposed_folders"]) == 1
    assert payload["planned_moves"][0]["source_path"] == r"S\n"
