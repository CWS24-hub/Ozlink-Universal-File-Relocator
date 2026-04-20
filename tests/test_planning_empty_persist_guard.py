"""Guards against silently overwriting non-empty planning JSON with []."""

from __future__ import annotations

import json
from pathlib import Path

from ozlink_console.memory import MemoryManager
from ozlink_console.models import AllocationRow, ProposedFolder


def _scoped_mm(tmp_path: Path) -> MemoryManager:
    mm = MemoryManager(tenant_domain="t.ex", operator_upn="u@t.ex")
    root = tmp_path / "Memory"
    root.mkdir(parents=True, exist_ok=True)
    mm.root = root
    mm.backups = root / "Backups"
    mm.backups.mkdir(parents=True, exist_ok=True)
    mm.paths = {
        "allocations": root / "Draft-AllocationQueue.json",
        "allocations_recovery": root / "Draft-AllocationQueue.recovery.json",
        "proposed": root / "Draft-ProposedFolders.json",
        "proposed_recovery": root / "Draft-ProposedFolders.recovery.json",
        "session": root / "Draft-SessionState.json",
        "session_recovery": root / "Draft-SessionState.recovery.json",
        "manifest": root / "MemoryManifest.json",
        "workspace_snapshot": root / "WorkspaceSnapshot.json",
    }
    mm.initialize_store()
    return mm


def _row(i: int) -> AllocationRow:
    return AllocationRow(
        RequestId=f"r{i}",
        SourceItemName="n",
        SourcePath=rf"S\a{i}",
        SourceType="folder",
        RequestedDestinationPath=rf"D\a{i}",
        AllocationMethod="",
        RequestedBy="",
        RequestedDate="",
        Status="Pending",
    )


def _pf(i: int) -> ProposedFolder:
    return ProposedFolder(FolderName=f"P{i}", DestinationPath=rf"D\p{i}")


def test_allocation_empty_write_blocked_during_startup_restore_keeps_rows_and_logs(monkeypatch, tmp_path):
    mm = _scoped_mm(tmp_path)
    rows = [_row(i) for i in range(17)]
    mm.save_allocations(rows, allow_empty_planning_persist=True, save_reason="seed")
    assert len(json.loads(mm.paths["allocations"].read_text(encoding="utf-8"))) == 17

    events: list[tuple[str, dict]] = []

    def _cap(msg: str, **kw):
        events.append((msg, kw))

    monkeypatch.setattr("ozlink_console.memory.log_info", _cap)

    persist = {
        "memory_restore_in_progress": True,
        "startup_restore_in_progress": True,
    }
    mm.save_allocations(
        [],
        save_reason="replay_autosave",
        persist_context=persist,
    )
    assert len(json.loads(mm.paths["allocations"].read_text(encoding="utf-8"))) == 17
    blocked = [e for e in events if e[0] == "allocation_queue_empty_write_blocked"]
    assert len(blocked) == 1


def test_proposed_empty_write_blocked_on_autosave_unless_explicit_clear(monkeypatch, tmp_path):
    mm = _scoped_mm(tmp_path)
    mm.save_proposed([_pf(1)], allow_empty_planning_persist=True, save_reason="seed")

    events: list[str] = []

    def _cap(msg: str, **kw):
        events.append(msg)

    monkeypatch.setattr("ozlink_console.memory.log_info", _cap)

    mm.save_proposed(
        [],
        save_reason="_on_planning_mutation_autosave_timer",
        persist_context={"suppress_autosave": False},
    )
    assert len(json.loads(mm.paths["proposed"].read_text(encoding="utf-8"))) == 1
    assert "proposed_folders_empty_write_blocked" in events

    mm.save_proposed(
        [],
        allow_empty_planning_persist=True,
        save_reason="user_clear_all_planning",
    )
    assert json.loads(mm.paths["proposed"].read_text(encoding="utf-8")) == []


def test_explicit_clear_empty_write_allowed_logs(monkeypatch, tmp_path):
    mm = _scoped_mm(tmp_path)
    mm.save_allocations([_row(0)], allow_empty_planning_persist=True, save_reason="seed")

    events: list[str] = []

    def _cap(msg: str, **kw):
        events.append(msg)

    monkeypatch.setattr("ozlink_console.memory.log_info", _cap)

    mm.save_allocations(
        [],
        allow_empty_planning_persist=True,
        save_reason="apply_draft_reset_after_backup_new_session",
    )
    assert json.loads(mm.paths["allocations"].read_text(encoding="utf-8")) == []
    assert "allocation_queue_empty_write_allowed_explicit_clear" in events


def test_restore_rebind_style_save_blocked_without_explicit_clear(monkeypatch, tmp_path):
    mm = _scoped_mm(tmp_path)
    mm.save_allocations([_row(0)], allow_empty_planning_persist=True, save_reason="seed")

    events: list[str] = []

    def _cap(msg: str, **kw):
        events.append(msg)

    monkeypatch.setattr("ozlink_console.memory.log_info", _cap)

    mm.save_allocations(
        [],
        save_reason="destination_restore_rebind",
        persist_context={"memory_restore_in_progress": False, "suppress_autosave": False},
    )
    assert len(json.loads(mm.paths["allocations"].read_text(encoding="utf-8"))) == 1
    assert "allocation_queue_empty_write_blocked" in events


def test_shutdown_after_restore_failure_empty_blocked(monkeypatch, tmp_path):
    mm = _scoped_mm(tmp_path)
    mm.save_allocations([_row(0)], allow_empty_planning_persist=True, save_reason="seed")

    events: list[str] = []

    def _cap(msg: str, **kw):
        events.append(msg)

    monkeypatch.setattr("ozlink_console.memory.log_info", _cap)

    mm.save_allocations(
        [],
        save_reason="closeEvent_shutdown_draft_save",
        persist_context={"memory_restore_in_progress": True},
    )
    assert len(json.loads(mm.paths["allocations"].read_text(encoding="utf-8"))) == 1
    assert "allocation_queue_empty_write_blocked" in events


def test_recovery_candidate_logged_when_primary_empty_but_backup_has_rows(monkeypatch, tmp_path):
    mm = _scoped_mm(tmp_path)
    mm.paths["allocations"].write_text(json.dumps([]), encoding="utf-8")
    (mm.backups / "Draft-AllocationQueue_backup.json").write_text(
        json.dumps([_row(0).to_dict()]),
        encoding="utf-8",
    )

    events: list[str] = []

    def _cap(msg: str, **kw):
        events.append(msg)

    monkeypatch.setattr("ozlink_console.memory.log_info", _cap)

    mm.save_allocations([], save_reason="autosave_empty_queue")
    assert json.loads(mm.paths["allocations"].read_text(encoding="utf-8")) == []
    assert "planning_memory_recovery_candidate_found" in events


def test_backup_not_overwritten_with_empty_when_primary_had_rows(tmp_path):
    mm = _scoped_mm(tmp_path)
    backup_path = mm.backups / "Draft-AllocationQueue_rotated.json"
    backup_path.write_text(json.dumps([_row(1).to_dict()]), encoding="utf-8")
    mm.save_allocations([_row(0), _row(2)], allow_empty_planning_persist=True, save_reason="seed")

    mm.save_allocations([], save_reason="bad_autosave")
    primary = json.loads(mm.paths["allocations"].read_text(encoding="utf-8"))
    backup_rows = json.loads(backup_path.read_text(encoding="utf-8"))
    assert len(primary) == 2
    assert len(backup_rows) == 1
