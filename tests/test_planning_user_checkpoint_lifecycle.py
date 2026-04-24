"""PlanningUserCheckpoint lifecycle: journal flush vs recovery mirrors (no full MainWindow)."""

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


def _alloc(n: int) -> AllocationRow:
    return AllocationRow(
        RequestId=f"r{n}",
        SourceItemName="n",
        SourcePath=rf"S\a{n}",
        SourceType="folder",
        RequestedDestinationPath=rf"D\a{n}",
        AllocationMethod="",
        RequestedBy="",
        RequestedDate="",
        Status="Pending",
    )


def _prop(n: int) -> ProposedFolder:
    return ProposedFolder(FolderName=f"P{n}", DestinationPath=rf"D\p{n}")


def test_planning_mirrors_match_true_after_save_allocations_proposed(tmp_path: Path) -> None:
    mm = _scoped_mm(tmp_path)
    arows = [_alloc(i) for i in range(14)]
    prows = [_prop(i) for i in range(7)]
    mm.save_allocations(arows, allow_empty_planning_persist=True, save_reason="test")
    mm.save_proposed(prows, allow_empty_planning_persist=True, save_reason="test")
    assert mm.planning_allocations_proposed_primary_match_recovery_copies() is True


def test_planning_mirrors_match_false_when_recovery_alloc_stale(tmp_path: Path) -> None:
    mm = _scoped_mm(tmp_path)
    arows = [_alloc(i) for i in range(2)]
    mm.save_allocations(arows, allow_empty_planning_persist=True, save_reason="test")
    mm.save_proposed([_prop(0)], allow_empty_planning_persist=True, save_reason="test")
    assert mm.planning_allocations_proposed_primary_match_recovery_copies() is True
    p_rec = Path(mm.paths["allocations_recovery"])
    data = json.loads(p_rec.read_text(encoding="utf-8"))
    data[0]["Status"] = "MISMATCH"
    p_rec.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    assert mm.planning_allocations_proposed_primary_match_recovery_copies() is False


def test_planning_mirrors_match_false_when_proposed_recovery_missing(tmp_path: Path) -> None:
    mm = _scoped_mm(tmp_path)
    mm.save_allocations([_alloc(0)], allow_empty_planning_persist=True, save_reason="test")
    mm.save_proposed([_prop(0)], allow_empty_planning_persist=True, save_reason="test")
    assert mm.planning_allocations_proposed_primary_match_recovery_copies() is True
    Path(mm.paths["proposed_recovery"]).unlink()
    assert mm.planning_allocations_proposed_primary_match_recovery_copies() is False
