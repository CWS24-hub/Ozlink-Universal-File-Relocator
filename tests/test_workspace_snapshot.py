"""WorkspaceSnapshot.json sidecar on export/import (optional file, backward compatible)."""

from __future__ import annotations

import json
import shutil

from ozlink_console.memory import MemoryManager, WORKSPACE_SNAPSHOT_SCHEMA_VERSION


def test_write_read_workspace_snapshot_roundtrip(tmp_path):
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
    payload = {
        "schema_version": WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
        "snapshot_type": "workspace_full",
        "app_version": "test",
        "created_at": "2099-01-01T00:00:00+00:00",
        "saved_trigger": "unit",
        "graph_linkage_audit": {"severity": "ok", "total_rows": 0},
    }
    mm.write_workspace_snapshot(payload)
    assert mm.paths["workspace_snapshot"].is_file()
    got = mm.read_workspace_snapshot_optional()
    assert got is not None
    assert got.get("snapshot_type") == "workspace_full"
    assert got.get("schema_version") == WORKSPACE_SNAPSHOT_SCHEMA_VERSION


def test_import_bundle_copies_workspace_snapshot_when_present(tmp_path):
    mm = MemoryManager(tenant_domain="t.ex", operator_upn="u@t.ex")
    root = tmp_path / "Memory"
    root.mkdir(parents=True, exist_ok=True)
    mm.root = root
    mm.backups = root / "Backups"
    mm.quarantine = root / "Quarantine"
    mm.exports = tmp_path / "Exports"
    mm.backups.mkdir(parents=True, exist_ok=True)
    mm.quarantine.mkdir(parents=True, exist_ok=True)
    mm.exports.mkdir(parents=True, exist_ok=True)
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

    src = tmp_path / "bundle"
    src.mkdir(parents=True, exist_ok=True)
    shutil.copy2(mm.paths["session"], src / "Draft-SessionState.json")
    shutil.copy2(mm.paths["allocations"], src / "Draft-AllocationQueue.json")
    shutil.copy2(mm.paths["proposed"], src / "Draft-ProposedFolders.json")
    side = {"snapshot_type": "workspace_full", "schema_version": 2, "graph_linkage_audit": {"severity": "warn"}}
    (src / "WorkspaceSnapshot.json").write_text(json.dumps(side), encoding="utf-8")

    mm.import_bundle(src)
    loaded = mm.read_workspace_snapshot_optional()
    assert loaded is not None
    assert (loaded.get("graph_linkage_audit") or {}).get("severity") == "warn"
