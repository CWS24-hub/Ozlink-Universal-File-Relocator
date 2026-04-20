"""Legacy backup migration sandbox + import guard."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from ozlink_console.legacy_backup_migration import (
    LegacyBackupDirectRestoreBlocked,
    MigrationIdentityPreflight,
    is_legacy_shaped_bundle,
    migrate_legacy_backup_folder,
)
from ozlink_console.memory import MemoryManager
from ozlink_console.models import AllocationRow, SessionState


def _write_legacy_bundle(folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    session = {
        "DraftId": "D-LEG",
        "SelectedSourceSiteKey": "",
        "SelectedDestinationSiteKey": "",
        "SelectedSourceLibraryId": "",
        "SelectedDestinationLibraryId": "",
        "DestinationTreeSnapshotIdentityDriveId": "",
        "DestinationTreeSnapshotIdentitySiteId": "",
    }
    allocations = [
        {
            "RequestId": "r1",
            "SourceItemName": "x",
            "SourcePath": r"S\a",
            "SourceType": "folder",
            "RequestedDestinationPath": r"Root3\HR\New",
            "AllocationMethod": "",
            "RequestedBy": "",
            "RequestedDate": "",
            "Status": "Pending",
            "SourceDriveId": "",
            "SourceItemId": "",
            "DestinationDriveId": "",
            "DestinationParentItemId": "",
        }
    ]
    proposed = [
        {
            "FolderName": "PF",
            "DestinationPath": r"Root3\PF1",
            "ParentPath": r"Root3",
            "DestinationDriveId": "",
            "DestinationParentItemId": "",
            "StableKey": "",
        }
    ]
    (folder / "Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")
    (folder / "Draft-AllocationQueue.json").write_text(json.dumps(allocations), encoding="utf-8")
    (folder / "Draft-ProposedFolders.json").write_text(json.dumps(proposed), encoding="utf-8")


def _identity() -> MigrationIdentityPreflight:
    return MigrationIdentityPreflight(
        source_site_key="site-src",
        source_site_id="ss1",
        source_drive_id="drive-src-1",
        destination_site_key="site-dst",
        destination_site_id="ds1",
        destination_drive_id="drive-dst-1",
        visible_destination_anchor="Root3",
    )


def test_a_allocation_stamped_with_destination_drive_id(tmp_path):
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    mid = _identity()
    res = migrate_legacy_backup_folder(src, tmp_path, identity=mid, skip_graph_resolution=True)
    assert res.ok and res.output_folder
    alloc = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert alloc[0]["DestinationDriveId"] == "drive-dst-1"


def test_b_graph_mock_resolves_ids_or_unresolved(tmp_path, monkeypatch):
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)

    class G:
        def get_drive_item_by_path(self, drive_id: str, relative_path: str):
            rel = str(relative_path or "").replace("\\", "/").strip("/")
            if "HR" in rel:
                return {"id": "parent-graph-1", "folder": {}}
            if rel.endswith("a") or "S" in rel:
                return {"id": "src-graph-1", "folder": {}}
            return None

    res = migrate_legacy_backup_folder(
        src,
        tmp_path,
        identity=_identity(),
        graph=G(),
        skip_graph_resolution=False,
    )
    assert res.ok
    alloc = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert alloc[0].get("DestinationParentItemId") == "parent-graph-1"
    assert alloc[0].get("SourceItemId") == "src-graph-1"


def test_c_missing_identity_returns_needs_confirmation(tmp_path):
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    res = migrate_legacy_backup_folder(src, tmp_path, identity=None)
    assert not res.ok
    assert res.needs_identity_confirmation


def test_d_original_files_unchanged(tmp_path):
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    before = (src / "Draft-AllocationQueue.json").read_bytes()
    migrate_legacy_backup_folder(src, tmp_path, identity=_identity(), skip_graph_resolution=True)
    assert (src / "Draft-AllocationQueue.json").read_bytes() == before


def test_e_foreign_hub_marked(tmp_path):
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    # Patch allocation path to wrong hub
    alloc = json.loads((src / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    alloc[0]["RequestedDestinationPath"] = r"OtherSiteHub\X"
    (src / "Draft-AllocationQueue.json").write_text(json.dumps(alloc), encoding="utf-8")
    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity(), skip_graph_resolution=True)
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert "_ForeignHubRejected" in str(out[0].get("Status", ""))


def test_f_live_duplicate_conflict_recorded(tmp_path):
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)

    class G:
        def get_drive_item_by_path(self, drive_id: str, relative_path: str):
            rp = str(relative_path or "").replace("\\", "/")
            if "PF1" in rp or "HR" in rp:
                return {"id": "live-existing", "folder": {}}
            return None

    res = migrate_legacy_backup_folder(
        src,
        tmp_path,
        identity=_identity(),
        graph=G(),
    )
    rep = res.report or {}
    assert rep.get("counts", {}).get("live_duplicate_detected", 0) >= 1
    assert len(rep.get("conflicts", [])) >= 1


def test_g_migrated_loaders_roundtrip(tmp_path):
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity(), skip_graph_resolution=True)
    sess = SessionState.from_dict(
        json.loads((res.output_folder / "Draft-SessionState.json").read_text(encoding="utf-8"))
    )
    assert str(sess.SelectedDestinationLibraryId) == "drive-dst-1"
    raw_a = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    rows = [AllocationRow.from_dict(x) for x in raw_a]
    assert rows[0].DestinationDriveId == "drive-dst-1"


def test_h_engine_has_no_main_window_import_or_persist_hook():
    import ozlink_console.legacy_backup_migration.engine as eng

    src = Path(eng.__file__).read_text(encoding="utf-8")
    assert "main_window" not in src.lower()
    assert not re.search(r"\b_persist_planning_change\s*\(", src)


def test_i_migrated_package_import_allowed(tmp_path):
    """Legacy-shaped bundle with LegacyMigrationReport.json is treated as migrated and imports."""
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

    legacy_src = tmp_path / "legacy_bundle"
    _write_legacy_bundle(legacy_src)
    (legacy_src / "LegacyMigrationReport.json").write_text(
        json.dumps({"schema_version": 1}),
        encoding="utf-8",
    )
    mm.import_bundle(legacy_src)


def test_i_direct_import_blocked_without_flag_or_migrated_marker(tmp_path):
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

    legacy_src = tmp_path / "legacy_bundle"
    _write_legacy_bundle(legacy_src)

    with pytest.raises(LegacyBackupDirectRestoreBlocked):
        mm.import_bundle(legacy_src)

    os.environ["OZLINK_ALLOW_LEGACY_DIRECT_RESTORE"] = "1"
    try:
        mm.import_bundle(legacy_src)
    finally:
        del os.environ["OZLINK_ALLOW_LEGACY_DIRECT_RESTORE"]


def test_legacy_shape_false_for_empty_planning(tmp_path):
    session = {"SelectedDestinationLibraryId": "", "DestinationTreeSnapshotIdentityDriveId": ""}
    assert is_legacy_shaped_bundle(session, [], []) == (False, [])


def test_migration_logs_started_completed(monkeypatch, tmp_path):
    msgs: list[str] = []

    def cap(m, **kw):
        msgs.append(str(m))

    monkeypatch.setattr("ozlink_console.legacy_backup_migration.engine.log_info", cap)
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    migrate_legacy_backup_folder(src, tmp_path, identity=_identity(), skip_graph_resolution=True)
    assert any("legacy_backup_migration_started" in m for m in msgs)
    assert any("legacy_backup_migration_completed" in m for m in msgs)
