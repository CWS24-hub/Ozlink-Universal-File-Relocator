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


def test_proposed_duplicate_no_match_when_only_parent_exists(tmp_path):
    """Graph has Finance folder but not Finance\\Follow up → no duplicate (A)."""
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    prop = [
        {
            "FolderName": "Follow up",
            "DestinationPath": "",
            "ParentPath": r"Root3\Finance",
            "DestinationDriveId": "",
            "DestinationParentItemId": "",
            "StableKey": "sk-a",
        }
    ]
    (src / "Draft-ProposedFolders.json").write_text(json.dumps(prop), encoding="utf-8")

    class G:
        def get_drive_item_by_path(self, drive_id: str, relative_path: str):
            rp = str(relative_path or "").replace("\\", "/").strip("/").lower()
            if rp == "root3/finance":
                return {"id": "parent-fin", "folder": {}}
            return None

    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity(), graph=G(), skip_graph_resolution=False)
    assert res.ok
    assert (res.report or {}).get("counts", {}).get("live_duplicate_detected", 0) == 0
    out = json.loads((res.output_folder / "Draft-ProposedFolders.json").read_text(encoding="utf-8"))
    assert "Follow up" in (out[0].get("DestinationPath") or "")


def test_proposed_duplicate_when_full_folder_path_exists(tmp_path):
    """Graph returns a folder at exact full proposed path → duplicate (B)."""
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    prop = [
        {
            "FolderName": "Follow up",
            "DestinationPath": "",
            "ParentPath": r"Root3\Finance",
            "DestinationDriveId": "",
            "DestinationParentItemId": "",
            "StableKey": "sk-b",
        }
    ]
    (src / "Draft-ProposedFolders.json").write_text(json.dumps(prop), encoding="utf-8")

    class G:
        def get_drive_item_by_path(self, drive_id: str, relative_path: str):
            rp = str(relative_path or "").replace("\\", "/").strip("/")
            if "finance" in rp.lower() and "follow" in rp.lower():
                return {"id": "live-fold", "name": "Follow up", "folder": {}, "webUrl": "https://example.invalid/u"}
            if "HR" in rp:
                return {"id": "hr-parent", "folder": {}}
            return None

    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity(), graph=G(), skip_graph_resolution=False)
    rep = res.report or {}
    assert rep.get("counts", {}).get("live_duplicate_detected", 0) >= 1
    conf = [c for c in (rep.get("conflicts") or []) if c.get("kind") == "live_duplicate_proposed_folder"]
    assert conf
    assert conf[0].get("checked_graph_path") or conf[0].get("path")
    assert conf[0].get("proposed_full_path")
    assert conf[0].get("live_item_id")


def test_proposed_duplicate_uses_parent_plus_folder_when_destination_path_empty(tmp_path):
    """Empty DestinationPath: duplicate check uses ParentPath + FolderName (C)."""
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    prop = [
        {
            "FolderName": "X",
            "DestinationPath": "",
            "ParentPath": r"Root3\Q",
            "DestinationDriveId": "",
            "DestinationParentItemId": "",
            "StableKey": "sk-c",
        }
    ]
    (src / "Draft-ProposedFolders.json").write_text(json.dumps(prop), encoding="utf-8")

    seen: list[str] = []

    class G:
        def get_drive_item_by_path(self, drive_id: str, relative_path: str):
            seen.append(str(relative_path or ""))
            if "Root3" in relative_path and "Q" in relative_path and "X" in relative_path:
                return {"id": "id-x", "name": "X", "folder": {}}
            if "HR" in relative_path:
                return {"id": "hr", "folder": {}}
            return None

    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity(), graph=G(), skip_graph_resolution=False)
    assert res.ok
    assert any("Q" in s and "X" in s.replace("\\", "/") for s in seen)


def test_proposed_conflict_report_has_full_path_and_live_fields(tmp_path):
    """Report stores proposed_full_path, checked_graph_path, live item ids (D)."""
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    prop = [
        {
            "FolderName": "D",
            "DestinationPath": "",
            "ParentPath": r"Root3\Z",
            "DestinationDriveId": "",
            "DestinationParentItemId": "",
            "StableKey": "sk-d1",
        }
    ]
    (src / "Draft-ProposedFolders.json").write_text(json.dumps(prop), encoding="utf-8")

    class G:
        def get_drive_item_by_path(self, drive_id: str, relative_path: str):
            rp = str(relative_path or "").replace("\\", "/")
            if "Z" in rp and "D" in rp:
                return {
                    "id": "item-d",
                    "name": "D",
                    "folder": {},
                    "webUrl": "https://tenant.sharepoint.com/D",
                }
            if "HR" in relative_path:
                return {"id": "hr", "folder": {}}
            return None

    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity(), graph=G(), skip_graph_resolution=False)
    c = next(x for x in (res.report or {}).get("conflicts", []) if x.get("kind") == "live_duplicate_proposed_folder")
    assert c.get("proposed_full_path")
    assert (c.get("checked_graph_path") or c.get("path")) != "Root3"
    assert c.get("live_item_id") == "item-d"
    assert "sharepoint.com" in (c.get("live_item_web_url") or "")


def test_no_conflict_path_is_anchor_only_root3(tmp_path):
    """A lone 'Root3' segment must not be reported as the duplicate path (E)."""
    src = tmp_path / "legacy"
    _write_legacy_bundle(src)
    prop = [
        {
            "FolderName": "Leaf",
            "DestinationPath": "",
            "ParentPath": r"Root3\A\B",
            "DestinationDriveId": "",
            "DestinationParentItemId": "",
            "StableKey": "sk-e",
        }
    ]
    (src / "Draft-ProposedFolders.json").write_text(json.dumps(prop), encoding="utf-8")

    class G:
        def get_drive_item_by_path(self, drive_id: str, relative_path: str):
            rp = str(relative_path or "").replace("\\", "/").strip("/").lower()
            if rp == "root3":
                return {"id": "bad-root", "folder": {}}
            if "a" in rp and "b" in rp and "leaf" in rp:
                return {"id": "dup-leaf", "folder": {}}
            if "HR" in relative_path:
                return {"id": "hr", "folder": {}}
            return None

    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity(), graph=G(), skip_graph_resolution=False)
    for c in (res.report or {}).get("conflicts", []):
        cg = str(c.get("checked_graph_path") or c.get("path") or "")
        assert cg.lower().strip("/") != "root3"


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
