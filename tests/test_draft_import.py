"""Draft import classification, migrated validation, and migration conflict helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ozlink_console.draft_import import (
    INVALID_UNKNOWN,
    MIGRATED_LEGACY_PACKAGE,
    MODERN_EXPORT,
    RAW_LEGACY_EXPORT,
    build_migration_report_summary_text,
    classify_import_bundle,
    is_valid_migration_drive_id,
    load_migration_conflicts_for_review,
    validate_migrated_import_bundle,
)
from ozlink_console.legacy_backup_migration import migrate_legacy_backup_folder
from ozlink_console.legacy_backup_migration.types import MigrationIdentityPreflight
from ozlink_console.memory import MemoryManager


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _scoped_mm(tmp_path: Path) -> MemoryManager:
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
    return mm


def _write_modern_session(folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    session = {
        "DraftId": "D1",
        "SelectedSourceSiteKey": "https://x.sharepoint.com/sites/S",
        "SelectedDestinationSiteKey": "https://x.sharepoint.com/sites/S",
        "SelectedSourceLibraryId": "b!1111111111111111111111111111111aa",
        "SelectedDestinationLibraryId": "b!22222222222222222222222222222222bb",
        "DestinationTreeSnapshotIdentityDriveId": "b!22222222222222222222222222222222bb",
        "DestinationTreeSnapshotIdentitySiteId": "site-id",
    }
    allocations = [
        {
            "RequestId": "r1",
            "SourceItemName": "a",
            "SourcePath": r"Documents\a",
            "SourceType": "folder",
            "RequestedDestinationPath": r"Shared Documents\Dest\a",
            "AllocationMethod": "",
            "RequestedBy": "",
            "RequestedDate": "",
            "Status": "Pending",
            "SourceDriveId": "b!1111111111111111111111111111111aa",
            "SourceItemId": "si1",
            "DestinationDriveId": "b!22222222222222222222222222222222bb",
            "DestinationParentItemId": "dp1",
            "DestinationSiteId": "site-id",
        }
    ]
    proposed: list[dict] = []
    (folder / "Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")
    (folder / "Draft-AllocationQueue.json").write_text(json.dumps(allocations), encoding="utf-8")
    (folder / "Draft-ProposedFolders.json").write_text(json.dumps(proposed), encoding="utf-8")


def _write_legacy_like_test_legacy_migration(folder: Path) -> None:
    """Same shape as tests.test_legacy_backup_migration._write_legacy_bundle."""
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


def test_classify_modern_export(tmp_path: Path) -> None:
    d = tmp_path / "m"
    _write_modern_session(d)
    kind, _meta = classify_import_bundle(d)
    assert kind == MODERN_EXPORT


def test_classify_raw_legacy_without_report(tmp_path: Path) -> None:
    d = tmp_path / "raw"
    _write_legacy_like_test_legacy_migration(d)
    kind, meta = classify_import_bundle(d)
    assert kind == RAW_LEGACY_EXPORT
    assert meta.get("legacy_migration_report_present") is None


def test_classify_invalid_missing_file(tmp_path: Path) -> None:
    d = tmp_path / "inv"
    d.mkdir(parents=True, exist_ok=True)
    (d / "Draft-SessionState.json").write_text("{}", encoding="utf-8")
    kind, _meta = classify_import_bundle(d)
    assert kind == INVALID_UNKNOWN


def test_raw_legacy_migration_only_then_import_target_migrated(tmp_path: Path) -> None:
    """A: raw bundle is migrated first; Memory import path should be the Migrated_* folder, not the raw folder."""
    src = tmp_path / "raw"
    _write_legacy_like_test_legacy_migration(src)
    assert classify_import_bundle(src)[0] == RAW_LEGACY_EXPORT
    out_parent = tmp_path / "out"
    ident = MigrationIdentityPreflight(
        source_site_key="s",
        source_site_id="sid",
        source_drive_id="b!ssssssssssssssssssssssssssssssss",
        destination_site_key="d",
        destination_site_id="did",
        destination_drive_id="b!dddddddddddddddddddddddddddddddddd",
        visible_destination_anchor="Root3",
    )
    res = migrate_legacy_backup_folder(src, out_parent, identity=ident, skip_graph_resolution=True)
    assert res.ok and res.output_folder
    assert classify_import_bundle(res.output_folder)[0] == MIGRATED_LEGACY_PACKAGE
    assert not (res.output_folder.resolve() == src.resolve())


def test_migrated_known_good_folder_validation_passes(repo_root: Path) -> None:
    """C + L: validation passes for shipped dry-run package including legacy_shape_reasons."""
    pkg = (
        repo_root
        / "MigrationDryRun"
        / "_real_graph_export_20260325_dryrun_fix"
        / "Migrated_20260420-183324"
    )
    if not pkg.is_dir():
        pytest.skip("Known-good migrated package not in working tree")
    r = validate_migrated_import_bundle(pkg)
    assert r.ok, r.error
    rep = r.report or {}
    assert rep.get("legacy_shape_reasons")
    cts = (rep.get("counts") or {})
    assert int(cts.get("allocations_input") or 0) == 14
    assert int(cts.get("proposed_input") or 0) == 7


def test_migrated_placeholder_drive_blocked(tmp_path: Path) -> None:
    d = tmp_path / "m"
    _write_legacy_like_test_legacy_migration(d)
    ident = {
        "source_drive_id": "bad",
        "destination_drive_id": "b!dddddddddddddddddddddddddddddddddd",
        "source_site_key": "s",
        "destination_site_key": "d",
    }
    report = {
        "schema_version": 1,
        "identity": ident,
        "counts": {
            "allocations_input": 1,
            "proposed_input": 1,
            "rows_rejected": 0,
            "allocation_parent_unresolved_after_planned_lookup": 0,
        },
    }
    (d / "LegacyMigrationReport.json").write_text(json.dumps(report), encoding="utf-8")
    vr = validate_migrated_import_bundle(d)
    assert not vr.ok


def test_rows_rejected_requires_confirmation(tmp_path: Path) -> None:
    d = tmp_path / "m"
    _write_legacy_like_test_legacy_migration(d)
    report = {
        "identity": {
            "source_drive_id": "b!ssssssssssssssssssssssssssssssss",
            "destination_drive_id": "b!dddddddddddddddddddddddddddddddddd",
            "source_site_key": "s",
            "destination_site_key": "d",
        },
        "counts": {
            "allocations_input": 1,
            "proposed_input": 1,
            "rows_rejected": 2,
            "allocation_parent_unresolved_after_planned_lookup": 0,
        },
    }
    (d / "LegacyMigrationReport.json").write_text(json.dumps(report), encoding="utf-8")
    r1 = validate_migrated_import_bundle(d, user_confirmed_rows_rejected=False)
    assert not r1.ok
    r2 = validate_migrated_import_bundle(d, user_confirmed_rows_rejected=True)
    assert r2.ok


def test_import_counts_match_after_migrated_roundtrip(tmp_path: Path) -> None:
    """F: migrated bundle file lengths match report counts."""
    src = tmp_path / "raw"
    _write_legacy_like_test_legacy_migration(src)
    ident = MigrationIdentityPreflight(
        source_site_key="s",
        source_site_id="sid",
        source_drive_id="b!ssssssssssssssssssssssssssssssss",
        destination_site_key="d",
        destination_site_id="did",
        destination_drive_id="b!dddddddddddddddddddddddddddddddddd",
        visible_destination_anchor="Root3",
    )
    res = migrate_legacy_backup_folder(src, tmp_path, identity=ident, skip_graph_resolution=True)
    assert res.ok and res.output_folder
    vr = validate_migrated_import_bundle(
        res.output_folder,
        user_confirmed_offline_migration=True,
    )
    assert vr.ok


def test_quarantine_logs_and_import(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """G / I: import creates quarantine; logs are emitted; modern bundle imports."""
    from ozlink_console import memory as memory_mod

    logs: list[tuple[str, dict]] = []

    def cap(msg: str, **kw):
        logs.append((msg, kw))

    monkeypatch.setattr(memory_mod, "log_info", cap)
    mm = _scoped_mm(tmp_path)
    src = tmp_path / "bundle"
    _write_modern_session(src)
    mm.import_bundle(src)
    names = [m for m, _ in logs]
    assert "import_memory_quarantine_created" in names
    assert "import_memory_quarantine_file_count" in names
    assert any(mm.quarantine.iterdir()), "quarantine folder should exist"


def test_live_duplicate_conflict_loaded(tmp_path: Path) -> None:
    d = tmp_path / "m"
    d.mkdir(parents=True, exist_ok=True)
    _write_legacy_like_test_legacy_migration(d)
    report = {
        "conflicts": [
            {
                "kind": "live_duplicate_proposed_folder",
                "path": "Root3/X",
                "proposed_full_path": r"Root3\Dup",
                "checked_graph_path": r"Root3\Dup",
                "live_item_id": "L1",
                "live_item_web_url": "https://x.sharepoint.com/dupe",
                "proposed_row_index": 0,
                "proposed_stable_key": "sk",
            }
        ]
    }
    (d / "LegacyMigrationReport.json").write_text(json.dumps(report), encoding="utf-8")
    rows = load_migration_conflicts_for_review(d)
    assert len(rows) == 1
    assert rows[0].get("review_type") == "migration_live_duplicate_proposed_folder"
    assert rows[0].get("migration_live_item_id") == "L1"


def test_build_migration_report_summary_non_empty() -> None:
    txt = build_migration_report_summary_text(
        {"counts": {"allocations_input": 1, "proposed_input": 0, "rows_rejected": 0}, "output_folder": "Z:\\out"}
    )
    assert "Allocations" in txt
    assert "Z:" in txt or "out" in txt


def test_is_valid_migration_drive_id() -> None:
    assert is_valid_migration_drive_id("b!0123456789012345678901234567890ab")
    assert not is_valid_migration_drive_id("")
    assert not is_valid_migration_drive_id("lib-relative-path")


def test_empty_write_guard_still_blocks_after_import(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """K: empty persist guard remains active after a bundle import."""
    from ozlink_console import memory as memory_mod
    from ozlink_console.models import AllocationRow

    events: list[str] = []

    def cap(msg: str, **_kw):
        events.append(msg)

    monkeypatch.setattr(memory_mod, "log_info", cap)
    mm = _scoped_mm(tmp_path)
    src = tmp_path / "b"
    _write_modern_session(src)
    mm.import_bundle(src)
    mm.save_allocations(
        [AllocationRow(RequestId="r", SourceItemName="n", SourcePath=r"S\a", SourceType="folder", RequestedDestinationPath=r"D\x", AllocationMethod="", RequestedBy="", RequestedDate="", Status="Pending")],
        allow_empty_planning_persist=True,
        save_reason="seed",
    )
    mm.save_allocations([], save_reason="_on_planning_mutation_autosave_timer", persist_context={"suppress_autosave": False})
    assert "allocation_queue_empty_write_blocked" in events

