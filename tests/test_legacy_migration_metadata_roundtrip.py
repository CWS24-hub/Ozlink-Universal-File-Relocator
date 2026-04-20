"""Migration review fields survive AllocationRow / ProposedFolder load-save (autosave) round trips."""

from __future__ import annotations

from ozlink_console.models import AllocationRow, ProposedFolder


def test_allocation_row_legacy_migration_metadata_roundtrip():
    raw = {
        "RequestId": "RECOVERED-20260324-001",
        "SourceItemName": "X",
        "SourcePath": "S\\x",
        "SourceType": "Folder",
        "RequestedDestinationPath": "Root\\HR\\X",
        "AllocationMethod": "Recovered from Log - Manual",
        "RequestedBy": "Gary",
        "RequestedDate": "2026-03-24 15:03",
        "Status": "Draft_LegacyReanchorFailed",
        "LegacyMigrationAnchorClassification": "unresolved_ambiguous_anchor",
        "LegacyMigrationPlannedScaffoldOnly": True,
        "LegacyMigrationRootNotLiveConfirmed": True,
        "LegacyMigrationUnresolvedGraphAnchor": True,
        "LegacyForeignRootBlocked": False,
        "DestinationDriveId": "b!DEST",
        "DestinationParentItemId": "",
    }
    row = AllocationRow.from_dict(raw)
    out = row.to_dict()
    assert out["LegacyMigrationAnchorClassification"] == "unresolved_ambiguous_anchor"
    assert out["LegacyMigrationPlannedScaffoldOnly"] is True
    assert out["LegacyMigrationRootNotLiveConfirmed"] is True
    assert out["LegacyMigrationUnresolvedGraphAnchor"] is True
    assert out["LegacyForeignRootBlocked"] is False
    assert out["Status"] == "Draft_LegacyReanchorFailed"
    assert out["DestinationParentItemId"] == ""
    assert out["DestinationDriveId"] == "b!DEST"


def test_proposed_folder_legacy_migration_metadata_roundtrip():
    raw = {
        "FolderName": "Follow up",
        "DestinationPath": "",
        "DestinationId": "",
        "ParentPath": "Root3\\Finance",
        "IsSelectable": True,
        "IsProposed": True,
        "Status": "Draft",
        "StableKey": "abc",
        "LegacyMigrationAnchorClassification": "reanchored_to_live_graph",
        "LegacyMigrationPlannedScaffoldOnly": False,
        "LegacyMigrationRootNotLiveConfirmed": False,
        "LegacyMigrationUnresolvedGraphAnchor": False,
        "LegacyForeignRootBlocked": False,
        "DestinationDriveId": "b!DEST",
        "DestinationParentItemId": "01PID",
    }
    pf = ProposedFolder.from_dict(raw)
    out = pf.to_dict()
    assert out["LegacyMigrationAnchorClassification"] == "reanchored_to_live_graph"
    assert out["LegacyMigrationPlannedScaffoldOnly"] is False
    assert out["DestinationParentItemId"] == "01PID"
    assert out["Status"] == "Draft"


def test_migration_metadata_defaults_when_absent():
    row = AllocationRow.from_dict(
        {
            "RequestId": "1",
            "SourceItemName": "n",
            "SourcePath": "s",
            "SourceType": "f",
            "RequestedDestinationPath": "d",
            "AllocationMethod": "m",
            "RequestedBy": "",
            "RequestedDate": "",
        }
    )
    d = row.to_dict()
    assert d["LegacyMigrationAnchorClassification"] == ""
    assert d["LegacyMigrationPlannedScaffoldOnly"] is False
