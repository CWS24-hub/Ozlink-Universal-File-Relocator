"""Display-only labels for recovered / legacy migration planning rows."""

from __future__ import annotations

from ozlink_console.legacy_backup_migration.types import (
    ANCHOR_CLASS_PLANNED_PARENT_MISSING_DESCENDANT,
    ANCHOR_CLASS_PLANNED_SCAFFOLD_EMPTY_LIBRARY,
)
from ozlink_console.models import AllocationRow, ProposedFolder
from ozlink_console.recovered_planning_display import recovered_planning_display_label


def test_a_graph_parent_resolved_by_resolution_field() -> None:
    row = {
        "Status": "Draft_LegacyReanchorFailed_GraphResolved",
        "LegacyMigrationDestinationParentResolution": "graph",
    }
    assert recovered_planning_display_label(row) == "Recovered — Graph parent resolved"


def test_a_graph_parent_resolved_by_destination_ids() -> None:
    row = {
        "Status": "Draft_LegacyGraphParents",
        "LegacyMigrationDestinationParentResolution": "",
        "DestinationDriveId": "b!" + "x" * 22,
        "DestinationParentItemId": "parent-item-id",
    }
    assert recovered_planning_display_label(row) == "Recovered — Graph parent resolved"


def test_b_planned_parent_resolved() -> None:
    row = {
        "Status": "Draft_LegacyReanchorFailed_PlannedParentResolved",
        "LegacyMigrationDestinationParentResolution": "planned_parent",
        "LegacyMigrationPlannedParentResolved": True,
    }
    assert recovered_planning_display_label(row) == "Recovered — planned parent resolved"


def test_c_partial_planned_parent_match() -> None:
    row = {
        "Status": "Draft_LegacyReanchorFailed_PlannedParentPartial",
        "LegacyMigrationPlannedParentMatchKind": "missing_descendant",
        "LegacyMigrationDestinationParentResolution": "planned_parent",
    }
    assert recovered_planning_display_label(row) == "Recovered — partial planned parent match"


def test_c_partial_by_classification() -> None:
    row = {
        "Status": "Draft_X",
        "LegacyMigrationAnchorClassification": ANCHOR_CLASS_PLANNED_PARENT_MISSING_DESCENDANT,
    }
    assert recovered_planning_display_label(row) == "Recovered — partial planned parent match"


def test_d_planned_scaffold() -> None:
    row = {
        "Status": "Draft_LegacyMigrationUnresolved",
        "LegacyMigrationAnchorClassification": ANCHOR_CLASS_PLANNED_SCAFFOLD_EMPTY_LIBRARY,
        "LegacyMigrationPlannedScaffoldOnly": True,
    }
    assert recovered_planning_display_label(row) == "Recovered — planned scaffold"


def test_e_legacy_reanchor_failed_needs_review() -> None:
    row = {
        "Status": "Draft_LegacyReanchorFailed",
        "LegacyMigrationDestinationParentResolution": "",
        "LegacyMigrationPlannedParentResolved": False,
    }
    assert recovered_planning_display_label(row) == "Recovered — needs review"


def test_f_non_migrated_pending_unchanged() -> None:
    row = {"Status": "Pending", "DestinationDriveId": "", "DestinationParentItemId": ""}
    assert recovered_planning_display_label(row) is None


def test_g_raw_status_roundtrips_through_allocation_row() -> None:
    raw_status = "Draft_LegacyReanchorFailed_PlannedParentResolved"
    d = {
        "RequestId": "R1",
        "SourceItemName": "s",
        "SourcePath": "a\\b",
        "SourceType": "File",
        "RequestedDestinationPath": "d\\e",
        "AllocationMethod": "Manual",
        "RequestedBy": "u",
        "RequestedDate": "2020-01-01",
        "Status": raw_status,
        "LegacyMigrationDestinationParentResolution": "planned_parent",
        "LegacyMigrationPlannedParentResolved": True,
    }
    a1 = AllocationRow.from_dict(d)
    a2 = AllocationRow.from_dict(a1.to_dict())
    assert a2.Status == raw_status

def test_proposed_folder_object_supported() -> None:
    pf = ProposedFolder(
        FolderName="f",
        DestinationPath="d",
        Status="Draft_LegacyReanchorFailed",
        LegacyMigrationDestinationParentResolution="graph",
    )
    assert recovered_planning_display_label(pf) == "Recovered — Graph parent resolved"
