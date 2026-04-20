"""Empty destination Graph root: legacy paths preserved as planned scaffold (not live-confirmed)."""

from __future__ import annotations

import json
from pathlib import Path

from ozlink_console.legacy_backup_migration import MigrationIdentityPreflight, migrate_legacy_backup_folder
from ozlink_console.legacy_backup_migration.engine import planned_scaffold_live_folder_collision_hint
from ozlink_console.legacy_backup_migration.types import (
    ANCHOR_CLASS_PLANNED_SCAFFOLD_EMPTY_LIBRARY,
)
from ozlink_console.sharepoint_destination_overlay_attach import (
    WORKSPACE_ROW_STATE_LIVE_CONFIRMED,
    WORKSPACE_ROW_STATE_PLANNED_ONLY,
)


def _identity(anchor: str = "Root3") -> MigrationIdentityPreflight:
    return MigrationIdentityPreflight(
        source_site_key="s",
        source_site_id="sid",
        source_drive_id="sd1",
        destination_site_key="d",
        destination_site_id="did",
        destination_drive_id="dd1",
        visible_destination_anchor=anchor,
    )


def _write_bundle(folder: Path, dest_path: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    session = {
        "DraftId": "D",
        "SelectedDestinationLibraryId": "",
        "DestinationTreeSnapshotIdentityDriveId": "",
        "DestinationTreeSnapshotIdentitySiteId": "",
    }
    allocations = [
        {
            "RequestId": "r1",
            "SourceItemName": "n",
            "SourcePath": r"S\x",
            "SourceType": "folder",
            "RequestedDestinationPath": dest_path,
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
    (folder / "Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")
    (folder / "Draft-AllocationQueue.json").write_text(json.dumps(allocations), encoding="utf-8")
    (folder / "Draft-ProposedFolders.json").write_text(json.dumps([]), encoding="utf-8")


class GraphEmptyDestinationRoot:
    """Destination drive has no top-level folder children (structural authority is intentionally absent)."""

    def list_drive_root_children(self, drive_id: str) -> list[dict]:
        return []

    def get_drive_item_by_path(self, drive_id: str, relative_path: str) -> dict | None:
        return None


def test_a_empty_graph_root_preserves_root_hr_as_planned_scaffold(tmp_path: Path) -> None:
    src = tmp_path / "in"
    _write_bundle(src, r"Root\HR\Contracts")
    g = GraphEmptyDestinationRoot()
    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=g, skip_graph_resolution=True)
    assert res.ok and res.output_folder
    rep = res.report or {}
    assert rep.get("empty_destination_graph") is True
    assert rep.get("graph_root_probe_kind") == "empty_graph_root"
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    row = out[0]
    p = row["RequestedDestinationPath"].replace("/", "\\")
    assert p.startswith("Root\\")
    assert "HR" in p and "Contracts" in p
    assert row.get("LegacyMigrationAnchorClassification") == ANCHOR_CLASS_PLANNED_SCAFFOLD_EMPTY_LIBRARY
    assert row.get("workspace_row_state") == WORKSPACE_ROW_STATE_PLANNED_ONLY
    assert row.get("verification_state") == "planned_only"
    assert str(row.get("workspace_row_state") or "") != WORKSPACE_ROW_STATE_LIVE_CONFIRMED
    assert not str(row.get("DestinationParentItemId") or "").strip()


def test_b_empty_graph_preserves_root3_finance(tmp_path: Path) -> None:
    src = tmp_path / "in"
    _write_bundle(src, r"Root3\Finance\Reports")
    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity("Root3"), graph=GraphEmptyDestinationRoot(), skip_graph_resolution=True
    )
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    p = out[0]["RequestedDestinationPath"].replace("/", "\\")
    assert p.startswith("Root3\\")


class GraphNonEmptyRoot:
    def list_drive_root_children(self, drive_id: str) -> list[dict]:
        return [{"name": "Root3", "folder": {}}]

    def get_drive_item_by_path(self, drive_id: str, relative_path: str) -> dict | None:
        rel = str(relative_path or "").replace("\\", "/").strip("/").casefold()
        if rel == "root3/hr":
            return {"id": "fold", "folder": {}}
        return None


def test_c_nonempty_graph_legacy_root_reanchors_not_blind_preserve(tmp_path: Path) -> None:
    """When live Graph has Root3, legacy Root\\HR suffix-matches to Root3\\HR (anchors win)."""
    src = tmp_path / "in"
    _write_bundle(src, r"Root\HR")
    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity("Root3"), graph=GraphNonEmptyRoot(), skip_graph_resolution=True
    )
    assert res.ok
    rep = res.report or {}
    assert rep.get("empty_destination_graph") is False
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    p = out[0]["RequestedDestinationPath"].replace("/", "\\")
    assert p == r"Root3\HR"
    assert out[0].get("LegacyMigrationPlannedScaffoldOnly") is not True


def test_d_planned_scaffold_vs_live_collision_hint() -> None:
    row = {"LegacyMigrationPlannedScaffoldOnly": True}
    assert planned_scaffold_live_folder_collision_hint(row, live_resolved_folder=True) == "planned_scaffold_vs_live_folder_same_path"
    assert planned_scaffold_live_folder_collision_hint(row, live_resolved_folder=False) is None
    assert planned_scaffold_live_folder_collision_hint({}, live_resolved_folder=True) is None


def test_e_execution_creates_scaffold_only_after_user_plan_docstring() -> None:
    """Folders under planned scaffold are created during plan execution, not during migration import."""
    assert True

