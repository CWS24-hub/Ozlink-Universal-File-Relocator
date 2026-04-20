"""Planned/proposed parent index fallback for legacy allocation migration."""

from __future__ import annotations

import json
from pathlib import Path

from ozlink_console.legacy_backup_migration import MigrationIdentityPreflight, migrate_legacy_backup_folder
from ozlink_console.models import AllocationRow


def _identity(anchor: str) -> MigrationIdentityPreflight:
    return MigrationIdentityPreflight(
        source_site_key="https://contoso.sharepoint.com/sites/S1",
        destination_site_key="https://contoso.sharepoint.com/sites/S1",
        source_drive_id="srcDrive",
        destination_drive_id="dstDrive",
        visible_destination_anchor=anchor,
    )


def _write_session(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.joinpath("Draft-SessionState.json").write_text(
        json.dumps({"SelectedDestinationLibraryId": "dstDrive", "SelectedSourceLibraryId": "srcDrive"}),
        encoding="utf-8",
    )


def test_a_allocation_maps_to_planned_parent_root3_hr(tmp_path):
    """Proposed Root3\\HR exists; allocation under Root\\HR maps to planned parent."""
    src = tmp_path / "in"
    _write_session(src)
    src.joinpath("Draft-AllocationQueue.json").write_text(
        json.dumps(
            [
                {
                    "RequestId": "1",
                    "SourceItemName": "f",
                    "SourcePath": "s\\f",
                    "SourceType": "File",
                    "RequestedDestinationPath": r"Root\HR\File.xlsx",
                    "AllocationMethod": "m",
                    "RequestedBy": "",
                    "RequestedDate": "",
                    "Status": "Draft",
                }
            ]
        ),
        encoding="utf-8",
    )
    src.joinpath("Draft-ProposedFolders.json").write_text(
        json.dumps(
            [
                {
                    "FolderName": "Policies",
                    "DestinationPath": "",
                    "ParentPath": r"Root3\HR",
                    "StableKey": "sk",
                }
            ]
        ),
        encoding="utf-8",
    )

    class G:
        def list_drive_root_children(self, drive_id: str):
            return [{"name": "Root3", "folder": {}}]

        def get_drive_item_by_path(self, drive_id: str, rel_path: str):
            return None

    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity("Root3"), graph=G(), skip_graph_resolution=False
    )
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    row = out[0]
    assert r"Root3\HR" in row["RequestedDestinationPath"].replace("/", "\\")
    assert row.get("LegacyMigrationPlannedParentResolved") is True
    assert row.get("DestinationParentItemId") in ("", None)
    assert row.get("LegacyMigrationDestinationParentResolution") == "planned_parent"


def test_b_deeper_chain_parent_planned(tmp_path):
    """Proposed chain Root3\\HR\\Employee Files — allocation under matching Root\\HR\\Employee Files."""
    src = tmp_path / "in"
    _write_session(src)
    src.joinpath("Draft-AllocationQueue.json").write_text(
        json.dumps(
            [
                {
                    "RequestId": "1",
                    "SourceItemName": "x",
                    "SourcePath": "s\\x",
                    "SourceType": "Folder",
                    "RequestedDestinationPath": r"Root\HR\Employee Files\Team",
                    "AllocationMethod": "m",
                    "RequestedBy": "",
                    "RequestedDate": "",
                    "Status": "Draft",
                }
            ]
        ),
        encoding="utf-8",
    )
    src.joinpath("Draft-ProposedFolders.json").write_text(
        json.dumps(
            [
                {
                    "FolderName": "Employee Files",
                    "DestinationPath": "",
                    "ParentPath": r"Root3\HR",
                    "StableKey": "a",
                }
            ]
        ),
        encoding="utf-8",
    )

    class G:
        def list_drive_root_children(self, drive_id: str):
            return [{"name": "Root3", "folder": {}}]

        def get_drive_item_by_path(self, drive_id: str, rel_path: str):
            return None

    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity("Root3"), graph=G(), skip_graph_resolution=False
    )
    assert res.ok
    rep = res.report["counts"]
    assert rep.get("allocation_planned_parent_resolved", 0) >= 1


def test_c_root_to_root3_aligns_with_proposed(tmp_path):
    """When only Root3 is live at root, Root\\Finance aligns to Root3 for planned lookup."""
    src = tmp_path / "in"
    _write_session(src)
    src.joinpath("Draft-AllocationQueue.json").write_text(
        json.dumps(
            [
                {
                    "RequestId": "1",
                    "SourceItemName": "x",
                    "SourcePath": "s",
                    "SourceType": "File",
                    "RequestedDestinationPath": r"Root\Finance\a.txt",
                    "AllocationMethod": "m",
                    "RequestedBy": "",
                    "RequestedDate": "",
                    "Status": "Draft",
                }
            ]
        ),
        encoding="utf-8",
    )
    src.joinpath("Draft-ProposedFolders.json").write_text(
        json.dumps([{"FolderName": "F", "DestinationPath": "", "ParentPath": r"Root3\Finance", "StableKey": "s"}]),
        encoding="utf-8",
    )

    class G:
        def list_drive_root_children(self, drive_id: str):
            return [{"name": "Root3", "folder": {}}]

        def get_drive_item_by_path(self, drive_id: str, rel_path: str):
            return None

    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity("Root3"), graph=G(), skip_graph_resolution=False
    )
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert "Root3\\Finance" in out[0]["RequestedDestinationPath"].replace("/", "\\")


def test_d_graph_parent_wins_no_fake_planned_id(tmp_path):
    """When Graph resolves parent, DestinationParentItemId is set and planned_parent is not used."""

    class G:
        def list_drive_root_children(self, drive_id: str):
            return [{"name": "Root3", "folder": {}}]

        def get_drive_item_by_path(self, drive_id: str, rel_path: str):
            if "parent" in rel_path.replace("\\", "/").lower():
                return {"id": "pid-graph", "folder": {}}
            return None

    src = tmp_path / "in"
    _write_session(src)
    src.joinpath("Draft-AllocationQueue.json").write_text(
        json.dumps(
            [
                {
                    "RequestId": "1",
                    "SourceItemName": "f",
                    "SourcePath": "s\\f",
                    "SourceType": "File",
                    "RequestedDestinationPath": r"Root3\Parent\f.txt",
                    "AllocationMethod": "m",
                    "RequestedBy": "",
                    "RequestedDate": "",
                    "Status": "Draft",
                }
            ]
        ),
        encoding="utf-8",
    )
    src.joinpath("Draft-ProposedFolders.json").write_text(json.dumps([]), encoding="utf-8")

    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=G(), skip_graph_resolution=False)
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert out[0].get("DestinationParentItemId") == "pid-graph"
    assert out[0].get("LegacyMigrationPlannedParentResolved") is not True


def test_e_neither_graph_nor_planned_unresolved(tmp_path):
    class G:
        def list_drive_root_children(self, drive_id: str):
            return [{"name": "Root3", "folder": {}}]

        def get_drive_item_by_path(self, drive_id: str, rel_path: str):
            return None

    src = tmp_path / "in"
    _write_session(src)
    src.joinpath("Draft-AllocationQueue.json").write_text(
        json.dumps(
            [
                {
                    "RequestId": "1",
                    "SourceItemName": "f",
                    "SourcePath": "s",
                    "SourceType": "File",
                    "RequestedDestinationPath": r"Root3\Nowhere\Deep\f.txt",
                    "AllocationMethod": "m",
                    "RequestedBy": "",
                    "RequestedDate": "",
                    "Status": "Draft",
                }
            ]
        ),
        encoding="utf-8",
    )
    src.joinpath("Draft-ProposedFolders.json").write_text(json.dumps([]), encoding="utf-8")

    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=G(), skip_graph_resolution=False)
    assert res.ok
    assert res.report["counts"].get("allocation_parent_unresolved_after_planned_lookup", 0) >= 1


def test_f_partial_ancestor_missing_descendant_classification(tmp_path):
    """Gap under a planned folder yields missing_descendant kind when next step is not in index."""

    class G:
        def list_drive_root_children(self, drive_id: str):
            return [{"name": "Root3", "folder": {}}]

        def get_drive_item_by_path(self, drive_id: str, rel_path: str):
            return None

    src = tmp_path / "in"
    _write_session(src)
    src.joinpath("Draft-AllocationQueue.json").write_text(
        json.dumps(
            [
                {
                    "RequestId": "1",
                    "SourceItemName": "f",
                    "SourcePath": "s",
                    "SourceType": "File",
                    "RequestedDestinationPath": r"Root3\HR\Missing\Deep\f.txt",
                    "AllocationMethod": "m",
                    "RequestedBy": "",
                    "RequestedDate": "",
                    "Status": "Draft",
                }
            ]
        ),
        encoding="utf-8",
    )
    src.joinpath("Draft-ProposedFolders.json").write_text(
        json.dumps([{"FolderName": "X", "DestinationPath": "", "ParentPath": r"Root3\HR", "StableKey": "k"}]),
        encoding="utf-8",
    )

    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=G(), skip_graph_resolution=False)
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert out[0].get("LegacyMigrationPlannedParentMatchKind") in ("missing_descendant", "ancestor", "exact")


def test_g_no_fake_destination_parent_item_id_for_planned(tmp_path):
    class G:
        def list_drive_root_children(self, drive_id: str):
            return [{"name": "Root3", "folder": {}}]

        def get_drive_item_by_path(self, drive_id: str, rel_path: str):
            return None

    src = tmp_path / "in"
    _write_session(src)
    src.joinpath("Draft-AllocationQueue.json").write_text(
        json.dumps(
            [
                {
                    "RequestId": "1",
                    "SourceItemName": "f",
                    "SourcePath": "s",
                    "SourceType": "File",
                    "RequestedDestinationPath": r"Root3\HR\f.txt",
                    "AllocationMethod": "m",
                    "RequestedBy": "",
                    "RequestedDate": "",
                    "Status": "Draft",
                }
            ]
        ),
        encoding="utf-8",
    )
    src.joinpath("Draft-ProposedFolders.json").write_text(
        json.dumps([{"FolderName": "P", "DestinationPath": "", "ParentPath": r"Root3\HR", "StableKey": "k"}]),
        encoding="utf-8",
    )

    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=G(), skip_graph_resolution=False)
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert not str(out[0].get("DestinationParentItemId") or "").strip()


def test_h_planned_metadata_round_trips_through_allocation_row():
    raw = {
        "RequestId": "1",
        "SourceItemName": "n",
        "SourcePath": "s",
        "SourceType": "f",
        "RequestedDestinationPath": "d",
        "AllocationMethod": "m",
        "RequestedBy": "",
        "RequestedDate": "",
        "LegacyMigrationPlannedParentResolved": True,
        "LegacyMigrationDestinationParentResolution": "planned_parent",
        "DestinationParentPlannedPath": r"Root3\HR",
        "LegacyMigrationPlannedParentMatchKind": "exact",
    }
    row = AllocationRow.from_dict(raw)
    out = row.to_dict()
    assert out["LegacyMigrationPlannedParentResolved"] is True
    assert out["DestinationParentPlannedPath"] == r"Root3\HR"
    assert out["Status"] == "Pending"
