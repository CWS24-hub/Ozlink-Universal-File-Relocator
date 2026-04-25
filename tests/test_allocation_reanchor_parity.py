"""Allocation vs proposed parity for live skeleton re-anchor (legacy Root hub is not a foreign namespace)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ozlink_console.legacy_backup_migration import MigrationIdentityPreflight, migrate_legacy_backup_folder
from ozlink_console.legacy_backup_migration.live_path_reanchor import reanchor_manifest_destination_path_against_live_skeleton
from tests.test_live_skeleton_reanchor_migration import GraphStub, _identity


def _session_with_libraries() -> dict:
    return {
        "DraftId": "D",
        "SelectedDestinationLibrary": "Documents",
        "SelectedSourceLibrary": "Files to be Migrated",
        "SelectedDestinationLibraryId": "",
        "DestinationTreeSnapshotIdentityDriveId": "",
        "DestinationTreeSnapshotIdentitySiteId": "",
    }


def _write_alloc_bundle(folder: Path, dest_path: str, *, proposed_rows: list | None = None) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    session = _session_with_libraries()
    allocations = [
        {
            "RequestId": "r1",
            "SourceItemName": "n",
            "SourcePath": r"Files to be Migrated\FTBM\x",
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
    prop = proposed_rows if proposed_rows is not None else []
    (folder / "Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")
    (folder / "Draft-AllocationQueue.json").write_text(json.dumps(allocations), encoding="utf-8")
    (folder / "Draft-ProposedFolders.json").write_text(json.dumps(prop), encoding="utf-8")


def _identity_with_libs(anchor: str = "Root3") -> MigrationIdentityPreflight:
    return MigrationIdentityPreflight(
        source_site_key="s",
        source_site_id="sid",
        source_drive_id="sd1",
        destination_site_key="d",
        destination_site_id="did",
        destination_drive_id="dd1",
        visible_destination_anchor=anchor,
        source_library_display_name="Files to be Migrated",
        destination_library_display_name="Documents",
    )


def test_a_allocation_documents_root_hr_reanchors_to_root3hr(tmp_path):
    """A. Documents\\Root\\HR -> strip -> Root\\HR -> re-anchor to Root3\\HR when live path exists."""
    src = tmp_path / "in"
    _write_alloc_bundle(src, r"Documents\Root\HR")
    g = GraphStub(root_names=["Root3"], resolve_paths={"Root3/HR"})
    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity_with_libs(), graph=g, skip_graph_resolution=True
    )
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert out[0]["RequestedDestinationPath"].replace("/", "\\") == r"Root3\HR"
    assert "ForeignRoot" not in str(out[0].get("Status", ""))


def test_b_allocation_root_projects_x_reanchors(tmp_path):
    """B. Root\\Projects\\X -> re-anchor when Root3/Projects/X resolves."""
    src = tmp_path / "in"
    _write_alloc_bundle(src, r"Root\Projects\Sub")
    g = GraphStub(
        root_names=["Root3"],
        resolve_paths={"Root3/Projects/Sub"},
    )
    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity_with_libs(), graph=g, skip_graph_resolution=True
    )
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert "Root3" in out[0]["RequestedDestinationPath"]
    assert "ForeignRoot" not in str(out[0].get("Status", ""))


def test_c_true_foreign_hub_other_site_rejected_before_reanchor(tmp_path):
    """C. OtherSiteHub\\X still rejected by engine foreign-hub guard (not re-anchor)."""
    src = tmp_path / "in"
    _write_alloc_bundle(src, r"OtherSiteHub\Team")
    g = GraphStub(root_names=["Root3"], resolve_paths=set())
    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity_with_libs("Root3"), graph=g, skip_graph_resolution=True
    )
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert "ForeignHubRejected" in str(out[0].get("Status", ""))


def test_d_proposed_parent_matches_allocation_reanchor_rule(tmp_path):
    """D. Same Graph stub: allocation and proposed ParentPath both re-anchor Root -> Root3."""
    src = tmp_path / "in"
    _write_alloc_bundle(
        src,
        r"Documents\Root\Dept\Leaf",
        proposed_rows=[
            {
                "FolderName": "f",
                "DestinationPath": "",
                "ParentPath": r"Documents\Root\Dept",
                "DestinationDriveId": "",
                "DestinationParentItemId": "",
                "StableKey": "k",
            }
        ],
    )
    g = GraphStub(
        root_names=["Root3"],
        resolve_paths={
            "Root3/Dept/Leaf",
            "Root3/Dept",
        },
    )
    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity_with_libs(), graph=g, skip_graph_resolution=True
    )
    assert res.ok
    alloc = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    prop = json.loads((res.output_folder / "Draft-ProposedFolders.json").read_text(encoding="utf-8"))
    assert "Root3" in alloc[0]["RequestedDestinationPath"]
    assert "ForeignRoot" not in str(alloc[0].get("Status", ""))
    pp = prop[0]["ParentPath"].replace("/", "\\")
    assert pp.startswith("Root3\\")


@pytest.mark.parametrize(
    "path,expect_kind",
    [
        (r"Root\No\Match\Here", "failed"),
        (r"Documents\OrphanOnly", "foreign_root_blocked"),
    ],
)
def test_unit_reanchor_root_vs_documents_when_unresolved(path, expect_kind):
    g = GraphStub(root_names=["Root3"], resolve_paths=set())
    o = reanchor_manifest_destination_path_against_live_skeleton(
        path,
        destination_drive_id="d",
        graph=g,
        live_top_level_names=["Root3"],
        row_index=0,
        row_kind="allocation",
    )
    assert o.kind == expect_kind
