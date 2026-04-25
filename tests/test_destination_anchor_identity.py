"""Destination visible-anchor: Graph item id authority vs mutable path/name display state."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ozlink_console.destination_anchor_delta import (
    AnchorDeltaKind,
    classify_anchor_tree_observation,
    rebase_allocation_and_proposed_paths_for_anchor_rename,
    rebase_path_under_anchor_prefix,
)
from ozlink_console.legacy_backup_migration import migrate_legacy_backup_folder
from ozlink_console.legacy_backup_migration.types import MigrationIdentityPreflight


@pytest.fixture
def _legacy_bundle_writer(tmp_path: Path):
    def _go(folder: Path) -> None:
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
                "RequestedDestinationPath": r"X\HR\New",
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
                "DestinationPath": r"X\PF1",
                "ParentPath": r"X",
                "DestinationDriveId": "",
                "DestinationParentItemId": "",
                "StableKey": "",
            }
        ]
        (folder / "Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")
        (folder / "Draft-AllocationQueue.json").write_text(json.dumps(allocations), encoding="utf-8")
        (folder / "Draft-ProposedFolders.json").write_text(json.dumps(proposed), encoding="utf-8")

    return _go


def test_a_migration_stamps_destination_anchor_item_id_in_session(tmp_path: Path, _legacy_bundle_writer) -> None:
    src = tmp_path / "legacy"
    _legacy_bundle_writer(src)
    ident = MigrationIdentityPreflight(
        source_site_key="s",
        source_site_id="sid",
        source_drive_id="b!ssssssssssssssssssssssssssssssss",
        destination_site_key="d",
        destination_site_id="did",
        destination_drive_id="b!dddddddddddddddddddddddddddddddddd",
        visible_destination_anchor="VisibleHub",
        destination_anchor_item_id="anchor-graph-id-99",
        destination_anchor_drive_id="b!dddddddddddddddddddddddddddddddddd",
        destination_anchor_display_path=r"Lib\VisibleHub",
        destination_anchor_path_verified_at_utc="2099-01-01T00:00:00Z",
        destination_anchor_path_only_binding=False,
    )
    res = migrate_legacy_backup_folder(src, tmp_path, identity=ident, skip_graph_resolution=True)
    assert res.ok and res.output_folder
    sess = json.loads((res.output_folder / "Draft-SessionState.json").read_text(encoding="utf-8"))
    assert sess.get("DestinationAnchorItemId") == "anchor-graph-id-99"
    assert sess.get("DestinationAnchorDriveId") == "b!dddddddddddddddddddddddddddddddddd"
    assert r"VisibleHub" in str(sess.get("DestinationAnchorDisplayPath", ""))


def test_b_rename_updates_child_paths_not_identity() -> None:
    old_a = r"Lib\OldSeg"
    new_a = r"Lib\NewSeg"
    assert rebase_path_under_anchor_prefix(old_a, old_a, new_a) == new_a
    assert (
        rebase_path_under_anchor_prefix(r"Lib\OldSeg\Dept\File", old_a, new_a) == r"Lib\NewSeg\Dept\File"
    )
    n, alloc, prop = rebase_allocation_and_proposed_paths_for_anchor_rename(
        [
            {
                "RequestedDestinationPath": r"Lib\OldSeg\A",
                "DestinationParentPlannedPath": "",
            }
        ],
        [{"DestinationPath": r"Lib\OldSeg\B", "ParentPath": r"Lib\OldSeg", "DestinationParentPlannedPath": ""}],
        old_anchor_path=old_a,
        new_anchor_path=new_a,
    )
    assert n >= 1
    assert r"NewSeg" in alloc[0]["RequestedDestinationPath"]
    assert r"NewSeg" in prop[0]["DestinationPath"]


def test_c_different_graph_item_id_is_not_same_anchor() -> None:
    assert (
        classify_anchor_tree_observation(
            stored_item_id="id-old",
            stored_display_path=r"Lib\Seg",
            live_item_id="id-other",
            live_canonical_path=r"Lib\Seg",
        )
        == AnchorDeltaKind.MISSING
    )


def test_d_anchor_absent_when_live_item_unknown() -> None:
    assert (
        classify_anchor_tree_observation(
            stored_item_id="tracked",
            stored_display_path=r"Lib\Seg",
            live_item_id=None,
            live_canonical_path=None,
        )
        == AnchorDeltaKind.MISSING
    )


def test_e_same_item_id_rename_is_renamed_not_foreign() -> None:
    assert (
        classify_anchor_tree_observation(
            stored_item_id="same",
            stored_display_path=r"Lib\Old",
            live_item_id="same",
            live_canonical_path=r"Lib\NewName",
        )
        == AnchorDeltaKind.RENAMED
    )


def test_f_migration_report_distinguishes_anchor_identity_from_display_path(tmp_path: Path, _legacy_bundle_writer) -> None:
    src = tmp_path / "legacy"
    _legacy_bundle_writer(src)
    ident = MigrationIdentityPreflight(
        source_site_key="s",
        source_site_id="sid",
        source_drive_id="b!ssssssssssssssssssssssssssssssss",
        destination_site_key="d",
        destination_site_id="did",
        destination_drive_id="b!dddddddddddddddddddddddddddddddddd",
        visible_destination_anchor="Seg",
        destination_anchor_item_id="gid-1",
        destination_anchor_drive_id="b!dddddddddddddddddddddddddddddddddd",
        destination_anchor_display_path=r"Root\Seg",
        destination_anchor_path_verified_at_utc="2099-01-02T00:00:00Z",
        destination_anchor_path_only_binding=False,
    )
    res = migrate_legacy_backup_folder(src, tmp_path, identity=ident, skip_graph_resolution=True)
    rep = json.loads((res.output_folder / "LegacyMigrationReport.json").read_text(encoding="utf-8"))
    ident_rep = (rep.get("identity") or {}).get("destination_visible_anchor") or {}
    assert ident_rep.get("graph_item_id") == "gid-1"
    assert ident_rep.get("display_path_canonical") == r"Root\Seg"
    assert ident_rep.get("top_segment_display_name") == "Seg"
