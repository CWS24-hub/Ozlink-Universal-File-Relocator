"""Live Graph skeleton re-anchoring for legacy backup migration (semantic, not hardcoded hub maps)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ozlink_console.legacy_backup_migration import MigrationIdentityPreflight, migrate_legacy_backup_folder
from ozlink_console.legacy_backup_migration.live_path_reanchor import (
    reanchor_manifest_destination_path_against_live_skeleton,
    strip_internal_root_when_not_live_top_name,
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


class GraphStub:
    """Minimal Graph: shallow root listing + path resolution rules for tests."""

    def __init__(
        self,
        *,
        root_names: list[str],
        resolve_paths: set[str],
        ambiguous_paths: set[str] | None = None,
    ) -> None:
        self.root_names = root_names
        self.resolve_paths = {p.replace("\\", "/").strip("/").casefold() for p in resolve_paths}
        self.ambiguous_paths = (
            {p.replace("\\", "/").strip("/").casefold() for p in (ambiguous_paths or set())}
            if ambiguous_paths is not None
            else set()
        )

    def list_drive_root_children(self, drive_id: str) -> list[dict]:
        return [{"name": n, "folder": {}} for n in self.root_names]

    def get_drive_item_by_path(self, drive_id: str, relative_path: str) -> dict | None:
        rel = str(relative_path or "").replace("\\", "/").strip("/")
        cf = rel.casefold()
        if cf in self.ambiguous_paths:
            return {"id": "x", "folder": {}}
        if cf in self.resolve_paths:
            return {"id": f"id-{cf}", "folder": {"childCount": 0}}
        return None


def _write_min_bundle(folder: Path, dest_path: str) -> None:
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


def test_a_integration_root_hr_to_root3_hr(tmp_path):
    """Legacy Root\\HR re-anchors to Root3\\HR when exactly one live path resolves."""
    src = tmp_path / "in"
    _write_min_bundle(src, r"Root\HR")
    g = GraphStub(
        root_names=["Root3"],
        resolve_paths={"Root3/HR"},
    )
    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=g, skip_graph_resolution=True)
    assert res.ok and res.output_folder
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert out[0]["RequestedDestinationPath"] == r"Root3\HR"
    assert not out[0]["RequestedDestinationPath"].lower().startswith(r"root\hr")


def test_b_does_not_emit_stale_root_prefix_when_live_is_root3(tmp_path):
    src = tmp_path / "in"
    _write_min_bundle(src, r"Root\Dept\X")
    g = GraphStub(
        root_names=["Root3"],
        resolve_paths={"Root3/Dept/X"},
    )
    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=g, skip_graph_resolution=True)
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    p = out[0]["RequestedDestinationPath"].replace("/", "\\")
    assert p.startswith("Root3\\")
    assert not p.startswith("Root\\Dept")  # must not preserve legacy synthetic Root as first segment


def test_c_synthetic_internal_root_stripped_only_when_safe(tmp_path):
    """Leading legacy internal Root\\ may be stripped when first segment is not a live top name."""
    src = tmp_path / "in"
    _write_min_bundle(src, r"Root\HR")
    g = GraphStub(root_names=["Root3"], resolve_paths={"Root3/HR"})
    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=g, skip_graph_resolution=True)
    assert res.ok
    rep = res.report or {}
    assert int(rep.get("counts", {}).get("synthetic_root_stripped", 0)) >= 1


def test_d_business_folder_root_preserved(tmp_path):
    """A real top-level folder named Root is not treated as a strip-only legacy token."""
    src = tmp_path / "in"
    _write_min_bundle(src, r"Root\Projects")
    g = GraphStub(
        root_names=["Root", "Root3"],
        resolve_paths={"Root/Projects", "Root3/HR"},
    )
    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=g, skip_graph_resolution=True)
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert r"Root\Projects" in out[0]["RequestedDestinationPath"] or "Root/Projects" in out[0]["RequestedDestinationPath"]


def test_e_ambiguous_live_anchors_marks_unresolved(tmp_path):
    src = tmp_path / "in"
    _write_min_bundle(src, r"LegacyPrefix\HR")
    g = GraphStub(
        root_names=["Alpha", "Beta"],
        resolve_paths={"Alpha/HR", "Beta/HR"},
    )
    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Alpha"), graph=g, skip_graph_resolution=True)
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert "LegacyReanchorAmbiguous" in str(out[0].get("Status", ""))


def test_f_no_suffix_match_marks_failed_or_unresolved(tmp_path):
    src = tmp_path / "in"
    _write_min_bundle(src, r"OldTop\Z99\Missing")
    g = GraphStub(root_names=["Root3"], resolve_paths=set())
    res = migrate_legacy_backup_folder(src, tmp_path, identity=_identity("Root3"), graph=g, skip_graph_resolution=True)
    assert res.ok
    out = json.loads((res.output_folder / "Draft-AllocationQueue.json").read_text(encoding="utf-8"))
    assert "LegacyReanchor" in str(out[0].get("Status", "")) or "ForeignRoot" in str(out[0].get("Status", ""))


def test_g_conflict_when_live_folder_exists_at_target(tmp_path):
    src = tmp_path / "in"
    _write_min_bundle(src, r"Root\HR\NewFolder")
    g = GraphStub(
        root_names=["Root3"],
        resolve_paths={"Root3/HR/NewFolder"},
    )

    class G(GraphStub):
        def get_drive_item_by_path(self, drive_id: str, relative_path: str):
            rel = str(relative_path or "").replace("\\", "/").strip("/")
            if rel.casefold() == "root3/hr/newfolder".casefold():
                return {"id": "live", "folder": {}}
            return super().get_drive_item_by_path(drive_id, relative_path)

    res = migrate_legacy_backup_folder(
        src, tmp_path, identity=_identity("Root3"), graph=G(root_names=["Root3"], resolve_paths={"Root3/HR/NewFolder"})
    )
    assert res.ok
    rep = res.report or {}
    assert int(rep.get("counts", {}).get("live_duplicate_detected", 0)) >= 1
    assert any("live_duplicate" in str(c.get("kind", "")) for c in (rep.get("conflicts") or []))


def test_unit_strip_respects_live_folder_named_root():
    live = ["Root", "Root3"]
    s, _ = strip_internal_root_when_not_live_top_name(
        r"Root\Sub",
        live_top_level_names=live,
        row_index=0,
        row_kind="test",
    )
    assert s == r"Root\Sub"


def test_unit_reanchor_unique():
    g = GraphStub(root_names=["Root3"], resolve_paths={"Root3/HR/Contracts"})
    o = reanchor_manifest_destination_path_against_live_skeleton(
        r"Contoso\HR\Contracts",
        destination_drive_id="d",
        graph=g,
        live_top_level_names=["Root3"],
        row_index=0,
        row_kind="allocation",
    )
    assert o.kind == "reanchored"
    assert o.path_manifest == r"Root3\HR\Contracts"
