"""Tests for draft_snapshot foundation (contracts, adapters, detached, environment, run_log)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ozlink_console.draft_snapshot import (
    SNAPSHOT_SCHEMA_V1,
    ConnectedEnvironmentContext,
    SnapshotPipelineEvent,
    from_bundle_folder,
    from_req_payload_dict,
    load_detached_from_bundle_folder,
    load_detached_from_canonical_json_bytes,
    new_snapshot_id,
    normalize_submitted_snapshot_id,
    parse_canonical_submitted_snapshot_dict,
    to_library_relative_path,
    validate_environment_against_snapshot,
    validate_snapshot_minimal_ready,
    validation_report_as_dict,
)
from ozlink_console.version_info import APP_VERSION
from ozlink_console.draft_snapshot.contracts import _snapshot_hash_from_body_dict
from ozlink_console.draft_snapshot.errors import SnapshotValidationError


def _minimal_canonical_dict() -> dict:
    sid2 = new_snapshot_id()
    return {
        "snapshot_schema": SNAPSHOT_SCHEMA_V1,
        "engine_version": 1,
        "snapshot_id": sid2,
        "draft_id": "draft-1",
        "draft_version": "1",
        "submitted_at_utc": "2026-01-01T00:00:00+00:00",
        "submitted_by": "tester",
        "app_version": "test",
        "tenant": {"tenant_id": "tid-1", "tenant_domain": "contoso.com", "tenant_label": "Contoso", "client_key": ""},
        "source": {
            "platform": "sharepoint",
            "site_library": {
                "site_id": "src-site",
                "site_name": "Source",
                "library_drive_id": "drive-src",
                "library_name": "LibSrc",
            },
        },
        "destination": {
            "platform": "sharepoint",
            "site_library": {
                "site_id": "dst-site",
                "site_name": "Dest",
                "library_drive_id": "drive-dst",
                "library_name": "LibDst",
            },
        },
        "mapping_items": [
            {
                "mapping_id": "m1",
                "item_type": "file",
                "source_path": "a/b.txt",
                "source_name": "b.txt",
                "destination_path": "c/",
                "destination_name": "b.txt",
                "assignment_mode": "move",
            }
        ],
        "proposed_folder_items": [],
        "execution_options": {},
    }


def test_parse_canonical_and_roundtrip_hash():
    d = _minimal_canonical_dict()
    snap = parse_canonical_submitted_snapshot_dict(d)
    assert snap.snapshot_id == d["snapshot_id"]
    assert snap.snapshot_hash
    d2 = snap.to_json_dict()
    assert d2["snapshot_hash"] == snap.snapshot_hash
    snap2 = parse_canonical_submitted_snapshot_dict(d2)
    assert snap2.snapshot_hash == snap.snapshot_hash


def test_normalize_submitted_snapshot_id_accepts_uuid_and_legacy_strings():
    n, leg = normalize_submitted_snapshot_id("550E8400-E29B-41D4-A716-446655440000")
    assert n == "550e8400e29b41d4a716446655440000"
    assert leg == "550E8400-E29B-41D4-A716-446655440000"
    n2, leg2 = normalize_submitted_snapshot_id("REQ-2026-legacy")
    assert len(n2) == 32
    assert leg2 == "REQ-2026-legacy"


def test_parse_accepts_dashed_uuid_snapshot_id():
    d = _minimal_canonical_dict()
    d["snapshot_id"] = "550e8400-e29b-41d4-a716-446655440000"
    snap = parse_canonical_submitted_snapshot_dict(d)
    assert snap.snapshot_id == "550e8400e29b41d4a716446655440000"
    assert snap.snapshot_id_submitted == "550e8400-e29b-41d4-a716-446655440000"


def test_legacy_snapshot_hash_when_wire_id_was_dashed_uuid():
    """Older payloads may sign hash over dashed ``snapshot_id``; import still normalizes."""
    hex_id = "550e8400e29b41d4a716446655440000"
    dashed = "550e8400-e29b-41d4-a716-446655440000"
    d0 = _minimal_canonical_dict()
    d0["snapshot_id"] = hex_id
    sn = parse_canonical_submitted_snapshot_dict(d0)
    body = dict(sn.to_json_dict())
    body.pop("snapshot_hash", None)
    body["snapshot_id"] = dashed
    body.pop("snapshot_id_submitted", None)
    legacy_h = _snapshot_hash_from_body_dict(body)
    d1 = _minimal_canonical_dict()
    d1["snapshot_id"] = dashed
    d1["snapshot_hash"] = legacy_h
    sf = parse_canonical_submitted_snapshot_dict(d1)
    assert sf.snapshot_hash == sn.snapshot_hash
    assert sf.snapshot_id == hex_id


def test_parse_rejects_bad_hash():
    d = _minimal_canonical_dict()
    snap = parse_canonical_submitted_snapshot_dict(d)
    d_bad = snap.to_json_dict()
    d_bad["snapshot_hash"] = "0" * 64
    with pytest.raises(SnapshotValidationError):
        parse_canonical_submitted_snapshot_dict(d_bad)


def test_req_adapter_normalizes():
    req = {
        "RequestId": "REQ-1",
        "Tenant": {"TenantId": "t1", "TenantLabel": "L"},
        "SourceContext": {"SiteName": "S", "DriveId": "d1", "LibraryName": "L1"},
        "DestinationContext": {"SiteName": "D", "DriveId": "d2"},
        "PlannedMoves": [
            {
                "RequestId": "r1",
                "SourceItemName": "f.txt",
                "SourcePath": "p/f.txt",
                "SourceType": "File",
                "RequestedDestinationPath": "q/",
                "AllocationMethod": "Move",
                "RequestedBy": "u",
                "RequestedDate": "2026-01-01",
                "Status": "Pending",
                "SourceItemId": "id1",
                "SourceDriveId": "sd",
            }
        ],
        "ProposedFolders": [],
        "SubmittedBy": {"DisplayName": "Alice"},
        "CreatedOn": "2026-01-02T00:00:00",
        "Version": "Python-PySide6-v1",
    }
    snap = from_req_payload_dict(req)
    assert snap.mapping_items[0].mapping_id == "r1"
    assert snap.mapping_items[0].source_graph_item_id == "id1"
    assert snap.source.site_library.library_drive_id == "d1"
    assert snap.destination.site_library.site_name == "D"
    assert len(snap.snapshot_id) == 32
    assert snap.draft_version == "Python-PySide6-v1"
    assert snap.app_version == ""

    req2 = {**req, "DraftVersion": "draft-content-v3", "AppVersion": "2.8.0"}
    snap2 = from_req_payload_dict(req2)
    assert snap2.draft_version == "draft-content-v3"
    assert snap2.app_version == "2.8.0"


def test_bundle_folder_adapter(tmp_path: Path):
    session = {
        "DraftId": "bundle-draft",
        "SelectedSourceSite": "SrcSite",
        "SelectedSourceSiteKey": "src-key",
        "SelectedSourceLibrary": "SrcLib",
        "SelectedDestinationSite": "DstSite",
        "SelectedDestinationSiteKey": "dst-key",
        "SelectedDestinationLibrary": "DstLib",
        "SourceSelectedPath": "",
        "DestinationSelectedPath": "",
        "SourceTreeSnapshot": [],
        "DestinationTreeSnapshot": [],
    }
    allocations = [
        {
            "RequestId": "a1",
            "SourceItemName": "x",
            "SourcePath": "p/x",
            "SourceType": "Folder",
            "RequestedDestinationPath": "q",
            "AllocationMethod": "RecursiveMove",
            "RequestedBy": "u",
            "RequestedDate": "2026-01-01",
            "Status": "Pending",
        }
    ]
    proposed: list = []
    tmp_path.joinpath("Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")
    tmp_path.joinpath("Draft-AllocationQueue.json").write_text(json.dumps(allocations), encoding="utf-8")
    tmp_path.joinpath("Draft-ProposedFolders.json").write_text(json.dumps(proposed), encoding="utf-8")
    snap = from_bundle_folder(tmp_path)
    assert snap.draft_id == "bundle-draft"
    assert snap.source.site_library.site_name == "SrcSite"
    assert snap.mapping_items[0].item_type == "folder"
    assert snap.mapping_items[0].assignment_mode == "move_recursive"


def test_req_adapter_strips_library_prefix_on_proposed_paths():
    req = {
        "RequestId": "REQ-x",
        "Tenant": {"TenantId": "t1"},
        "SourceContext": {"LibraryName": "SrcLib"},
        "DestinationContext": {"SiteName": "D", "DriveId": "d2", "LibraryName": "Archive Lib"},
        "PlannedMoves": [],
        "ProposedFolders": [
            {
                "FolderName": "FY26",
                "DestinationPath": "Archive Lib/Projects",
                "DestinationId": "pf-1",
                "ParentPath": "Archive Lib",
            }
        ],
        "SubmittedBy": {"DisplayName": "Bob"},
        "CreatedOn": "2026-04-01T00:00:00",
        "Version": "Python-PySide6-v1",
    }
    snap = from_req_payload_dict(req)
    assert snap.proposed_folder_items[0].destination_path == "Projects"
    assert snap.proposed_folder_items[0].parent_path == ""


def test_to_library_relative_path():
    assert to_library_relative_path("Archive Lib/Projects", "Archive Lib") == "Projects"
    assert to_library_relative_path(r"Archive Lib\Deep\X", "Archive Lib") == "Deep/X"


def test_detached_load_canonical():
    d = _minimal_canonical_dict()
    raw = json.dumps(d).encode("utf-8")
    det = load_detached_from_canonical_json_bytes(raw, run_id="run-fixed")
    assert det.run_id == "run-fixed"
    assert det.snapshot.snapshot_id == d["snapshot_id"]
    assert det.import_kind == "canonical_json"


def test_detached_bundle_no_memory_mutation(tmp_path: Path):
    session = {"DraftId": "d", "SourceTreeSnapshot": [], "DestinationTreeSnapshot": []}
    tmp_path.joinpath("Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")
    tmp_path.joinpath("Draft-AllocationQueue.json").write_text(json.dumps([]), encoding="utf-8")
    tmp_path.joinpath("Draft-ProposedFolders.json").write_text(json.dumps([]), encoding="utf-8")
    det = load_detached_from_bundle_folder(tmp_path)
    assert det.snapshot.draft_id == "d"
    assert "tenant identity" in det.normalization_notes[0].lower()


def test_strict_canonical_requires_connected_tenant_id():
    d = _minimal_canonical_dict()
    d["adapter_source"] = "req_json"
    snap = parse_canonical_submitted_snapshot_dict(d)
    rep = validate_environment_against_snapshot(
        snap, ConnectedEnvironmentContext(tenant_domain="contoso.com"), run_id="r"
    )
    assert not rep.passed
    assert any(c.check_id == "canonical_connected_tenant_id" for c in rep.errors())


def test_strict_canonical_tenant_disabled_allows_empty_connected():
    d = _minimal_canonical_dict()
    d["adapter_source"] = "req_json"
    snap = parse_canonical_submitted_snapshot_dict(d)
    rep = validate_environment_against_snapshot(
        snap,
        ConnectedEnvironmentContext(tenant_domain="contoso.com"),
        run_id="r",
        strict_canonical_tenant=False,
    )
    assert not any(c.check_id == "canonical_connected_tenant_id" for c in rep.checks)


def test_environment_validation_pass_and_fail():
    d = _minimal_canonical_dict()
    snap = parse_canonical_submitted_snapshot_dict(d)
    ok = ConnectedEnvironmentContext(
        tenant_id="tid-1",
        tenant_domain="contoso.com",
        source_drive_id="drive-src",
        destination_drive_id="drive-dst",
        source_site_id="src-site",
        destination_site_id="dst-site",
        source_site_name="Source",
        destination_site_name="Dest",
        source_library_name="LibSrc",
        destination_library_name="LibDst",
    )
    rep = validate_environment_against_snapshot(snap, ok, run_id="r1")
    assert rep.passed
    assert not rep.errors()

    bad = ConnectedEnvironmentContext(tenant_id="other", tenant_domain="contoso.com")
    rep2 = validate_environment_against_snapshot(snap, bad, run_id="r1")
    assert not rep2.passed
    assert rep2.errors()


def test_validation_report_as_dict():
    d = _minimal_canonical_dict()
    snap = parse_canonical_submitted_snapshot_dict(d)
    rep = validate_environment_against_snapshot(snap, ConnectedEnvironmentContext(), run_id="r")
    blob = validation_report_as_dict(rep)
    assert "checks" in blob
    assert blob["snapshot_id"] == snap.snapshot_id


def test_minimal_ready_warnings():
    d = _minimal_canonical_dict()
    d["mapping_items"] = []
    snap = parse_canonical_submitted_snapshot_dict(d)
    w = validate_snapshot_minimal_ready(snap)
    assert any("no mapping_items" in x for x in w)


def test_pipeline_event_payload():
    ev = SnapshotPipelineEvent(
        phase="snapshot_import",
        snapshot_id="a" * 32,
        run_id="b" * 32,
        adapter_source="req_json",
        import_kind="req_json",
        mapping_id="m-42",
        message="ok",
        extra={"k": 1},
    )
    data = ev.to_log_data()
    assert data["pipeline"] == "draft_snapshot"
    assert data["snapshot_id"] == "a" * 32
    assert data["run_id"] == "b" * 32
    assert data["mapping_id"] == "m-42"
    assert data["k"] == 1
