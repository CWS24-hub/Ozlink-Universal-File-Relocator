"""End-to-end harness tests: detached load → normalize → env → resolve → plan → bridge (non-live)."""

from __future__ import annotations

import json
from pathlib import Path

from ozlink_console.draft_snapshot import (
    ConnectedEnvironmentContext,
    SNAPSHOT_SCHEMA_V1,
    draft_pipeline_run_result_to_dict,
    new_snapshot_id,
    parse_canonical_submitted_snapshot_dict,
    run_pipeline_from_bundle_folder,
    run_pipeline_from_canonical_json_bytes,
    run_pipeline_from_req_json_bytes,
)
from ozlink_console.draft_snapshot.execution_plan_contracts import ExecutionPlan, ExecutionStep, default_materialization_policy
from ozlink_console.draft_snapshot.pipeline_harness import DraftPipelineHarnessRequest, run_draft_snapshot_pipeline
from ozlink_console.draft_snapshot.detached import load_detached_from_canonical_json_bytes
from tests.test_draft_snapshot_foundation import _minimal_canonical_dict


class _HarnessGraphClient:
    """Graph client aligned with plan/resolution tests (library-relative paths)."""

    def __init__(self) -> None:
        self._counter = 0
        self._existing: dict[str, dict[str, dict[str, str]]] = {}
        # Default happy-path resolution for canonical FY26 fixture (overridable via set_existing_path).
        self._existing.setdefault("src-drive", {})["A/doc.txt"] = {"id": "SRC-ITEM-1"}
        self._existing.setdefault("dst-drive", {})["Projects"] = {"id": "DST-PARENT-1"}
        self._existing.setdefault("dst-drive", {})["Projects/FY26"] = {"id": "DST-PARENT-2"}

    def set_existing_path(self, drive_id: str, rel_path: str, item_id: str) -> None:
        self._existing.setdefault(drive_id, {})[rel_path.replace("\\", "/").strip("/")] = {"id": item_id}

    def get_drive_item_by_path(self, drive_id: str, relative_path: str):
        p = str(relative_path or "").replace("\\", "/").strip("/")
        return self._existing.get(drive_id, {}).get(p)

    def get_drive_root_item(self, drive_id: str):
        if drive_id == "dst-drive":
            return {"id": "DST-ROOT"}
        if drive_id == "src-drive":
            return {"id": "SRC-ROOT"}
        return None

    def create_child_folder(self, drive_id: str, parent_item_id: str, name: str, *, conflict_behavior: str = "fail"):
        self._counter += 1
        return {"id": f"FOLDER-{self._counter}", "name": name}

    def start_drive_item_copy(
        self,
        *,
        source_drive_id: str,
        source_item_id: str,
        dest_drive_id: str,
        dest_parent_item_id: str,
        name: str | None = None,
        conflict_behavior: str = "fail",
    ):
        return {"monitor_url": "fake://copy"}

    def wait_graph_async_operation(self, monitor: dict[str, str], timeout_sec: float = 60.0):
        return {"status": "completed"}


class _HarnessGraphClientDestSubtreeMissing(_HarnessGraphClient):
    """Source files resolve by path; destination library has drive root only (no target folders yet)."""

    def __init__(self) -> None:
        super().__init__()
        self._existing["dst-drive"] = {}
        self._existing["src-drive"] = {}
        for i in range(8):
            rel = f"Camera/file{i:03d}.jpg"
            self._existing["src-drive"][rel.replace("\\", "/")] = {"id": f"SRC-FILE-{i:03d}"}


class _HarnessGraphClientDestSubtreeMissing100GOPRO(_HarnessGraphClient):
    """Same as flat Camera/file*.jpg fixture but sources live under Camera/100GOPRO/ (recursive folder shape)."""

    def __init__(self) -> None:
        super().__init__()
        self._existing["dst-drive"] = {}
        self._existing["src-drive"] = {}
        for i in range(8):
            rel = f"Camera/100GOPRO/file{i:03d}.jpg"
            self._existing["src-drive"][rel.replace("\\", "/")] = {"id": f"SRC-GOPRO-{i:03d}"}


def _connected_ok() -> ConnectedEnvironmentContext:
    return ConnectedEnvironmentContext(
        tenant_id="tid-1",
        tenant_domain="contoso.com",
        source_drive_id="src-drive",
        destination_drive_id="dst-drive",
        source_site_id="src-site",
        destination_site_id="dst-site",
        source_site_name="SrcSite",
        destination_site_name="DstSite",
        source_library_name="SrcLib",
        destination_library_name="DstLib",
    )


def _canonical_eight_files_dest_subtree_missing_bytes() -> bytes:
    """Eight file mappings under Root/Personal/...; Graph has sources only (mirrors browsed-recursive + new dest subtree)."""
    d = _minimal_canonical_dict()
    d.pop("snapshot_hash", None)
    d["source"]["site_library"]["library_drive_id"] = "src-drive"
    d["destination"]["site_library"]["library_drive_id"] = "dst-drive"
    d["mapping_items"] = []
    for i in range(8):
        d["mapping_items"].append(
            {
                "mapping_id": f"recsub-{i:03d}",
                "item_type": "file",
                "source_path": f"Camera\\file{i:03d}.jpg",
                "source_name": f"file{i:03d}.jpg",
                "destination_path": f"Root/Personal/file{i:03d}.jpg",
                "destination_name": f"file{i:03d}.jpg",
                "assignment_mode": "copy",
            }
        )
    d["proposed_folder_items"] = []
    snap = parse_canonical_submitted_snapshot_dict(d)
    return json.dumps(snap.to_json_dict()).encode("utf-8")


def _canonical_eight_files_personal_100gopro_bytes() -> bytes:
    """REGRESSION: browsed-recursive shape must keep source folder leaf (100GOPRO) under Personal (not flattened)."""
    d = _minimal_canonical_dict()
    d.pop("snapshot_hash", None)
    d["source"]["site_library"]["library_drive_id"] = "src-drive"
    d["destination"]["site_library"]["library_drive_id"] = "dst-drive"
    d["mapping_items"] = []
    for i in range(8):
        d["mapping_items"].append(
            {
                "mapping_id": f"recsub-gopro-{i:03d}",
                "item_type": "file",
                "source_path": f"Camera\\100GOPRO\\file{i:03d}.jpg",
                "source_name": f"file{i:03d}.jpg",
                "destination_path": f"Root/Personal/100GOPRO/file{i:03d}.jpg",
                "destination_name": f"file{i:03d}.jpg",
                "assignment_mode": "copy",
            }
        )
    d["proposed_folder_items"] = []
    snap = parse_canonical_submitted_snapshot_dict(d)
    return json.dumps(snap.to_json_dict()).encode("utf-8")


def _canonical_happy_bytes() -> bytes:
    d = _minimal_canonical_dict()
    d.pop("snapshot_hash", None)
    d["source"]["site_library"]["library_drive_id"] = "src-drive"
    d["destination"]["site_library"]["library_drive_id"] = "dst-drive"
    d["mapping_items"] = [
        {
            "mapping_id": "m1",
            "item_type": "file",
            "source_path": "A/doc.txt",
            "source_name": "doc.txt",
            "destination_path": "Projects/FY26/doc.txt",
            "destination_name": "doc.txt",
            "assignment_mode": "copy",
        }
    ]
    d["proposed_folder_items"] = [
        {
            "proposed_id": "pf-1",
            "folder_name": "FY26",
            "destination_path": "Projects/FY26",
            "parent_path": "Projects",
        }
    ]
    snap = parse_canonical_submitted_snapshot_dict(d)
    return json.dumps(snap.to_json_dict()).encode("utf-8")


def test_harness_eight_files_missing_graph_dest_parent_passes_resolution_gate_with_audit():
    """
    Regression: without deferral, eight file rows with missing Graph destination parents were
    unresolved_count=8 and block_on_resolution_gaps stopped before plan_build (plan_id null).
    """
    det = load_detached_from_canonical_json_bytes(
        _canonical_eight_files_dest_subtree_missing_bytes(), run_id="harness-eight-defer"
    )
    g = _HarnessGraphClientDestSubtreeMissing()
    r = run_draft_snapshot_pipeline(
        DraftPipelineHarnessRequest(
            detached=det,
            connected=_connected_ok(),
            graph_client=g,
            bridge_dry_run=True,
            block_on_resolution_gaps=True,
        )
    )
    assert r.environment_passed is True
    assert r.resolution_summary is not None
    assert r.resolution_summary.get("unresolved_count") == 0
    assert r.resolution_summary.get("ambiguous_count") == 0
    assert r.stopped_at != "resolution", (r.stopped_at, r.errors)
    assert r.failure_boundary != "resolution"
    assert "resolution_unresolved_or_ambiguous" not in r.errors
    mapping_brief = [x for x in r.resolution_items_brief if x.get("item_kind") == "mapping_item"]
    assert len(mapping_brief) == 8
    assert all(x.get("status") == "resolved" for x in mapping_brief)
    assert all(x.get("message") == "resolved_dest_parent_deferred_to_materialization_chain" for x in mapping_brief)
    assert all(x.get("deferred_destination_parent_to_mkdir_chain") is True for x in mapping_brief)
    assert r.plan_id
    assert "plan_build" in r.phases_completed
    assert {x["mapping_id"] for x in mapping_brief} == {f"recsub-{i:03d}" for i in range(8)}


def test_harness_personal_100gopro_segment_preserved_end_to_end():
    """
    REGRESSION LOCK: materialized/bridge steps must carry Root/Personal/100GOPRO/... (no Root/Personal/file flattening).
    """
    det = load_detached_from_canonical_json_bytes(
        _canonical_eight_files_personal_100gopro_bytes(), run_id="harness-100gopro-shape"
    )
    g = _HarnessGraphClientDestSubtreeMissing100GOPRO()
    r = run_draft_snapshot_pipeline(
        DraftPipelineHarnessRequest(
            detached=det,
            connected=_connected_ok(),
            graph_client=g,
            bridge_dry_run=True,
            block_on_resolution_gaps=True,
        )
    )
    assert r.environment_passed is True
    assert r.resolution_summary is not None
    assert r.resolution_summary.get("unresolved_count") == 0
    assert r.failure_boundary != "resolution"
    assert r.plan_id
    assert r.success is True
    assert "execution_bridge" in r.phases_completed
    assert "summary" in r.phases_completed
    assert r.failure_boundary == ""

    copy_rows = [
        row
        for row in r.bridge_step_outcomes.values()
        if str(row.get("step_type") or "") == "copy_item"
    ]
    assert len(copy_rows) == 8
    for row in copy_rows:
        dp = str(row.get("destination_path") or "").replace("\\", "/")
        assert "100GOPRO" in dp, dp
        assert "Personal/100GOPRO/" in dp, dp

    mapping_brief = [x for x in r.resolution_items_brief if x.get("item_kind") == "mapping_item"]
    assert len(mapping_brief) == 8
    for x in mapping_brief:
        rid = str(x.get("mapping_id") or "")
        assert rid.startswith("recsub-gopro-")


def test_harness_canonical_happy_path_dry_run():
    g = _HarnessGraphClient()
    r = run_pipeline_from_canonical_json_bytes(
        _canonical_happy_bytes(),
        connected=_connected_ok(),
        graph_client=g,
        bridge_dry_run=True,
        run_id="harness-happy",
    )
    assert r.environment_passed is True
    assert r.plan_id
    assert r.resolution_summary and r.resolution_summary.get("resolved_count", 0) >= 1
    assert "execution_bridge" in r.phases_completed
    assert r.bridge_summary
    blob = draft_pipeline_run_result_to_dict(r)
    assert blob["snapshot_id"]
    assert blob["plan_hash"]


def test_harness_legacy_bundle_missing_ids_unresolved(tmp_path: Path):
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
            "SourceType": "File",
            "RequestedDestinationPath": "q/",
            "AllocationMethod": "Move",
            "RequestedBy": "u",
            "RequestedDate": "2026-01-01",
            "Status": "Pending",
        }
    ]
    tmp_path.joinpath("Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")
    tmp_path.joinpath("Draft-AllocationQueue.json").write_text(json.dumps(allocations), encoding="utf-8")
    tmp_path.joinpath("Draft-ProposedFolders.json").write_text(json.dumps([]), encoding="utf-8")

    g = _HarnessGraphClient()
    r = run_pipeline_from_bundle_folder(
        tmp_path,
        connected=ConnectedEnvironmentContext(),
        graph_client=g,
        bridge_dry_run=True,
        run_id="harness-bundle",
    )
    assert r.environment_passed is True
    assert r.unresolved_mapping_ids
    assert r.resolution_summary and r.resolution_summary.get("unresolved_count", 0) >= 1


def test_harness_legacy_bundle_missing_ids_blocks_when_resolution_gate_enabled(tmp_path: Path):
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
            "SourceType": "File",
            "RequestedDestinationPath": "q/",
            "AllocationMethod": "Move",
            "RequestedBy": "u",
            "RequestedDate": "2026-01-01",
            "Status": "Pending",
        }
    ]
    tmp_path.joinpath("Draft-SessionState.json").write_text(json.dumps(session), encoding="utf-8")
    tmp_path.joinpath("Draft-AllocationQueue.json").write_text(json.dumps(allocations), encoding="utf-8")
    tmp_path.joinpath("Draft-ProposedFolders.json").write_text(json.dumps([]), encoding="utf-8")

    g = _HarnessGraphClient()
    r = run_pipeline_from_bundle_folder(
        tmp_path,
        connected=ConnectedEnvironmentContext(),
        graph_client=g,
        bridge_dry_run=True,
        run_id="harness-bundle-block",
        block_on_resolution_gaps=True,
    )
    assert r.stopped_at == "resolution"
    assert "resolution_unresolved_or_ambiguous" in r.errors
    assert r.plan_id is None
    assert "execution_bridge" not in r.phases_completed


def test_harness_plan_build_exception_stops_at_plan_build(monkeypatch):
    g = _HarnessGraphClient()

    def _boom(*_a, **_k):
        raise RuntimeError("simulated plan build failure")

    monkeypatch.setattr(
        "ozlink_console.draft_snapshot.pipeline_harness.build_execution_plan_from_resolved",
        _boom,
    )
    r = run_pipeline_from_canonical_json_bytes(
        _canonical_happy_bytes(),
        connected=_connected_ok(),
        graph_client=g,
        bridge_dry_run=True,
    )
    assert r.stopped_at == "plan_build"
    assert r.failure_boundary == "plan_build"
    assert r.boundary_detail == "plan_build_exception"
    assert r.plan_id is None
    assert any("plan_build_exception" in e for e in r.errors)
    assert "execution_bridge" not in r.phases_completed


def test_harness_tenant_mismatch_blocks_before_execution():
    g = _HarnessGraphClient()
    r = run_pipeline_from_canonical_json_bytes(
        _canonical_happy_bytes(),
        connected=ConnectedEnvironmentContext(tenant_id="wrong-tenant", tenant_domain="contoso.com"),
        graph_client=g,
        bridge_dry_run=True,
    )
    assert r.environment_passed is False
    assert r.stopped_at == "environment_validation"
    assert r.failure_boundary == "environment"
    assert r.boundary_detail == "environment_validation"
    assert r.plan_id is None
    assert "execution_bridge" not in r.phases_completed


def test_harness_file_exists_skipped_existing():
    g = _HarnessGraphClient()
    g.set_existing_path("dst-drive", "Projects/FY26/doc.txt", "EXISTING")
    r = run_pipeline_from_canonical_json_bytes(
        _canonical_happy_bytes(),
        connected=_connected_ok(),
        graph_client=g,
        bridge_dry_run=False,
    )
    file_step = next(x for x in r.bridge_step_outcomes.values() if x.get("mapping_id") == "m1")
    assert file_step["outcome"] == "skipped_existing"


def test_harness_folder_exists_merge_no_op_via_plan_override():
    """Plan override: deterministic merge path when Graph reports folder already exists."""
    d = _minimal_canonical_dict()
    d.pop("snapshot_hash", None)
    d["source"]["site_library"]["library_drive_id"] = "src-drive"
    d["destination"]["site_library"]["library_drive_id"] = "dst-drive"
    d["mapping_items"] = []
    d["proposed_folder_items"] = []
    snap = parse_canonical_submitted_snapshot_dict(d)
    raw = json.dumps(snap.to_json_dict()).encode("utf-8")
    det = load_detached_from_canonical_json_bytes(raw, run_id="harness-merge")

    plan = ExecutionPlan(
        plan_id="plan-merge",
        snapshot_id=snap.snapshot_id,
        run_id=det.run_id,
        resolver_name="harness",
        policy=default_materialization_policy(),
        steps=[
            ExecutionStep(
                step_id="mkdir-projects",
                step_type="create_folder",
                mapping_id="dst_chain:Projects",
                item_type="folder",
                assignment_mode="copy_recursive",
                source_drive_id="",
                source_item_id="",
                destination_drive_id="dst-drive",
                destination_parent_item_id="DST-ROOT",
                destination_name="Projects",
                destination_path="Projects",
                depth=0,
            )
        ],
    )
    g = _HarnessGraphClient()
    g.set_existing_path("dst-drive", "Projects", "PROJ-EXISTING")
    r = run_draft_snapshot_pipeline(
        DraftPipelineHarnessRequest(
            detached=det,
            connected=_connected_ok(),
            graph_client=g,
            bridge_dry_run=False,
            execution_plan_override=plan,
        )
    )
    st = r.bridge_step_outcomes["mkdir-projects"]
    assert st["outcome"] == "succeeded"
    assert "merge-compatible" in st["detail"]


def test_harness_move_item_blocked_compatibility_full_pipeline():
    d = _minimal_canonical_dict()
    d.pop("snapshot_hash", None)
    d["source"]["site_library"]["library_drive_id"] = "src-drive"
    d["destination"]["site_library"]["library_drive_id"] = "dst-drive"
    d["mapping_items"] = [
        {
            "mapping_id": "m1",
            "item_type": "file",
            "source_path": "A/doc.txt",
            "source_name": "doc.txt",
            "destination_path": "Projects/FY26/doc.txt",
            "destination_name": "doc.txt",
            "assignment_mode": "move",
        }
    ]
    d["proposed_folder_items"] = []
    snap = parse_canonical_submitted_snapshot_dict(d)
    raw = json.dumps(snap.to_json_dict()).encode("utf-8")
    g = _HarnessGraphClient()
    r = run_pipeline_from_canonical_json_bytes(
        raw,
        connected=_connected_ok(),
        graph_client=g,
        bridge_dry_run=False,
    )
    file_step = next(x for x in r.bridge_step_outcomes.values() if x.get("mapping_id") == "m1")
    assert file_step["outcome"] == "blocked_compatibility"
    assert r.success is False
    assert r.failure_boundary == "bridge"
    assert r.boundary_detail == "bridge_compatibility_blocked"


def test_harness_parent_step_id_dependency_with_plan_override():
    """Override: plan shape requiring parent_step_id (full Graph resolution cannot emit this yet)."""
    sid = new_snapshot_id()
    d = {
        "snapshot_schema": SNAPSHOT_SCHEMA_V1,
        "engine_version": 1,
        "snapshot_id": sid,
        "draft_id": "d1",
        "draft_version": "v1",
        "submitted_at_utc": "2026-04-01T00:00:00+00:00",
        "submitted_by": "t",
        "app_version": "app",
        "tenant": {"tenant_id": "tid-1", "tenant_domain": "contoso.com", "tenant_label": "", "client_key": ""},
        "source": {
            "platform": "sharepoint",
            "site_library": {
                "site_name": "S",
                "library_name": "L",
                "library_drive_id": "src-drive",
            },
        },
        "destination": {
            "platform": "sharepoint",
            "site_library": {
                "site_name": "D",
                "library_name": "L2",
                "library_drive_id": "dst-drive",
            },
        },
        "mapping_items": [],
        "proposed_folder_items": [],
        "execution_options": {},
    }
    snap = parse_canonical_submitted_snapshot_dict(d)
    raw = json.dumps(snap.to_json_dict()).encode("utf-8")
    det = load_detached_from_canonical_json_bytes(raw, run_id="harness-parent")

    plan = ExecutionPlan(
        plan_id="plan-parent",
        snapshot_id=sid,
        run_id=det.run_id,
        resolver_name="harness",
        policy=default_materialization_policy(),
        steps=[
            ExecutionStep(
                step_id="s1",
                step_type="create_folder",
                mapping_id="chain-p1",
                item_type="folder",
                assignment_mode="copy_recursive",
                source_drive_id="",
                source_item_id="",
                destination_drive_id="dst-drive",
                destination_parent_item_id="DST-ROOT",
                destination_name="Projects",
                destination_path="Projects",
                depth=0,
            ),
            ExecutionStep(
                step_id="s2",
                step_type="create_folder",
                mapping_id="chain-p2",
                item_type="folder",
                assignment_mode="copy_recursive",
                source_drive_id="",
                source_item_id="",
                destination_drive_id="dst-drive",
                destination_parent_item_id="",
                destination_name="FY26",
                destination_path="Projects/FY26",
                depth=1,
                parent_step_id="s1",
            ),
            ExecutionStep(
                step_id="s3",
                step_type="copy_item",
                mapping_id="m1",
                item_type="file",
                assignment_mode="copy",
                source_drive_id="src-drive",
                source_item_id="SRC-ITEM",
                destination_drive_id="dst-drive",
                destination_parent_item_id="",
                destination_name="doc.txt",
                destination_path="Projects/FY26/doc.txt",
                depth=2,
                parent_step_id="s2",
            ),
        ],
    )
    class _ParentChainGraph(_HarnessGraphClient):
        """Clear default dst path hits so mkdir steps create folders and expose output_item_id."""

        def __init__(self) -> None:
            super().__init__()
            self._existing["dst-drive"] = {}

    gx = _ParentChainGraph()
    r = run_draft_snapshot_pipeline(
        DraftPipelineHarnessRequest(
            detached=det,
            connected=_connected_ok(),
            graph_client=gx,
            bridge_dry_run=False,
            execution_plan_override=plan,
        )
    )
    assert r.bridge_step_outcomes["s2"]["parent_resolution_source"] == "parent_step_output"
    assert r.bridge_step_outcomes["s3"]["parent_resolution_source"] == "parent_step_output"
    assert r.bridge_step_outcomes["s3"]["outcome"] == "succeeded"


def test_harness_req_json_input_path():
    req = {
        "RequestId": "REQ-1",
        "Tenant": {"TenantId": "tid-1", "TenantDomain": "contoso.com", "TenantLabel": "L"},
        "SourceContext": {"SiteName": "S", "DriveId": "src-drive", "LibraryName": "SrcLib"},
        "DestinationContext": {"SiteName": "D", "DriveId": "dst-drive", "LibraryName": "DstLib"},
        "PlannedMoves": [
            {
                "RequestId": "r1",
                "SourceItemName": "f.txt",
                "SourcePath": "p/f.txt",
                "SourceType": "File",
                "RequestedDestinationPath": "q/",
                "AllocationMethod": "Copy",
                "RequestedBy": "u",
                "RequestedDate": "2026-01-01",
                "Status": "Pending",
                "SourceItemId": "sid-1",
                "SourceDriveId": "src-drive",
            }
        ],
        "ProposedFolders": [],
        "SubmittedBy": {"DisplayName": "Alice"},
        "CreatedOn": "2026-01-02T00:00:00",
        "Version": "Python-PySide6-v1",
    }
    g = _HarnessGraphClient()
    g.set_existing_path("src-drive", "p/f.txt", "FILE-1")
    g.set_existing_path("dst-drive", "q", "PARENT-Q")
    r = run_pipeline_from_req_json_bytes(
        json.dumps(req).encode("utf-8"),
        connected=_connected_ok(),
        graph_client=g,
        bridge_dry_run=True,
    )
    assert r.import_kind == "req_json"
    assert r.environment_passed is True
    assert r.plan_id
