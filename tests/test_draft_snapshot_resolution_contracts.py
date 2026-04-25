from __future__ import annotations

from dataclasses import asdict

from ozlink_console.draft_snapshot import (
    BuildExecutionPlanRequest,
    CanonicalMappingItem,
    CanonicalSubmittedSnapshot,
    ContractOnlyExecutionPlanBuilder,
    ExecutionStep,
    GraphResolveIdsService,
    ItemResolutionResult,
    ProposedFolderMappingItem,
    ResolvedSnapshot,
    ResolutionSummary,
    execution_step_sort_key,
    log_resolution_item_state,
    sort_execution_steps,
    summarize_execution_plan_steps,
)
from ozlink_console.draft_snapshot.contracts import (
    DestinationContextSnapshot,
    SiteLibraryContextSnapshot,
    SourceContextSnapshot,
    TenantIdentitySnapshot,
)
from ozlink_console.draft_snapshot.plan_materialization import build_execution_plan_from_resolved


class _FakeGraphClient:
    def get_drive_item_by_path(self, drive_id: str, relative_path: str):
        p = str(relative_path or "").replace("\\", "/").strip("/")
        if drive_id == "src-drive" and p == "A/doc.txt":
            return {"id": "SRC-ITEM-1", "name": "doc.txt"}
        if drive_id == "dst-drive" and p in ("Projects", "Projects/FY26"):
            return {"id": "DST-PARENT-1", "name": p.split("/")[-1]}
        return None

    def get_drive_root_item(self, drive_id: str):
        if drive_id == "dst-drive":
            return {"id": "DST-ROOT"}
        return None


class _FakeGraphDestParentsMissing(_FakeGraphClient):
    """Source file exists on Graph; destination subtree folders do not exist yet."""

    def get_drive_item_by_path(self, drive_id: str, relative_path: str):
        p = str(relative_path or "").replace("\\", "/").strip("/")
        if drive_id == "src-drive" and p == "A/doc.txt":
            return {"id": "SRC-ITEM-1", "name": "doc.txt"}
        return None


def _snapshot() -> CanonicalSubmittedSnapshot:
    return CanonicalSubmittedSnapshot(
        snapshot_id="a" * 32,
        draft_id="d1",
        draft_version="v1",
        submitted_at_utc="2026-04-01T00:00:00+00:00",
        submitted_by="tester",
        app_version="app",
        tenant=TenantIdentitySnapshot(),
        source=SourceContextSnapshot(
            platform="sharepoint",
            site_library=SiteLibraryContextSnapshot(
                site_name="SrcSite",
                library_name="SrcLib",
                library_drive_id="src-drive",
            ),
        ),
        destination=DestinationContextSnapshot(
            platform="sharepoint",
            site_library=SiteLibraryContextSnapshot(
                site_name="DstSite",
                library_name="DstLib",
                library_drive_id="dst-drive",
            ),
        ),
        mapping_items=[
            CanonicalMappingItem(
                mapping_id="m1",
                item_type="file",
                source_path="A/doc.txt",
                source_name="doc.txt",
                destination_path="Projects/FY26/doc.txt",
                destination_name="doc.txt",
                assignment_mode="move",
            )
        ],
        proposed_folder_items=[
            ProposedFolderMappingItem(
                proposed_id="pf-1",
                folder_name="FY26",
                destination_path="Projects/FY26",
                parent_path="Projects",
            )
        ],
    )


def test_graph_resolve_ids_service_returns_structured_results():
    service = GraphResolveIdsService(graph_client=_FakeGraphClient())
    resolved = service.resolve(_snapshot(), run_id="run-1")
    assert resolved.snapshot_id == "a" * 32
    assert resolved.summary.total_items == 2
    assert resolved.mapping_results[0].status == "resolved"
    assert resolved.mapping_results[0].source_item_id == "SRC-ITEM-1"
    assert resolved.mapping_results[0].destination_parent_item_id == "DST-PARENT-1"
    assert resolved.proposed_folder_results[0].status == "resolved"


def test_unresolved_item_reporting():
    snap = _snapshot()
    snap.mapping_items[0].source_path = "missing/source"
    service = GraphResolveIdsService(graph_client=_FakeGraphClient())
    resolved = service.resolve(snap, run_id="run-2")
    assert resolved.summary.unresolved_count >= 1
    first = resolved.mapping_results[0]
    assert first.status == "unresolved"
    assert "missing_source_item_id" in first.unresolved_reasons


def test_resolver_defers_missing_graph_dest_parent_when_mkdir_chain_will_apply():
    """Regression: PROP/stale parent cleared → Graph cannot see parent yet; plan chain still valid."""
    snap = _snapshot()
    snap.proposed_folder_items = []
    service = GraphResolveIdsService(graph_client=_FakeGraphDestParentsMissing())
    resolved = service.resolve(snap, run_id="run-defer-dest")
    mr = resolved.mapping_results[0]
    assert mr.status == "resolved"
    assert mr.source_item_id == "SRC-ITEM-1"
    assert mr.destination_parent_item_id == ""
    assert mr.message == "resolved_dest_parent_deferred_to_materialization_chain"
    assert mr.raw.get("deferred_destination_parent_to_mkdir_chain") is True
    assert resolved.summary.unresolved_count == 0

    plan = build_execution_plan_from_resolved(resolved, run_id="run-defer-dest", graph_client=_FakeGraphDestParentsMissing())
    mkdirs = [s for s in plan.steps if s.step_type == "create_folder" and s.metadata.get("item_kind") == "destination_chain"]
    assert len(mkdirs) == 2
    file_steps = [s for s in plan.steps if s.item_type == "file"]
    assert len(file_steps) == 1
    assert file_steps[0].parent_step_id


def test_deferred_dest_parent_still_materializes_file_when_bundle_omits_dest_library_drive_id():
    """Regression: legacy bundle has no destination library_drive_id; rows still carry DestinationDriveId."""
    snap = _snapshot()
    snap.proposed_folder_items = []
    snap.destination.site_library.library_drive_id = ""
    snap.mapping_items[0].destination_graph_drive_id = "dst-drive"
    service = GraphResolveIdsService(graph_client=_FakeGraphDestParentsMissing())
    resolved = service.resolve(snap, run_id="run-defer-no-lib-drive")
    assert resolved.mapping_results[0].message == "resolved_dest_parent_deferred_to_materialization_chain"
    plan = build_execution_plan_from_resolved(
        resolved, run_id="run-defer-no-lib-drive", graph_client=_FakeGraphDestParentsMissing()
    )
    file_steps = [s for s in plan.steps if s.item_type == "file"]
    assert len(file_steps) == 1
    assert file_steps[0].mapping_id == "m1"
    assert file_steps[0].parent_step_id
    assert not plan.materialization.excluded_mapping_missing_ids


def test_execution_step_deterministic_ordering():
    steps = [
        ExecutionStep(
            step_id="3",
            step_type="move_item",
            mapping_id="file-deep",
            item_type="file",
            assignment_mode="move",
            source_drive_id="s",
            source_item_id="i3",
            destination_drive_id="d",
            destination_parent_item_id="p",
            destination_name="x.txt",
            destination_path="a/b/x.txt",
            depth=2,
        ),
        ExecutionStep(
            step_id="1",
            step_type="create_folder",
            mapping_id="folder-shallow",
            item_type="folder",
            assignment_mode="copy_recursive",
            source_drive_id="s",
            source_item_id="i1",
            destination_drive_id="d",
            destination_parent_item_id="p",
            destination_name="a",
            destination_path="a",
            depth=0,
        ),
        ExecutionStep(
            step_id="2",
            step_type="create_folder",
            mapping_id="folder-deep",
            item_type="folder",
            assignment_mode="copy_recursive",
            source_drive_id="s",
            source_item_id="i2",
            destination_drive_id="d",
            destination_parent_item_id="p",
            destination_name="b",
            destination_path="a/b",
            depth=1,
        ),
    ]
    ordered = sort_execution_steps(steps)
    assert [x.mapping_id for x in ordered] == ["folder-shallow", "folder-deep", "file-deep"]
    summary = summarize_execution_plan_steps(ordered)
    assert summary.folder_steps == 2
    assert summary.file_steps == 1


def test_contract_only_plan_builder_unwired():
    builder = ContractOnlyExecutionPlanBuilder()
    req = BuildExecutionPlanRequest(
        resolved_snapshot=ResolvedSnapshot(
            snapshot=_snapshot(),
            run_id="r",
            resolver_name="resolver",
            summary=ResolutionSummary(total_items=0),
        ),
        run_id="r",
    )
    try:
        builder.build_plan(req)
        assert False, "expected NotImplementedError"
    except NotImplementedError:
        pass


def test_resolution_log_payload_shape():
    result = ItemResolutionResult(
        item_kind="mapping_item",
        mapping_id="m1",
        item_type="file",
        status="resolved",
        message="ok",
    )
    # function should not raise and payload remains serializable
    log_resolution_item_state(snapshot_id="s" * 32, run_id="r" * 8, result=result)
    payload = asdict(result)
    assert payload["mapping_id"] == "m1"
    assert execution_step_sort_key(
        ExecutionStep(
            step_id="x",
            step_type="verify_only",
            mapping_id="m1",
            item_type="file",
            assignment_mode="move",
            source_drive_id="",
            source_item_id="",
            destination_drive_id="",
            destination_parent_item_id="",
            destination_name="",
            destination_path="",
        )
    )

