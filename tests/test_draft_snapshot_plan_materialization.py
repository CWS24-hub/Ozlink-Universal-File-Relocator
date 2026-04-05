from __future__ import annotations

from ozlink_console.draft_snapshot import (
    BuildExecutionPlanRequest,
    CanonicalMappingItem,
    CanonicalSubmittedSnapshot,
    GraphResolveIdsService,
    ItemResolutionResult,
    PlanMaterializationSummary,
    ProposedFolderMappingItem,
    ResolvedSnapshot,
    ResolvedSnapshotExecutionPlanBuilder,
    ResolutionSummary,
    build_execution_plan_from_resolved,
    compute_execution_plan_hash,
    default_materialization_policy,
    sort_execution_steps,
    summarize_resolution,
)
from ozlink_console.draft_snapshot.plan_materialization import build_destination_chain_steps
from ozlink_console.draft_snapshot.contracts import (
    DestinationContextSnapshot,
    SiteLibraryContextSnapshot,
    SourceContextSnapshot,
    TenantIdentitySnapshot,
)
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


class _FakeGraphDestFoldersMissing(_FakeGraphClient):
    """Destination library has root only; no Projects / FY26 folders yet."""

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
                assignment_mode="copy",
            ),
            CanonicalMappingItem(
                mapping_id="m2",
                item_type="folder",
                source_path="Fold",
                source_name="Fold",
                destination_path="Projects/FY26/Fold",
                destination_name="Fold",
                assignment_mode="copy_recursive",
            ),
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


def test_plan_materialization_emits_folder_then_file_deterministic_order_without_graph_chain():
    service = GraphResolveIdsService(graph_client=_FakeGraphClient())
    resolved = service.resolve(_snapshot(), run_id="run-plan-1")
    plan = build_execution_plan_from_resolved(resolved, run_id="run-plan-1")
    assert len(plan.steps) == 2
    assert plan.summary.folder_steps == 1
    assert plan.summary.file_steps == 1
    assert plan.steps[0].step_type == "create_folder"
    assert plan.steps[0].mapping_id == "pf-1"
    assert plan.steps[1].step_type == "copy_item"
    assert plan.steps[1].mapping_id == "m1"
    ordered = sort_execution_steps(list(reversed(plan.steps)))
    assert [s.mapping_id for s in ordered] == ["pf-1", "m1"]


def test_plan_materialization_with_graph_skips_redundant_proposed_when_folders_exist():
    service = GraphResolveIdsService(graph_client=_FakeGraphClient())
    resolved = service.resolve(_snapshot(), run_id="run-plan-g1")
    plan = build_execution_plan_from_resolved(resolved, run_id="run-plan-g1", graph_client=_FakeGraphClient())
    assert plan.materialization.destination_chain_mkdir_steps == 0
    assert plan.materialization.destination_chain_skipped_duplicate_proposed == 1
    assert len(plan.steps) == 1
    assert plan.steps[0].step_type == "copy_item"
    assert plan.steps[0].destination_parent_item_id == "DST-PARENT-1"


def _snapshot_file_only_no_proposed() -> CanonicalSubmittedSnapshot:
    s = _snapshot()
    s.mapping_items = [s.mapping_items[0]]
    s.proposed_folder_items = []
    return s


def test_plan_materialization_graph_chain_ignores_prop_placeholder_destination_parent():
    """Resolver must not leave PROP-* as parent; chain mkdir + parent_step_id still applies."""
    snap = _snapshot_file_only_no_proposed()
    mr = ItemResolutionResult(
        item_kind="mapping_item",
        mapping_id="m1",
        item_type="file",
        status="resolved",
        message="synthetic_prop_parent_bug",
        source_drive_id="src-drive",
        source_item_id="SRC-ITEM-1",
        destination_drive_id="dst-drive",
        destination_parent_item_id="PROP-11597316",
        destination_name="doc.txt",
    )
    summary = summarize_resolution([mr], [])
    resolved = ResolvedSnapshot(
        snapshot=snap,
        run_id="run-plan-g-prop",
        resolver_name="synthetic",
        summary=summary,
        mapping_results=[mr],
        proposed_folder_results=[],
    )
    plan = build_execution_plan_from_resolved(
        resolved, run_id="run-plan-g-prop", graph_client=_FakeGraphDestFoldersMissing()
    )
    file_step = [s for s in plan.steps if s.item_type == "file"][0]
    assert file_step.destination_parent_item_id == ""
    assert file_step.parent_step_id
    mkdir_deep = [s for s in plan.steps if s.destination_path == "Projects/FY26"][0]
    assert file_step.parent_step_id == mkdir_deep.step_id


def test_plan_materialization_graph_emits_executable_chain_and_file_parent_step():
    snap = _snapshot_file_only_no_proposed()
    mr = ItemResolutionResult(
        item_kind="mapping_item",
        mapping_id="m1",
        item_type="file",
        status="resolved",
        message="synthetic_for_chain_test",
        source_drive_id="src-drive",
        source_item_id="SRC-ITEM-1",
        destination_drive_id="dst-drive",
        destination_parent_item_id="",
        destination_name="doc.txt",
    )
    summary = summarize_resolution([mr], [])
    resolved = ResolvedSnapshot(
        snapshot=snap,
        run_id="run-plan-g2",
        resolver_name="synthetic",
        summary=summary,
        mapping_results=[mr],
        proposed_folder_results=[],
    )
    plan = build_execution_plan_from_resolved(
        resolved, run_id="run-plan-g2", graph_client=_FakeGraphDestFoldersMissing()
    )
    mkdirs = [s for s in plan.steps if s.step_type == "create_folder" and s.metadata.get("item_kind") == "destination_chain"]
    assert len(mkdirs) == 2
    shallow = [s for s in mkdirs if s.destination_path == "Projects"][0]
    deep = [s for s in mkdirs if s.destination_path == "Projects/FY26"][0]
    assert shallow.destination_parent_item_id == "DST-ROOT"
    assert shallow.parent_step_id == ""
    assert deep.destination_parent_item_id == ""
    assert deep.parent_step_id == shallow.step_id
    file_step = [s for s in plan.steps if s.item_type == "file"][0]
    assert file_step.parent_step_id == deep.step_id
    assert file_step.destination_parent_item_id == ""
    assert plan.materialization.destination_chain_mkdir_steps == 2
    assert plan.materialization.proposed_folder_steps_emitted == 0


def test_build_destination_chain_steps_module_smoke():
    snap = _snapshot()
    mat = PlanMaterializationSummary()
    steps, _existing, planned, mat2 = build_destination_chain_steps(
        _FakeGraphDestFoldersMissing(), snap, "p" * 32, ["Projects", "Projects/FY26"], mat
    )
    assert len(steps) == 2
    assert "Projects" in planned and "Projects/FY26" in planned
    assert mat2.destination_chain_mkdir_steps == 2


def test_unresolved_and_non_file_mapping_excluded_but_visible_in_summary():
    service = GraphResolveIdsService(graph_client=_FakeGraphClient())
    snap = _snapshot()
    snap.mapping_items[0].source_path = "missing/source"
    snap.mapping_items[1].source_graph_item_id = "folder-src"
    snap.mapping_items[1].destination_parent_graph_item_id = "folder-dst-parent"
    resolved = service.resolve(snap, run_id="run-plan-2")
    plan = build_execution_plan_from_resolved(resolved, run_id="run-plan-2")
    file_steps = [s for s in plan.steps if s.item_type == "file"]
    assert len(file_steps) == 0
    assert "m1" in plan.materialization.unresolved_mapping_ids or "m1" in resolved.summary.unresolved_mapping_ids
    assert "m2" in plan.materialization.excluded_mapping_not_file


def test_default_materialization_policy_matches_spec():
    p = default_materialization_policy()
    assert p.file_conflict_policy == "skip"
    assert p.folder_conflict_policy == "merge"
    assert p.allow_overwrite is False
    assert p.rename_on_conflict is False
    assert p.stop_on_error is False
    assert p.continue_on_error is True
    assert p.integrity_verify is True


def test_plan_hash_is_deterministic_for_same_payload():
    a = compute_execution_plan_hash({"b": 1, "a": 2})
    b = compute_execution_plan_hash({"b": 1, "a": 2})
    assert a == b and len(a) == 64


def test_contract_only_builder_still_unwired():
    from ozlink_console.draft_snapshot import ContractOnlyExecutionPlanBuilder

    builder = ContractOnlyExecutionPlanBuilder()
    try:
        builder.build_plan(
            BuildExecutionPlanRequest(
                resolved_snapshot=ResolvedSnapshot(
                    snapshot=_snapshot(),
                    run_id="r",
                    resolver_name="resolver",
                    summary=ResolutionSummary(total_items=0),
                ),
                run_id="r",
            )
        )
        assert False
    except NotImplementedError:
        pass


def test_resolved_snapshot_builder_implements_protocol():
    b = ResolvedSnapshotExecutionPlanBuilder()
    service = GraphResolveIdsService(graph_client=_FakeGraphClient())
    resolved = service.resolve(_snapshot(), run_id="r")
    plan = b.build_plan(BuildExecutionPlanRequest(resolved_snapshot=resolved, run_id="r"))
    assert plan.plan_hash
    assert plan.metadata.get("schema") == "ozlink.execution_plan_materialization/v1"
    assert "plan_hash_payload" in plan.metadata
