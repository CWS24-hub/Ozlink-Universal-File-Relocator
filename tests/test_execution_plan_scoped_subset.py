from __future__ import annotations

import json

from ozlink_console.draft_snapshot.execution_plan_contracts import (
    ExecutionPlan,
    ExecutionPlanSummary,
    ExecutionPolicy,
    ExecutionStep,
    PlanMaterializationSummary,
)
from ozlink_console.draft_snapshot.plan_materialization import (
    _chain_mapping_id,
    subset_execution_plan_by_dependency_closure,
    subset_execution_plan_by_mapping_ids,
)
from ozlink_console.draft_snapshot import parse_canonical_submitted_snapshot_dict
from ozlink_console.draft_snapshot.detached import load_detached_from_canonical_json_bytes
from ozlink_console.draft_snapshot.pipeline_harness import DraftPipelineHarnessRequest, run_draft_snapshot_pipeline
from tests.test_draft_snapshot_foundation import _minimal_canonical_dict
from tests.test_draft_snapshot_pipeline_harness import _connected_ok


def _minimal_step(mapping_id: str, step_id: str) -> ExecutionStep:
    return ExecutionStep(
        step_id=step_id,
        step_type="copy_item",
        mapping_id=mapping_id,
        item_type="file",
        assignment_mode="copy",
        source_drive_id="s",
        source_item_id="si",
        destination_drive_id="d",
        destination_parent_item_id="dp",
        destination_name="n",
        destination_path="p",
    )


def _mkdir_step(
    *,
    step_id: str,
    mapping_id: str,
    dest_path: str,
    parent_step_id: str = "",
) -> ExecutionStep:
    return ExecutionStep(
        step_id=step_id,
        step_type="create_folder",
        mapping_id=mapping_id,
        item_type="folder",
        assignment_mode="copy_recursive",
        source_drive_id="",
        source_item_id="",
        destination_drive_id="d",
        destination_parent_item_id="root",
        destination_name=dest_path.rsplit("/", 1)[-1] if "/" in dest_path else dest_path,
        destination_path=dest_path,
        depth=dest_path.count("/"),
        parent_step_id=parent_step_id,
    )


def test_subset_keeps_only_allowed_mapping_ids():
    plan = ExecutionPlan(
        plan_id="pid",
        snapshot_id="sid",
        run_id="rid",
        resolver_name="r",
        policy=ExecutionPolicy(),
        steps=[
            _minimal_step("m-a", "step-a"),
            _minimal_step("m-b", "step-b"),
            _minimal_step("dst_chain:x", "chain-1"),
        ],
        summary=ExecutionPlanSummary(total_steps=3),
        materialization=PlanMaterializationSummary(),
        plan_hash="old",
        metadata={},
        notes=[],
    )
    out = subset_execution_plan_by_mapping_ids(plan, frozenset({"m-b"}))
    assert out is not None
    assert len(out.steps) == 1
    assert out.steps[0].mapping_id == "m-b"
    assert out.summary.total_steps == 1
    assert out.plan_hash != "old"
    assert any("scoped_execution_mapping_ids" in n for n in out.notes)


def test_subset_returns_none_when_no_match():
    plan = ExecutionPlan(
        plan_id="pid",
        snapshot_id="sid",
        run_id="rid",
        resolver_name="r",
        policy=ExecutionPolicy(),
        steps=[_minimal_step("m-a", "step-a")],
        summary=ExecutionPlanSummary(total_steps=1),
        materialization=PlanMaterializationSummary(),
        plan_hash="h",
        metadata={},
        notes=[],
    )
    assert subset_execution_plan_by_mapping_ids(plan, frozenset({"other"})) is None


def test_dependency_closure_includes_parent_step_id_ancestors():
    mid_x = _chain_mapping_id("X")
    mid_xy = _chain_mapping_id("X/Y")
    chain_x = _mkdir_step(step_id="s-x", mapping_id=mid_x, dest_path="X", parent_step_id="")
    chain_xy = _mkdir_step(step_id="s-xy", mapping_id=mid_xy, dest_path="X/Y", parent_step_id="s-x")
    file_m = ExecutionStep(
        step_id="file-m1",
        step_type="copy_item",
        mapping_id="m1",
        item_type="file",
        assignment_mode="copy",
        source_drive_id="s",
        source_item_id="si",
        destination_drive_id="d",
        destination_parent_item_id="",
        destination_name="f.txt",
        destination_path="X/Y/f.txt",
        depth=2,
        parent_step_id="s-xy",
    )
    plan = ExecutionPlan(
        plan_id="pid",
        snapshot_id="sid",
        run_id="rid",
        resolver_name="r",
        policy=ExecutionPolicy(),
        steps=[chain_x, chain_xy, file_m],
        summary=ExecutionPlanSummary(total_steps=3),
        materialization=PlanMaterializationSummary(),
        plan_hash="h",
        metadata={},
        notes=[],
    )
    out = subset_execution_plan_by_dependency_closure(plan, frozenset({"m1"}))
    assert out is not None
    mids = {s.mapping_id for s in out.steps}
    assert mids == {mid_x, mid_xy, "m1"}
    assert any("scoped_execution_dependency_closure" in n for n in out.notes)


def test_dependency_closure_excludes_sibling_mapping():
    f1 = _minimal_step("m1", "a")
    f1.destination_path = "A/x.txt"
    f2 = _minimal_step("m2", "b")
    f2.destination_path = "B/y.txt"
    plan = ExecutionPlan(
        plan_id="pid",
        snapshot_id="sid",
        run_id="rid",
        resolver_name="r",
        policy=ExecutionPolicy(),
        steps=[f1, f2],
        summary=ExecutionPlanSummary(total_steps=2),
        materialization=PlanMaterializationSummary(),
        plan_hash="h",
        metadata={},
        notes=[],
    )
    out = subset_execution_plan_by_dependency_closure(plan, frozenset({"m1"}))
    assert out is not None
    assert len(out.steps) == 1
    assert out.steps[0].mapping_id == "m1"


def test_dependency_closure_prefers_chain_over_proposed_same_path():
    pdir = "X/Y"
    mid_chain = _chain_mapping_id(pdir)
    chain = _mkdir_step(step_id="c1", mapping_id=mid_chain, dest_path=pdir, parent_step_id="")
    proposed = _mkdir_step(step_id="p1", mapping_id="prop-99", dest_path=pdir, parent_step_id="")
    file_m = ExecutionStep(
        step_id="f1",
        step_type="copy_item",
        mapping_id="m1",
        item_type="file",
        assignment_mode="copy",
        source_drive_id="s",
        source_item_id="si",
        destination_drive_id="d",
        destination_parent_item_id="dp",
        destination_name="f.txt",
        destination_path="X/Y/f.txt",
        depth=2,
        parent_step_id="",
    )
    plan = ExecutionPlan(
        plan_id="pid",
        snapshot_id="sid",
        run_id="rid",
        resolver_name="r",
        policy=ExecutionPolicy(),
        steps=[chain, proposed, file_m],
        summary=ExecutionPlanSummary(total_steps=3),
        materialization=PlanMaterializationSummary(),
        plan_hash="h",
        metadata={},
        notes=[],
    )
    out = subset_execution_plan_by_dependency_closure(plan, frozenset({"m1"}))
    assert out is not None
    mids = {s.mapping_id for s in out.steps}
    assert mid_chain in mids
    assert "prop-99" not in mids
    assert "m1" in mids


def test_harness_routes_strict_vs_dependency_closure(monkeypatch):
    d = _minimal_canonical_dict()
    d["source"]["site_library"]["library_drive_id"] = "src-drive"
    d["destination"]["site_library"]["library_drive_id"] = "dst-drive"
    raw = json.dumps(parse_canonical_submitted_snapshot_dict(d).to_json_dict()).encode("utf-8")
    det = load_detached_from_canonical_json_bytes(raw, run_id="route-test")
    plan = ExecutionPlan(
        plan_id="p1",
        snapshot_id=det.snapshot.snapshot_id,
        run_id=det.run_id,
        resolver_name="t",
        policy=ExecutionPolicy(),
        steps=[
            ExecutionStep(
                step_id="only",
                step_type="copy_item",
                mapping_id="m1",
                item_type="file",
                assignment_mode="copy",
                source_drive_id="src-drive",
                source_item_id="SRC-ITEM-1",
                destination_drive_id="dst-drive",
                destination_parent_item_id="DST-PARENT-1",
                destination_name="b.txt",
                destination_path="c/b.txt",
                depth=0,
            )
        ],
        summary=ExecutionPlanSummary(total_steps=1),
        materialization=PlanMaterializationSummary(),
        plan_hash="x",
        metadata={},
        notes=[],
    )
    called: list[str] = []

    def track_strict(p, s):
        called.append("strict")
        return p

    def track_dep(p, s):
        called.append("dependency_closure")
        return p

    import ozlink_console.draft_snapshot.pipeline_harness as ph

    monkeypatch.setattr(ph, "subset_execution_plan_by_mapping_ids", track_strict)
    monkeypatch.setattr(ph, "subset_execution_plan_by_dependency_closure", track_dep)

    class _G:
        def get_drive_root_item(self, drive_id: str):
            return {"id": "ROOT"}

        def get_drive_item_by_path(self, drive_id: str, rel: str):
            return None

        def create_child_folder(self, *a, **k):
            return {"id": "NEW"}

        def start_drive_item_copy(self, **k):
            return {"monitor_url": "x"}

        def wait_graph_async_operation(self, *a, **k):
            return {"status": "completed"}

    run_draft_snapshot_pipeline(
        DraftPipelineHarnessRequest(
            detached=det,
            connected=_connected_ok(),
            graph_client=_G(),
            bridge_dry_run=True,
            execution_plan_override=plan,
            snapshot_scoped_mode="strict",
            scoped_seed_mapping_ids=frozenset({"m1"}),
        )
    )
    assert called == ["strict"]
    called.clear()
    run_draft_snapshot_pipeline(
        DraftPipelineHarnessRequest(
            detached=det,
            connected=_connected_ok(),
            graph_client=_G(),
            bridge_dry_run=True,
            execution_plan_override=plan,
            snapshot_scoped_mode="dependency_closure",
            scoped_seed_mapping_ids=frozenset({"m1"}),
        )
    )
    assert called == ["dependency_closure"]
