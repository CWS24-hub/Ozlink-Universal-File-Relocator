from __future__ import annotations

from ozlink_console.draft_snapshot import ExecutionPlanBridge, default_materialization_policy
from ozlink_console.draft_snapshot.execution_plan_contracts import ExecutionPlan, ExecutionStep


class _FakeGraphClient:
    def __init__(self) -> None:
        self._counter = 0
        self.copy_calls: list[dict[str, str]] = []
        self._existing_paths: dict[str, dict[str, dict[str, str]]] = {}

    def create_child_folder(self, drive_id: str, parent_item_id: str, name: str, *, conflict_behavior: str = "fail"):
        self._counter += 1
        item_id = f"FOLDER-{self._counter}"
        return {"id": item_id, "name": name, "driveId": drive_id, "parentId": parent_item_id}

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
        self.copy_calls.append(
            {
                "source_drive_id": source_drive_id,
                "source_item_id": source_item_id,
                "dest_drive_id": dest_drive_id,
                "dest_parent_item_id": dest_parent_item_id,
                "name": str(name or ""),
                "conflict_behavior": conflict_behavior,
            }
        )
        return {"monitor_url": "fake://ok"}

    def wait_graph_async_operation(self, monitor: dict[str, str], timeout_sec: float = 60.0):
        return {"status": "completed", "monitor": monitor, "timeout_sec": timeout_sec}

    def set_existing_path(self, drive_id: str, rel_path: str, item_id: str) -> None:
        d = self._existing_paths.setdefault(drive_id, {})
        d[rel_path.replace("\\", "/").strip("/")] = {"id": item_id}

    def get_drive_item_by_path(self, drive_id: str, relative_path: str):
        return (
            self._existing_paths.get(drive_id, {}).get(relative_path.replace("\\", "/").strip("/"))
            if drive_id in self._existing_paths
            else None
        )


class _FailingFolderGraphClient(_FakeGraphClient):
    def create_child_folder(self, drive_id: str, parent_item_id: str, name: str, *, conflict_behavior: str = "fail"):
        if name == "Projects":
            raise RuntimeError("simulated mkdir failure")
        return super().create_child_folder(drive_id, parent_item_id, name, conflict_behavior=conflict_behavior)


def _plan_with_parent_chain() -> ExecutionPlan:
    steps = [
        ExecutionStep(
            step_id="s1",
            step_type="create_folder",
            mapping_id="m-folder-1",
            item_type="folder",
            assignment_mode="copy_recursive",
            source_drive_id="",
            source_item_id="",
            destination_drive_id="dst",
            destination_parent_item_id="ROOT",
            destination_name="Projects",
            destination_path="Projects",
            depth=0,
        ),
        ExecutionStep(
            step_id="s2",
            step_type="create_folder",
            mapping_id="m-folder-2",
            item_type="folder",
            assignment_mode="copy_recursive",
            source_drive_id="",
            source_item_id="",
            destination_drive_id="dst",
            destination_parent_item_id="",
            destination_name="FY26",
            destination_path="Projects/FY26",
            depth=1,
            parent_step_id="s1",
        ),
        ExecutionStep(
            step_id="s3",
            step_type="copy_item",
            mapping_id="m-file-1",
            item_type="file",
            assignment_mode="copy",
            source_drive_id="src",
            source_item_id="SRC-ITEM",
            destination_drive_id="dst",
            destination_parent_item_id="",
            destination_name="doc.txt",
            destination_path="Projects/FY26/doc.txt",
            depth=2,
            parent_step_id="s2",
        ),
    ]
    return ExecutionPlan(
        plan_id="p1",
        snapshot_id="a" * 32,
        run_id="r1",
        resolver_name="resolver",
        policy=default_materialization_policy(),
        steps=steps,
    )


def test_execution_bridge_resolves_parent_step_chain_and_runs_backend():
    plan = _plan_with_parent_chain()
    graph = _FakeGraphClient()
    bridge = ExecutionPlanBridge()
    state = bridge.execute_plan(plan, graph_client=graph, dry_run=False)

    s1 = state.step_states["s1"]
    s2 = state.step_states["s2"]
    s3 = state.step_states["s3"]

    assert s1.status == "ok"
    assert s1.outcome == "succeeded"
    assert s1.output_item_id == "FOLDER-1"
    assert s2.status == "ok"
    assert s2.parent_resolution_source == "parent_step_output"
    assert s2.resolved_destination_parent_item_id == "FOLDER-1"
    assert s2.output_item_id == "FOLDER-2"
    assert s3.status == "ok"
    assert s3.outcome == "succeeded"
    assert s3.parent_resolution_source == "parent_step_output"
    assert s3.resolved_destination_parent_item_id == "FOLDER-2"
    assert len(graph.copy_calls) == 1
    assert graph.copy_calls[0]["dest_parent_item_id"] == "FOLDER-2"


def test_execution_bridge_blocks_dependent_steps_when_parent_fails():
    plan = _plan_with_parent_chain()
    graph = _FailingFolderGraphClient()
    bridge = ExecutionPlanBridge()
    state = bridge.execute_plan(plan, graph_client=graph, dry_run=False)

    assert state.step_states["s1"].status == "failed"
    assert state.step_states["s1"].outcome == "failed"
    assert state.step_states["s2"].status == "blocked"
    assert state.step_states["s2"].outcome == "failed"
    assert "cannot resolve dependent destination parent" in state.step_states["s2"].detail
    assert state.step_states["s3"].status == "blocked"


def test_execution_bridge_surfaces_compatibility_notes():
    bridge = ExecutionPlanBridge()
    state = bridge.execute_plan(_plan_with_parent_chain(), graph_client=_FakeGraphClient(), dry_run=True)
    joined = "\n".join(state.compatibility_notes)
    assert "move_item" in joined
    assert "skipped_existing" in joined
    assert "merge" in joined


def test_execution_bridge_file_conflict_skip_marks_skipped_existing():
    plan = _plan_with_parent_chain()
    graph = _FakeGraphClient()
    graph.set_existing_path("dst", "Projects/FY26/doc.txt", "EXISTING-FILE")
    bridge = ExecutionPlanBridge()
    state = bridge.execute_plan(plan, graph_client=graph, dry_run=False)
    s3 = state.step_states["s3"]
    assert s3.status == "skipped"
    assert s3.outcome == "skipped_existing"
    assert "file_conflict_policy=skip" in s3.detail
    assert len(graph.copy_calls) == 0


def test_execution_bridge_folder_conflict_merge_treats_existing_as_success():
    plan = _plan_with_parent_chain()
    graph = _FakeGraphClient()
    graph.set_existing_path("dst", "Projects", "EXISTING-FOLDER")
    bridge = ExecutionPlanBridge()
    state = bridge.execute_plan(plan, graph_client=graph, dry_run=False)
    s1 = state.step_states["s1"]
    assert s1.status == "ok"
    assert s1.outcome == "succeeded"
    assert "merge-compatible success" in s1.detail


def test_execution_bridge_move_item_blocked_compatibility_by_default():
    plan = _plan_with_parent_chain()
    plan.steps[2].step_type = "move_item"
    bridge = ExecutionPlanBridge()
    state = bridge.execute_plan(plan, graph_client=_FakeGraphClient(), dry_run=False)
    s3 = state.step_states["s3"]
    assert s3.status == "blocked"
    assert s3.outcome == "blocked_compatibility"
    assert "blocked" in s3.detail

